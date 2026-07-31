"""
AWS Lambda Handler for the Rule Engine.

Architecture:
    This Lambda does NOT build the graph at runtime.
    It loads a PRE-BUILT, serialized RuleGraph from S3 (published by deploy/graph_publisher.py)
    and executes it against the input payload.

    Deploy-time flow (graph_publisher):
        Rule JSONs → GraphBuilder → validate → Neptune (persist) → S3 (serialize)

    Runtime flow (this Lambda):
        S3 (serialized graph) → deserialize → execute against payload → return result

Invocation contract:
  Event (input):
    {
      "graph_s3_bucket": "my-rule-engine-artifacts",   # optional, uses env var GRAPH_S3_BUCKET
      "graph_s3_key": "graphs/loan_pricing/graph.json",# optional, uses env var GRAPH_S3_KEY
      "payload": {
        "loan_amount": 250000,
        "property_value": 350000,
        "credit_score": 720,
        "monthly_debt": 1500,
        "monthly_income": 7000,
        "loan_type": "CONVENTIONAL",
        "loan_term_years": 30,
        "employment_status": "FULL_TIME"
      },
      "return_full_context": false,                    # optional: return all fields vs leaf outputs only
      "fail_fast": false                               # optional: stop on first rule error
    }

  Response (output):
    {
      "statusCode": 200,
      "body": {
        "success": true,
        "outputs": {
          "decision": "APPROVED",
          "final_rate": 0.0685,
          "monthly_payment": 1635.42,
          "risk_score": 35
        },
        "execution_summary": {
          "rules_fired": 15,
          "rules_skipped": 0,
          "rules_errored": 0,
          "total_duration_ms": 12.4,
          "graph_load_ms": 5.2
        },
        "errors": []
      }
    }

Graph loading strategy:
  1. On cold start: download serialized graph JSON from S3, deserialize, cache in module scope
  2. On warm invocations: reuse cached graph (no S3 round-trip)
  3. Force reload: set event["force_reload"] = true

Environment variables:
  GRAPH_S3_BUCKET  : S3 bucket containing the serialized graph JSON
  GRAPH_S3_KEY     : S3 key for the graph JSON artifact
  LOG_LEVEL        : logging level (default: INFO)
  FAIL_FAST        : "true" | "false" (default: false)
"""

from __future__ import annotations
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))

# ------------------------------------------------------------------ #
#  Module-level cache (persists across warm Lambda invocations)       #
# ------------------------------------------------------------------ #
_cached_graph = None
_cached_graph_key: Optional[str] = None


def _get_s3_client():
    import boto3
    return boto3.client("s3")


def _load_graph_from_s3(bucket: str, key: str):
    """
    Download the pre-built, serialized RuleGraph from S3 and deserialize it.

    The graph was published by deploy/graph_publisher.py at deploy time.
    This avoids graph building at Lambda runtime — just deserialize and go.
    """
    from autograph_rule_engine.core.models import RuleGraph

    s3 = _get_s3_client()
    logger.info(f"Loading pre-built graph from s3://{bucket}/{key}")

    response = s3.get_object(Bucket=bucket, Key=key)
    graph_json = response["Body"].read().decode("utf-8")

    # Deserialize using Pydantic
    graph = RuleGraph.parse_raw(graph_json)

    logger.info(
        f"Graph loaded: {len(graph.rules)} rules, "
        f"execution order: {graph.execution_order}"
    )
    return graph


def _get_or_load_graph(bucket: str, key: str, force_reload: bool = False):
    """Return cached graph or load from S3."""
    global _cached_graph, _cached_graph_key

    cache_key = f"{bucket}/{key}"

    if _cached_graph is not None and _cached_graph_key == cache_key and not force_reload:
        logger.info("Using cached rule graph (warm invocation).")
        return _cached_graph, 0.0

    start = time.perf_counter()
    graph = _load_graph_from_s3(bucket, key)
    load_ms = (time.perf_counter() - start) * 1000

    _cached_graph = graph
    _cached_graph_key = cache_key
    return graph, load_ms


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda entry point.

    Handles:
    - Loading the pre-built graph from S3 (with warm-invocation caching)
    - Executing the graph against the input payload
    - Returning structured output
    """
    try:
        # ---- Parse event ----
        bucket = event.get("graph_s3_bucket") or os.environ.get("GRAPH_S3_BUCKET", "")
        key = event.get("graph_s3_key") or os.environ.get("GRAPH_S3_KEY", "")
        payload = event.get("payload", {})
        return_full_context = event.get("return_full_context", False)
        fail_fast = event.get("fail_fast", os.environ.get("FAIL_FAST", "false").lower() == "true")
        force_reload = event.get("force_reload", False)

        if not payload:
            return _error_response(400, "Missing required field: 'payload'")

        if not bucket or not key:
            return _error_response(
                400,
                "No graph location specified. Set 'graph_s3_bucket' and 'graph_s3_key' "
                "in event or GRAPH_S3_BUCKET / GRAPH_S3_KEY env vars."
            )

        # ---- Load pre-built graph from S3 ----
        graph, graph_load_ms = _get_or_load_graph(bucket, key, force_reload=force_reload)

        # ---- Validate payload has required fields ----
        missing_fields = set(graph.payload_fields) - set(payload.keys())
        if missing_fields:
            return _error_response(
                400,
                f"Payload missing required fields: {sorted(missing_fields)}"
            )

        # ---- Execute ----
        from autograph_rule_engine.core.executor import RuleExecutor

        executor = RuleExecutor(fail_fast=fail_fast)
        result = executor.execute(graph, payload)

        # ---- Build response ----
        if return_full_context:
            outputs = result.context
        else:
            outputs = result.get_leaf_outputs(graph)

        execution_summary = {
            "rules_fired": sum(1 for r in result.rule_results if r.fired),
            "rules_skipped": sum(1 for r in result.rule_results if r.skipped),
            "rules_errored": sum(1 for r in result.rule_results if r.error),
            "total_duration_ms": round(result.total_duration_ms, 3),
            "graph_load_ms": round(graph_load_ms, 3),
            "execution_order": graph.execution_order,
        }

        body = {
            "success": result.success,
            "outputs": outputs,
            "execution_summary": execution_summary,
            "errors": result.errors,
        }

        # Include per-rule trace if requested
        if event.get("include_trace", False):
            body["rule_trace"] = [
                {
                    "rule_id": rr.rule_id,
                    "name": rr.rule_name,
                    "fired": rr.fired,
                    "skipped": rr.skipped,
                    "outputs": rr.outputs,
                    "error": rr.error,
                    "duration_ms": round(rr.duration_ms, 3),
                }
                for rr in result.rule_results
            ]

        return {
            "statusCode": 200 if result.success else 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(body, default=str),
        }

    except ValueError as e:
        logger.error(f"Validation / input error: {e}")
        return _error_response(400, str(e))
    except Exception as e:
        logger.exception(f"Unexpected error in lambda_handler: {e}")
        return _error_response(500, f"Internal error: {str(e)}")


def _error_response(status_code: int, message: str) -> Dict:
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"success": False, "error": message}),
    }


# ------------------------------------------------------------------ #
#  Local test runner (bypasses S3 by building graph locally)          #
# ------------------------------------------------------------------ #
if __name__ == "__main__":
    """
    Local smoke test — builds graph from local files (simulates what
    graph_publisher would do), then executes like Lambda would.

    Run from project root:
        python -m autograph_rule_engine.compute.lambda_handler
    """
    from autograph_rule_engine.core.parser import RuleParser
    from autograph_rule_engine.core.graph_builder import GraphBuilder
    from autograph_rule_engine.core.validator import GraphValidator
    from autograph_rule_engine.core.executor import RuleExecutor

    rules_dir = Path(__file__).parent.parent / ".." / ".." / "rules" / "loan_pricing"
    parser = RuleParser()
    rules = parser.load_directory(rules_dir)

    builder = GraphBuilder()
    graph = builder.build(rules)

    validator = GraphValidator()
    val_result = validator.validate(graph)
    print(val_result.summary())
    val_result.raise_if_invalid()

    test_payload = {
        "loan_amount": 280000,
        "property_value": 350000,
        "credit_score": 725,
        "monthly_debt": 1800,
        "monthly_income": 8000,
        "loan_type": "CONVENTIONAL",
        "loan_term_years": 30,
        "employment_status": "FULL_TIME",
    }

    executor = RuleExecutor()
    exec_result = executor.execute(graph, test_payload)
    print(exec_result.summary())
    print("\nLeaf outputs:")
    for k, v in exec_result.get_leaf_outputs(graph).items():
        print(f"  {k}: {v}")
