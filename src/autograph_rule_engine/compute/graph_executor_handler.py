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
_cached_graph_etag: Optional[str] = None
_cached_graph_version_id: Optional[str] = None
_cached_graph_checked_at: float = 0.0

# How long a cached graph is trusted before S3 is asked whether it changed. The check is a
# conditional GET, so a 304 costs one round trip and no transfer; 0 revalidates every
# invocation.
REVALIDATE_SECONDS = float(os.environ.get("GRAPH_REVALIDATE_SECONDS", "30"))


def _get_s3_client():
    import boto3
    return boto3.client("s3")


def _load_graph_from_s3(bucket: str, key: str, if_none_match: Optional[str] = None):
    """
    Download the serialized RuleGraph from S3 and deserialize it.

    Returns (graph, etag, version_id). When `if_none_match` is supplied and the object has
    not changed, returns (None, if_none_match, None) so the caller keeps its cached copy.
    """
    from botocore.exceptions import ClientError
    from autograph_rule_engine.core.models import RuleGraph

    s3 = _get_s3_client()

    kwargs = {"Bucket": bucket, "Key": key}
    if if_none_match:
        kwargs["IfNoneMatch"] = if_none_match

    try:
        response = s3.get_object(**kwargs)
    except ClientError as error:
        # S3 answers a conditional GET with 304, which botocore raises as a ClientError.
        if error.response.get("Error", {}).get("Code") in ("304", "NotModified"):
            logger.info("Graph unchanged in S3 (304); keeping the cached copy.")
            return None, if_none_match, None
        raise

    graph_json = response["Body"].read().decode("utf-8")
    graph = RuleGraph.model_validate_json(graph_json)

    etag = response.get("ETag")
    version_id = response.get("VersionId")

    logger.info(
        f"Graph loaded from s3://{bucket}/{key}: {len(graph.rules)} rules, "
        f"etag={etag}, version_id={version_id}"
    )
    return graph, etag, version_id


def _get_or_load_graph(bucket: str, key: str, force_reload: bool = False):
    """
    Return the cached graph, revalidating against S3 when the TTL has elapsed.

    The cache used to key on bucket/key alone — object *identity*, never content — so a warm
    container served whatever it first loaded for the rest of its life, however many times
    the graph was republished underneath it. The generator overwrites the same key in place
    and has no way to signal running containers, so the only safe signal is asking S3.
    """
    global _cached_graph, _cached_graph_key, _cached_graph_etag
    global _cached_graph_version_id, _cached_graph_checked_at

    cache_key = f"{bucket}/{key}"
    now = time.monotonic()
    cached_is_usable = _cached_graph is not None and _cached_graph_key == cache_key

    if cached_is_usable and not force_reload:
        if now - _cached_graph_checked_at < REVALIDATE_SECONDS:
            logger.info("Using cached rule graph (within revalidation window).")
            return _cached_graph, 0.0

    start = time.perf_counter()
    graph, etag, version_id = _load_graph_from_s3(
        bucket,
        key,
        if_none_match=_cached_graph_etag if cached_is_usable and not force_reload else None,
    )
    load_ms = (time.perf_counter() - start) * 1000

    _cached_graph_checked_at = now

    if graph is None:
        # 304 — unchanged; keep the cached graph but record that it was just checked.
        return _cached_graph, load_ms

    _cached_graph = graph
    _cached_graph_key = cache_key
    _cached_graph_etag = etag
    _cached_graph_version_id = version_id
    return graph, load_ms


def _graph_version() -> Optional[str]:
    """
    Identifier for the graph that answered this request.

    The S3 ETag is already a content hash of the published artifact, so it needs no
    separate version scheme: equal ETag means equal graph.
    """
    if _cached_graph_etag:
        return _cached_graph_etag.strip('"')
    return None


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
            # Which graph produced this result. Without it there is no way to tell whether
            # a response came from the current rule set or a stale cached one.
            "graph_version": _graph_version(),
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
