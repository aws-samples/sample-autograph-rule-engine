"""
AWS Lambda Handler for Graph Publishing.

Runs INSIDE the VPC so it can reach both Neptune and S3.
Triggered by:
  - S3 event (rule JSON uploaded/modified)
  - Manual invocation (force rebuild)
  - CI/CD pipeline step

Flow:
  1. Load rule JSON files from S3 (rules bucket)
  2. Build + validate the dependency graph
  3. Upsert graph into Neptune (persistent queryable store)
  4. Serialize graph to S3 (artifact for executor Lambda)

Event formats supported:

  S3 trigger (automatic on rule upload):
    Standard S3 event notification — extracts bucket/prefix from the event.

  Manual invocation:
    {
      "rules_s3_bucket": "my-rules-bucket",
      "rules_s3_prefix": "rules/loan_pricing/",
      "graph_s3_bucket": "my-artifacts-bucket",
      "graph_s3_key": "graphs/default/graph.json",
      "neptune_endpoint": "wss://...",
      "namespace": "loan_pricing",
      "skip_neptune": false
    }

Environment variables:
  RULES_S3_BUCKET    : S3 bucket containing rule JSON files
  RULES_S3_PREFIX    : S3 prefix for rule files (e.g. "rules/loan_pricing/")
  GRAPH_S3_BUCKET    : S3 bucket for the serialized graph artifact
  GRAPH_S3_KEY       : S3 key for graph JSON (e.g. "graphs/default/graph.json")
  NEPTUNE_ENDPOINT   : Neptune Gremlin WebSocket endpoint
  NAMESPACE          : Neptune namespace (default: "default")
  LOG_LEVEL          : Logging level (default: INFO)
"""

from __future__ import annotations
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


def graph_generator_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda entry point for graph publishing.

    Supports both S3 event triggers and manual invocation.
    """
    try:
        start = time.perf_counter()

        # ---- Resolve configuration ----
        config = _resolve_config(event)
        logger.info(f"Publisher config: {json.dumps(config, default=str)}")

        # ---- Load rules from S3 ----
        rules = _load_rules_from_s3(config["rules_s3_bucket"], config["rules_s3_prefix"])
        logger.info(f"Loaded {len(rules)} rules.")

        # ---- Build + validate graph ----
        from autograph_rule_engine.core.graph_builder import GraphBuilder
        from autograph_rule_engine.core.validator import GraphValidator

        builder = GraphBuilder()
        graph = builder.build(rules)

        validator = GraphValidator()
        val_result = validator.validate(graph)
        val_result.raise_if_invalid()
        logger.info(f"Graph valid. {len(graph.rules)} rules, order: {graph.execution_order}")

        # ---- Serialize to S3 first ----
        # The executor reads this artifact, so publishing it before the Neptune
        # sync keeps rule execution healthy even if the graph store is struggling.
        _upload_graph_to_s3(graph, config["graph_s3_bucket"], config["graph_s3_key"], config["namespace"])

        # ---- Upsert to Neptune ----
        # Deliberately NOT swallowed. Reporting success over a failed sync is
        # what hid a duplicated graph: S3 looked correct while Neptune was not.
        # Raising lets Lambda's async retry and the DLQ do their job.
        neptune_synced = False
        if config["skip_neptune"] or not config["neptune_endpoint"]:
            logger.info("Neptune sync skipped by configuration.")
        else:
            _upsert_to_neptune(graph, config["neptune_endpoint"], config["namespace"])
            neptune_synced = True

        total_ms = (time.perf_counter() - start) * 1000

        body = {
            "success": True,
            "num_rules": len(graph.rules),
            "execution_order": graph.execution_order,
            "payload_fields": graph.payload_fields,
            "neptune_synced": neptune_synced,
            "graph_s3_location": f"s3://{config['graph_s3_bucket']}/{config['graph_s3_key']}",
            "total_duration_ms": round(total_ms, 1),
        }

        logger.info(f"Publish complete: {json.dumps(body)}")
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(body),
        }

    except Exception as e:
        logger.exception(f"Publisher error: {e}")

        # Lambda only retries — and eventually dead-letters — an event-driven
        # invocation when the invocation itself FAILS. Returning an error
        # payload looks like success to the platform, so the SQS message would
        # be deleted and the failure lost. Manual/synchronous callers still get
        # a readable JSON envelope.
        if _is_event_driven(event):
            raise

        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"success": False, "error": str(e)}),
        }


def _is_event_driven(event: Dict[str, Any]) -> bool:
    """True when this invocation came from SQS or S3 rather than a direct call."""
    return bool(event.get("Records"))


def _resolve_config(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Resolve configuration from event (manual invoke) or environment (S3 trigger).
    S3 event notifications are detected and the bucket/prefix extracted.
    """
    trigger = _extract_s3_trigger(event)
    if trigger:
        rules_bucket, rules_prefix = trigger
        logger.info(f"S3 event trigger: s3://{rules_bucket}/{rules_prefix}")

        return {
            "rules_s3_bucket": rules_bucket,
            "rules_s3_prefix": rules_prefix,
            "graph_s3_bucket": os.environ.get("GRAPH_S3_BUCKET", ""),
            "graph_s3_key": os.environ.get("GRAPH_S3_KEY", "graphs/default/graph.json"),
            "neptune_endpoint": os.environ.get("NEPTUNE_ENDPOINT", ""),
            "namespace": os.environ.get("NAMESPACE", "default"),
            "skip_neptune": os.environ.get("SKIP_NEPTUNE", "false").lower() == "true",
        }

    # Manual invocation — merge event with env vars
    return {
        "rules_s3_bucket": event.get("rules_s3_bucket") or os.environ.get("RULES_S3_BUCKET", ""),
        "rules_s3_prefix": event.get("rules_s3_prefix") or os.environ.get("RULES_S3_PREFIX", "rules/"),
        "graph_s3_bucket": event.get("graph_s3_bucket") or os.environ.get("GRAPH_S3_BUCKET", ""),
        "graph_s3_key": event.get("graph_s3_key") or os.environ.get("GRAPH_S3_KEY", "graphs/default/graph.json"),
        "neptune_endpoint": event.get("neptune_endpoint") or os.environ.get("NEPTUNE_ENDPOINT", ""),
        "namespace": event.get("namespace") or os.environ.get("NAMESPACE", "default"),
        "skip_neptune": event.get("skip_neptune", os.environ.get("SKIP_NEPTUNE", "false").lower() == "true"),
    }


def _extract_s3_trigger(event: Dict[str, Any]) -> Optional[Tuple[str, str]]:
    """
    Find the (bucket, prefix) this event is about, or None for manual invokes.

    Handles a direct S3 notification and an SQS envelope carrying one, since
    rule uploads are funnelled through SQS to collapse a bulk sync into a
    single rebuild. Every record in a batch shares the rules prefix, so the
    first S3 record found describes the whole batch.
    """
    for record in event.get("Records") or []:
        source = record.get("eventSource")

        if source == "aws:s3":
            return _s3_bucket_and_prefix(record)

        if source == "aws:sqs":
            try:
                body = json.loads(record.get("body") or "{}")
            except json.JSONDecodeError:
                logger.warning("Skipping SQS record with non-JSON body.")
                continue

            # S3 sends a one-off s3:TestEvent when a notification is created.
            if body.get("Event") == "s3:TestEvent":
                logger.info("Ignoring S3 test event.")
                continue

            for inner in body.get("Records") or []:
                if inner.get("eventSource") == "aws:s3":
                    return _s3_bucket_and_prefix(inner)

    return None


def _s3_bucket_and_prefix(record: Dict[str, Any]) -> Tuple[str, str]:
    """Bucket name and containing prefix for an S3 notification record."""
    s3_info = record["s3"]
    key = s3_info["object"]["key"]
    prefix = "/".join(key.split("/")[:-1]) + "/"
    return s3_info["bucket"]["name"], prefix


def _load_rules_from_s3(bucket: str, prefix: str) -> Dict:
    """Download rule JSONs from S3 and parse them."""
    import boto3
    from autograph_rule_engine.core.parser import RuleParser

    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    rule_dicts: List[Dict] = []
    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json"):
                continue
            logger.info(f"Loading: s3://{bucket}/{key}")
            response = s3.get_object(Bucket=bucket, Key=key)
            content = response["Body"].read().decode("utf-8")
            rule_dicts.append(json.loads(content))

    if not rule_dicts:
        raise ValueError(f"No rule JSON files found at s3://{bucket}/{prefix}")

    parser = RuleParser()
    return parser.load_dicts(rule_dicts)


def _upsert_to_neptune(graph, endpoint: str, namespace: str):
    """Write graph to Neptune."""
    from autograph_rule_engine.backends.neptune_backend import NeptuneGraphBackend

    logger.info(f"Upserting to Neptune: {endpoint} (namespace={namespace})")
    backend = NeptuneGraphBackend(endpoint=endpoint)
    backend.connect()
    try:
        backend.upsert_rule_graph(graph, namespace=namespace)
        logger.info("Neptune upsert complete.")
    finally:
        backend.close()


def _upload_graph_to_s3(graph, bucket: str, key: str, namespace: str):
    """Serialize graph and upload to S3."""
    import boto3

    graph_json = graph.model_dump_json(indent=2)

    logger.info(f"Uploading graph to s3://{bucket}/{key}")
    s3 = boto3.client("s3")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=graph_json.encode("utf-8"),
        ContentType="application/json",
        Metadata={
            "num_rules": str(len(graph.rules)),
            "namespace": namespace,
            "execution_order": ",".join(graph.execution_order),
        },
    )
    logger.info("S3 upload complete.")
