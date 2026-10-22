"""
AWS Lambda Handler for Neptune Graph Queries.

Runs INSIDE the VPC so it can reach Neptune.
Provides read-only graph traversal queries for:
  - Impact analysis
  - Lineage (ancestors/descendants)
  - Path finding
  - Graph statistics

Event format:
    {
      "query": "impact" | "ancestors" | "descendants" | "paths" | "stats" | "rule" | "rules",
      "rule_id": "R01",              # required for impact, ancestors, descendants, rule
      "source_rule_id": "R01",       # required for paths
      "target_rule_id": "R13",       # required for paths
      "namespace": "default"         # optional, defaults to env var or "default"
    }

Environment variables:
  NEPTUNE_ENDPOINT   : Neptune Gremlin WebSocket endpoint
  NAMESPACE          : Default namespace (default: "default")
  LOG_LEVEL          : Logging level (default: INFO)
"""

from __future__ import annotations
import json
import logging
import os
import time
from typing import Any, Dict

logger = logging.getLogger()
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda entry point for Neptune graph queries.
    """
    try:
        start = time.perf_counter()

        query_type = event.get("query")
        if not query_type:
            return _error_response(400, "Missing required field: 'query'")

        namespace = event.get("namespace") or os.environ.get("NAMESPACE", "default")
        endpoint = os.environ.get("NEPTUNE_ENDPOINT", "")

        if not endpoint:
            return _error_response(500, "NEPTUNE_ENDPOINT not configured")

        from autograph_rule_engine.backends.neptune_backend import NeptuneGraphBackend

        backend = NeptuneGraphBackend(endpoint=endpoint)
        backend.connect()

        try:
            result = _dispatch_query(backend, query_type, event, namespace)
        finally:
            backend.close()

        duration_ms = (time.perf_counter() - start) * 1000

        body = {
            "success": True,
            "query": query_type,
            "namespace": namespace,
            "result": result,
            "duration_ms": round(duration_ms, 1),
        }

        logger.info(f"Query '{query_type}' completed in {duration_ms:.1f}ms")
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(body, default=str),
        }

    except ValueError as e:
        return _error_response(400, str(e))
    except Exception as e:
        logger.exception(f"Query error: {e}")
        return _error_response(500, str(e))


def _dispatch_query(backend, query_type: str, event: dict, namespace: str) -> Any:
    """Route to the appropriate query method."""

    if query_type == "impact":
        rule_id = _require_field(event, "rule_id")
        return backend.get_impact(rule_id, namespace)

    elif query_type == "ancestors":
        rule_id = _require_field(event, "rule_id")
        ancestors = backend.get_ancestors(rule_id, namespace)
        return {
            "rule_id": rule_id,
            "ancestors": sorted(ancestors),
            "count": len(ancestors),
        }

    elif query_type == "descendants":
        rule_id = _require_field(event, "rule_id")
        descendants = backend.get_descendants(rule_id, namespace)
        return {
            "rule_id": rule_id,
            "descendants": sorted(descendants),
            "count": len(descendants),
        }

    elif query_type == "paths":
        # Imported here rather than at module scope, matching how the backend itself is
        # imported lazily in this handler.
        from autograph_rule_engine.backends.neptune_backend import MAX_PATH_RESULTS

        source = _require_field(event, "source_rule_id")
        target = _require_field(event, "target_rule_id")
        paths = backend.find_paths(source, target, namespace)
        return {
            "source": source,
            "target": target,
            "paths": paths,
            "count": len(paths),
            # find_paths caps its result set, so say when the answer is partial rather
            # than letting the caller read a truncated list as the complete one.
            "truncated": len(paths) >= MAX_PATH_RESULTS,
        }

    elif query_type == "order":
        # Exposed so the Neptune-side topological sort is reachable and therefore
        # verifiable: it can be compared against the order baked into the S3 artifact.
        order = backend.get_execution_order(namespace)
        return {
            "execution_order": order,
            "count": len(order),
        }

    elif query_type == "stats":
        return backend.get_graph_stats(namespace)

    elif query_type == "rule":
        rule_id = _require_field(event, "rule_id")
        rule = backend.get_rule(rule_id, namespace)
        if not rule:
            raise ValueError(f"Rule '{rule_id}' not found in namespace '{namespace}'")
        return rule

    elif query_type == "rules":
        return backend.get_all_rules(namespace)

    else:
        valid = ["impact", "ancestors", "descendants", "paths", "order", "stats", "rule", "rules"]
        raise ValueError(f"Unknown query type: '{query_type}'. Valid: {valid}")


def _require_field(event: dict, field: str) -> str:
    """Extract a required field from the event."""
    value = event.get(field)
    if not value:
        raise ValueError(f"Missing required field: '{field}'")
    return value


def _error_response(status_code: int, message: str) -> Dict[str, Any]:
    """Return a structured error response."""
    return {
        "statusCode": status_code,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"success": False, "error": message}),
    }
