"""
Tests for the graph generator Lambda handler.

Covers the two behaviours that let a corrupt graph reach production unnoticed:
  - a failed Neptune sync was swallowed and still reported success
  - rule uploads now arrive wrapped in an SQS envelope, not as raw S3 events

AWS calls are stubbed; rules come from the local rules/ directory.
"""
import json
import pytest
from pathlib import Path

from autograph_rule_engine.core.parser import RuleParser
from autograph_rule_engine.compute import graph_generator_handler as handler_module
from autograph_rule_engine.compute.graph_generator_handler import (
    _extract_s3_trigger,
    _is_event_driven,
    _resolve_config,
    graph_generator_handler,
)

RULES_DIR = Path(__file__).parent.parent / "rules" / "loan_pricing"
RULE_KEY = "rules/loan_pricing/R01_compute_ltv.json"


def s3_record(bucket="test-rules-bucket", key=RULE_KEY):
    return {
        "eventSource": "aws:s3",
        "s3": {"bucket": {"name": bucket}, "object": {"key": key}},
    }


def sqs_event(*s3_records):
    """An S3 notification as delivered through the rule-event queue."""
    return {
        "Records": [
            {
                "eventSource": "aws:sqs",
                "body": json.dumps({"Records": list(s3_records)}),
            }
        ]
    }


# ------------------------------------------------------------------ #
#  Event parsing                                                      #
# ------------------------------------------------------------------ #

def test_direct_s3_event_is_recognised():
    trigger = _extract_s3_trigger({"Records": [s3_record()]})

    assert trigger == ("test-rules-bucket", "rules/loan_pricing/")


def test_sqs_wrapped_s3_event_is_recognised():
    trigger = _extract_s3_trigger(sqs_event(s3_record()))

    assert trigger == ("test-rules-bucket", "rules/loan_pricing/")


def test_batched_sqs_records_resolve_to_one_rebuild():
    """A bulk sync batches many files that all share one rules prefix."""
    event = sqs_event(
        s3_record(key="rules/loan_pricing/R01_compute_ltv.json"),
        s3_record(key="rules/loan_pricing/R02_compute_dti.json"),
    )

    assert _extract_s3_trigger(event) == ("test-rules-bucket", "rules/loan_pricing/")


def test_s3_test_event_is_ignored():
    """S3 posts a one-off s3:TestEvent when a notification is created."""
    event = {
        "Records": [
            {"eventSource": "aws:sqs", "body": json.dumps({"Event": "s3:TestEvent"})}
        ]
    }

    assert _extract_s3_trigger(event) is None


def test_malformed_sqs_body_is_skipped():
    event = {"Records": [{"eventSource": "aws:sqs", "body": "not json"}]}

    assert _extract_s3_trigger(event) is None


def test_manual_invocation_has_no_trigger():
    assert _extract_s3_trigger({"namespace": "loan_pricing"}) is None


def test_manual_config_prefers_event_over_environment(monkeypatch):
    monkeypatch.setenv("RULES_S3_BUCKET", "env-bucket")
    monkeypatch.setenv("NAMESPACE", "env-namespace")

    config = _resolve_config({"rules_s3_bucket": "event-bucket"})

    assert config["rules_s3_bucket"] == "event-bucket"
    assert config["namespace"] == "env-namespace"


# ------------------------------------------------------------------ #
#  Neptune failures must not be reported as success                   #
# ------------------------------------------------------------------ #

@pytest.fixture
def stubbed_aws(monkeypatch):
    """Stub S3 load/upload so the handler runs without AWS."""
    uploads = []

    monkeypatch.setattr(
        handler_module, "_load_rules_from_s3",
        lambda bucket, prefix: RuleParser().load_directory(RULES_DIR),
    )
    monkeypatch.setattr(
        handler_module, "_upload_graph_to_s3",
        lambda graph, bucket, key, namespace: uploads.append(key),
    )
    return uploads


def base_event():
    return {
        "rules_s3_bucket": "test-rules-bucket",
        "graph_s3_bucket": "test-graph-bucket",
        "graph_s3_key": "graphs/default/graph.json",
        "neptune_endpoint": "wss://unused.example:8182/gremlin",
        "namespace": "default",
    }


def test_successful_publish_reports_neptune_synced(stubbed_aws, monkeypatch):
    monkeypatch.setattr(
        handler_module, "_upsert_to_neptune",
        lambda graph, endpoint, namespace: None,
    )

    response = graph_generator_handler(base_event(), None)
    body = json.loads(response["body"])

    assert response["statusCode"] == 200
    assert body["success"] is True
    assert body["neptune_synced"] is True
    assert body["num_rules"] == 15


def test_neptune_failure_is_not_swallowed(stubbed_aws, monkeypatch):
    """A failed sync must surface so Lambda retries and the DLQ can catch it."""
    def explode(graph, endpoint, namespace):
        raise RuntimeError("ConcurrentModificationException")

    monkeypatch.setattr(handler_module, "_upsert_to_neptune", explode)

    response = graph_generator_handler(base_event(), None)
    body = json.loads(response["body"])

    assert response["statusCode"] == 500
    assert body["success"] is False
    assert "ConcurrentModificationException" in body["error"]


def test_graph_artifact_is_published_even_when_neptune_fails(stubbed_aws, monkeypatch):
    """The executor reads S3, so the artifact must land before Neptune is touched."""
    def explode(graph, endpoint, namespace):
        raise RuntimeError("boom")

    monkeypatch.setattr(handler_module, "_upsert_to_neptune", explode)

    graph_generator_handler(base_event(), None)

    assert stubbed_aws == ["graphs/default/graph.json"]


def test_event_driven_failure_raises_so_lambda_retries(stubbed_aws, monkeypatch):
    """
    An SQS-sourced failure must fail the invocation.

    Lambda deletes the message when the handler returns normally, so returning
    an error envelope here would drop the event instead of retrying it into
    the DLQ.
    """
    def explode(graph, endpoint, namespace):
        raise RuntimeError("ConcurrentModificationException")

    monkeypatch.setattr(handler_module, "_upsert_to_neptune", explode)
    monkeypatch.setenv("GRAPH_S3_BUCKET", "test-graph-bucket")
    monkeypatch.setenv("NEPTUNE_ENDPOINT", "wss://unused.example:8182/gremlin")

    with pytest.raises(RuntimeError, match="ConcurrentModificationException"):
        graph_generator_handler(sqs_event(s3_record()), None)


def test_event_driven_detection():
    assert _is_event_driven(sqs_event(s3_record())) is True
    assert _is_event_driven({"Records": [s3_record()]}) is True
    assert _is_event_driven({"namespace": "default"}) is False


def test_skip_neptune_reports_unsynced_without_failing(stubbed_aws, monkeypatch):
    def fail(*args, **kwargs):
        raise AssertionError("Neptune must not be contacted when skipped")

    monkeypatch.setattr(handler_module, "_upsert_to_neptune", fail)

    event = dict(base_event(), skip_neptune=True)
    response = graph_generator_handler(event, None)
    body = json.loads(response["body"])

    assert response["statusCode"] == 200
    assert body["success"] is True
    assert body["neptune_synced"] is False
