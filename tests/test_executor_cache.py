"""
Tests for the executor's graph cache.

The cache used to key on `bucket/key` alone — object identity, never content — so a warm
container kept serving whatever it first loaded for the rest of its life, no matter how many
times the generator republished the graph over the same key. There is no push invalidation
(the graph bucket has no notification to the executor), so the only reliable signal is
asking S3 whether the object changed.
"""
import io
import json
import pytest
from pathlib import Path

from botocore.exceptions import ClientError

from autograph_rule_engine.compute import graph_executor_handler as handler
from autograph_rule_engine.core.graph_builder import GraphBuilder
from autograph_rule_engine.core.parser import RuleParser

RULES_DIR = Path(__file__).parent.parent / "rules" / "loan_pricing"
BUCKET, KEY = "test-graph-bucket", "graphs/default/graph.json"


def graph_json():
    graph = GraphBuilder().build(RuleParser().load_directory(RULES_DIR))
    return graph.model_dump_json()


class FakeS3:
    """Records get_object calls and replays scripted responses."""

    def __init__(self, etag='"aaa"', body=None):
        self.etag = etag
        self.body = body or graph_json()
        self.calls = []

    def get_object(self, **kwargs):
        self.calls.append(kwargs)

        if kwargs.get("IfNoneMatch") == self.etag:
            raise ClientError(
                {"Error": {"Code": "304", "Message": "Not Modified"}}, "GetObject"
            )

        return {
            "Body": io.BytesIO(self.body.encode("utf-8")),
            "ETag": self.etag,
            "VersionId": "v1",
        }


@pytest.fixture(autouse=True)
def reset_cache(monkeypatch):
    """Module-level cache persists across invocations, so clear it per test."""
    monkeypatch.setattr(handler, "_cached_graph", None, raising=False)
    monkeypatch.setattr(handler, "_cached_graph_key", None, raising=False)
    monkeypatch.setattr(handler, "_cached_graph_etag", None, raising=False)
    monkeypatch.setattr(handler, "_cached_graph_version_id", None, raising=False)
    monkeypatch.setattr(handler, "_cached_graph_checked_at", 0.0, raising=False)


@pytest.fixture
def s3(monkeypatch):
    fake = FakeS3()
    monkeypatch.setattr(handler, "_get_s3_client", lambda: fake)
    return fake


# ------------------------------------------------------------------ #
#  Loading and caching                                                #
# ------------------------------------------------------------------ #

def test_first_call_loads_from_s3(s3):
    graph, load_ms = handler._get_or_load_graph(BUCKET, KEY)

    assert len(graph.rules) == 15
    assert len(s3.calls) == 1
    assert "IfNoneMatch" not in s3.calls[0], "nothing is cached yet, so no conditional GET"


def test_second_call_inside_the_window_does_not_touch_s3(s3, monkeypatch):
    monkeypatch.setattr(handler, "REVALIDATE_SECONDS", 30.0)

    handler._get_or_load_graph(BUCKET, KEY)
    handler._get_or_load_graph(BUCKET, KEY)

    assert len(s3.calls) == 1, "the TTL window must suppress revalidation"


def test_unchanged_object_serves_the_cache(s3, monkeypatch):
    """A 304 arrives as a ClientError; it must keep the cached graph, not crash."""
    monkeypatch.setattr(handler, "REVALIDATE_SECONDS", 0.0)

    first, _ = handler._get_or_load_graph(BUCKET, KEY)
    second, _ = handler._get_or_load_graph(BUCKET, KEY)

    assert len(s3.calls) == 2
    assert s3.calls[1]["IfNoneMatch"] == '"aaa"'
    assert second is first, "a 304 must reuse the cached object"


def test_changed_object_is_reloaded(s3, monkeypatch):
    monkeypatch.setattr(handler, "REVALIDATE_SECONDS", 0.0)

    handler._get_or_load_graph(BUCKET, KEY)
    assert handler._graph_version() == "aaa"

    # Republish: new ETag, so the conditional GET no longer matches.
    s3.etag = '"bbb"'
    handler._get_or_load_graph(BUCKET, KEY)

    assert handler._graph_version() == "bbb", "a republished graph must be picked up"
    assert len(s3.calls) == 2


def test_force_reload_bypasses_both_cache_and_conditional_get(s3, monkeypatch):
    monkeypatch.setattr(handler, "REVALIDATE_SECONDS", 30.0)

    handler._get_or_load_graph(BUCKET, KEY)
    handler._get_or_load_graph(BUCKET, KEY, force_reload=True)

    assert len(s3.calls) == 2
    assert "IfNoneMatch" not in s3.calls[1]


def test_a_different_key_is_loaded_fresh(s3, monkeypatch):
    monkeypatch.setattr(handler, "REVALIDATE_SECONDS", 30.0)

    handler._get_or_load_graph(BUCKET, KEY)
    handler._get_or_load_graph(BUCKET, "graphs/other/graph.json")

    assert len(s3.calls) == 2
    assert "IfNoneMatch" not in s3.calls[1], "a different object cannot reuse the cached ETag"


# ------------------------------------------------------------------ #
#  Version identity                                                   #
# ------------------------------------------------------------------ #

def test_graph_version_strips_etag_quotes(s3):
    handler._get_or_load_graph(BUCKET, KEY)
    assert handler._graph_version() == "aaa"


def test_graph_version_is_none_before_any_load():
    assert handler._graph_version() is None


# ------------------------------------------------------------------ #
#  Deserialization                                                    #
# ------------------------------------------------------------------ #

def test_pydantic_v2_artifact_parses(s3):
    """
    The publishers serialize with Pydantic v2 `model_dump_json`; the reader used the v1
    `parse_raw` shim, which is removed in Pydantic v3.
    """
    graph, _ = handler._get_or_load_graph(BUCKET, KEY)

    assert len(graph.execution_order) == 15
    assert graph.execution_order[0] == "R01"
    assert "loan_amount" in graph.payload_fields


def test_non_304_client_errors_propagate(monkeypatch):
    class Failing:
        def get_object(self, **kwargs):
            raise ClientError(
                {"Error": {"Code": "AccessDenied", "Message": "nope"}}, "GetObject"
            )

    monkeypatch.setattr(handler, "_get_s3_client", lambda: Failing())

    with pytest.raises(ClientError):
        handler._get_or_load_graph(BUCKET, KEY)
