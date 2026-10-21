"""
Tests for the Neptune write path: idempotent upserts, pruning, retry.

These guard the incident where a bulk rule sync fanned out 15 concurrent
generator invocations, each running drop-then-insert against Neptune. The
result was 9 interleaved copies of the graph (135 rules instead of 15) and a
ConcurrentModificationException that the handler swallowed.

Gremlin calls are captured by a fake traversal source, so no Neptune cluster
is needed; what is asserted is the *shape* of the traversals (match-or-create
rather than blind addV) and the retry/pruning logic.
"""
import pytest
from pathlib import Path

from autograph_rule_engine.core.parser import RuleParser
from autograph_rule_engine.core.graph_builder import GraphBuilder
from autograph_rule_engine.backends.neptune_backend import (
    MAX_UPSERT_ATTEMPTS,
    NeptuneGraphBackend,
    NeptuneWriteConflictError,
    expected_edges,
    is_retryable_neptune_error,
    rule_properties,
)

RULES_DIR = Path(__file__).parent.parent / "rules" / "loan_pricing"

CONFLICT_MESSAGE = (
    '500: {"code":"ConcurrentModificationException",'
    '"message":"Failed to complete operation due to conflicting concurrent operations."}'
)


# ------------------------------------------------------------------ #
#  Fakes                                                              #
# ------------------------------------------------------------------ #

class FakeTraversal:
    """Records the Gremlin steps invoked on it instead of talking to Neptune."""

    def __init__(self, calls, to_list_result=None):
        self.calls = calls
        self._to_list_result = to_list_result if to_list_result is not None else []

    def __getattr__(self, name):
        def step(*args, **kwargs):
            self.calls.append(name)
            if name == "toList":
                return self._to_list_result
            return self

        return step


@pytest.fixture
def calls():
    return []


@pytest.fixture
def backend(calls):
    backend = NeptuneGraphBackend(endpoint="wss://unused.example:8182/gremlin")
    backend._g = FakeTraversal(calls)
    return backend


@pytest.fixture(scope="module")
def loan_graph():
    return GraphBuilder().build(RuleParser().load_directory(RULES_DIR))


# ------------------------------------------------------------------ #
#  Idempotent writes                                                  #
# ------------------------------------------------------------------ #

def test_upsert_never_wipes_the_namespace(backend, loan_graph):
    """The full-namespace drop is what made concurrent writers duplicate data."""
    def fail(*args, **kwargs):
        raise AssertionError("upsert_rule_graph must not drop the whole namespace")

    backend._drop_namespace = fail
    backend.upsert_rule_graph(loan_graph, namespace="default")


def test_rule_vertex_upsert_is_match_or_create(backend, calls, loan_graph):
    backend._upsert_rule_vertex("R01", loan_graph.rules["R01"], "default")

    assert "fold" in calls, "expected a fold/coalesce match-or-create traversal"
    assert "coalesce" in calls
    assert "addV" not in calls, "addV must sit inside coalesce, not run unconditionally"


def test_rule_properties_are_written_with_single_cardinality(backend, calls, loan_graph):
    """
    Neptune vertex properties default to set cardinality, so on a matched vertex a plain
    property(k, v) appends instead of replacing. Editing a rule then leaves the stale value
    alongside the new one — R15's `outputs` was observed holding both
    '["decision", "decision_reason"]' and '["decision"]'.
    """
    from gremlin_python.process.traversal import Cardinality

    seen = []

    class Recording(FakeTraversal):
        def __getattr__(self, name):
            inner = super().__getattr__(name)

            def step(*args, **kwargs):
                if name == "property":
                    seen.append(args)
                return inner(*args, **kwargs)

            return step

    backend._g = Recording(calls)
    backend._upsert_rule_vertex("R01", loan_graph.rules["R01"], "default")

    refreshed = [args for args in seen if len(args) == 3]
    assert refreshed, f"expected cardinality-qualified property writes, saw {seen}"
    assert all(args[0] is Cardinality.single for args in refreshed)
    # Identity properties are set inside addV(), which only runs on create, so they do
    # not need a cardinality and must not be counted here.
    assert {args[1] for args in refreshed} == set(rule_properties(loan_graph.rules["R01"]))


def test_payload_vertex_upsert_is_match_or_create(backend, calls):
    backend._upsert_payload_vertex("loan_amount", "default")

    assert "fold" in calls
    assert "coalesce" in calls
    assert "addV" not in calls


def test_edge_upsert_is_match_or_create(backend, calls):
    backend._upsert_edge("R01", "R03", ["ltv_ratio"], "default")

    assert "fold" in calls
    assert "coalesce" in calls
    assert "addE" not in calls, "addE must sit inside coalesce, not run unconditionally"


def test_edge_upsert_writes_namespace(backend, calls):
    """
    Edges carried only `via`, so every query filtering edges by namespace matched nothing
    — which is what made the Neptune topological sort silently wrong.
    """
    recorded = []

    class Recording(FakeTraversal):
        def __getattr__(self, name):
            inner = super().__getattr__(name)

            def step(*args, **kwargs):
                if name == "property":
                    recorded.append(args[0])
                return inner(*args, **kwargs)

            return step

    backend._g = Recording(calls)
    backend._upsert_edge("R01", "R03", ["ltv_ratio"], "default")

    assert "namespace" in recorded, f"edge properties written: {recorded}"
    assert "via" in recorded


def test_upsert_writes_every_rule_and_edge(backend, loan_graph, monkeypatch):
    """Each rule, payload field and edge gets exactly one write per run."""
    written_rules = []
    written_fields = []
    written_edges = []

    monkeypatch.setattr(
        backend, "_upsert_rule_vertex",
        lambda rule_id, rule, ns: written_rules.append(rule_id),
    )
    monkeypatch.setattr(
        backend, "_upsert_payload_vertex",
        lambda field, ns: written_fields.append(field),
    )
    monkeypatch.setattr(
        backend, "_upsert_edge",
        lambda f, t, via, ns: written_edges.append((f, t)),
    )

    backend.upsert_rule_graph(loan_graph, namespace="default")

    assert len(written_rules) == 15
    assert len(written_rules) == len(set(written_rules))
    assert sorted(written_fields) == sorted(loan_graph.payload_fields)
    assert len(written_edges) == len(set(written_edges)) == 18


# ------------------------------------------------------------------ #
#  Pruning                                                            #
# ------------------------------------------------------------------ #

def test_prune_drops_only_stale_edges(backend, loan_graph, monkeypatch):
    dropped = []

    monkeypatch.setattr(
        backend, "_existing_edges",
        lambda namespace: {"edge-stale": ("R01", "R15"), "edge-valid": ("R01", "R03")},
    )

    class DroppingSource:
        def E(self, edge_id):
            dropped.append(edge_id)
            return FakeTraversal([])

    monkeypatch.setattr(
        NeptuneGraphBackend, "g", property(lambda self: DroppingSource())
    )

    backend._prune_stale_edges(loan_graph, "default")

    assert dropped == ["edge-stale"], "a real edge must never be pruned"


def test_prune_avoids_the_unsupported_iterate_step(backend, calls, loan_graph):
    """
    Neptune rejects iterate().

    This gremlinpython version compiles iterate() to a `discard` step, and
    Neptune answers InvalidParameterException: "Unable to find Traversal steps
    for the graph operator: discard" — which silently left deleted rules in
    the graph.
    """
    backend._prune_stale_vertices(loan_graph, "default")

    assert "iterate" not in calls, "iterate() is not supported by Neptune"
    assert "toList" in calls


def test_prune_keeps_current_rules(backend, calls, loan_graph):
    """Pruning filters on the surviving rule ids rather than dropping blindly."""
    backend._prune_stale_vertices(loan_graph, "default")

    assert "not_" in calls, "expected an exclusion filter before drop"
    assert "drop" in calls


# ------------------------------------------------------------------ #
#  Retry on transient conflicts                                       #
# ------------------------------------------------------------------ #

def test_conflict_errors_are_recognised():
    assert is_retryable_neptune_error(RuntimeError(CONFLICT_MESSAGE))


def test_unrelated_errors_are_not_retryable():
    assert not is_retryable_neptune_error(ValueError("malformed traversal"))


def test_retry_succeeds_after_transient_conflict(backend, monkeypatch):
    monkeypatch.setattr("time.sleep", lambda _: None)
    attempts = []

    def flaky():
        attempts.append(1)
        if len(attempts) < 3:
            raise RuntimeError(CONFLICT_MESSAGE)
        return "ok"

    assert backend._with_retry(flaky) == "ok"
    assert len(attempts) == 3


def test_retry_gives_up_with_a_clear_error(backend, monkeypatch):
    monkeypatch.setattr("time.sleep", lambda _: None)
    attempts = []

    def always_conflicts():
        attempts.append(1)
        raise RuntimeError(CONFLICT_MESSAGE)

    with pytest.raises(NeptuneWriteConflictError):
        backend._with_retry(always_conflicts)

    assert len(attempts) == MAX_UPSERT_ATTEMPTS


def test_non_retryable_errors_propagate_immediately(backend):
    attempts = []

    def bad_query():
        attempts.append(1)
        raise ValueError("malformed traversal")

    with pytest.raises(ValueError):
        backend._with_retry(bad_query)

    assert len(attempts) == 1, "non-retryable errors must not be retried"


# ------------------------------------------------------------------ #
#  Pure helpers                                                       #
# ------------------------------------------------------------------ #

def test_expected_edges_carry_shared_fields(loan_graph):
    edges = expected_edges(loan_graph)

    assert len(edges) == 18
    ltv = [via for f, t, via in edges if (f, t) == ("R01", "R03")]
    assert ltv == [["ltv_ratio"]]


def test_rule_properties_exclude_identity_fields(loan_graph):
    props = rule_properties(loan_graph.rules["R01"])

    assert "rule_id" not in props
    assert "namespace" not in props
    assert props["name"]
    assert props["enabled"] is True


# ------------------------------------------------------------------ #
#  Query correctness and traversal bounds                             #
# ------------------------------------------------------------------ #

class ScriptedTraversal(FakeTraversal):
    """Returns a queued result for each successive toList() call."""

    def __init__(self, calls, queued):
        super().__init__(calls)
        self._queued = list(queued)

    def __getattr__(self, name):
        def step(*args, **kwargs):
            self.calls.append(name)
            if name == "toList":
                return self._queued.pop(0) if self._queued else []
            if name == "next":
                return self._queued.pop(0) if self._queued else 0
            return self

        return step


def test_execution_order_filters_edges_by_label_and_namespace(backend, calls):
    backend._g = ScriptedTraversal(calls, [[{"from": "A", "to": "B"}], ["A", "B"]])

    assert backend.get_execution_order("default") == ["A", "B"]
    assert "hasLabel" in calls, "edges must be filtered to FEEDS"


def test_execution_order_raises_when_incomplete(backend, calls):
    """
    Kahn's algorithm only yields a full order on an acyclic graph. A short result used to
    be returned anyway — all rules in arbitrary order, labelled as a topological sort.
    """
    cyclic = [{"from": "A", "to": "B"}, {"from": "B", "to": "A"}]
    backend._g = ScriptedTraversal(calls, [cyclic, ["A", "B"]])

    with pytest.raises(RuntimeError, match="topological order"):
        backend.get_execution_order("default")


def test_graph_stats_scopes_the_edge_count(backend, calls):
    backend._g = ScriptedTraversal(calls, [15, 18, [], []])
    stats = backend.get_graph_stats("default")

    assert stats["num_edges"] == 18
    assert "hasLabel" in calls, "the edge count must be restricted to FEEDS in this namespace"


def test_find_paths_is_bounded(backend, calls):
    backend._g = ScriptedTraversal(calls, [15, []])
    backend.find_paths("R01", "R13", "default")

    assert "limit" in calls, "an unreachable target must not enumerate the whole search space"


def test_transitive_traversals_are_bounded(backend, calls, loan_graph):
    for method in (backend.get_ancestors, backend.get_descendants):
        calls.clear()
        backend._g = ScriptedTraversal(calls, [15, []])
        method("R15", "default")

        assert "times" in calls, f"{method.__name__} must bound its repeat()"
        assert "dedup" in calls
