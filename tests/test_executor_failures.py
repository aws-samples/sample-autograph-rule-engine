"""
Tests that evaluation failures surface instead of masquerading as non-matches.

Both `_action_lookup` and `_action_decision` used to wrap their entry/branch loop in
`except Exception: continue`. A missing field, an unknown operator or an incomparable
comparison was therefore indistinguishable from a condition that simply did not match, so
the rule fell through to `default_result` and reported `fired=True` — for the shipped R15
that meant a silent "DECLINED".
"""
import pytest

from autograph_rule_engine.core.executor import RuleExecutor
from autograph_rule_engine.core.graph_builder import GraphBuilder
from autograph_rule_engine.core.models import Rule


def graph_of(*rules):
    return GraphBuilder().build({r.rule_id: r for r in rules})


def lookup_rule(field="score"):
    return Rule(
        rule_id="L01",
        name="Lookup",
        inputs=[field],
        outputs=["band"],
        action={
            "type": "lookup",
            "entries": [
                {"condition": {"field": field, "op": "gt", "value": 700}, "result": "GOOD"},
            ],
            "default_result": "POOR",
        },
    )


def decision_rule():
    """Two conditions; `flag` decides whether the second is reached by short-circuit."""
    return Rule(
        rule_id="D01",
        name="Decision",
        inputs=["flag", "absent"],
        outputs=["verdict"],
        action={
            "type": "decision",
            "branches": [
                {
                    "conditions": [
                        {"field": "flag", "op": "eq", "value": True},
                        {"field": "absent", "op": "gt", "value": 1},
                    ],
                    "result": "YES",
                },
            ],
            "default_result": "NO",
        },
    )


def run(rule, payload):
    return RuleExecutor().execute(graph_of(rule), payload)


# ------------------------------------------------------------------ #
#  Lookup                                                            #
# ------------------------------------------------------------------ #

def test_lookup_with_a_missing_field_errors_rather_than_defaulting():
    result = run(lookup_rule(), {})

    rr = result.rule_results[0]
    assert rr.error is not None, "a missing field must not look like a non-match"
    assert rr.fired is False
    assert result.success is False
    assert "band" not in result.context, "no output may be published from a failed rule"


def test_lookup_still_defaults_when_nothing_matches_cleanly():
    """The default is for 'nothing matched', which is still a legitimate outcome."""
    result = run(lookup_rule(), {"score": 500})

    rr = result.rule_results[0]
    assert rr.error is None
    assert rr.fired is True
    assert result.context["band"] == "POOR"


def test_lookup_matches_when_it_should():
    result = run(lookup_rule(), {"score": 750})
    assert result.context["band"] == "GOOD"
    assert result.success is True


# ------------------------------------------------------------------ #
#  Decision — order independence                                      #
# ------------------------------------------------------------------ #

@pytest.mark.parametrize("flag", [True, False])
def test_decision_error_is_independent_of_short_circuit_order(flag):
    """
    The regression test for payload-dependent error swallowing.

    `all(...)` short-circuits, so with flag=False the broken second condition was never
    reached and no exception was raised; with flag=True it was reached and the exception
    was swallowed. Either way the rule reported fired=True on default_result, but whether
    anything was *wrong* depended on the payload. Both payloads must now behave the same.
    """
    result = run(decision_rule(), {"flag": flag})

    rr = result.rule_results[0]
    assert rr.error is not None, f"flag={flag} must surface the broken condition"
    assert rr.fired is False
    assert result.success is False
    assert "verdict" not in result.context


def test_decision_defaults_cleanly_when_conditions_are_evaluable():
    rule = Rule(
        rule_id="D02",
        name="Decision",
        inputs=["flag"],
        outputs=["verdict"],
        action={
            "type": "decision",
            "branches": [
                {"conditions": [{"field": "flag", "op": "eq", "value": True}], "result": "YES"},
            ],
            "default_result": "NO",
        },
    )

    assert run(rule, {"flag": True}).context["verdict"] == "YES"
    assert run(rule, {"flag": False}).context["verdict"] == "NO"


def test_unknown_operator_is_reported():
    """An unparseable operator is rejected at parse time, not silently ignored."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        Rule(
            rule_id="D03",
            name="Bad operator",
            inputs=["a"],
            outputs=["b"],
            action={
                "type": "decision",
                "branches": [
                    {"conditions": [{"field": "a", "op": "approximately", "value": 1}], "result": "X"},
                ],
                "default_result": "Y",
            },
        )


# ------------------------------------------------------------------ #
#  Aggregate                                                          #
# ------------------------------------------------------------------ #

def test_aggregating_a_boolean_is_rejected():
    """float(True) is 1.0, so a boolean used to be summed as a number silently."""
    rule = Rule(
        rule_id="A01",
        name="Aggregate",
        inputs=["flag", "amount"],
        outputs=["total"],
        action={"type": "aggregate", "agg_func": "sum", "fields": ["flag", "amount"]},
    )

    result = run(rule, {"flag": True, "amount": 2.0})

    rr = result.rule_results[0]
    assert rr.error is not None
    assert "boolean" in rr.error.lower()
    assert result.success is False


def test_aggregate_sums_numbers():
    rule = Rule(
        rule_id="A02",
        name="Aggregate",
        inputs=["x", "y"],
        outputs=["total"],
        action={"type": "aggregate", "agg_func": "sum", "fields": ["x", "y"]},
    )

    assert run(rule, {"x": 1.5, "y": 2.5}).context["total"] == 4.0


def test_aggregate_rejects_non_numeric():
    rule = Rule(
        rule_id="A03",
        name="Aggregate",
        inputs=["x", "y"],
        outputs=["total"],
        action={"type": "aggregate", "agg_func": "sum", "fields": ["x", "y"]},
    )

    result = run(rule, {"x": 1.0, "y": "HIGH"})
    assert result.rule_results[0].error is not None
    assert "not numeric" in result.rule_results[0].error
