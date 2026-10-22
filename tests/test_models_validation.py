"""
Tests for the parse-time rule validation added to core/models.py.

Three defects are guarded here:
  - a rule could read a field it never declared in `inputs`, which creates no graph edge
    and therefore no ordering guarantee relative to the rule producing that field
  - a malformed operator/value pair (`between` with one element, `in` with a scalar) only
    failed at execution time, inside a handler that swallowed the error
  - every action produces one value, but a rule could declare several outputs and silently
    receive the same value in each
"""
import json
import pytest
from pathlib import Path
from pydantic import ValidationError

from autograph_rule_engine.core.models import Action, Condition, Rule
from autograph_rule_engine.core.parser import RuleParser

RULES_DIR = Path(__file__).parent.parent / "rules" / "loan_pricing"


def rule(**overrides):
    """A minimal valid compute rule, with overrides applied."""
    base = {
        "rule_id": "T01",
        "name": "Test rule",
        "inputs": ["a", "b"],
        "outputs": ["c"],
        "action": {"type": "compute", "expression": "a + b"},
    }
    base.update(overrides)
    return base


# ------------------------------------------------------------------ #
#  Declared field reads                                               #
# ------------------------------------------------------------------ #

def test_compute_expression_may_only_read_declared_inputs():
    with pytest.raises(ValidationError, match="undeclared field"):
        Rule(**rule(action={"type": "compute", "expression": "a + mystery"}))


def test_aggregate_fields_must_be_declared():
    with pytest.raises(ValidationError, match="undeclared field"):
        Rule(**rule(action={"type": "aggregate", "agg_func": "sum", "fields": ["a", "typo"]}))


def test_lookup_entry_condition_field_must_be_declared():
    with pytest.raises(ValidationError, match="undeclared field"):
        Rule(**rule(action={
            "type": "lookup",
            "entries": [{"condition": {"field": "typo", "op": "gt", "value": 1}, "result": "HIGH"}],
            "default_result": "LOW",
        }))


def test_decision_branch_condition_field_must_be_declared():
    with pytest.raises(ValidationError, match="undeclared field"):
        Rule(**rule(action={
            "type": "decision",
            "branches": [{"conditions": [{"field": "typo", "op": "eq", "value": True}], "result": "YES"}],
            "default_result": "NO",
        }))


def test_rule_level_condition_field_must_be_declared():
    with pytest.raises(ValidationError, match="undeclared field"):
        Rule(**rule(conditions=[{"field": "typo", "op": "gt", "value": 0}]))


def test_declared_inputs_are_accepted():
    parsed = Rule(**rule(action={"type": "compute", "expression": "a * b"}))
    assert parsed.rule_id == "T01"


def test_allowed_functions_are_not_mistaken_for_fields():
    """min/max/sqrt are function names, not undeclared field reads."""
    parsed = Rule(**rule(action={"type": "compute", "expression": "min(a, b) + sqrt(a)"}))
    assert parsed.action.expression


def test_unparseable_expression_is_rejected():
    with pytest.raises(ValidationError):
        Rule(**rule(action={"type": "compute", "expression": "a +* b"}))


# ------------------------------------------------------------------ #
#  Operator / value shape                                             #
# ------------------------------------------------------------------ #

def test_between_requires_two_values():
    with pytest.raises(ValidationError, match="exactly two"):
        Condition(field="a", op="between", value=[1])


def test_between_accepts_two_values():
    assert Condition(field="a", op="between", value=[1, 5]).value == [1, 5]


def test_in_requires_a_list():
    with pytest.raises(ValidationError, match="requires a list"):
        Condition(field="a", op="in", value="HIGH")


def test_scalar_operator_rejects_a_list():
    with pytest.raises(ValidationError, match="does not accept a list"):
        Condition(field="a", op="gt", value=[1, 2])


# ------------------------------------------------------------------ #
#  Output arity                                                       #
# ------------------------------------------------------------------ #

def test_multiple_outputs_are_rejected():
    """
    Each action produces a single value; the handlers copied it into every declared
    output, which is how decision_reason came to duplicate decision.
    """
    with pytest.raises(ValidationError, match="single value"):
        Rule(**rule(outputs=["c", "d"]))


def test_single_output_is_accepted():
    assert Rule(**rule(outputs=["c"])).outputs == ["c"]


# ------------------------------------------------------------------ #
#  Boolean fidelity                                                   #
# ------------------------------------------------------------------ #

def test_condition_value_false_stays_bool():
    """JSON `false` used to be coerced to 0.0 by Union[float, int, str]."""
    parsed = Condition(field="is_eligible", op="eq", value=False)
    assert parsed.value is False
    assert isinstance(parsed.value, bool)


def test_condition_value_true_stays_bool():
    parsed = Condition(field="is_eligible", op="eq", value=True)
    assert parsed.value is True
    assert isinstance(parsed.value, bool)


def test_integer_condition_value_stays_int():
    parsed = Condition(field="risk_score", op="lte", value=30)
    assert parsed.value == 30
    assert isinstance(parsed.value, int) and not isinstance(parsed.value, bool)


def test_lookup_result_false_stays_bool():
    action = Action(
        type="lookup",
        entries=[{"condition": {"field": "a", "op": "gt", "value": 1}, "result": False}],
        default_result=True,
    )
    assert action.entries[0].result is False
    assert action.default_result is True


# ------------------------------------------------------------------ #
#  The shipped rules must still parse                                 #
# ------------------------------------------------------------------ #

def test_all_shipped_rules_parse():
    """The new validators must be additive, not a migration."""
    rules = RuleParser().load_directory(RULES_DIR)
    assert len(rules) == 15


def test_every_shipped_rule_declares_one_output():
    rules = RuleParser().load_directory(RULES_DIR)
    multi = {rid: r.outputs for rid, r in rules.items() if len(r.outputs) != 1}
    assert not multi, f"rules declaring more than one output: {multi}"


def test_shipped_boolean_rule_keeps_bool_type():
    """
    R06 produces is_eligible from a decision action whose branch results are JSON
    booleans. They used to parse as 0.0/1.0, so `is_eligible` was never a bool and
    R15's `op: eq, value: false` only matched because 0.0 == False in Python.
    """
    raw = json.loads((RULES_DIR / "R06_check_eligibility.json").read_text())
    parsed = Rule(**raw)

    results = [branch.result for branch in parsed.action.branches]
    assert results == [False, False, True], results
    assert all(isinstance(r, bool) for r in results), [type(r).__name__ for r in results]
    assert parsed.action.default_result is False
