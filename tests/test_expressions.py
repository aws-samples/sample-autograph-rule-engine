"""
Tests for compute-expression handling.

simpleeval's default function table includes rand() and randint(), so before this any
compute rule could return a different value for the same payload on each run — a decision
engine cannot audit that. The function table is now explicit.
"""
import pytest
from pathlib import Path
from simpleeval import FunctionNotDefined, NameNotDefined, SimpleEval

from autograph_rule_engine.core.expressions import ALLOWED_FUNCTIONS, expression_field_names
from autograph_rule_engine.core.parser import RuleParser

RULES_DIR = Path(__file__).parent.parent / "rules" / "loan_pricing"


def evaluate(expression, **names):
    """Evaluate through the same restricted evaluator the executor builds."""
    return SimpleEval(names=names, functions=ALLOWED_FUNCTIONS).eval(expression)


# ------------------------------------------------------------------ #
#  Non-determinism is unavailable                                     #
# ------------------------------------------------------------------ #

def test_rand_is_not_callable():
    with pytest.raises(FunctionNotDefined):
        evaluate("rand()")


def test_randint_is_not_callable():
    with pytest.raises(FunctionNotDefined):
        evaluate("randint(10)")


def test_compound_type_constructors_are_not_callable():
    """SimpleEval rather than EvalWithCompoundTypes; no rule needs these."""
    for expression in ("list()", "dict()", "set()", "tuple()"):
        with pytest.raises(FunctionNotDefined):
            evaluate(expression)


def test_allowlisted_maths_works():
    assert evaluate("abs(-3)") == 3
    assert evaluate("round(1.567, 2)") == 1.57
    assert evaluate("min(a, b)", a=2, b=5) == 2
    assert evaluate("max(a, b)", a=2, b=5) == 5
    assert evaluate("floor(1.9)") == 1
    assert evaluate("ceil(1.1)") == 2
    assert evaluate("sqrt(9)") == 3.0


def test_undeclared_name_raises():
    with pytest.raises(NameNotDefined):
        evaluate("a + missing", a=1)


# ------------------------------------------------------------------ #
#  Field extraction                                                   #
# ------------------------------------------------------------------ #

def test_field_names_extracted():
    assert expression_field_names("loan_amount / property_value") == {
        "loan_amount", "property_value",
    }


def test_function_names_are_not_fields():
    assert expression_field_names("min(a, b) + sqrt(c)") == {"a", "b", "c"}


def test_constants_are_not_fields():
    assert expression_field_names("a * 12 + 0.5") == {"a"}
    assert expression_field_names("a if b else c") == {"a", "b", "c"}


def test_unparseable_expression_raises():
    with pytest.raises(ValueError, match="not parseable"):
        expression_field_names("a +* b")


# ------------------------------------------------------------------ #
#  The shipped expressions must still work                            #
# ------------------------------------------------------------------ #

def test_shipped_compute_expressions_evaluate():
    """
    Every compute rule in the sample must still evaluate under the restricted function
    table — including R13's nested exponent, which is the most complex one.
    """
    rules = RuleParser().load_directory(RULES_DIR)
    compute_rules = {
        rid: r for rid, r in rules.items()
        if (r.action.type if isinstance(r.action.type, str) else r.action.type.value) == "compute"
    }
    assert compute_rules, "expected at least one compute rule in the sample"

    for rid, r in compute_rules.items():
        names = {field: 2.0 for field in expression_field_names(r.action.expression)}
        result = evaluate(r.action.expression, **names)
        assert isinstance(result, (int, float)), f"{rid} produced {result!r}"
