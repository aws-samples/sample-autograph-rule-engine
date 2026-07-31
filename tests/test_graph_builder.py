"""
Tests for GraphBuilder: dependency auto-discovery, topological sort, cycle detection.
"""
import pytest
from pathlib import Path

from autograph_rule_engine.core.parser import RuleParser
from autograph_rule_engine.core.graph_builder import GraphBuilder, GraphBuildError
from autograph_rule_engine.core.models import RuleGraph

RULES_DIR = Path(__file__).parent.parent / "rules" / "loan_pricing"

parser = RuleParser()
builder = GraphBuilder()


# ------------------------------------------------------------------ #
#  Fixtures                                                           #
# ------------------------------------------------------------------ #

@pytest.fixture(scope="module")
def loan_rules():
    return parser.load_directory(RULES_DIR)


@pytest.fixture(scope="module")
def loan_graph(loan_rules):
    return builder.build(loan_rules)


# ------------------------------------------------------------------ #
#  Graph structure                                                    #
# ------------------------------------------------------------------ #

def test_all_rules_loaded(loan_rules):
    assert len(loan_rules) == 15
    assert "R01" in loan_rules
    assert "R15" in loan_rules


def test_graph_has_correct_node_count(loan_graph):
    assert len(loan_graph.rules) == 15


def test_payload_fields_auto_detected(loan_graph):
    expected = {
        "credit_score", "employment_status", "loan_amount",
        "loan_term_years", "loan_type", "monthly_debt",
        "monthly_income", "property_value",
    }
    assert set(loan_graph.payload_fields) == expected


def test_root_rules(loan_graph):
    """Rules with no upstream rule dependencies."""
    roots = set(loan_graph.get_root_rules())
    assert "R01" in roots
    assert "R02" in roots
    assert "R04" in roots
    assert "R07" in roots
    assert "R11" in roots


def test_leaf_rules(loan_graph):
    """Terminal output rules."""
    leaves = set(loan_graph.get_leaf_rules())
    assert "R13" in leaves
    assert "R15" in leaves


def test_execution_order_length(loan_graph):
    assert len(loan_graph.execution_order) == 15


def test_topological_order_respected(loan_graph):
    """For every edge A->B, A must appear before B in execution_order."""
    order = loan_graph.execution_order
    pos = {rule_id: i for i, rule_id in enumerate(order)}
    for from_id, to_ids in loan_graph.edges.items():
        for to_id in to_ids:
            assert pos[from_id] < pos[to_id], (
                f"Edge {from_id}->{to_id} violated: {from_id} at {pos[from_id]}, "
                f"{to_id} at {pos[to_id]}"
            )


def test_specific_edges(loan_graph):
    """Verify key dependency edges exist."""
    assert "R03" in loan_graph.edges["R01"]  # ltv_ratio
    assert "R08" in loan_graph.edges["R03"]  # ltv_category
    assert "R13" in loan_graph.edges["R12"]  # final_rate
    assert "R15" in loan_graph.edges["R06"]  # is_eligible
    assert "R15" in loan_graph.edges["R14"]  # risk_score


def test_r12_has_5_upstream_rules(loan_graph):
    """R12 aggregates from R07, R08, R09, R10, R11."""
    upstream = loan_graph.reverse_edges["R12"]
    assert len(upstream) == 5
    assert set(upstream) == {"R07", "R08", "R09", "R10", "R11"}


# ------------------------------------------------------------------ #
#  Cycle detection                                                    #
# ------------------------------------------------------------------ #

def test_cycle_detection_simple():
    """A->B->C->A should raise GraphBuildError."""
    rules = parser.load_dicts([
        {"rule_id": "A", "name": "A", "inputs": ["x"], "outputs": ["y"],
         "action": {"type": "compute", "expression": "x + 1"}},
        {"rule_id": "B", "name": "B", "inputs": ["y"], "outputs": ["z"],
         "action": {"type": "compute", "expression": "y + 1"}},
        {"rule_id": "C", "name": "C", "inputs": ["z"], "outputs": ["x"],
         "action": {"type": "compute", "expression": "z + 1"}},
    ])
    with pytest.raises(GraphBuildError, match="Cycle detected"):
        builder.build(rules)


def test_duplicate_output_field_raises():
    """Two rules producing the same output field should raise."""
    rules = parser.load_dicts([
        {"rule_id": "X", "name": "X", "inputs": ["a"], "outputs": ["shared"],
         "action": {"type": "compute", "expression": "a + 1"}},
        {"rule_id": "Y", "name": "Y", "inputs": ["b"], "outputs": ["shared"],
         "action": {"type": "compute", "expression": "b + 1"}},
    ])
    with pytest.raises(GraphBuildError, match="produced by multiple rules"):
        builder.build(rules)


def test_single_rule_no_edges():
    """A single rule with no dependencies should build fine."""
    rules = parser.load_dicts([
        {"rule_id": "Solo", "name": "Solo", "inputs": ["x"], "outputs": ["y"],
         "action": {"type": "compute", "expression": "x * 2"}},
    ])
    graph = builder.build(rules)
    assert graph.execution_order == ["Solo"]
    assert graph.payload_fields == ["x"]
    assert graph.get_root_rules() == ["Solo"]
    assert graph.get_leaf_rules() == ["Solo"]
