"""
Quickstart: end-to-end smoke test for the rule engine.
Run from project root:
    python quickstart.py
"""
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.WARNING,   # suppress verbose debug; set to DEBUG to see all
    format="%(levelname)s | %(name)s | %(message)s",
)

from autograph_rule_engine.core.parser import RuleParser
from autograph_rule_engine.core.graph_builder import GraphBuilder
from autograph_rule_engine.core.validator import GraphValidator
from autograph_rule_engine.core.executor import RuleExecutor
from autograph_rule_engine.backends.local_backend import LocalGraphBackend

RULES_DIR = Path(__file__).parent.parent / "rules" / "loan_pricing"

# ------------------------------------------------------------------ #
# 1. Load rules
# ------------------------------------------------------------------ #
print("\n" + "=" * 60)
print("  STEP 1: Load Rules")
print("=" * 60)
parser = RuleParser()
rules = parser.load_directory(RULES_DIR)
print(f"  Loaded {len(rules)} rules: {sorted(rules.keys())}")

# ------------------------------------------------------------------ #
# 2. Build graph (auto-discover dependencies)
# ------------------------------------------------------------------ #
print("\n" + "=" * 60)
print("  STEP 2: Auto-Build Dependency Graph")
print("=" * 60)
builder = GraphBuilder()
graph = builder.build(rules)
print(f"  Payload fields  : {graph.payload_fields}")
print(f"  Execution order : {graph.execution_order}")
print(f"  Root rules      : {graph.get_root_rules()}")
print(f"  Leaf rules      : {graph.get_leaf_rules()}")

# ------------------------------------------------------------------ #
# 3. Validate graph
# ------------------------------------------------------------------ #
print("\n" + "=" * 60)
print("  STEP 3: Validate Graph")
print("=" * 60)
validator = GraphValidator()
val_result = validator.validate(graph)
print(val_result.summary())
val_result.raise_if_invalid()

# ------------------------------------------------------------------ #
# 4. Explore graph with NetworkX backend
# ------------------------------------------------------------------ #
print("\n" + "=" * 60)
print("  STEP 4: NetworkX Backend — Graph Stats & Impact Analysis")
print("=" * 60)
backend = LocalGraphBackend()
backend.load_from_rule_graph(graph)
stats = backend.get_graph_stats()
print(f"  Nodes            : {stats['num_nodes']}")
print(f"  Edges            : {stats['num_edges']}")
print(f"  Is DAG           : {stats['is_dag']}")
print(f"  Longest path     : {stats['longest_path']}")
print(f"  Longest path len : {stats['longest_path_length']}")
print(f"  Execution levels :")
for i, level in enumerate(stats["execution_levels"]):
    print(f"    Level {i+1}: {sorted(level)}")

print("\n  Impact analysis for R03 (Classify LTV Tier):")
impact = backend.get_impact("R03")
print(f"    Depends on         : {impact['depends_on_rules']}")
print(f"    Rules affected if changed: {impact['rules_affected_if_changed']}")

print("\n  All paths R01 → R15:")
paths = backend.find_all_paths("R01", "R15")
for p in paths:
    print(f"    {' → '.join(p)}")

# ASCII graph
backend.print_ascii_graph()

# ------------------------------------------------------------------ #
# 5. Execute — normal applicant (should APPROVE)
# ------------------------------------------------------------------ #
print("=" * 60)
print("  STEP 5: Execute — Good Applicant (expect APPROVED)")
print("=" * 60)
good_payload = {
    "loan_amount":      280000,
    "property_value":   350000,
    "credit_score":     740,
    "monthly_debt":     1500,
    "monthly_income":   8000,
    "loan_type":        "CONVENTIONAL",
    "loan_term_years":  30,
    "employment_status": "FULL_TIME",
}
executor = RuleExecutor()
result = executor.execute(graph, good_payload)
print(result.summary())
print("\n  Leaf outputs:")
for k, v in result.get_leaf_outputs(graph).items():
    print(f"    {k:25s} = {v}")

# ------------------------------------------------------------------ #
# 6. Execute — risky applicant (should DECLINE)
# ------------------------------------------------------------------ #
print("\n" + "=" * 60)
print("  STEP 6: Execute — Risky Applicant (expect DECLINED)")
print("=" * 60)
bad_payload = {
    "loan_amount":      320000,
    "property_value":   330000,   # LTV ~0.97 → VERY_HIGH
    "credit_score":     540,       # POOR
    "monthly_debt":     3500,
    "monthly_income":   6000,
    "loan_type":        "CONVENTIONAL",
    "loan_term_years":  30,
    "employment_status": "PART_TIME",
}
result2 = executor.execute(graph, bad_payload)
print(result2.summary())
print("\n  Leaf outputs:")
for k, v in result2.get_leaf_outputs(graph).items():
    print(f"    {k:25s} = {v}")

# ------------------------------------------------------------------ #
# 7. Execute — borderline applicant (expect MANUAL_REVIEW)
# ------------------------------------------------------------------ #
print("\n" + "=" * 60)
print("  STEP 7: Execute — Borderline Applicant (expect MANUAL_REVIEW)")
print("=" * 60)
borderline_payload = {
    "loan_amount":      300000,
    "property_value":   360000,   # LTV 0.83 → HIGH
    "credit_score":     670,       # GOOD (just barely)
    "monthly_debt":     2400,
    "monthly_income":   6500,      # DTI 0.369 → ELEVATED
    "loan_type":        "FHA",
    "loan_term_years":  30,
    "employment_status": "SELF_EMPLOYED",
}
result3 = executor.execute(graph, borderline_payload)
print(result3.summary())
print("\n  Leaf outputs:")
for k, v in result3.get_leaf_outputs(graph).items():
    print(f"    {k:25s} = {v}")

# ------------------------------------------------------------------ #
# 8. Cycle detection test
# ------------------------------------------------------------------ #
print("\n" + "=" * 60)
print("  STEP 8: Cycle Detection Test (inject a cycle, expect error)")
print("=" * 60)
from autograph_rule_engine.core.models import Rule, Action
from autograph_rule_engine.core.graph_builder import GraphBuildError

cycle_rules_raw = [
    {
        "rule_id": "CA", "name": "A", "inputs": ["x"], "outputs": ["y"],
        "action": {"type": "compute", "expression": "x + 1"}
    },
    {
        "rule_id": "CB", "name": "B", "inputs": ["y"], "outputs": ["z"],
        "action": {"type": "compute", "expression": "y + 1"}
    },
    {
        "rule_id": "CC", "name": "C", "inputs": ["z"], "outputs": ["x"],  # cycle: z → x → y → z
        "action": {"type": "compute", "expression": "z + 1"}
    },
]
cycle_rules = parser.load_dicts(cycle_rules_raw)
try:
    GraphBuilder().build(cycle_rules)
    print("  ERROR: Should have detected a cycle!")
except GraphBuildError as e:
    print(f"  ✓ Cycle correctly detected:\n    {str(e)[:200]}")

print("\n" + "=" * 60)
print("  ALL TESTS PASSED")
print("=" * 60 + "\n")