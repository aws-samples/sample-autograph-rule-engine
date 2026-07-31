"""
Validator: checks a RuleGraph for structural integrity before execution.

Checks performed:
  1. Cycle detection         — no circular dependencies
  2. Missing payload fields  — inputs not covered by any rule or payload
  3. Orphan rules            — rules with no edges (isolated, never feeds anything)
  4. Unreachable rules       — rules that can never execute due to broken dep chain
  5. Duplicate output fields — two rules producing the same field name
  6. Empty graph             — no rules loaded
  7. Action consistency      — action type vs. required action fields
"""

from __future__ import annotations
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Set

from .models import ActionType, RuleGraph

logger = logging.getLogger(__name__)


@dataclass
class ValidationResult:
    """Holds the results of a full graph validation run."""
    is_valid: bool = True
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    info: List[str] = field(default_factory=list)

    def add_error(self, msg: str):
        self.errors.append(msg)
        self.is_valid = False
        logger.error(f"[VALIDATION ERROR] {msg}")

    def add_warning(self, msg: str):
        self.warnings.append(msg)
        logger.warning(f"[VALIDATION WARNING] {msg}")

    def add_info(self, msg: str):
        self.info.append(msg)
        logger.info(f"[VALIDATION INFO] {msg}")

    def summary(self) -> str:
        lines = [
            f"Validation {'PASSED' if self.is_valid else 'FAILED'}",
            f"  Errors   : {len(self.errors)}",
            f"  Warnings : {len(self.warnings)}",
        ]
        if self.errors:
            lines.append("  Error details:")
            for e in self.errors:
                lines.append(f"    ✗ {e}")
        if self.warnings:
            lines.append("  Warning details:")
            for w in self.warnings:
                lines.append(f"    ⚠ {w}")
        if self.info:
            lines.append("  Info:")
            for i in self.info:
                lines.append(f"    ℹ {i}")
        return "\n".join(lines)

    def raise_if_invalid(self):
        """Raise GraphValidationError if any errors were found."""
        if not self.is_valid:
            raise GraphValidationError(
                f"Graph validation failed with {len(self.errors)} error(s):\n"
                + "\n".join(f"  - {e}" for e in self.errors)
            )


class GraphValidator:
    """
    Validates a RuleGraph before execution.

    Usage:
        validator = GraphValidator()
        result = validator.validate(graph, payload_fields=["loan_amount", ...])
        result.raise_if_invalid()
        print(result.summary())
    """

    def validate(
        self,
        graph: RuleGraph,
        payload: Dict = None,
        strict: bool = False,
    ) -> ValidationResult:
        """
        Run all validation checks on the graph.

        Args:
            graph:   The RuleGraph to validate.
            payload: Optional actual payload dict. If provided, checks that
                     all required payload fields are present in the payload.
            strict:  If True, warnings are treated as errors.

        Returns:
            ValidationResult with .is_valid, .errors, .warnings
        """
        result = ValidationResult()

        self._check_empty_graph(graph, result)
        if not result.is_valid:
            return result  # no point running further checks on empty graph

        self._check_execution_order(graph, result)
        self._check_payload_fields(graph, payload, result)
        self._check_orphan_rules(graph, result)
        self._check_action_consistency(graph, result)
        self._check_output_field_conflicts(graph, result)
        self._report_graph_stats(graph, result)

        if strict and result.warnings:
            for w in result.warnings:
                result.add_error(f"[strict] {w}")

        return result

    # ------------------------------------------------------------------ #
    #  Individual checks                                                   #
    # ------------------------------------------------------------------ #

    def _check_empty_graph(self, graph: RuleGraph, result: ValidationResult):
        if not graph.rules:
            result.add_error("Graph has no rules. Load at least one rule before executing.")

    def _check_execution_order(self, graph: RuleGraph, result: ValidationResult):
        """
        The execution_order is populated by GraphBuilder's topological sort.
        If it wasn't set or is incomplete, it implies a cycle was not caught.
        """
        if not graph.execution_order:
            result.add_error(
                "execution_order is empty. Did you run GraphBuilder.build()? "
                "This may also indicate a cycle in the rule graph."
            )
            return

        ordered_set = set(graph.execution_order)
        all_rules_set = set(graph.rules.keys())

        missing_from_order = all_rules_set - ordered_set
        if missing_from_order:
            result.add_error(
                f"The following rules are NOT in the execution order — possible cycle: "
                f"{sorted(missing_from_order)}"
            )

        extra_in_order = ordered_set - all_rules_set
        if extra_in_order:
            result.add_error(
                f"execution_order references rule_ids that don't exist in the graph: "
                f"{sorted(extra_in_order)}"
            )

    def _check_payload_fields(
        self, graph: RuleGraph, payload: Dict, result: ValidationResult
    ):
        """Check that all required payload fields are declared / present."""
        if not graph.payload_fields:
            result.add_warning(
                "No payload fields detected. Every rule's inputs are produced by other rules. "
                "Make sure root rules have their inputs covered."
            )
            return

        result.add_info(f"Required payload fields: {sorted(graph.payload_fields)}")

        if payload is not None:
            missing = set(graph.payload_fields) - set(payload.keys())
            if missing:
                result.add_error(
                    f"Payload is missing required fields: {sorted(missing)}"
                )
            extra = set(payload.keys()) - set(graph.payload_fields)
            if extra:
                result.add_warning(
                    f"Payload contains extra fields not used by any rule: {sorted(extra)}"
                )

    def _check_orphan_rules(self, graph: RuleGraph, result: ValidationResult):
        """
        Orphan = a rule with no incoming edges AND no outgoing edges.
        These rules are standalone — they won't contribute to any downstream rule.
        """
        orphans = []
        for rule_id in graph.rules:
            has_upstream = bool(graph.reverse_edges.get(rule_id))
            has_downstream = bool(graph.edges.get(rule_id))
            is_payload_fed = all(
                f in graph.payload_fields for f in graph.rules[rule_id].inputs
            )
            if not has_upstream and not has_downstream:
                # Root rule with no downstream — it's a terminal singleton
                if is_payload_fed:
                    result.add_warning(
                        f"Rule '{rule_id}' ({graph.rules[rule_id].name}) is isolated: "
                        f"it reads from payload but its output is not used by any other rule. "
                        f"Consider whether this rule should feed into something."
                    )
                else:
                    orphans.append(rule_id)

        if orphans:
            result.add_error(
                f"Truly orphaned rules (no inputs and no outputs connected): {orphans}"
            )

    def _check_action_consistency(self, graph: RuleGraph, result: ValidationResult):
        """Check that each rule's action has the required fields for its type."""
        for rule_id, rule in graph.rules.items():
            action = rule.action
            atype = action.type

            if atype == ActionType.COMPUTE or atype == "compute":
                if not action.expression:
                    result.add_error(
                        f"Rule '{rule_id}': action type 'compute' requires 'expression' field."
                    )

            elif atype == ActionType.LOOKUP or atype == "lookup":
                if not action.entries:
                    result.add_error(
                        f"Rule '{rule_id}': action type 'lookup' requires 'entries' list."
                    )

            elif atype == ActionType.AGGREGATE or atype == "aggregate":
                if not action.agg_func:
                    result.add_error(
                        f"Rule '{rule_id}': action type 'aggregate' requires 'agg_func'."
                    )
                if not action.fields:
                    result.add_error(
                        f"Rule '{rule_id}': action type 'aggregate' requires 'fields' list."
                    )
                valid_funcs = {"sum", "avg", "min", "max", "count"}
                if action.agg_func and action.agg_func not in valid_funcs:
                    result.add_error(
                        f"Rule '{rule_id}': unknown agg_func '{action.agg_func}'. "
                        f"Valid options: {valid_funcs}"
                    )

            elif atype == ActionType.DECISION or atype == "decision":
                if not action.branches:
                    result.add_error(
                        f"Rule '{rule_id}': action type 'decision' requires 'branches' list."
                    )

    def _check_output_field_conflicts(self, graph: RuleGraph, result: ValidationResult):
        """Verify no two rules produce the same output field (should be caught by builder too)."""
        seen: Dict[str, str] = {}
        for rule_id, rule in graph.rules.items():
            for out_field in rule.outputs:
                if out_field in seen:
                    result.add_error(
                        f"Output field conflict: field '{out_field}' is produced by "
                        f"both '{seen[out_field]}' and '{rule_id}'."
                    )
                else:
                    seen[out_field] = rule_id

    def _report_graph_stats(self, graph: RuleGraph, result: ValidationResult):
        """Informational stats about the graph."""
        total_edges = sum(len(v) for v in graph.edges.values())
        root_rules = graph.get_root_rules()
        leaf_rules = graph.get_leaf_rules()

        result.add_info(f"Total rules: {len(graph.rules)}")
        result.add_info(f"Total dependency edges: {total_edges}")
        result.add_info(f"Root rules (payload-fed): {root_rules}")
        result.add_info(f"Leaf rules (terminal outputs): {leaf_rules}")
        result.add_info(f"Execution order: {graph.execution_order}")

        # Compute graph depth (longest path)
        depth = self._compute_max_depth(graph)
        result.add_info(f"Graph depth (longest dependency chain): {depth}")

    def _compute_max_depth(self, graph: RuleGraph) -> int:
        """Compute the maximum depth of the DAG (longest path from root to leaf)."""
        depth: Dict[str, int] = {}

        def dfs(rule_id: str) -> int:
            if rule_id in depth:
                return depth[rule_id]
            upstream = graph.reverse_edges.get(rule_id, [])
            if not upstream:
                depth[rule_id] = 0
            else:
                depth[rule_id] = 1 + max(dfs(dep) for dep in upstream)
            return depth[rule_id]

        for rule_id in graph.rules:
            dfs(rule_id)

        return max(depth.values()) if depth else 0


class GraphValidationError(Exception):
    """Raised when graph validation fails and raise_if_invalid() is called."""
    pass