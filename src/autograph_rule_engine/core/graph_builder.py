"""
Graph Builder: auto-discovers dependencies between rules by matching
output fields of one rule to input fields of another.

No explicit 'depends_on' needed — the graph is built purely from
the data-flow: if Rule A outputs field X, and Rule B inputs field X,
then A → B (A must execute before B).
"""

from __future__ import annotations
import logging
from typing import Dict, List, Optional, Set, Tuple

from .models import Rule, RuleGraph

logger = logging.getLogger(__name__)


class GraphBuilder:
    """
    Builds a RuleGraph from a collection of Rule objects.

    Algorithm:
    1. Index all rules by their output fields: field_name → [rule_id, ...]
    2. For each rule R, look at R.inputs:
       - If an input field is produced by another rule A → add edge A → R
       - If an input field is not produced by any rule → it must come from the payload
    3. Detect payload fields: inputs not produced by any rule
    4. Run topological sort (Kahn's algorithm) to determine execution order
    5. Validate: cycle detection is implicit in Kahn's algorithm
    """

    def build(
        self,
        rules: Dict[str, Rule],
        payload_fields: Optional[List[str]] = None,
    ) -> RuleGraph:
        """
        Build a RuleGraph from a dict of rule_id → Rule.

        Args:
            rules:          All rules to include in the graph.
            payload_fields: Optional explicit list of fields in the input payload.
                            If None, any input field not produced by a rule is
                            assumed to be a payload field.

        Returns:
            A validated RuleGraph with execution_order populated.

        Raises:
            GraphBuildError: if a cycle is detected or required inputs are missing.
        """
        graph = RuleGraph()

        # Register all rules
        for rule in rules.values():
            graph.add_rule(rule)

        # Build output-field → rule_id index
        output_index: Dict[str, str] = {}  # field_name → rule_id that produces it
        for rule_id, rule in rules.items():
            for field in rule.outputs:
                if field in output_index:
                    raise GraphBuildError(
                        f"Field '{field}' is produced by multiple rules: "
                        f"'{output_index[field]}' and '{rule_id}'. "
                        f"Each output field must be produced by exactly one rule."
                    )
                output_index[field] = rule_id

        logger.debug(f"Output field index: {output_index}")

        # Discover edges and payload fields
        discovered_payload_fields: Set[str] = set()
        for rule_id, rule in rules.items():
            for input_field in rule.inputs:
                if input_field in output_index:
                    # This input is produced by another rule → add dependency edge
                    producer_rule_id = output_index[input_field]
                    if producer_rule_id != rule_id:  # no self-loops
                        graph.add_edge(producer_rule_id, rule_id)
                        logger.debug(
                            f"Edge: {producer_rule_id} → {rule_id}  "
                            f"(via field '{input_field}')"
                        )
                else:
                    # Not produced by any rule → must be a payload field
                    discovered_payload_fields.add(input_field)

        # Set payload fields
        if payload_fields is not None:
            # Validate that explicitly stated payload fields cover discovered ones
            missing = discovered_payload_fields - set(payload_fields)
            if missing:
                raise GraphBuildError(
                    f"The following fields are required as inputs but are neither "
                    f"produced by any rule nor listed in payload_fields: {sorted(missing)}"
                )
            graph.payload_fields = list(payload_fields)
        else:
            graph.payload_fields = sorted(discovered_payload_fields)
            logger.info(f"Auto-detected payload fields: {graph.payload_fields}")

        # Topological sort via Kahn's algorithm
        execution_order = self._topological_sort(graph)
        graph.execution_order = execution_order

        self._log_graph_summary(graph)
        return graph

    def _topological_sort(self, graph: RuleGraph) -> List[str]:
        """
        Kahn's algorithm for topological sort.
        Simultaneously detects cycles (if a cycle exists, not all nodes
        can be processed → remaining nodes are in the cycle).

        Returns ordered list of rule_ids.
        Raises GraphBuildError if a cycle is detected.
        """
        # In-degree: number of upstream dependencies each rule has
        in_degree: Dict[str, int] = {rid: len(deps) for rid, deps in graph.reverse_edges.items()}

        # Start with rules that have no dependencies (in-degree 0)
        # Break ties by priority (higher priority first), then rule_id (determinism)
        queue: List[str] = [
            rid for rid, deg in in_degree.items() if deg == 0
        ]
        queue.sort(key=lambda rid: (-graph.rules[rid].priority, rid))

        order: List[str] = []

        while queue:
            # Pick highest priority rule from the zero-in-degree set
            current = queue.pop(0)
            order.append(current)

            # For each rule that depends on current, reduce its in-degree
            downstream = sorted(
                graph.edges.get(current, []),
                key=lambda rid: (-graph.rules[rid].priority, rid)
            )
            for downstream_id in downstream:
                in_degree[downstream_id] -= 1
                if in_degree[downstream_id] == 0:
                    queue.append(downstream_id)
                    queue.sort(key=lambda rid: (-graph.rules[rid].priority, rid))

        # If not all rules were processed, there's a cycle
        if len(order) != len(graph.rules):
            remaining = set(graph.rules.keys()) - set(order)
            cycle_details = self._find_cycles(graph, remaining)
            raise GraphBuildError(
                f"Cycle detected in rule graph! Rules involved: {sorted(remaining)}\n"
                f"Cycle path: {cycle_details}"
            )

        logger.info(f"Execution order ({len(order)} rules): {order}")
        return order

    def _find_cycles(self, graph: RuleGraph, suspect_nodes: Set[str]) -> str:
        """
        DFS-based cycle finder to report the actual cycle path for debugging.
        Returns a human-readable cycle description.
        """
        visited: Set[str] = set()
        path: List[str] = []
        path_set: Set[str] = set()

        def dfs(node: str) -> Optional[List[str]]:
            if node in path_set:
                cycle_start = path.index(node)
                return path[cycle_start:] + [node]
            if node in visited:
                return None
            visited.add(node)
            path.append(node)
            path_set.add(node)
            for neighbor in graph.edges.get(node, []):
                if neighbor in suspect_nodes:
                    result = dfs(neighbor)
                    if result:
                        return result
            path.pop()
            path_set.discard(node)
            return None

        for node in suspect_nodes:
            result = dfs(node)
            if result:
                return " → ".join(result)

        return f"Could not trace exact path in: {sorted(suspect_nodes)}"

    def get_dependency_info(self, graph: RuleGraph) -> List[Dict]:
        """
        Returns a list of dicts describing each rule's position in the graph.
        Useful for visualization and debugging.
        """
        info = []
        for rule_id in graph.execution_order:
            rule = graph.rules[rule_id]
            info.append({
                "rule_id": rule_id,
                "name": rule.name,
                "inputs": rule.inputs,
                "outputs": rule.outputs,
                "depends_on": graph.reverse_edges.get(rule_id, []),
                "feeds_into": graph.edges.get(rule_id, []),
                "is_root": rule_id in graph.get_root_rules(),
                "is_leaf": rule_id in graph.get_leaf_rules(),
                "execution_step": graph.execution_order.index(rule_id) + 1,
            })
        return info

    def get_field_lineage(self, graph: RuleGraph, field_name: str) -> Dict:
        """
        Trace where a field comes from and which rules it flows into.
        Useful for impact analysis: "if I change rule X, what downstream rules are affected?"
        """
        producer = None
        consumers = []

        for rule_id, rule in graph.rules.items():
            if field_name in rule.outputs:
                producer = rule_id
            if field_name in rule.inputs:
                consumers.append(rule_id)

        return {
            "field": field_name,
            "produced_by": producer,
            "consumed_by": consumers,
            "is_payload_field": field_name in graph.payload_fields,
        }

    def _log_graph_summary(self, graph: RuleGraph):
        root_rules = graph.get_root_rules()
        leaf_rules = graph.get_leaf_rules()
        total_edges = sum(len(v) for v in graph.edges.values())
        logger.info(
            f"\nGraph Summary:\n"
            f"  Rules        : {len(graph.rules)}\n"
            f"  Edges        : {total_edges}\n"
            f"  Payload fields: {graph.payload_fields}\n"
            f"  Root rules   : {root_rules}\n"
            f"  Leaf rules   : {leaf_rules}\n"
            f"  Exec order   : {graph.execution_order}"
        )


class GraphBuildError(Exception):
    """Raised when the graph cannot be built due to cycles or missing fields."""
    pass