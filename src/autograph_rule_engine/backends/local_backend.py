"""
Local Backend: wraps NetworkX for graph storage, traversal, and visualization.

Provides:
  - NetworkX DiGraph representation of the RuleGraph
  - Graph visualization (matplotlib / ASCII)
  - Subgraph extraction (impact analysis)
  - Graph export (JSON, DOT format)
  - All operations run in-memory — no external services needed
"""

from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import networkx as nx

from ..core.models import RuleGraph

logger = logging.getLogger(__name__)


class LocalGraphBackend:
    """
    NetworkX-based local graph backend.

    Usage:
        backend = LocalGraphBackend()
        backend.load_from_rule_graph(graph)
        backend.visualize()
        backend.get_ancestors("R15")      # all upstream rules
        backend.get_descendants("R01")    # all downstream rules
        backend.export_json("graph.json")
    """

    def __init__(self):
        self._nx_graph: Optional[nx.DiGraph] = None
        self._rule_graph: Optional[RuleGraph] = None

    # ------------------------------------------------------------------ #
    #  Load                                                                #
    # ------------------------------------------------------------------ #

    def load_from_rule_graph(self, rule_graph: RuleGraph) -> nx.DiGraph:
        """
        Convert a RuleGraph into a NetworkX DiGraph.
        Each node = rule_id with rule metadata as attributes.
        Each edge = data-flow dependency (producer → consumer).
        """
        self._rule_graph = rule_graph
        G = nx.DiGraph()

        # Add nodes with metadata
        for rule_id, rule in rule_graph.rules.items():
            G.add_node(
                rule_id,
                name=rule.name,
                inputs=rule.inputs,
                outputs=rule.outputs,
                action_type=rule.action.type,
                priority=rule.priority,
                tags=rule.tags,
                enabled=rule.enabled,
                is_root=rule_id in rule_graph.get_root_rules(),
                is_leaf=rule_id in rule_graph.get_leaf_rules(),
            )

        # Add edges
        for from_id, to_ids in rule_graph.edges.items():
            for to_id in to_ids:
                # Find the shared field(s) that create this edge
                shared_fields = set(rule_graph.rules[from_id].outputs) & set(
                    rule_graph.rules[to_id].inputs
                )
                G.add_edge(from_id, to_id, via=sorted(shared_fields))

        self._nx_graph = G
        logger.info(
            f"LocalGraphBackend: loaded {G.number_of_nodes()} nodes, "
            f"{G.number_of_edges()} edges"
        )
        return G

    @property
    def graph(self) -> nx.DiGraph:
        if self._nx_graph is None:
            raise RuntimeError("No graph loaded. Call load_from_rule_graph() first.")
        return self._nx_graph

    # ------------------------------------------------------------------ #
    #  Traversal & Analysis                                                #
    # ------------------------------------------------------------------ #

    def get_ancestors(self, rule_id: str) -> Set[str]:
        """All rules that must execute BEFORE rule_id (transitive upstream)."""
        return nx.ancestors(self.graph, rule_id)

    def get_descendants(self, rule_id: str) -> Set[str]:
        """All rules that depend on rule_id (transitive downstream)."""
        return nx.descendants(self.graph, rule_id)

    def get_execution_levels(self) -> List[List[str]]:
        """
        Group rules into parallel execution levels.
        Rules within the same level have no inter-dependencies and could
        theoretically execute in parallel.
        """
        return list(nx.topological_generations(self.graph))

    def get_critical_path(self) -> List[str]:
        """
        Return the longest path through the DAG (most dependencies).
        This is the path that determines the minimum sequential depth.
        """
        return nx.dag_longest_path(self.graph)

    def get_impact(self, rule_id: str) -> Dict[str, Any]:
        """
        Impact analysis: if rule_id changes, what else is affected?
        Returns ancestors (what it depends on) and descendants (what depends on it).
        """
        ancestors = self.get_ancestors(rule_id)
        descendants = self.get_descendants(rule_id)
        return {
            "rule_id": rule_id,
            "depends_on_rules": sorted(ancestors),
            "rules_affected_if_changed": sorted(descendants),
            "total_impact_count": len(descendants),
        }

    def get_subgraph(self, rule_ids: List[str]) -> nx.DiGraph:
        """Extract a subgraph containing only the given rule_ids and edges between them."""
        return self.graph.subgraph(rule_ids).copy()

    def find_all_paths(self, source: str, target: str) -> List[List[str]]:
        """All paths from source rule to target rule."""
        return list(nx.all_simple_paths(self.graph, source, target))

    def has_cycle(self) -> bool:
        """Check if the graph has any cycles (should always be False for a valid RuleGraph)."""
        return not nx.is_directed_acyclic_graph(self.graph)

    def get_graph_stats(self) -> Dict[str, Any]:
        """Comprehensive graph statistics."""
        G = self.graph
        return {
            "num_nodes": G.number_of_nodes(),
            "num_edges": G.number_of_edges(),
            "is_dag": nx.is_directed_acyclic_graph(G),
            "is_connected": nx.is_weakly_connected(G),
            "density": nx.density(G),
            "longest_path": nx.dag_longest_path(G),
            "longest_path_length": nx.dag_longest_path_length(G),
            "root_nodes": [n for n, d in G.in_degree() if d == 0],
            "leaf_nodes": [n for n, d in G.out_degree() if d == 0],
            "execution_levels": [list(level) for level in nx.topological_generations(G)],
        }

    # ------------------------------------------------------------------ #
    #  Visualization                                                       #
    # ------------------------------------------------------------------ #

    def print_ascii_graph(self):
        """Print a simple ASCII representation of the graph in execution order."""
        G = self.graph
        rule_graph = self._rule_graph

        print("\n" + "=" * 70)
        print("  RULE DEPENDENCY GRAPH")
        print("=" * 70)

        if rule_graph:
            levels = list(nx.topological_generations(G))
            for level_idx, level in enumerate(levels):
                print(f"\n  Level {level_idx + 1}:")
                for rule_id in sorted(level):
                    rule = rule_graph.rules.get(rule_id)
                    name = rule.name if rule else rule_id
                    inputs = rule.inputs if rule else []
                    outputs = rule.outputs if rule else []
                    upstream = list(G.predecessors(rule_id))
                    downstream = list(G.successors(rule_id))

                    print(f"    [{rule_id}] {name}")
                    print(f"      inputs  : {inputs}")
                    print(f"      outputs : {outputs}")
                    if upstream:
                        print(f"      ← from  : {upstream}")
                    if downstream:
                        print(f"      → feeds : {downstream}")
        print("=" * 70 + "\n")

    def visualize(
        self,
        output_path: Optional[str] = None,
        figsize: tuple = (16, 10),
        show: bool = True,
    ):
        """
        Render the graph using matplotlib.
        Nodes are color-coded by type (root=green, leaf=red, middle=blue).
        Edge labels show the field name that creates the dependency.
        """
        try:
            import matplotlib.pyplot as plt
            import matplotlib.patches as mpatches
        except ImportError:
            logger.warning("matplotlib not installed. Run: pip install matplotlib")
            self.print_ascii_graph()
            return

        G = self.graph
        rule_graph = self._rule_graph

        fig, ax = plt.subplots(figsize=figsize)

        # Layout: hierarchical using layers from topological generations
        try:
            pos = nx.multipartite_layout(
                G,
                subset_key=lambda n: next(
                    i for i, level in enumerate(nx.topological_generations(G)) if n in level
                ),
                align="vertical",
            )
        except Exception:
            pos = nx.spring_layout(G, seed=42)

        # Color nodes
        node_colors = []
        for node in G.nodes():
            if G.in_degree(node) == 0:
                node_colors.append("#2ecc71")   # green = root
            elif G.out_degree(node) == 0:
                node_colors.append("#e74c3c")   # red = leaf
            else:
                node_colors.append("#3498db")   # blue = middle

        # Node labels: rule_id + name
        labels = {}
        for node in G.nodes():
            rule = rule_graph.rules.get(node) if rule_graph else None
            labels[node] = f"{node}\n{rule.name}" if rule else node

        nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=2000, ax=ax, alpha=0.9)
        nx.draw_networkx_labels(G, pos, labels=labels, font_size=7, ax=ax)
        nx.draw_networkx_edges(G, pos, edge_color="#555555", arrows=True,
                               arrowsize=20, ax=ax, connectionstyle="arc3,rad=0.1")

        # Edge labels: field names
        edge_labels = {(u, v): ",".join(d.get("via", [])) for u, v, d in G.edges(data=True)}
        nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=6, ax=ax)

        # Legend
        legend_handles = [
            mpatches.Patch(color="#2ecc71", label="Root (payload-fed)"),
            mpatches.Patch(color="#3498db", label="Intermediate"),
            mpatches.Patch(color="#e74c3c", label="Leaf (terminal output)"),
        ]
        ax.legend(handles=legend_handles, loc="upper left")
        ax.set_title("Rule Dependency Graph", fontsize=14, fontweight="bold")
        ax.axis("off")
        plt.tight_layout()

        if output_path:
            plt.savefig(output_path, dpi=150, bbox_inches="tight")
            logger.info(f"Graph saved to {output_path}")

        if show:
            plt.show()

        return fig

    # ------------------------------------------------------------------ #
    #  Export                                                              #
    # ------------------------------------------------------------------ #

    def export_json(self, path: Optional[str] = None) -> Dict:
        """Export the graph as a JSON-serializable dict (node-link format)."""
        G = self.graph
        data = nx.node_link_data(G)
        if path:
            with open(path, "w") as f:
                json.dump(data, f, indent=2, default=str)
            logger.info(f"Graph exported to {path}")
        return data

    def export_dot(self, path: str):
        """Export the graph in DOT format (for Graphviz rendering)."""
        try:
            from networkx.drawing.nx_pydot import write_dot
            write_dot(self.graph, path)
            logger.info(f"Graph DOT file saved to {path}")
        except ImportError:
            logger.warning("pydot not installed. Run: pip install pydot")
            # Fallback: manual DOT generation
            lines = ["digraph RuleEngine {", '  rankdir=LR;', '  node [shape=box];']
            for node in self.graph.nodes():
                lines.append(f'  "{node}";')
            for u, v, d in self.graph.edges(data=True):
                via = ",".join(d.get("via", []))
                lines.append(f'  "{u}" -> "{v}" [label="{via}"];')
            lines.append("}")
            with open(path, "w") as f:
                f.write("\n".join(lines))
            logger.info(f"Graph DOT file (manual) saved to {path}")