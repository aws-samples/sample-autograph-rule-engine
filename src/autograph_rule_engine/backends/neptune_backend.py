"""
Neptune Backend: stores and queries the rule graph in Amazon Neptune
using the Gremlin graph traversal language.

Architecture:
  - Each Rule   → Vertex  with label "Rule" and all rule metadata as properties
  - Each Edge   → Edge    with label "FEEDS" and "via" property (shared field name)
  - Payload fields → Vertices with label "PayloadField"

Prerequisites:
  pip install gremlinpython

Neptune endpoint format:
  wss://<neptune-endpoint>:8182/gremlin

Usage:
    backend = NeptuneGraphBackend(endpoint="wss://my-neptune.cluster.region.neptune.amazonaws.com:8182/gremlin")
    backend.connect()
    backend.upsert_rule_graph(graph)
    backend.get_execution_order()
    backend.get_ancestors("R15")
    backend.close()
"""

from __future__ import annotations
import json
import logging
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from ..core.models import RuleGraph

logger = logging.getLogger(__name__)

# Neptune asks the caller to retry these — they signal a transient write
# conflict between concurrent writers, not a malformed query.
RETRYABLE_NEPTUNE_ERRORS = ("ConcurrentModificationException",)
MAX_UPSERT_ATTEMPTS = 5
INITIAL_BACKOFF_SECONDS = 0.2


class NeptuneWriteConflictError(RuntimeError):
    """Raised when Neptune writes keep conflicting after every retry."""


def is_retryable_neptune_error(error: Exception) -> bool:
    """True when Neptune signalled a transient conflict worth retrying."""
    message = str(error)
    return any(marker in message for marker in RETRYABLE_NEPTUNE_ERRORS)


def rule_properties(rule) -> Dict[str, Any]:
    """Neptune property map for a rule, excluding its identity properties."""
    return {
        "name": rule.name,
        "description": rule.description or "",
        "inputs": json.dumps(rule.inputs),
        "outputs": json.dumps(rule.outputs),
        "action_type": str(rule.action.type),
        "priority": rule.priority,
        "tags": json.dumps(rule.tags),
        "enabled": rule.enabled,
    }


def expected_edges(rule_graph: RuleGraph) -> List[Tuple[str, str, List[str]]]:
    """(from_id, to_id, via_fields) for every edge implied by the rule set."""
    edges: List[Tuple[str, str, List[str]]] = []
    for from_id, to_ids in rule_graph.edges.items():
        for to_id in to_ids:
            shared_fields = set(rule_graph.rules[from_id].outputs) & set(
                rule_graph.rules[to_id].inputs
            )
            edges.append((from_id, to_id, sorted(shared_fields)))
    return edges


class NeptuneGraphBackend:
    """
    Amazon Neptune (Gremlin) backend for rule graph storage and traversal.

    Stores the full rule graph persistently in Neptune.
    Queries use Gremlin traversal to compute ancestors, descendants,
    topological ordering, and impact analysis — directly in the graph DB.
    """

    def __init__(
        self,
        endpoint: str,
        traversal_source: str = "g",
        pool_size: int = 4,
        max_workers: int = 4,
        region: str = "us-east-1",
    ):
        """
        Args:
            endpoint:         Neptune Gremlin WebSocket endpoint.
                              e.g. "wss://my-cluster.cluster-xxxx.us-east-1.neptune.amazonaws.com:8182/gremlin"
            traversal_source: Gremlin traversal source name (default: "g")
            pool_size:        Connection pool size
            max_workers:      Max concurrent workers
            region:           AWS region (used for IAM auth if enabled)
        """
        self.endpoint = endpoint
        self.traversal_source = traversal_source
        self.pool_size = pool_size
        self.max_workers = max_workers
        self.region = region

        self._connection = None
        self._g = None  # Gremlin traversal source

    # ------------------------------------------------------------------ #
    #  Connection                                                          #
    # ------------------------------------------------------------------ #

    def connect(self):
        """Establish connection to Neptune."""
        try:
            from gremlin_python.driver import client, serializer
            from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
            from gremlin_python.process.anonymous_traversal import traversal
            from gremlin_python.process.graph_traversal import __

            self._connection = DriverRemoteConnection(
                self.endpoint,
                self.traversal_source,
                pool_size=self.pool_size,
                max_workers=self.max_workers,
            )
            self._g = traversal().withRemote(self._connection)
            logger.info(f"Connected to Neptune: {self.endpoint}")
        except ImportError:
            raise RuntimeError(
                "gremlinpython not installed. Run: pip install gremlinpython"
            )
        except Exception as e:
            raise RuntimeError(f"Failed to connect to Neptune at {self.endpoint}: {e}")

    def close(self):
        """Close the Neptune connection."""
        if self._connection:
            self._connection.close()
            logger.info("Neptune connection closed.")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, *args):
        self.close()

    @property
    def g(self):
        if self._g is None:
            raise RuntimeError("Not connected to Neptune. Call connect() first.")
        return self._g

    # ------------------------------------------------------------------ #
    #  Write: Upsert rule graph into Neptune                              #
    # ------------------------------------------------------------------ #

    def upsert_rule_graph(self, rule_graph: RuleGraph, namespace: str = "default"):
        """
        Converge Neptune onto the state described by rule_graph.

        Every write is match-or-create, so repeating this call — or running it
        concurrently from several Lambda invocations — leaves exactly one copy
        of the graph instead of appending duplicates. Vertices and edges that
        the rule set no longer implies are pruned afterwards, so deleting a
        rule JSON still removes it from Neptune.

        Args:
            rule_graph: The built RuleGraph.
            namespace:  Logical namespace to allow multiple rule sets in one cluster.
        """
        logger.info(
            f"Upserting {len(rule_graph.rules)} rules into Neptune (namespace={namespace})"
        )
        self._with_retry(lambda: self._converge_graph(rule_graph, namespace))
        logger.info("Neptune upsert complete.")

    def _converge_graph(self, rule_graph: RuleGraph, namespace: str):
        """Write the desired state, then remove whatever is left over."""
        for rule_id, rule in rule_graph.rules.items():
            self._upsert_rule_vertex(rule_id, rule, namespace)

        for field in rule_graph.payload_fields:
            self._upsert_payload_vertex(field, namespace)

        for from_id, to_id, via_fields in expected_edges(rule_graph):
            self._upsert_edge(from_id, to_id, via_fields, namespace)

        self._prune_stale_edges(rule_graph, namespace)
        self._prune_stale_vertices(rule_graph, namespace)

    def _with_retry(self, operation):
        """
        Run operation, retrying transient Neptune write conflicts with backoff.

        Neptune raises ConcurrentModificationException when writers collide and
        explicitly asks the caller to retry; giving up silently is what let a
        duplicated graph reach production.
        """
        last_error: Optional[Exception] = None

        for attempt in range(1, MAX_UPSERT_ATTEMPTS + 1):
            try:
                return operation()
            except Exception as error:
                if not is_retryable_neptune_error(error):
                    raise
                last_error = error
                if attempt == MAX_UPSERT_ATTEMPTS:
                    break
                backoff = INITIAL_BACKOFF_SECONDS * (2 ** (attempt - 1))
                logger.warning(
                    f"Neptune write conflict (attempt {attempt}/{MAX_UPSERT_ATTEMPTS}); "
                    f"retrying in {backoff:.2f}s: {error}"
                )
                time.sleep(backoff)

        raise NeptuneWriteConflictError(
            f"Neptune write failed after {MAX_UPSERT_ATTEMPTS} attempts: {last_error}"
        ) from last_error

    def _drop_namespace(self, namespace: str):
        """
        Remove every vertex and edge for this namespace.

        Destructive full wipe, deliberately NOT part of upsert_rule_graph —
        concurrent drop-then-insert is what duplicated the graph. Kept for
        explicit administrative resets only.
        """
        self.g.V().has("namespace", namespace).drop().toList()
        logger.debug(f"Dropped namespace '{namespace}' from Neptune.")

    def _upsert_rule_vertex(self, rule_id: str, rule, namespace: str):
        """Match-or-create a Rule vertex, then refresh its properties."""
        from gremlin_python.process.graph_traversal import __

        traversal = (
            self.g.V()
            .has("Rule", "rule_id", rule_id)
            .has("namespace", namespace)
            .fold()
            .coalesce(
                __.unfold(),
                __.addV("Rule")
                .property("rule_id", rule_id)
                .property("namespace", namespace),
            )
        )
        for key, value in rule_properties(rule).items():
            traversal = traversal.property(key, value)
        traversal.next()

    def _upsert_payload_vertex(self, field_name: str, namespace: str):
        """Match-or-create a PayloadField vertex."""
        from gremlin_python.process.graph_traversal import __

        (
            self.g.V()
            .has("PayloadField", "field_name", field_name)
            .has("namespace", namespace)
            .fold()
            .coalesce(
                __.unfold(),
                __.addV("PayloadField")
                .property("field_name", field_name)
                .property("namespace", namespace),
            )
            .next()
        )

    def _upsert_edge(self, from_id: str, to_id: str, via_fields: List[str], namespace: str):
        """Match-or-create a FEEDS edge between two Rule vertices."""
        from gremlin_python.process.graph_traversal import __

        (
            self.g.V()
            .has("Rule", "rule_id", from_id)
            .has("namespace", namespace)
            .outE("FEEDS")
            .where(__.inV().has("Rule", "rule_id", to_id).has("namespace", namespace))
            .fold()
            .coalesce(
                __.unfold(),
                __.V()
                .has("Rule", "rule_id", from_id)
                .has("namespace", namespace)
                .addE("FEEDS")
                .to(
                    __.V()
                    .has("Rule", "rule_id", to_id)
                    .has("namespace", namespace)
                ),
            )
            .property("via", json.dumps(via_fields))
            .next()
        )

    def _prune_stale_edges(self, rule_graph: RuleGraph, namespace: str):
        """Drop FEEDS edges the current rule set no longer implies."""
        wanted = {(from_id, to_id) for from_id, to_id, _ in expected_edges(rule_graph)}

        for edge_id, pair in self._existing_edges(namespace).items():
            if pair in wanted:
                continue
            logger.info(f"Pruning stale edge {pair[0]} -> {pair[1]}")
            # toList() rather than iterate(): this gremlinpython version
            # compiles iterate() to a `discard` step that Neptune rejects
            # with InvalidParameterException.
            self.g.E(edge_id).drop().toList()

    def _existing_edges(self, namespace: str) -> Dict[Any, Tuple[str, str]]:
        """Map edge id -> (from_rule_id, to_rule_id) for FEEDS edges in a namespace."""
        from gremlin_python.process.graph_traversal import __

        rows = (
            self.g.V()
            .has("Rule", "namespace", namespace)
            .outE("FEEDS")
            .project("id", "from", "to")
            .by(__.id_())
            .by(__.outV().values("rule_id"))
            .by(__.inV().values("rule_id"))
            .toList()
        )
        return {row["id"]: (row["from"], row["to"]) for row in rows}

    def _prune_stale_vertices(self, rule_graph: RuleGraph, namespace: str):
        """Drop Rule/PayloadField vertices the current rule set no longer contains."""
        self._drop_vertices_not_in(
            "Rule", "rule_id", sorted(rule_graph.rules), namespace
        )
        self._drop_vertices_not_in(
            "PayloadField", "field_name", sorted(rule_graph.payload_fields), namespace
        )

    def _drop_vertices_not_in(
        self, label: str, key: str, keep: List[str], namespace: str
    ):
        """Drop vertices of a label in this namespace whose key is not in keep."""
        from gremlin_python.process.graph_traversal import __
        from gremlin_python.process.traversal import P

        traversal = self.g.V().has(label, "namespace", namespace)
        if keep:
            traversal = traversal.not_(__.has(key, P.within(*keep)))
        # See _prune_stale_edges: iterate() compiles to an unsupported
        # `discard` step on Neptune.
        traversal.drop().toList()

    # ------------------------------------------------------------------ #
    #  Read: Query graph from Neptune                                      #
    # ------------------------------------------------------------------ #

    def get_all_rules(self, namespace: str = "default") -> List[Dict]:
        """Retrieve all rule vertices for a namespace."""
        results = (
            self.g.V()
            .has("Rule", "namespace", namespace)
            .valueMap(True)
            .toList()
        )
        return [self._vertex_to_dict(v) for v in results]

    def get_rule(self, rule_id: str, namespace: str = "default") -> Optional[Dict]:
        """Retrieve a single rule vertex."""
        results = (
            self.g.V()
            .has("Rule", "rule_id", rule_id)
            .has("namespace", namespace)
            .valueMap(True)
            .toList()
        )
        return self._vertex_to_dict(results[0]) if results else None

    def get_ancestors(self, rule_id: str, namespace: str = "default") -> List[str]:
        """
        All upstream rules (transitive) that rule_id depends on.
        Uses Gremlin repeat() traversal to walk edges backwards.
        """
        results = (
            self.g.V()
            .has("Rule", "rule_id", rule_id)
            .has("namespace", namespace)
            .repeat(__.in_("FEEDS"))
            .emit()
            .values("rule_id")
            .dedup()
            .toList()
        )
        return results

    def get_descendants(self, rule_id: str, namespace: str = "default") -> List[str]:
        """
        All downstream rules (transitive) that depend on rule_id.
        Uses Gremlin repeat() traversal to walk edges forwards.
        """
        results = (
            self.g.V()
            .has("Rule", "rule_id", rule_id)
            .has("namespace", namespace)
            .repeat(__.out("FEEDS"))
            .emit()
            .values("rule_id")
            .dedup()
            .toList()
        )
        return results

    def get_root_rules(self, namespace: str = "default") -> List[str]:
        """Rules with no incoming FEEDS edges (payload-fed root rules)."""
        results = (
            self.g.V()
            .has("Rule", "namespace", namespace)
            .where(__.not_(__.in_("FEEDS")))
            .values("rule_id")
            .toList()
        )
        return results

    def get_leaf_rules(self, namespace: str = "default") -> List[str]:
        """Rules with no outgoing FEEDS edges (terminal output rules)."""
        results = (
            self.g.V()
            .has("Rule", "namespace", namespace)
            .where(__.not_(__.out("FEEDS")))
            .values("rule_id")
            .toList()
        )
        return results

    def get_execution_order(self, namespace: str = "default") -> List[str]:
        """
        Compute topological execution order via Gremlin.
        Uses iterative BFS from root nodes, processing each level in turn.
        Returns ordered list of rule_ids.
        """
        # Get all rules and edges from Neptune, then compute order locally
        # (Full graph-DB-native topo sort in Gremlin is complex; hybrid approach is practical)
        edges_raw = (
            self.g.E()
            .has("namespace", namespace)  # edges inherit namespace via adjacent vertices
            .project("from", "to")
            .by(__.outV().values("rule_id"))
            .by(__.inV().values("rule_id"))
            .toList()
        )

        all_rule_ids = (
            self.g.V()
            .has("Rule", "namespace", namespace)
            .values("rule_id")
            .toList()
        )

        # Build local in-degree map and run Kahn's
        in_degree = {rid: 0 for rid in all_rule_ids}
        adjacency: Dict[str, List[str]] = {rid: [] for rid in all_rule_ids}

        for edge in edges_raw:
            from_id = edge["from"]
            to_id = edge["to"]
            adjacency[from_id].append(to_id)
            in_degree[to_id] += 1

        queue = [rid for rid, deg in in_degree.items() if deg == 0]
        order = []
        while queue:
            node = queue.pop(0)
            order.append(node)
            for neighbor in adjacency.get(node, []):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        return order

    def get_impact(self, rule_id: str, namespace: str = "default") -> Dict[str, Any]:
        """Impact analysis using Neptune graph traversal."""
        ancestors = self.get_ancestors(rule_id, namespace)
        descendants = self.get_descendants(rule_id, namespace)
        return {
            "rule_id": rule_id,
            "depends_on_rules": sorted(ancestors),
            "rules_affected_if_changed": sorted(descendants),
            "total_impact_count": len(descendants),
        }

    def find_paths(
        self, source_rule_id: str, target_rule_id: str, namespace: str = "default"
    ) -> List[List[str]]:
        """Find all paths from source to target using Gremlin path traversal."""
        results = (
            self.g.V()
            .has("Rule", "rule_id", source_rule_id)
            .has("namespace", namespace)
            .repeat(__.out("FEEDS").simplePath())
            .until(__.has("rule_id", target_rule_id))
            .path()
            .by("rule_id")
            .toList()
        )
        return [list(p) for p in results]

    def get_graph_stats(self, namespace: str = "default") -> Dict[str, Any]:
        """Basic graph statistics from Neptune."""
        num_rules = (
            self.g.V().has("Rule", "namespace", namespace).count().next()
        )
        num_edges = (
            self.g.E().count().next()  # approximate: namespace filter on edges is complex
        )
        root_rules = self.get_root_rules(namespace)
        leaf_rules = self.get_leaf_rules(namespace)

        return {
            "num_rules": num_rules,
            "num_edges": num_edges,
            "root_rules": root_rules,
            "leaf_rules": leaf_rules,
            "namespace": namespace,
        }

    # ------------------------------------------------------------------ #
    #  Utilities                                                           #
    # ------------------------------------------------------------------ #

    def _vertex_to_dict(self, vertex_map: dict) -> Dict:
        """Convert Neptune valueMap response to a clean Python dict."""
        result = {}
        for k, v in vertex_map.items():
            if k == "T.id" or str(k) == "T.id":
                result["_id"] = str(v)
            elif k == "T.label" or str(k) == "T.label":
                result["_label"] = str(v)
            elif isinstance(v, list) and len(v) == 1:
                val = v[0]
                # Try to deserialize JSON strings back to lists
                if isinstance(val, str) and val.startswith("["):
                    try:
                        val = json.loads(val)
                    except Exception:
                        pass
                result[str(k)] = val
            else:
                result[str(k)] = v
        return result

    def execute_gremlin(self, query: str) -> Any:
        """
        Execute a raw Gremlin query string.
        Useful for debugging and ad-hoc exploration.
        """
        from gremlin_python.driver import client, serializer

        gremlin_client = client.Client(
            self.endpoint,
            self.traversal_source,
            message_serializer=serializer.GraphSONSerializersV2d0(),
        )
        result = gremlin_client.submit(query).all().result()
        gremlin_client.close()
        return result


# ------------------------------------------------------------------ #
#  Convenience: import Gremlin __ for use in traversal queries        #
# ------------------------------------------------------------------ #
try:
    from gremlin_python.process.graph_traversal import __
except ImportError:
    pass  # graceful degradation when gremlinpython not installed