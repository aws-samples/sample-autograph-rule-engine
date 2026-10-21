"""
Wait until Neptune's graph matches the local rule set.

`make publish-rules` used to sleep for a fixed 15s and then declare success,
which reported "Done" over a graph that was duplicated or not yet written.
This polls the graph query Lambda and only succeeds when the rule count in
Neptune equals the number of rule JSON files published — so both a slow
rebuild and a duplicated graph fail loudly instead of passing silently.

Usage:
    .venv/bin/python scripts/wait_for_graph.py

Environment:
    QUERY_FUNCTION      : query Lambda name (default: autograph-graph-query)
    GRAPH_WAIT_TIMEOUT  : seconds to wait before failing (default: 180)
    RULES_SUBDIR        : rules subdirectory to count (default: loan_pricing)
"""
import json
import os
import sys
import time
from pathlib import Path

import boto3

QUERY_FUNCTION = os.environ.get("QUERY_FUNCTION", "autograph-graph-query")
TIMEOUT_SECONDS = int(os.environ.get("GRAPH_WAIT_TIMEOUT", "180"))
POLL_INTERVAL_SECONDS = 5
RULES_DIR = (
    Path(__file__).parent.parent / "rules" / os.environ.get("RULES_SUBDIR", "loan_pricing")
)


def expected_counts() -> tuple:
    """
    (rules, edges) the published graph should contain, built from the local rule files.

    The rule count alone is not a sufficient convergence signal: it reaches its final
    value while the edge upserts are still being applied, so polling on it can report
    success over a half-written graph.
    """
    from autograph_rule_engine.core.graph_builder import GraphBuilder
    from autograph_rule_engine.core.parser import RuleParser

    graph = GraphBuilder().build(RuleParser().load_directory(RULES_DIR))
    edges = sum(len(targets) for targets in graph.edges.values())
    return len(graph.rules), edges


def query_stats(client) -> dict:
    """Invoke the query Lambda and return its graph statistics."""
    response = client.invoke(
        FunctionName=QUERY_FUNCTION,
        Payload=json.dumps({"query": "stats"}).encode("utf-8"),
    )
    envelope = json.load(response["Payload"])
    body = json.loads(envelope["body"])

    if not body.get("success"):
        raise RuntimeError(body.get("error", "query Lambda reported failure"))

    return body["result"]


def main() -> int:
    expected_rules, expected_edges = expected_counts()
    if expected_rules == 0:
        print(f"ERROR: no rule JSON files found in {RULES_DIR}", file=sys.stderr)
        return 1

    client = boto3.client("lambda")
    deadline = time.monotonic() + TIMEOUT_SECONDS
    last_stats = None

    while time.monotonic() < deadline:
        try:
            stats = query_stats(client)
            last_stats = stats
            rules = stats.get("num_rules")
            edges = stats.get("num_edges")

            if rules == expected_rules and edges == expected_edges:
                print(f"Graph synced: {rules} rules, {edges} edges.")
                return 0

            # More rules than the rule set holds means duplicate vertices, not slowness.
            state = "duplicated" if rules and rules > expected_rules else "converging"
            print(
                f"Waiting - Neptune reports {rules} rules / {edges} edges ({state}); "
                f"expected {expected_rules} / {expected_edges}."
            )
        except Exception as error:
            print(f"Waiting - query not ready yet: {error}")

        time.sleep(POLL_INTERVAL_SECONDS)

    print(
        f"ERROR: graph did not converge to {expected_rules} rules / {expected_edges} edges "
        f"within {TIMEOUT_SECONDS}s. Last stats: {last_stats}",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
