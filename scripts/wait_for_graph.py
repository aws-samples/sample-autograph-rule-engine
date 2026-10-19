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


def expected_rule_count() -> int:
    """Number of rule JSON files that should be present in the graph."""
    return len(list(RULES_DIR.glob("*.json")))


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
    expected = expected_rule_count()
    if expected == 0:
        print(f"ERROR: no rule JSON files found in {RULES_DIR}", file=sys.stderr)
        return 1

    client = boto3.client("lambda")
    deadline = time.monotonic() + TIMEOUT_SECONDS
    last_stats = None

    while time.monotonic() < deadline:
        try:
            stats = query_stats(client)
            last_stats = stats
            actual = stats.get("num_rules")

            if actual == expected:
                print(f"Graph synced: {actual} rules, {stats.get('num_edges')} edges.")
                return 0

            # More rules than files means duplicate vertices, not slowness.
            state = "duplicated" if actual and actual > expected else "incomplete"
            print(f"Waiting — Neptune reports {actual} rules ({state}), expected {expected}.")
        except Exception as error:
            print(f"Waiting — query not ready yet: {error}")

        time.sleep(POLL_INTERVAL_SECONDS)

    print(
        f"ERROR: graph did not converge to {expected} rules within {TIMEOUT_SECONDS}s. "
        f"Last stats: {last_stats}",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    sys.exit(main())
