"""Build a JSON payload for the graph query Lambda."""
import json
import os
import sys

q = os.environ.get("Q", "")
if not q:
    sys.exit("Error: Q is required. Example: make query Q=stats")

payload = {"query": q}

rule = os.environ.get("RULE", "")
src = os.environ.get("FROM", "")
tgt = os.environ.get("TO", "")
ns = os.environ.get("NS", "")

if rule:
    payload["rule_id"] = rule
if src:
    payload["source_rule_id"] = src
if tgt:
    payload["target_rule_id"] = tgt
if ns:
    payload["namespace"] = ns

print(json.dumps(payload))
