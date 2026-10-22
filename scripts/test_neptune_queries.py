"""
Test Neptune Graph Queries via Lambda.

Since Neptune is inside a VPC, this script invokes a Lambda function
(which has VPC access) to run Gremlin queries and return results.

Usage:
    .venv/bin/python scripts/test_neptune_queries.py
"""

import boto3
import json
import os

# Get stack outputs
cfn = boto3.client("cloudformation")
stack = cfn.describe_stacks(StackName="AutoGraphRuleEngineStack")["Stacks"][0]
outputs = {o["OutputKey"]: o["OutputValue"] for o in stack["Outputs"]}

neptune_endpoint = outputs["NeptuneEndpoint"]
generator_arn = outputs["GeneratorLambdaArn"]

print(f"Neptune endpoint: {neptune_endpoint}")
print(f"Generator Lambda: {generator_arn}")
print()

# We'll invoke the generator Lambda with a custom payload that triggers
# Neptune queries. Since the generator handler doesn't have a query mode,
# we'll use a direct Lambda invoke with inline code via a helper Lambda.
# Instead, let's use the executor Lambda's VPC access to run gremlin queries.

# Simpler approach: invoke a raw gremlin query via boto3 neptune-data API
# Neptune HTTP endpoint (non-WSS) for data API
neptune_cluster_endpoint = neptune_endpoint.replace("wss://", "").replace(":8182/gremlin", "")

print("=" * 70)
print("  NEPTUNE GRAPH QUERY EXAMPLES")
print("=" * 70)

# Use neptune-data API (neptune must have IAM auth enabled or we need VPC access)
# Since IAM auth is disabled, we need to go through Lambda.
# Let's create a simple inline invocation.

lambda_client = boto3.client("lambda")


def invoke_neptune_query(query_code: str) -> dict:
    """Invoke the generator Lambda with a query payload."""
    # We'll use a trick: invoke the generator with skip_neptune=true
    # and a special "query" field that our handler doesn't know about.
    # Instead, let's just create a one-off Lambda invocation with custom code.
    
    # Actually the cleanest way: use boto3 to invoke a Lambda that 
    # imports NeptuneGraphBackend and runs queries.
    # The generator Lambda has the code + VPC access + deps layer.
    
    # We'll invoke it with an event that triggers an error in normal flow
    # but we can check logs. Better approach: add a query endpoint.
    pass


# Better approach: Write a small Lambda payload that the executor can handle
# OR just test via the Neptune notebook/console.

# BEST approach for demo: Use the aws neptune-graph or execute-gremlin-query API
# But that requires the Neptune Analytics API, not serverless.

# Let's test via a custom Lambda invocation payload
# Create inline test code that the generator can execute

print("\nApproach: Invoking generator Lambda to run Neptune queries...")
print("(Generator has VPC access + gremlinpython in its layer)\n")

# Create a test event that will be handled specially
# Since we can't modify the handler, let's create a quick Lambda for queries

# Actually - simplest working approach: create a temporary invoke script 
# that we deploy as a one-shot Lambda in the same VPC

# The REAL simplest approach: use the executor Lambda but with a 
# "neptune_query" mode. Let me check if we can just add this.

print("Creating temporary Neptune query Lambda...")

# Create a simple inline Lambda to query Neptune
query_lambda_code = '''
import json
import os
import sys

def handler(event, context):
    """Run Neptune Gremlin queries and return results."""
    # gremlinpython is in the layer
    from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
    from gremlin_python.process.anonymous_traversal import traversal
    from gremlin_python.process.graph_traversal import __
    
    endpoint = os.environ["NEPTUNE_ENDPOINT"]
    namespace = "default"
    
    conn = DriverRemoteConnection(endpoint, "g")
    g = traversal().withRemote(conn)
    
    results = {}
    
    # 1. Graph stats
    num_rules = g.V().has("Rule", "namespace", namespace).count().next()
    num_edges = g.E().hasLabel("FEEDS").has("namespace", namespace).count().next()
    root_rules = g.V().has("Rule", "namespace", namespace).where(__.not_(__.in_("FEEDS"))).values("rule_id").toList()
    leaf_rules = g.V().has("Rule", "namespace", namespace).where(__.not_(__.out("FEEDS"))).values("rule_id").toList()
    
    results["graph_stats"] = {
        "num_rules": num_rules,
        "num_edges": num_edges,
        "root_rules": sorted(root_rules),
        "leaf_rules": sorted(leaf_rules),
    }
    
    # 2. Impact analysis for R01_compute_ltv
    descendants = g.V().has("Rule", "rule_id", "R01").has("namespace", namespace).repeat(__.out("FEEDS")).emit().values("rule_id").dedup().toList()
    ancestors_of_r01 = g.V().has("Rule", "rule_id", "R01").has("namespace", namespace).repeat(__.in_("FEEDS")).emit().values("rule_id").dedup().toList()
    
    results["impact_R01"] = {
        "rule_id": "R01_compute_ltv",
        "depends_on_rules": sorted(ancestors_of_r01),
        "rules_affected_if_changed": sorted(descendants),
        "total_impact_count": len(descendants),
    }
    
    # 3. Ancestors of R15_loan_decision (lineage)
    ancestors_r15 = g.V().has("Rule", "rule_id", "R15").has("namespace", namespace).repeat(__.in_("FEEDS")).emit().values("rule_id").dedup().toList()
    
    results["lineage_R15"] = {
        "rule_id": "R15_loan_decision",
        "all_upstream_dependencies": sorted(ancestors_r15),
        "total_dependencies": len(ancestors_r15),
    }
    
    # 4. Path from R01 to R13
    paths = g.V().has("Rule", "rule_id", "R01").has("namespace", namespace).repeat(__.out("FEEDS").simplePath()).until(__.has("rule_id", "R13")).path().by("rule_id").toList()
    
    results["paths_R01_to_R13"] = [list(p) for p in paths]
    
    # 5. All edges (data flow)
    edges = g.E().hasLabel("FEEDS").has("namespace", namespace).project("from", "to", "via").by(__.outV().values("rule_id")).by(__.inV().values("rule_id")).by("via").toList()
    results["data_flows"] = [{"from": e["from"], "to": e["to"], "via": e["via"]} for e in edges[:20]]
    
    conn.close()
    
    return {
        "statusCode": 200,
        "body": json.dumps(results, indent=2, default=str)
    }
'''

# We can't create a new Lambda easily, but we CAN invoke the generator Lambda
# with a modified handler. Actually let's just use the existing layer + VPC config
# and create a one-shot Lambda via boto3.

import zipfile
import io
import time

# Create deployment package
zip_buffer = io.BytesIO()
with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
    zf.writestr('lambda_function.py', query_lambda_code)
zip_buffer.seek(0)

# Get the generator Lambda config to reuse VPC, layer, role
lambda_client = boto3.client("lambda")
gen_config = lambda_client.get_function_configuration(FunctionName="autograph-graph-generator")

vpc_config = gen_config.get("VpcConfig", {})
layers = [l["Arn"] for l in gen_config.get("Layers", [])]
role = gen_config["Role"]

# Create temporary query Lambda
func_name = "autograph-neptune-query-test"
try:
    lambda_client.delete_function(FunctionName=func_name)
except:
    pass

print(f"  Creating Lambda: {func_name}")
lambda_client.create_function(
    FunctionName=func_name,
    Runtime="python3.13",
    Role=role,
    Handler="lambda_function.handler",
    Code={"ZipFile": zip_buffer.read()},
    Timeout=60,
    MemorySize=512,
    VpcConfig={
        "SubnetIds": vpc_config.get("SubnetIds", []),
        "SecurityGroupIds": vpc_config.get("SecurityGroupIds", []),
    },
    Layers=layers,
    Environment={
        "Variables": {
            "NEPTUNE_ENDPOINT": neptune_endpoint,
        }
    },
)

# Wait for Lambda to be active
print("  Waiting for Lambda to become active...")
for _ in range(30):
    status = lambda_client.get_function_configuration(FunctionName=func_name)["State"]
    if status == "Active":
        break
    time.sleep(2)

print(f"  Lambda status: {status}")
print("  Invoking Neptune queries...")
print()

# Invoke
response = lambda_client.invoke(
    FunctionName=func_name,
    InvocationType="RequestResponse",
    Payload=json.dumps({}),
)

payload = json.loads(response["Payload"].read())

if payload.get("statusCode") == 200:
    results = json.loads(payload["body"])
    
    print("=" * 70)
    print("  1. GRAPH STATS")
    print("=" * 70)
    print(json.dumps(results["graph_stats"], indent=2))
    
    print()
    print("=" * 70)
    print("  2. IMPACT ANALYSIS: R01_compute_ltv")
    print("     'If this rule changes, what else is affected?'")
    print("=" * 70)
    print(json.dumps(results["impact_R01"], indent=2))
    
    print()
    print("=" * 70)
    print("  3. LINEAGE: R15_loan_decision")
    print("     'What does the final decision depend on?'")
    print("=" * 70)
    print(json.dumps(results["lineage_R15"], indent=2))
    
    print()
    print("=" * 70)
    print("  4. PATH ANALYSIS: R01 → R13 (how does LTV reach monthly_payment?)")
    print("=" * 70)
    for i, path in enumerate(results["paths_R01_to_R13"], 1):
        print(f"  Path {i}: {' → '.join(path)}")
    
    print()
    print("=" * 70)
    print("  5. DATA FLOWS (edges with field names)")
    print("=" * 70)
    for flow in results["data_flows"]:
        print(f"  {flow['from']} --[{flow['via']}]--> {flow['to']}")
    
    print()
    print("=" * 70)
    print("  ALL NEPTUNE QUERIES SUCCESSFUL ✓")
    print("=" * 70)
else:
    print("ERROR:")
    print(json.dumps(payload, indent=2, default=str))

# Cleanup
print("\nCleaning up temporary Lambda...")
lambda_client.delete_function(FunctionName=func_name)
print("Done.")
