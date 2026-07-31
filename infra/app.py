#!/usr/bin/env python3
"""
CDK App entry point for the AutoGraph Rule Engine.

Deploy:
    cdk deploy --app ".venv/bin/python infra/app.py"

Synth (preview CloudFormation + security checks):
    cdk synth --app ".venv/bin/python infra/app.py"
"""

import aws_cdk as cdk
from cdk_nag import AwsSolutionsChecks
from stack import AutoGraphRuleEngineStack

app = cdk.App()

AutoGraphRuleEngineStack(
    app,
    "AutoGraphRuleEngineStack",
    description="AutoGraph Rule Engine — Neptune + S3 + Lambda infrastructure",
    # Uncomment to target a specific account/region:
    # env=cdk.Environment(account="123456789012", region="us-east-1"),
)

# Run AWS Solutions security checks on every synth
cdk.Aspects.of(app).add(AwsSolutionsChecks())

app.synth()
