#!/usr/bin/env python3
"""
CDK App entry point for the AutoGraph Rule Engine.

Deploy:
    cdk deploy --app ".venv/bin/python infra/app.py"

Synth (preview CloudFormation + security checks):
    cdk synth --app ".venv/bin/python infra/app.py"
"""

import os

import aws_cdk as cdk
from cdk_nag import AwsSolutionsChecks
from stack import AutoGraphRuleEngineStack

DEFAULT_REGION = "us-east-1"

app = cdk.App()

# The region is pinned rather than inherited from the environment. An env-agnostic stack
# follows whatever AWS_REGION happens to be set, which silently sends deploy, diff and
# destroy to different regions than intended: a `cdk diff` run with AWS_REGION=us-west-2
# compares against a non-existent stack and reports every resource as new, while the real
# stack keeps running elsewhere. Override deliberately with `-c region=<region>`.
region = app.node.try_get_context("region") or os.environ.get("AUTOGRAPH_REGION") or DEFAULT_REGION

AutoGraphRuleEngineStack(
    app,
    "AutoGraphRuleEngineStack",
    description="AutoGraph Rule Engine - Neptune + S3 + Lambda infrastructure",
    # Account comes from the active credentials; the region is explicit.
    env=cdk.Environment(account=os.environ.get("CDK_DEFAULT_ACCOUNT"), region=region),
)

# Run AWS Solutions security checks on every synth
cdk.Aspects.of(app).add(AwsSolutionsChecks())

app.synth()
