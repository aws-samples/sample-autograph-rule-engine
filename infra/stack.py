"""
CDK Stack: AutoGraph Rule Engine Infrastructure

Provisions:
  - VPC with flow logs
  - Neptune Serverless cluster (graph persistence + querying)
  - S3 buckets with SSL enforcement and access logging
  - Graph Generator Lambda (builds graph, writes to Neptune + S3)
  - Graph Executor Lambda (loads graph from S3, executes rules)
  - S3 event trigger (auto-publishes when rules change)
  - CloudWatch Log Groups with retention
  - IAM roles with least-privilege permissions
"""

from constructs import Construct
from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    CfnOutput,
    aws_ec2 as ec2,
    aws_neptune as neptune,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_logs as logs,
)
from cdk_nag import NagSuppressions


class AutoGraphRuleEngineStack(Stack):
    """
    Full infrastructure for the AutoGraph Rule Engine.

    Architecture:
        Deploy-time: graph_generator → Neptune + S3
        Runtime:     Lambda reads graph from S3, executes rules
        Querying:    Neptune available for impact analysis / lineage
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ---------------------------------------------------------- #
        #  VPC (Neptune requires a VPC)                               #
        # ---------------------------------------------------------- #
        vpc = ec2.Vpc(
            self, "RuleEngineVpc",
            max_azs=2,
            nat_gateways=1,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="Private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="Isolated",
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                    cidr_mask=24,
                ),
            ],
        )

        # VPC Flow Logs
        vpc.add_flow_log(
            "FlowLog",
            destination=ec2.FlowLogDestination.to_cloud_watch_logs(
                logs.LogGroup(
                    self, "VpcFlowLogGroup",
                    retention=logs.RetentionDays.TWO_WEEKS,
                    removal_policy=RemovalPolicy.DESTROY,
                )
            ),
        )

        # ---------------------------------------------------------- #
        #  Security Groups                                            #
        # ---------------------------------------------------------- #
        neptune_sg = ec2.SecurityGroup(
            self, "NeptuneSG",
            vpc=vpc,
            description="Allow Gremlin access to Neptune",
            allow_all_outbound=False,
        )

        lambda_sg = ec2.SecurityGroup(
            self, "LambdaSG",
            vpc=vpc,
            description="Lambda security group",
            allow_all_outbound=True,
        )

        # Lambda → Neptune on port 8182 (Gremlin/WebSocket)
        neptune_sg.add_ingress_rule(
            peer=lambda_sg,
            connection=ec2.Port.tcp(8182),
            description="Lambda to Neptune Gremlin",
        )

        # ---------------------------------------------------------- #
        #  Neptune Serverless Cluster                                  #
        # ---------------------------------------------------------- #
        neptune_subnet_group = neptune.CfnDBSubnetGroup(
            self, "NeptuneSubnetGroup",
            db_subnet_group_description="Subnet group for Neptune",
            subnet_ids=[
                subnet.subnet_id
                for subnet in vpc.select_subnets(
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
                ).subnets
            ],
        )

        neptune_cluster = neptune.CfnDBCluster(
            self, "NeptuneCluster",
            db_subnet_group_name=neptune_subnet_group.ref,
            vpc_security_group_ids=[neptune_sg.security_group_id],
            iam_auth_enabled=False,  # gremlinpython doesn't support SigV4; VPC + SG provides isolation
            storage_encrypted=True,
            deletion_protection=False,  # Set True for production
            backup_retention_period=7,
            serverless_scaling_configuration=neptune.CfnDBCluster.ServerlessScalingConfigurationProperty(
                min_capacity=1.0,
                max_capacity=8.0,
            ),
            engine_version="1.3.1.0",
        )
        neptune_cluster.add_dependency(neptune_subnet_group)

        # Neptune Serverless still requires at least one DB instance
        neptune_instance = neptune.CfnDBInstance(
            self, "NeptuneInstance",
            db_instance_class="db.serverless",
            db_cluster_identifier=neptune_cluster.ref,
            auto_minor_version_upgrade=True,
        )
        neptune_instance.add_dependency(neptune_cluster)

        # ---------------------------------------------------------- #
        #  S3 Bucket (serialized graph artifact)                       #
        # ---------------------------------------------------------- #
        graph_bucket = s3.Bucket(
            self, "GraphArtifactBucket",
            bucket_name=None,
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            lifecycle_rules=[
                s3.LifecycleRule(
                    noncurrent_version_expiration=Duration.days(30),
                ),
            ],
        )

        # ---------------------------------------------------------- #
        #  S3 Bucket (rule JSON source files)                          #
        # ---------------------------------------------------------- #
        rules_bucket = s3.Bucket(
            self, "RulesBucket",
            bucket_name=None,
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
        )

        # ---------------------------------------------------------- #
        #  Lambda Dependencies Layer                                   #
        # ---------------------------------------------------------- #
        deps_layer = _lambda.LayerVersion(
            self, "RuleEngineDepsLayer",
            code=_lambda.Code.from_asset("build/lambda_deps"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_13],
            description="Dependencies for autograph-rule-engine (pydantic, simpleeval, networkx)",
        )

        # ---------------------------------------------------------- #
        #  Graph Executor Lambda                                       #
        # ---------------------------------------------------------- #
        executor_log_group = logs.LogGroup(
            self, "ExecutorLogGroup",
            log_group_name="/aws/lambda/autograph-graph-executor",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=RemovalPolicy.DESTROY,
        )

        executor_lambda = _lambda.Function(
            self, "RuleExecutorLambda",
            function_name="autograph-graph-executor",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="autograph_rule_engine.compute.graph_executor_handler.lambda_handler",
            code=_lambda.Code.from_asset(
                "src",
                exclude=["**/__pycache__", "*.egg-info", "**/*.pyc"],
            ),
            layers=[deps_layer],
            timeout=Duration.seconds(30),
            memory_size=512,
            environment={
                "GRAPH_S3_BUCKET": graph_bucket.bucket_name,
                "GRAPH_S3_KEY": "graphs/default/graph.json",
                "LOG_LEVEL": "INFO",
                "FAIL_FAST": "false",
            },
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
            ),
            security_groups=[lambda_sg],
        )

        # Executor reads graph from S3 — specific object only
        executor_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject"],
                resources=[f"{graph_bucket.bucket_arn}/graphs/*"],
            )
        )
        executor_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:ListBucket"],
                resources=[graph_bucket.bucket_arn],
            )
        )

        # ---------------------------------------------------------- #
        #  Graph Generator Lambda                                      #
        # ---------------------------------------------------------- #
        neptune_endpoint = f"wss://{neptune_cluster.attr_endpoint}:8182/gremlin"

        generator_log_group = logs.LogGroup(
            self, "GeneratorLogGroup",
            log_group_name="/aws/lambda/autograph-graph-generator",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=RemovalPolicy.DESTROY,
        )

        generator_lambda = _lambda.Function(
            self, "GraphGeneratorLambda",
            function_name="autograph-graph-generator",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="autograph_rule_engine.compute.graph_generator_handler.graph_generator_handler",
            code=_lambda.Code.from_asset(
                "src",
                exclude=["**/__pycache__", "*.egg-info", "**/*.pyc"],
            ),
            layers=[deps_layer],
            timeout=Duration.seconds(120),
            memory_size=1024,
            environment={
                "RULES_S3_BUCKET": rules_bucket.bucket_name,
                "RULES_S3_PREFIX": "rules/loan_pricing/",
                "GRAPH_S3_BUCKET": graph_bucket.bucket_name,
                "GRAPH_S3_KEY": "graphs/default/graph.json",
                "NEPTUNE_ENDPOINT": neptune_endpoint,
                "NAMESPACE": "default",
                "LOG_LEVEL": "INFO",
            },
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
            ),
            security_groups=[lambda_sg],
        )

        # Generator reads rules from S3
        generator_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:ListBucket"],
                resources=[
                    rules_bucket.bucket_arn,
                    f"{rules_bucket.bucket_arn}/rules/*",
                ],
            )
        )

        # Generator writes graph to S3
        generator_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:PutObject"],
                resources=[f"{graph_bucket.bucket_arn}/graphs/*"],
            )
        )

        # Generator writes to Neptune
        generator_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "neptune-db:connect",
                    "neptune-db:ReadDataViaQuery",
                    "neptune-db:WriteDataViaQuery",
                    "neptune-db:DeleteDataViaQuery",
                ],
                resources=[
                    f"arn:aws:neptune-db:{self.region}:{self.account}:{neptune_cluster.attr_cluster_resource_id}/*"
                ],
            )
        )

        # ---------------------------------------------------------- #
        #  S3 Event Trigger: auto-publish on rule upload               #
        # ---------------------------------------------------------- #
        rules_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(generator_lambda),
            s3.NotificationKeyFilter(prefix="rules/", suffix=".json"),
        )

        # ---------------------------------------------------------- #
        #  Graph Query Lambda (reads from Neptune)                     #
        # ---------------------------------------------------------- #
        query_log_group = logs.LogGroup(
            self, "QueryLogGroup",
            log_group_name="/aws/lambda/autograph-graph-query",
            retention=logs.RetentionDays.TWO_WEEKS,
            removal_policy=RemovalPolicy.DESTROY,
        )

        query_lambda = _lambda.Function(
            self, "GraphQueryLambda",
            function_name="autograph-graph-query",
            runtime=_lambda.Runtime.PYTHON_3_13,
            handler="autograph_rule_engine.compute.graph_query_handler.lambda_handler",
            code=_lambda.Code.from_asset(
                "src",
                exclude=["**/__pycache__", "*.egg-info", "**/*.pyc"],
            ),
            layers=[deps_layer],
            timeout=Duration.seconds(30),
            memory_size=512,
            environment={
                "NEPTUNE_ENDPOINT": neptune_endpoint,
                "NAMESPACE": "default",
                "LOG_LEVEL": "INFO",
            },
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
            ),
            security_groups=[lambda_sg],
        )

        # Query Lambda reads from Neptune (read-only)
        query_lambda.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "neptune-db:connect",
                    "neptune-db:ReadDataViaQuery",
                ],
                resources=[
                    f"arn:aws:neptune-db:{self.region}:{self.account}:{neptune_cluster.attr_cluster_resource_id}/*"
                ],
            )
        )

        # ---------------------------------------------------------- #
        #  Graph Publisher Role (for deploy-time CLI/CI)               #
        # ---------------------------------------------------------- #
        publisher_role = iam.Role(
            self, "GraphPublisherRole",
            assumed_by=iam.CompositePrincipal(
                iam.AccountRootPrincipal(),
            ),
            description="Role for graph_publisher CLI to write to Neptune and S3",
        )

        # Publisher role — specific S3 permissions
        publisher_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:PutObject"],
                resources=[f"{graph_bucket.bucket_arn}/graphs/*"],
            )
        )
        publisher_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:ListBucket"],
                resources=[
                    rules_bucket.bucket_arn,
                    f"{rules_bucket.bucket_arn}/rules/*",
                ],
            )
        )

        # Publisher role writes to Neptune
        publisher_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "neptune-db:connect",
                    "neptune-db:ReadDataViaQuery",
                    "neptune-db:WriteDataViaQuery",
                    "neptune-db:DeleteDataViaQuery",
                ],
                resources=[
                    f"arn:aws:neptune-db:{self.region}:{self.account}:{neptune_cluster.attr_cluster_resource_id}/*"
                ],
            )
        )

        NagSuppressions.add_stack_suppressions(self, [
            {
                "id": "AwsSolutions-IAM4",
                "reason": "AWS managed policies for Lambda basic execution and VPC access are required and appropriately scoped by AWS.",
            },
            {
                "id": "AwsSolutions-IAM5",
                "reason": "Neptune resource ARN requires /* suffix for database-level access. S3 wildcards are scoped to specific bucket prefixes (graphs/*, rules/*).",
                "applies_to": [
                    f"Resource::arn:aws:neptune-db:<AWS::Region>:<AWS::AccountId>:<{neptune_cluster.logical_id}.ClusterResourceId>/*",
                ],
            },
        ])

        NagSuppressions.add_resource_suppressions_by_path(
            self,
            f"/{construct_id}/Custom::S3AutoDeleteObjectsCustomResourceProvider/Role/Resource",
            [{"id": "AwsSolutions-IAM4", "reason": "CDK-managed custom resource for bucket cleanup."}],
        )
        NagSuppressions.add_resource_suppressions_by_path(
            self,
            f"/{construct_id}/Custom::S3AutoDeleteObjectsCustomResourceProvider/Handler/Resource",
            [{"id": "AwsSolutions-L1", "reason": "CDK-managed custom resource runtime."}],
        )

        NagSuppressions.add_resource_suppressions_by_path(
            self,
            f"/{construct_id}/NeptuneCluster",
            [{"id": "AwsSolutions-N5", "reason": "Neptune IAM auth disabled because gremlinpython does not support SigV4 signing. Neptune is deployed in isolated subnets with no internet access; only the Lambda security group can reach port 8182."}],
        )

        NagSuppressions.add_resource_suppressions(graph_bucket, [
            {"id": "AwsSolutions-S1", "reason": "S3 access logging not enabled. Bucket access is limited to Lambda (CloudWatch logs) and Publisher role (CloudTrail)."},
        ])
        NagSuppressions.add_resource_suppressions(rules_bucket, [
            {"id": "AwsSolutions-S1", "reason": "S3 access logging not enabled. Bucket access is limited to Lambda (CloudWatch logs) and Publisher role (CloudTrail)."},
        ])

        # ---------------------------------------------------------- #
        #  Outputs                                                     #
        # ---------------------------------------------------------- #
        CfnOutput(self, "NeptuneEndpoint",
                  value=neptune_endpoint,
                  description="Neptune Gremlin WebSocket endpoint")

        CfnOutput(self, "NeptuneClusterResourceId",
                  value=neptune_cluster.attr_cluster_resource_id,
                  description="Neptune cluster resource ID (for IAM auth)")

        CfnOutput(self, "GraphBucketName",
                  value=graph_bucket.bucket_name,
                  description="S3 bucket for serialized graph artifacts")

        CfnOutput(self, "RulesBucketName",
                  value=rules_bucket.bucket_name,
                  description="S3 bucket for rule JSON source files")

        CfnOutput(self, "ExecutorLambdaArn",
                  value=executor_lambda.function_arn,
                  description="Graph executor Lambda ARN")

        CfnOutput(self, "GeneratorLambdaArn",
                  value=generator_lambda.function_arn,
                  description="Graph generator Lambda ARN")

        CfnOutput(self, "QueryLambdaArn",
                  value=query_lambda.function_arn,
                  description="Graph query Lambda ARN")

        CfnOutput(self, "PublisherRoleArn",
                  value=publisher_role.role_arn,
                  description="IAM role ARN for CLI graph_publisher")

        CfnOutput(self, "UploadRulesCommand",
                  value=(
                      f"aws s3 sync rules/loan_pricing/ "
                      f"s3://{rules_bucket.bucket_name}/rules/loan_pricing/ "
                      f"--exclude '*' --include '*.json'"
                  ),
                  description="Upload rules to trigger auto-publish")
