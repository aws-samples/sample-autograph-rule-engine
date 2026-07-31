"""
Graph Publisher: builds the rule graph, persists it to Neptune,
serializes it to S3 for Lambda consumption.

This is meant to run at DEPLOY TIME (CI/CD, CLI, or Step Function),
NOT at Lambda execution time.

Flow:
    1. Load rule JSON files (from local dir or S3)
    2. Build + validate the graph (NetworkX in-memory)
    3. Upsert the graph into Neptune (persistent graph store)
    4. Serialize the RuleGraph to JSON and upload to S3 (execution artifact)

Lambda then loads the pre-built graph from S3 — no graph-building at runtime.

Usage (CLI):
    python -m deploy.graph_publisher \
        --rules-dir rules/loan_pricing/ \
        --neptune-endpoint wss://my-cluster.region.neptune.amazonaws.com:8182/gremlin \
        --s3-bucket my-rule-engine-artifacts \
        --s3-key graphs/loan_pricing/graph.json \
        --namespace loan_pricing

Usage (programmatic):
    from deploy.graph_publisher import GraphPublisher

    publisher = GraphPublisher(
        neptune_endpoint="wss://...",
        s3_bucket="my-bucket",
        s3_key="graphs/loan_pricing/graph.json",
        namespace="loan_pricing",
    )
    publisher.publish_from_directory("rules/loan_pricing/")
"""

from __future__ import annotations
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class GraphPublisher:
    """
    Orchestrates the build → Neptune → S3 pipeline for rule graphs.

    Responsibilities:
    - Build and validate the rule graph from JSON rule files
    - Persist the graph structure in Neptune for querying/impact analysis
    - Serialize the graph to S3 as a JSON artifact for Lambda execution
    """

    def __init__(
        self,
        neptune_endpoint: Optional[str] = None,
        s3_bucket: Optional[str] = None,
        s3_key: str = "graphs/default/graph.json",
        namespace: str = "default",
        region: str = "us-east-1",
        skip_neptune: bool = False,
        skip_s3: bool = False,
    ):
        """
        Args:
            neptune_endpoint: Neptune Gremlin WebSocket endpoint.
                              If None and skip_neptune=False, raises error.
            s3_bucket:        S3 bucket for the serialized graph artifact.
                              If None and skip_s3=False, raises error.
            s3_key:           S3 object key for the graph JSON.
            namespace:        Neptune namespace (allows multiple graphs in one cluster).
            region:           AWS region for Neptune IAM auth and S3.
            skip_neptune:     If True, skip Neptune upsert (useful for local dev).
            skip_s3:          If True, skip S3 upload (useful for local dev).
        """
        self.neptune_endpoint = neptune_endpoint
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.namespace = namespace
        self.region = region
        self.skip_neptune = skip_neptune
        self.skip_s3 = skip_s3

    def publish_from_directory(self, rules_dir: str) -> Dict[str, Any]:
        """
        End-to-end: load rules from a local directory, build graph,
        push to Neptune, upload to S3.

        Args:
            rules_dir: Path to directory containing rule JSON files.

        Returns:
            Summary dict with graph stats and artifact locations.
        """
        from autograph_rule_engine.core.parser import RuleParser
        from autograph_rule_engine.core.graph_builder import GraphBuilder
        from autograph_rule_engine.core.validator import GraphValidator

        # Step 1: Load rules
        logger.info(f"Loading rules from: {rules_dir}")
        parser = RuleParser()
        rules = parser.load_directory(Path(rules_dir))
        logger.info(f"Loaded {len(rules)} rules.")

        # Step 2: Build + validate graph
        logger.info("Building dependency graph...")
        builder = GraphBuilder()
        graph = builder.build(rules)

        validator = GraphValidator()
        val_result = validator.validate(graph)
        val_result.raise_if_invalid()
        logger.info(f"Graph valid. Execution order: {graph.execution_order}")

        return self._publish_graph(graph)

    def publish_from_s3_rules(self, rules_bucket: str, rules_prefix: str) -> Dict[str, Any]:
        """
        Load rules from S3, build graph, push to Neptune, upload serialized graph to S3.

        Args:
            rules_bucket: S3 bucket containing rule JSON files.
            rules_prefix: S3 key prefix for rule files.

        Returns:
            Summary dict.
        """
        from autograph_rule_engine.core.parser import RuleParser
        from autograph_rule_engine.core.graph_builder import GraphBuilder
        from autograph_rule_engine.core.validator import GraphValidator

        # Step 1: Download rules from S3
        logger.info(f"Loading rules from s3://{rules_bucket}/{rules_prefix}")
        rule_dicts = self._download_rules_from_s3(rules_bucket, rules_prefix)

        parser = RuleParser()
        rules = parser.load_dicts(rule_dicts)
        logger.info(f"Loaded {len(rules)} rules from S3.")

        # Step 2: Build + validate graph
        logger.info("Building dependency graph...")
        builder = GraphBuilder()
        graph = builder.build(rules)

        validator = GraphValidator()
        val_result = validator.validate(graph)
        val_result.raise_if_invalid()
        logger.info(f"Graph valid. Execution order: {graph.execution_order}")

        return self._publish_graph(graph)

    def _publish_graph(self, graph) -> Dict[str, Any]:
        """
        Push built graph to Neptune and S3.

        Args:
            graph: A validated RuleGraph instance.

        Returns:
            Summary dict with stats and artifact locations.
        """
        summary = {
            "num_rules": len(graph.rules),
            "execution_order": graph.execution_order,
            "payload_fields": graph.payload_fields,
            "neptune_synced": False,
            "s3_uploaded": False,
        }

        # Step 3: Upsert into Neptune
        if not self.skip_neptune:
            self._upsert_to_neptune(graph)
            summary["neptune_synced"] = True
            summary["neptune_endpoint"] = self.neptune_endpoint
            summary["neptune_namespace"] = self.namespace

        # Step 4: Serialize and upload to S3
        if not self.skip_s3:
            self._upload_to_s3(graph)
            summary["s3_uploaded"] = True
            summary["s3_location"] = f"s3://{self.s3_bucket}/{self.s3_key}"

        logger.info(f"Publish complete: {summary}")
        return summary

    # ------------------------------------------------------------------ #
    #  Neptune                                                             #
    # ------------------------------------------------------------------ #

    def _upsert_to_neptune(self, graph):
        """Write the graph to Neptune."""
        if not self.neptune_endpoint:
            raise ValueError(
                "neptune_endpoint is required when skip_neptune=False"
            )

        from autograph_rule_engine.backends.neptune_backend import NeptuneGraphBackend

        logger.info(f"Upserting graph to Neptune: {self.neptune_endpoint}")
        backend = NeptuneGraphBackend(
            endpoint=self.neptune_endpoint,
            region=self.region,
        )
        backend.connect()
        try:
            backend.upsert_rule_graph(graph, namespace=self.namespace)
            logger.info("Neptune upsert complete.")
        finally:
            backend.close()

    # ------------------------------------------------------------------ #
    #  S3                                                                  #
    # ------------------------------------------------------------------ #

    def _upload_to_s3(self, graph):
        """Serialize RuleGraph to JSON and upload to S3."""
        if not self.s3_bucket:
            raise ValueError("s3_bucket is required when skip_s3=False")

        import boto3

        # Serialize the RuleGraph using Pydantic's model_dump_json
        graph_json = graph.model_dump_json(indent=2)

        logger.info(f"Uploading graph to s3://{self.s3_bucket}/{self.s3_key}")
        s3 = boto3.client("s3", region_name=self.region)
        s3.put_object(
            Bucket=self.s3_bucket,
            Key=self.s3_key,
            Body=graph_json.encode("utf-8"),
            ContentType="application/json",
            Metadata={
                "num_rules": str(len(graph.rules)),
                "namespace": self.namespace,
                "execution_order": ",".join(graph.execution_order),
            },
        )
        logger.info("S3 upload complete.")

    # ------------------------------------------------------------------ #
    #  Helpers                                                             #
    # ------------------------------------------------------------------ #

    def _download_rules_from_s3(self, bucket: str, prefix: str):
        """Download rule JSON files from S3."""
        import boto3

        s3 = boto3.client("s3", region_name=self.region)
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        rule_dicts = []
        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".json"):
                    continue
                logger.info(f"Downloading: s3://{bucket}/{key}")
                response = s3.get_object(Bucket=bucket, Key=key)
                content = response["Body"].read().decode("utf-8")
                rule_dicts.append(json.loads(content))

        if not rule_dicts:
            raise ValueError(f"No rule JSON files found at s3://{bucket}/{prefix}")

        return rule_dicts


# ------------------------------------------------------------------ #
#  CLI entry point                                                     #
# ------------------------------------------------------------------ #
if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s | %(name)s | %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Build rule graph, persist to Neptune, upload to S3."
    )
    parser.add_argument("--rules-dir", help="Local directory with rule JSON files")
    parser.add_argument("--rules-s3-bucket", help="S3 bucket with rule JSON files")
    parser.add_argument("--rules-s3-prefix", help="S3 prefix for rule JSON files")
    parser.add_argument("--neptune-endpoint", help="Neptune Gremlin WebSocket endpoint")
    parser.add_argument("--s3-bucket", required=True, help="S3 bucket for graph artifact")
    parser.add_argument("--s3-key", default="graphs/default/graph.json", help="S3 key for graph JSON")
    parser.add_argument("--namespace", default="default", help="Neptune namespace")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    parser.add_argument("--skip-neptune", action="store_true", help="Skip Neptune upsert")
    parser.add_argument("--skip-s3", action="store_true", help="Skip S3 upload")

    args = parser.parse_args()

    publisher = GraphPublisher(
        neptune_endpoint=args.neptune_endpoint,
        s3_bucket=args.s3_bucket,
        s3_key=args.s3_key,
        namespace=args.namespace,
        region=args.region,
        skip_neptune=args.skip_neptune or not args.neptune_endpoint,
        skip_s3=args.skip_s3,
    )

    if args.rules_dir:
        result = publisher.publish_from_directory(args.rules_dir)
    elif args.rules_s3_bucket and args.rules_s3_prefix:
        result = publisher.publish_from_s3_rules(args.rules_s3_bucket, args.rules_s3_prefix)
    else:
        parser.error("Provide either --rules-dir or both --rules-s3-bucket and --rules-s3-prefix")

    print("\n✓ Publish Summary:")
    for k, v in result.items():
        print(f"  {k}: {v}")
