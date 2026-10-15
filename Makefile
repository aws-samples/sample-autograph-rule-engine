.PHONY: install deploy destroy layer publish-rules invoke test visualize clean

# --------------------------------------------------------------------------- #
#  Local Development                                                           #
# --------------------------------------------------------------------------- #

install:
	python3 -m venv .venv
	.venv/bin/pip install -e ".[dev,infra]"

test:
	.venv/bin/pytest tests/ -v

visualize:
	.venv/bin/python scripts/visualize_graph.py

# --------------------------------------------------------------------------- #
#  AWS Deployment                                                              #
# --------------------------------------------------------------------------- #

layer:
	rm -rf build/lambda_deps && mkdir -p build/lambda_deps/python
	.venv/bin/pip install pydantic simpleeval networkx gremlinpython \
		--target build/lambda_deps/python \
		--platform manylinux2014_x86_64 \
		--implementation cp \
		--python-version 3.14 \
		--only-binary=:all: \
		--quiet

deploy: layer
	cdk deploy --app ".venv/bin/python infra/app.py" --require-approval never

destroy:
	cdk destroy --app ".venv/bin/python infra/app.py"

publish-rules:
	aws s3 sync --delete rules/loan_pricing/ \
		s3://$$(aws cloudformation describe-stacks \
			--stack-name AutoGraphRuleEngineStack \
			--query "Stacks[0].Outputs[?OutputKey=='RulesBucketName'].OutputValue" \
			--output text)/rules/loan_pricing/
	@echo "Waiting for the graph generator to converge..."
	@.venv/bin/python scripts/wait_for_graph.py

invoke:
	@aws lambda invoke \
		--function-name autograph-graph-executor \
		--cli-binary-format raw-in-base64-out \
		--payload '{"payload":{"loan_amount":280000,"property_value":350000,"credit_score":740,"monthly_debt":1500,"monthly_income":8000,"loan_type":"CONVENTIONAL","loan_term_years":30,"employment_status":"FULL_TIME"}}' \
		/tmp/autograph_response.json > /dev/null && \
		python3 -c "import json; body=json.loads(json.load(open('/tmp/autograph_response.json'))['body']); print(json.dumps(body, indent=2))"

# Usage:
#   make query Q=stats
#   make query Q=impact RULE=R01
#   make query Q=ancestors RULE=R15
#   make query Q=descendants RULE=R01
#   make query Q=paths FROM=R01 TO=R13
#   make query Q=rule RULE=R06
#   make query Q=rules
query:
	@Q=$(Q) RULE=$(RULE) FROM=$(FROM) TO=$(TO) NS=$(NS) python3 scripts/build_query_payload.py > /tmp/autograph_query_payload.json
	@aws lambda invoke \
		--function-name autograph-graph-query \
		--cli-binary-format raw-in-base64-out \
		--payload file:///tmp/autograph_query_payload.json \
		/tmp/autograph_query.json > /dev/null && \
		python3 -c "import json; body=json.loads(json.load(open('/tmp/autograph_query.json'))['body']); print(json.dumps(body, indent=2))"

# --------------------------------------------------------------------------- #
#  Cleanup                                                                     #
# --------------------------------------------------------------------------- #

clean:
	rm -rf cdk.out .pytest_cache build/lambda_deps
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
