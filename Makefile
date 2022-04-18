export PYTHONPATH = ${PWD}/lambda/src

install:
	python -m pip install -r requirements.txt

install-lambda-deps:
	python -m pip install -r lambda/requirements.txt

lambda_env ?= env/dev.env
run:
	ENV_VARS=$$(xargs < $(lambda_env)) && \
	export $$ENV_VARS && \
	python -c 'from hyp3_floods import lambda_handler; lambda_handler(None, None)'

test:
	pytest tests/

static: flake8 cfn-lint

flake8:
	flake8 --max-line-length=120 lambda tests

cfn-lint:
	cfn-lint lambda/cloudformation.yml --info --ignore-checks W3002
