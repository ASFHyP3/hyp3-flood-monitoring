export PYTHONPATH = ${PWD}/hyp3-floods/src

install:
	python -m pip install --upgrade pip && \
	python -m pip install -r requirements.txt

install-hyp3-floods-deps:
	python -m pip install --upgrade pip && \
	python -m pip install -r hyp3-floods/requirements.txt -t hyp3-floods/src/

lambda_env ?= env/dev.env
run:
	ENV_VARS=$$(xargs < $(lambda_env)) && \
	export $$ENV_VARS && \
	python -c 'from hyp3_floods import lambda_handler; lambda_handler(None, None)'

test:
	pytest tests/

static: flake8 cfn-lint

flake8:
	flake8 --max-line-length=120 hyp3-floods tests

cfn-lint:
	cfn-lint hyp3-floods/cloudformation.yml --info --ignore-checks W3002
