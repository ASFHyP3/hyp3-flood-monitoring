install:
	python -m pip install -r requirements.txt

install-lambda-deps:
	python -m pip install -r lambda/requirements.txt

test:
	python -m pytest tests/

# TODO flake8, mypy
static: cfn-lint

cfn-lint:
	cfn-lint lambda/cloudformation.yml --info --ignore-checks W3002
