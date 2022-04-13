install:
	python -m pip install -r requirements.txt

# TODO flake8, mypy
static: cfn-lint

cfn-lint:
	cfn-lint lambda/cloudformation.yml --info --ignore-checks W3002