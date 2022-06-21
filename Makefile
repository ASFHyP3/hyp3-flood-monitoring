export PYTHONPATH = ${PWD}/hyp3-floods/src:${PWD}/transfer-products/src

install:
	python -m pip install --upgrade pip && \
	python -m pip install -r requirements.txt

install-lambda-deps:
	python -m pip install --upgrade pip && \
	python -m pip install -r hyp3-floods/requirements.txt -t hyp3-floods/src/ \
	python -m pip install -r transfer-products/requirements.txt -t transfer-products/src/

test:
	pytest tests/

static: flake8 cfn-lint

flake8:
	flake8 --max-line-length=120

cfn-lint:
	cfn-lint --template `find . -name cloudformation.yml` --info --ignore-checks W3002
