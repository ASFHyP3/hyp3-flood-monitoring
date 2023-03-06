export PYTHONPATH = ${PWD}/hyp3-floods/src:${PWD}/transfer-products/src

install:
	python -m pip install --upgrade pip && \
	python -m pip install -r requirements-all.txt

install-lambda-deps:
	python -m pip install --upgrade pip && \
	python -m pip install -r requirements-hyp3-floods.txt -t hyp3-floods/src/ && \
	python -m pip install -r requirements-transfer-products.txt -t transfer-products/src/

test_file ?= 'tests/'
test:
	pytest $(test_file)

static: flake8 cfn-lint

flake8:
	flake8 --max-line-length=120

cfn-lint:
	cfn-lint --template `find . -name cloudformation.yml` --info --ignore-checks W3002
