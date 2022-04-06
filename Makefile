install:
	python -m pip install -r requirements.txt

run:
	ENV_VARS=$$(xargs < dev.env) && \
	export $$ENV_VARS && \
	python hyp3_floods.py

test:
	python -m pytest tests/
