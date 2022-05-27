import os
from unittest.mock import patch

import pytest

import hyp3_floods


MOCK_ENV = {
    'PDC_HAZARDS_AUTH_TOKEN': 'test-token',
    'EARTHDATA_USERNAME': 'test-user',
    'EARTHDATA_PASSWORD': 'test-pass'
}


@patch.dict(os.environ, MOCK_ENV, clear=True)
def test_lambda_handler():
    # TODO
    pass


@patch.dict(os.environ, {}, clear=True)
def test_lambda_handler_missing_env_var():
    with pytest.raises(hyp3_floods.MissingEnvVar):
        hyp3_floods.lambda_handler(None, None)
