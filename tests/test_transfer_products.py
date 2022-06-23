import os
from unittest.mock import patch

import pytest

import transfer_products


@patch.dict(os.environ, {}, clear=True)
def test_lambda_handler_missing_env_var():
    with pytest.raises(transfer_products.MissingEnvVar):
        transfer_products.lambda_handler(None, None)
