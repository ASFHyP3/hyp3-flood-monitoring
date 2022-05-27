import os
from unittest.mock import patch

import pytest

import hyp3_floods


MOCK_ENV = {
    'PDC_HAZARDS_AUTH_TOKEN': 'test-token',
    'EARTHDATA_USERNAME': 'test-user',
    'EARTHDATA_PASSWORD': 'test-pass'
}


def get_test_subscription(start: str, end: str, aoi: str, name: str) -> dict:
    return {
        'search_parameters': {
            'platform': 'S1',
            'processingLevel': 'SLC',
            'beamMode': ['IW'],
            'polarization': ['VV+VH'],
            'start': start,
            'end': end,
            'intersectsWith': aoi
        },
        'job_specification': {
            'job_type': 'WATER_MAP',
            'job_parameters': {
                'resolution': 30,
                'speckle_filter': True,
                'max_vv_threshold': -15.5,
                'max_vh_threshold': -23.0,
                'hand_threshold': 15.0,
                'hand_fraction': 0.8,
                'membership_threshold': 0.45
            },
            'name': name
        }
    }


@patch.dict(os.environ, MOCK_ENV, clear=True)
def test_lambda_handler():
    # TODO
    pass


@patch.dict(os.environ, {}, clear=True)
def test_lambda_handler_missing_env_var():
    with pytest.raises(hyp3_floods.MissingEnvVar):
        hyp3_floods.lambda_handler(None, None)
