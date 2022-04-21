import os
from datetime import date
from unittest.mock import patch, MagicMock

import hyp3_floods

# TODO consider adding more test cases: no active hazards, no enabled subscriptions, no new active hazards, etc.


def get_test_subscription(start: str, end: str, aoi: str, name: str) -> dict:
    return {
        'subscription': {
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
    }


@patch('hyp3_floods.disable_subscriptions')
@patch('hyp3_floods.get_today')
@patch('hyp3_floods.submit_subscriptions')
@patch('hyp3_floods.get_enabled_subscriptions')
@patch('hyp3_floods.get_active_hazards')
@patch('hyp3_floods.get_hyp3_api_session')
@patch.dict(
    os.environ,
    {'PDC_HAZARDS_AUTH_TOKEN': 'test-token', 'EARTHDATA_USERNAME': 'test-user', 'EARTHDATA_PASSWORD': 'test-pass'},
    clear=True
)
def test_lambda_handler(
        mock_get_hyp3_api_session: MagicMock,
        mock_get_active_hazards: MagicMock,
        mock_get_enabled_subscriptions: MagicMock,
        mock_submit_subscriptions: MagicMock,
        mock_get_today: MagicMock,
        mock_disable_subscriptions: MagicMock):

    mock_hyp3_api_session = mock_get_hyp3_api_session.return_value = 'mock hyp3 api session'
    mock_get_today.return_value = date(year=2022, month=4, day=18)

    mock_get_active_hazards.return_value = [
        {'uuid': 'new1',
         'type_ID': 'FLOOD',
         'start_Date': '1650388111000',
         'latitude': 38.39,
         'longitude': 47.94},
        {'uuid': 'existing1',
         'type_ID': 'FLOOD',
         'start_Date': '1650388111000',
         'latitude': 4.98,
         'longitude': -75.49},
        {'uuid': 'invalid-type-1',
         'type_ID': 'foo',
         'start_Date': '1650491866784',
         'latitude': 6.9894,
         'longitude': 126.9322},
        {'uuid': 'new2',
         'type_ID': 'FLOOD',
         'start_Date': '1613402506337',
         'latitude': -5.57076,
         'longitude': 24.6966},
        {'uuid': 'existing2',
         'type_ID': 'FLOOD',
         'start_Date': '1650474572000',
         'latitude': 18.46,
         'longitude': -73.52},
        {'uuid': 'invalid-type-2',
         'type_ID': 'bar',
         'start_Date': '1650474572000',
         'latitude': 38.72,
         'longitude': -1.96},
    ]

    mock_get_enabled_subscriptions.return_value = {
        'subscriptions': [
            {'subscription_id': 'sub1', 'job_specification': {'name': 'PDC-hazard-old1'}},
            {'subscription_id': 'sub2', 'job_specification': {'name': 'PDC-hazard-existing1'}},
            {'subscription_id': 'sub3', 'job_specification': {'name': 'PDC-hazard-old2'}},
            {'subscription_id': 'sub4', 'job_specification': {'name': 'PDC-hazard-existing2'}},
        ]
    }

    hyp3_floods.lambda_handler(None, None)

    new_subscriptions = [
        get_test_subscription(
            name='PDC-hazard-new1',
            aoi='POINT(47.94 38.39)',
            start='2022-04-18T17:08:31Z',
            end='2022-10-15T00:00:00Z'
        ),
        get_test_subscription(
            name='PDC-hazard-new2',
            aoi='POINT(24.6966 -5.57076)',
            start='2021-02-14T15:21:46Z',
            end='2022-10-15T00:00:00Z'
        )
    ]
    inactive_hazard_subscription_ids = ['sub1', 'sub3']

    mock_submit_subscriptions.assert_called_once_with(
        mock_hyp3_api_session,
        hyp3_floods.HYP3_URL_TEST,
        new_subscriptions
    )

    mock_disable_subscriptions.assert_called_once_with(
        mock_hyp3_api_session,
        hyp3_floods.HYP3_URL_TEST,
        inactive_hazard_subscription_ids
    )


def test_lambda_handler_duplicate_job_names():
    # TODO
    pass
