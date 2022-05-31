import os
from datetime import datetime, timedelta, timezone
from unittest.mock import NonCallableMock, patch, MagicMock, call

import pytest

import hyp3_floods


MOCK_ENV = {
    'PDC_HAZARDS_AUTH_TOKEN': 'test-token',
    'EARTHDATA_USERNAME': 'test-user',
    'EARTHDATA_PASSWORD': 'test-pass'
}


@patch('hyp3_floods.get_now')
@patch('hyp3_floods.process_active_hazard')
@patch('hyp3_floods.get_active_hazards')
@patch('hyp3_floods.HyP3SubscriptionsAPI')
@patch.dict(os.environ, MOCK_ENV, clear=True)
def test_lambda_handler(
        mock_hyp3_api_class: MagicMock,
        mock_get_active_hazards: MagicMock,
        mock_process_active_hazard: MagicMock,
        mock_get_now: MagicMock,
        ):
    mock_hyp3_api = NonCallableMock()
    mock_hyp3_api_class.return_value = mock_hyp3_api

    mock_get_active_hazards.return_value = [
        {'uuid': '1', 'type_ID': 'FLOOD'},
        {'uuid': '2', 'type_ID': 'foo'},
        {'uuid': '3', 'type_ID': 'FLOOD'},
        {'uuid': '4', 'type_ID': 'bar'},
    ]

    now = datetime(year=2022, month=5, day=27, hour=20, minute=14, second=34, microsecond=918420, tzinfo=timezone.utc)
    mock_get_now.return_value = now

    hyp3_floods.lambda_handler(None, None)

    mock_hyp3_api_class.assert_called_once_with(hyp3_floods.HYP3_URL_TEST, 'test-user', 'test-pass')
    mock_get_active_hazards.assert_called_once_with('test-token')
    mock_get_now.assert_called_once_with()

    end = '2022-05-27T23:14:34Z'
    assert mock_process_active_hazard.mock_calls == [
        call(mock_hyp3_api, {'uuid': '1', 'type_ID': 'FLOOD'}, end),
        call(mock_hyp3_api, {'uuid': '3', 'type_ID': 'FLOOD'}, end),
    ]


@patch.dict(os.environ, {}, clear=True)
def test_lambda_handler_missing_env_var():
    with pytest.raises(hyp3_floods.MissingEnvVar):
        hyp3_floods.lambda_handler(None, None)


def test_process_active_hazard_submit():
    mock_hyp3 = NonCallableMock(hyp3_floods.HyP3SubscriptionsAPI)
    mock_hyp3.get_subscriptions_by_name.return_value = {'subscriptions': []}
    mock_hyp3.submit_subscription.return_value = {'subscription': {'subscription_id': ''}}

    hazard = {
        'uuid': '123',
        'start_Date': '1650388111000',
        'latitude': 38.39,
        'longitude': 47.94
    }
    end = 'test-end-datetime'

    hyp3_floods.process_active_hazard(mock_hyp3, hazard, end)

    name = 'PDC-hazard-123'
    new_subscription = {
        'search_parameters': {
            'platform': 'S1',
            'processingLevel': 'SLC',
            'beamMode': ['IW'],
            'polarization': ['VV+VH'],
            'start': '2022-04-18T17:08:31Z',
            'end': end,
            'intersectsWith': 'POINT(47.94 38.39)'
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

    mock_hyp3.get_subscriptions_by_name.assert_called_once_with(name)
    mock_hyp3.submit_subscription.assert_called_once_with(new_subscription)


def test_process_active_hazard_update():
    mock_hyp3 = NonCallableMock(hyp3_floods.HyP3SubscriptionsAPI)
    mock_hyp3.get_subscriptions_by_name.return_value = {
        'subscriptions': [
            {
                'subscription_id': '789',
                'search_parameters': {'intersectsWith': 'POINT(47.94 38.39)'}
            }
        ]
    }

    hazard = {
        'uuid': '123',
        'latitude': 38.39,
        'longitude': 47.94
    }
    end = 'test-end-datetime'

    hyp3_floods.process_active_hazard(mock_hyp3, hazard, end)

    mock_hyp3.get_subscriptions_by_name.assert_called_once_with('PDC-hazard-123')
    mock_hyp3.update_subscription.assert_called_once_with('789', end)


def test_process_active_hazard_duplicate_subscription_names():
    mock_hyp3 = NonCallableMock(hyp3_floods.HyP3SubscriptionsAPI)
    mock_hyp3.get_subscriptions_by_name.return_value = {
        'subscriptions': [{'subscription_id': 'foo'}, {'subscription_id': 'bar'}]
    }

    hazard = {'uuid': '123'}

    with pytest.raises(hyp3_floods.DuplicateSubscriptionNames):
        hyp3_floods.process_active_hazard(mock_hyp3, hazard, 'test-end-datetime')

    mock_hyp3.get_subscriptions_by_name.assert_called_once_with('PDC-hazard-123')


def test_process_active_hazard_outdated_aoi():
    mock_hyp3 = NonCallableMock(hyp3_floods.HyP3SubscriptionsAPI)
    mock_hyp3.get_subscriptions_by_name.return_value = {
        'subscriptions': [
            {
                'subscription_id': '789',
                'search_parameters': {'intersectsWith': 'POINT(47.94 35.39)'}
            }
        ]
    }

    hazard = {
        'uuid': '123',
        'latitude': 38.39,
        'longitude': 47.94
    }

    with pytest.raises(hyp3_floods.OutdatedAOI):
        hyp3_floods.process_active_hazard(mock_hyp3, hazard, 'test-end-datetime')

    mock_hyp3.get_subscriptions_by_name.assert_called_once_with('PDC-hazard-123')


def test_filter_hazards():
    hazards = [
        {'hazard_ID': 0, 'type_ID': 'FLOOD'},
        {'hazard_ID': 1, 'type_ID': 'foo'},
        {'hazard_ID': 2, 'type_ID': 'FLOOD'},
        {'hazard_ID': 3, 'type_ID': 'bar'},
        {'hazard_ID': 4, 'type_ID': 'baz'},
        {'hazard_ID': 5, 'type_ID': 'FLOOD'},
    ]
    filtered = [
        {'hazard_ID': 0, 'type_ID': 'FLOOD'},
        {'hazard_ID': 2, 'type_ID': 'FLOOD'},
        {'hazard_ID': 5, 'type_ID': 'FLOOD'},
    ]
    assert hyp3_floods.filter_hazards(hazards) == filtered


def test_get_aoi():
    hazard = {'latitude': 37.949, 'longitude': -90.4527}
    aoi = 'POINT(-90.4527 37.949)'
    assert hyp3_floods.get_aoi(hazard) == aoi


def test_str_from_datetime():
    date_time = datetime(year=2021, month=12, day=10, hour=21, minute=9, second=3, tzinfo=timezone.utc)
    datetime_str = '2021-12-10T21:09:03Z'
    assert hyp3_floods.str_from_datetime(date_time) == datetime_str


def test_get_start_datetime_str():
    timestamp = 1639170543789
    assert hyp3_floods.get_start_datetime_str(timestamp, timedelta(0)) == '2021-12-10T21:09:03Z'
    assert hyp3_floods.get_start_datetime_str(timestamp, timedelta(days=1)) == '2021-12-09T21:09:03Z'
    assert hyp3_floods.get_start_datetime_str(timestamp, timedelta(days=2)) == '2021-12-08T21:09:03Z'
    assert hyp3_floods.get_start_datetime_str(timestamp) == '2021-12-09T21:09:03Z'


def test_get_end_datetime_str():
    now = datetime(year=2022, month=5, day=27, hour=20, minute=14, second=34, microsecond=918420, tzinfo=timezone.utc)
    datetime_str = '2022-05-27T23:14:34Z'
    assert hyp3_floods.get_end_datetime_str(now) == datetime_str


def test_subscription_name_from_hazard_uuid():
    uuid = '595467f9-77f2-4036-87d3-ef9e5e4ad939'
    name = 'PDC-hazard-595467f9-77f2-4036-87d3-ef9e5e4ad939'
    assert hyp3_floods.subscription_name_from_hazard_uuid(uuid) == name


def test_get_hyp3_subscription():
    start = 'test-start-datetime'
    end = 'test-end-datetime'
    aoi = 'test-aoi'
    name = 'test-subscription-name'

    subscription = {
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

    assert hyp3_floods.get_hyp3_subscription(start, end, aoi, name) == subscription
