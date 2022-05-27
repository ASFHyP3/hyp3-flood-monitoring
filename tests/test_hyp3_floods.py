from datetime import datetime, timedelta, timezone
from unittest.mock import NonCallableMock

import pytest

import hyp3_floods


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
    now = datetime(year=2022, month=5, day=27, hour=20, minute=14, second=34, microsecond=918420, tzinfo=timezone.utc)

    hyp3_floods.process_active_hazard(mock_hyp3, hazard, now)

    name = 'PDC-hazard-123'
    new_subscription = get_test_subscription(
        name=name,
        aoi='POINT(47.94 38.39)',
        start='2022-04-18T17:08:31Z',
        end='2022-05-27T23:14:34Z'
    )

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
        'start_Date': '1650388111000',
        'latitude': 38.39,
        'longitude': 47.94
    }
    now = datetime(year=2022, month=5, day=27, hour=20, minute=14, second=34, microsecond=918420, tzinfo=timezone.utc)

    hyp3_floods.process_active_hazard(mock_hyp3, hazard, now)

    mock_hyp3.get_subscriptions_by_name.assert_called_once_with('PDC-hazard-123')
    mock_hyp3.update_subscription.assert_called_once_with('789', '2022-05-27T23:14:34Z')


def test_process_active_hazard_duplicate_subscription_names():
    mock_hyp3 = NonCallableMock(hyp3_floods.HyP3SubscriptionsAPI)
    mock_hyp3.get_subscriptions_by_name.return_value = {
        'subscriptions': [{'subscription_id': 'foo'}, {'subscription_id': 'bar'}]
    }

    hazard = {'uuid': '123'}
    now = datetime(year=2022, month=5, day=27, hour=20, minute=14, second=34, microsecond=918420, tzinfo=timezone.utc)

    with pytest.raises(hyp3_floods.DuplicateSubscriptionNames):
        hyp3_floods.process_active_hazard(mock_hyp3, hazard, now)

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
    now = datetime(year=2022, month=5, day=27, hour=20, minute=14, second=34, microsecond=918420, tzinfo=timezone.utc)

    with pytest.raises(hyp3_floods.OutdatedAOI):
        hyp3_floods.process_active_hazard(mock_hyp3, hazard, now)

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
    timestamp = 1639170543000

    datetime_str = '2021-12-10T21:09:03Z'
    assert hyp3_floods.get_start_datetime_str(timestamp, timedelta(0)) == datetime_str

    datetime_str_with_delta = '2021-12-09T21:09:03Z'
    assert hyp3_floods.get_start_datetime_str(timestamp, timedelta(days=1)) == datetime_str_with_delta


def test_get_start_datetime_str_truncate():
    timestamp = 1639170543789

    datetime_str = '2021-12-10T21:09:03Z'
    assert hyp3_floods.get_start_datetime_str(timestamp, timedelta(0)) == datetime_str

    datetime_str_with_delta = '2021-12-09T21:09:03Z'
    assert hyp3_floods.get_start_datetime_str(timestamp, timedelta(days=1)) == datetime_str_with_delta


def test_get_end_datetime_str():
    now = datetime(year=2022, month=5, day=27, hour=20, minute=14, second=34, microsecond=918420, tzinfo=timezone.utc)
    datetime_str = '2022-05-27T23:14:34Z'
    assert hyp3_floods.get_end_datetime_str(now) == datetime_str


def test_subscription_name_from_hazard_uuid():
    uuid = '595467f9-77f2-4036-87d3-ef9e5e4ad939'
    name = 'PDC-hazard-595467f9-77f2-4036-87d3-ef9e5e4ad939'
    assert hyp3_floods.subscription_name_from_hazard_uuid(uuid) == name


def test_hazard_uuid_from_subscription_name():
    name = 'PDC-hazard-595467f9-77f2-4036-87d3-ef9e5e4ad939'
    uuid = '595467f9-77f2-4036-87d3-ef9e5e4ad939'
    assert hyp3_floods.hazard_uuid_from_subscription_name(name) == uuid


def test_get_hyp3_subscription():
    hazard = {
        'uuid': '595467f9-77f2-4036-87d3-ef9e5e4ad939',
        'start_Date': '1639170543789',
        'latitude': 37.949,
        'longitude': -90.4527,
    }

    now = datetime(year=2022, month=5, day=27, hour=20, minute=14, second=34, microsecond=918420, tzinfo=timezone.utc)

    # Adapted from:
    # https://github.com/ASFHyP3/hyp3-nasa-disasters/blob/main/data_management/pdc_brazil.json
    subscription = {
        'search_parameters': {
            'platform': 'S1',
            'processingLevel': 'SLC',
            'beamMode': ['IW'],
            'polarization': ['VV+VH'],
            'start': '2021-12-10T21:09:03Z',
            'end': '2022-05-27T23:14:34Z',
            'intersectsWith': 'POINT(-90.4527 37.949)'
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
            'name': 'PDC-hazard-595467f9-77f2-4036-87d3-ef9e5e4ad939'
        }
    }

    assert hyp3_floods.get_hyp3_subscription(hazard, now, start_delta=timedelta(0)) == subscription

    subscription['search_parameters']['start'] = '2021-12-09T21:09:03Z'
    assert hyp3_floods.get_hyp3_subscription(hazard, now) == subscription
