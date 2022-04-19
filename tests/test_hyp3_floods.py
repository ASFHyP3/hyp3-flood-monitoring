from datetime import date, datetime, timedelta, timezone

import pytest

import hyp3_floods


def test_get_subscription_names():
    subscriptions = {
        'subscriptions': [
            {'job_specification': {'name': 'aaa'}},
            {'job_specification': {'name': 'bbb'}},
            {'job_specification': {'name': 'ccc'}},
        ]
    }
    names = frozenset(['aaa', 'bbb', 'ccc'])
    assert hyp3_floods.get_subscription_names(subscriptions) == names


def test_get_subscription_names_duplicate_names():
    subscriptions = {
        'subscriptions': [
            {'job_specification': {'name': 'aaa'}},
            {'job_specification': {'name': 'bbb'}},
            {'job_specification': {'name': 'ccc'}},
            {'job_specification': {'name': 'bbb'}},
        ]
    }
    with pytest.raises(ValueError):
        hyp3_floods.get_subscription_names(subscriptions)


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


def test_datetime_from_timestamp_in_seconds():
    timestamp = 1639170543
    date_time = datetime(year=2021, month=12, day=10, hour=21, minute=9, second=3, tzinfo=timezone.utc)
    assert hyp3_floods.datetime_from_timestamp_in_seconds(timestamp) == date_time


def test_str_from_datetime():
    date_time = datetime(year=2021, month=12, day=10, hour=21, minute=9, second=3, tzinfo=timezone.utc)
    datetime_str = '2021-12-10T21:09:03Z'
    assert hyp3_floods.str_from_datetime(date_time) == datetime_str


def test_start_datetime_str_from_timestamp_in_ms():
    timestamp = 1639170543000

    datetime_str = '2021-12-10T21:09:03Z'
    assert hyp3_floods.start_datetime_str_from_timestamp_in_ms(timestamp, timedelta(0)) == datetime_str

    datetime_str_with_delta = '2021-12-09T21:09:03Z'
    assert hyp3_floods.start_datetime_str_from_timestamp_in_ms(timestamp, timedelta(1)) == datetime_str_with_delta


def test_start_datetime_str_from_timestamp_in_ms_truncate():
    timestamp = 1639170543789

    datetime_str = '2021-12-10T21:09:03Z'
    assert hyp3_floods.start_datetime_str_from_timestamp_in_ms(timestamp, timedelta(0)) == datetime_str

    datetime_str_with_delta = '2021-12-09T21:09:03Z'
    assert hyp3_floods.start_datetime_str_from_timestamp_in_ms(timestamp, timedelta(1)) == datetime_str_with_delta


def test_get_end_datetime_str():
    today = date(year=2022, month=4, day=18)
    datetime_str = '2022-10-15T00:00:00Z'
    assert hyp3_floods.get_end_datetime_str(today) == datetime_str


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

    today = date(year=2022, month=4, day=18)

    # Adapted from:
    # https://github.com/ASFHyP3/hyp3-nasa-disasters/blob/main/data_management/pdc_brazil.json
    subscription = {
        'subscription': {
            'search_parameters': {
                'platform': 'S1',
                'processingLevel': 'SLC',
                'beamMode': ['IW'],
                'polarization': ['VV+VH'],
                'start': '2021-12-10T21:09:03Z',
                'end': '2022-10-15T00:00:00Z',
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
    }

    assert hyp3_floods.get_hyp3_subscription(hazard, today, start_delta=timedelta(0)) == subscription

    subscription['subscription']['search_parameters']['start'] = '2021-12-09T21:09:03Z'
    assert hyp3_floods.get_hyp3_subscription(hazard, today) == subscription
