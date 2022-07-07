import os
from datetime import datetime, timedelta, timezone
from unittest.mock import NonCallableMock, patch, MagicMock, call

import pytest

import hyp3_floods


MOCK_ENV = {
    'PDC_HAZARDS_AUTH_TOKEN': 'test-token',
    'HYP3_URL': 'test-url',
    'EARTHDATA_USERNAME': 'test-user',
    'EARTHDATA_PASSWORD': 'test-pass'
}


@patch('hyp3_floods.get_current_time_in_ms')
@patch('hyp3_floods.process_active_hazard')
@patch('hyp3_floods.get_active_hazards')
@patch('hyp3_floods.HyP3SubscriptionsAPI')
@patch.dict(os.environ, MOCK_ENV, clear=True)
def test_lambda_handler(
        mock_hyp3_api_class: MagicMock,
        mock_get_active_hazards: MagicMock,
        mock_process_active_hazard: MagicMock,
        mock_get_current_time_in_ms: MagicMock,
        ):
    mock_hyp3_api = NonCallableMock()
    mock_hyp3_api_class.return_value = mock_hyp3_api

    active_hazards = [
        {'uuid': '0',
         'category_ID': 'EVENT',
         'severity_ID': 'WARNING',
         'type_ID': 'FLOOD',
         'start_Date': 1653658144630},
        {'uuid': '1',
         'category_ID': 'EVENT',
         'severity_ID': 'WARNING',
         'type_ID': 'foo',
         'start_Date': 1653658144630},
        {'uuid': '2',
         'category_ID': 'EVENT',
         'severity_ID': 'WARNING',
         'type_ID': 'FLOOD',
         'start_Date': 1653658144640},
        {'uuid': '3',
         'category_ID': 'EVENT',
         'severity_ID': 'WARNING',
         'type_ID': 'bar',
         'start_Date': 1653658144640},
        {'uuid': '4',
         'category_ID': 'EVENT',
         'severity_ID': 'WARNING',
         'type_ID': 'FLOOD',
         'start_Date': 1653658144650},
        {'uuid': '5',
         'category_ID': 'EVENT',
         'severity_ID': 'WARNING',
         'type_ID': 'baz',
         'start_Date': 1653658144650},
    ]
    mock_get_active_hazards.return_value = active_hazards

    mock_get_current_time_in_ms.return_value = 1653658144647

    hyp3_floods.lambda_handler(None, None)

    mock_hyp3_api_class.assert_called_once_with('test-url', 'test-user', 'test-pass')
    mock_get_active_hazards.assert_called_once_with('test-token')
    mock_get_current_time_in_ms.assert_called_once_with()

    end = '2022-05-27T16:29:04Z'
    assert mock_process_active_hazard.mock_calls == [
        call(mock_hyp3_api, active_hazards[0], end, dry_run=False),
        call(mock_hyp3_api, active_hazards[2], end, dry_run=False),
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

    hyp3_floods.process_active_hazard(mock_hyp3, hazard, end, dry_run=False)

    name = 'PDC-hazard-123'
    new_subscription = {
        'search_parameters': {
            'platform': 'S1',
            'processingLevel': 'SLC',
            'beamMode': ['IW'],
            'polarization': ['VV+VH'],
            'start': '2022-04-19T16:08:31Z',
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
    mock_hyp3.submit_subscription.assert_called_once_with(new_subscription, validate_only=False)


@patch('builtins.print')
def test_process_active_hazard_update(mock_print: MagicMock):
    mock_hyp3 = NonCallableMock(hyp3_floods.HyP3SubscriptionsAPI)
    mock_hyp3.get_subscriptions_by_name.return_value = {
        'subscriptions': [
            {
                'subscription_id': 'test-subscription-id',
                'search_parameters': {
                    'start': '2022-03-01T00:00:00Z',
                    'intersectsWith': 'POINT(0.0 0.0)',
                }
            }
        ]
    }

    hazard = {
        'uuid': '123',
        'start_Date': '1655251200000',
        'latitude': 1.0,
        'longitude': 2.0,
    }

    hyp3_floods.process_active_hazard(mock_hyp3, hazard, 'test-end-datetime', dry_run=False)

    mock_hyp3.get_subscriptions_by_name.assert_called_once_with('PDC-hazard-123')

    mock_hyp3.update_subscription.assert_called_once_with(
        subscription_id='test-subscription-id',
        start='2022-06-14T23:00:00Z',
        end='test-end-datetime',
        intersectsWith='POINT(2.0 1.0)',
        enabled=True,
    )

    assert call(
        'Updating start datetime for subscription test-subscription-id '
        'from 2022-03-01T00:00:00Z to 2022-06-14T23:00:00Z'
    ) in mock_print.mock_calls

    assert call(
        'Updating AOI for subscription test-subscription-id '
        'from POINT(0.0 0.0) to POINT(2.0 1.0)'
    ) in mock_print.mock_calls


def test_process_active_hazard_duplicate_subscription_names():
    mock_hyp3 = NonCallableMock(hyp3_floods.HyP3SubscriptionsAPI)
    mock_hyp3.get_subscriptions_by_name.return_value = {
        'subscriptions': [{'subscription_id': 'foo'}, {'subscription_id': 'bar'}]
    }

    hazard = {'uuid': '123', 'start_Date': '1', 'latitude': '', 'longitude': ''}

    with pytest.raises(hyp3_floods.DuplicateSubscriptionNames):
        hyp3_floods.process_active_hazard(mock_hyp3, hazard, 'test-end-datetime', dry_run=False)

    mock_hyp3.get_subscriptions_by_name.assert_called_once_with('PDC-hazard-123')


def test_filter_hazards():
    hazards = [
        {'uuid': 0, 'category_ID': 'EVENT', 'severity_ID': 'WARNING', 'type_ID': 'FLOOD', 'start_Date': 1},
        {'uuid': 1, 'category_ID': 'EVENT', 'severity_ID': 'WARNING', 'type_ID': 'foo', 'start_Date': 1},
        {'uuid': 2, 'category_ID': 'EVENT', 'severity_ID': 'WARNING', 'type_ID': 'FLOOD', 'start_Date': 2},
        {'uuid': 3, 'category_ID': 'EVENT', 'severity_ID': 'WARNING', 'type_ID': 'bar', 'start_Date': 2},
        {'uuid': 4, 'category_ID': 'EVENT', 'severity_ID': 'WARNING', 'type_ID': 'FLOOD', 'start_Date': 3},
        {'uuid': 5, 'category_ID': 'EVENT', 'severity_ID': 'WARNING', 'type_ID': 'baz', 'start_Date': 3},
    ]
    assert hyp3_floods.filter_hazards(hazards, current_time_in_ms=0) == []
    assert hyp3_floods.filter_hazards(hazards, current_time_in_ms=1) == [hazards[0]]
    assert hyp3_floods.filter_hazards(hazards, current_time_in_ms=2) == [hazards[0], hazards[2]]
    assert hyp3_floods.filter_hazards(hazards, current_time_in_ms=3) == [hazards[0], hazards[2], hazards[4]]


def test_is_valid_hazard_start_date():
    assert hyp3_floods.is_valid_hazard(
        {'category_ID': 'EVENT', 'severity_ID': 'WARNING', 'type_ID': 'FLOOD', 'start_Date': 1},
        current_time_in_ms=2
    ) is True

    assert hyp3_floods.is_valid_hazard(
        {'category_ID': 'EVENT', 'severity_ID': 'WARNING', 'type_ID': 'FLOOD', 'start_Date': 2},
        current_time_in_ms=2
    ) is True

    assert hyp3_floods.is_valid_hazard(
        {'category_ID': 'EVENT', 'severity_ID': 'WARNING', 'type_ID': 'FLOOD', 'start_Date': 3},
        current_time_in_ms=2
    ) is False


def test_is_valid_hazard_type():
    assert hyp3_floods.is_valid_hazard(
        {'category_ID': 'EVENT', 'severity_ID': 'WARNING', 'type_ID': 'FLOOD', 'start_Date': 1},
        current_time_in_ms=2
    ) is True

    assert hyp3_floods.is_valid_hazard(
        {'category_ID': 'EVENT', 'severity_ID': 'WARNING', 'type_ID': 'foo', 'start_Date': 1},
        current_time_in_ms=2
    ) is False


def test_is_valid_hazard_category():
    assert hyp3_floods.is_valid_hazard(
        {'category_ID': 'EVENT', 'severity_ID': 'WARNING', 'type_ID': 'FLOOD', 'start_Date': 1},
        current_time_in_ms=2
    ) is True

    assert hyp3_floods.is_valid_hazard(
        {'category_ID': 'foo', 'severity_ID': 'WARNING', 'type_ID': 'FLOOD', 'start_Date': 1},
        current_time_in_ms=2
    ) is False


def test_is_valid_hazard_severity():
    assert hyp3_floods.is_valid_hazard(
        {'category_ID': 'EVENT', 'severity_ID': 'WARNING', 'type_ID': 'FLOOD', 'start_Date': 1},
        current_time_in_ms=2
    ) is True

    assert hyp3_floods.is_valid_hazard(
        {'category_ID': 'EVENT', 'severity_ID': 'foo', 'type_ID': 'FLOOD', 'start_Date': 1},
        current_time_in_ms=2
    ) is False


def test_get_aoi():
    hazard = {'latitude': 37.949, 'longitude': -90.4527}
    assert hyp3_floods.get_aoi(hazard) == 'POINT(-90.4527 37.949)'


def test_str_from_datetime():
    date_time = datetime(year=2021, month=12, day=10, hour=21, minute=9, second=3, tzinfo=timezone.utc)
    assert hyp3_floods.str_from_datetime(date_time) == '2021-12-10T21:09:03Z'


def test_get_start_datetime_str_delta():
    timestamp = 1639170543789
    minimum = datetime(2020, 1, 1, tzinfo=timezone.utc)
    assert hyp3_floods.get_start_datetime_str(timestamp, delta=timedelta(0), minimum=minimum) == \
           '2021-12-10T21:09:03Z'
    assert hyp3_floods.get_start_datetime_str(timestamp, delta=timedelta(hours=1), minimum=minimum) == \
           '2021-12-10T20:09:03Z'
    assert hyp3_floods.get_start_datetime_str(timestamp, delta=timedelta(hours=2), minimum=minimum) == \
           '2021-12-10T19:09:03Z'
    assert hyp3_floods.get_start_datetime_str(timestamp, minimum=minimum) == \
           '2021-12-10T20:09:03Z'


def test_get_start_datetime_str_minimum():
    timestamp = 1640995200000

    assert hyp3_floods.get_start_datetime_str(
        timestamp,
        delta=timedelta(0),
        minimum=datetime(1, 1, 1, tzinfo=timezone.utc)
    ) == '2022-01-01T00:00:00Z'

    assert hyp3_floods.get_start_datetime_str(
        timestamp,
        delta=timedelta(0),
        minimum=datetime(2022, 1, 1, tzinfo=timezone.utc)
    ) == '2022-01-01T00:00:00Z'

    assert hyp3_floods.get_start_datetime_str(
        timestamp - 3_600_000,
        delta=timedelta(0),
        minimum=datetime(2022, 1, 1, tzinfo=timezone.utc)
    ) == '2022-01-01T00:00:00Z'

    assert hyp3_floods.get_start_datetime_str(
        timestamp,
        delta=timedelta(hours=1),
        minimum=datetime(2022, 1, 1, tzinfo=timezone.utc)
    ) == '2022-01-01T00:00:00Z'

    assert hyp3_floods.get_start_datetime_str(
        timestamp,
        delta=timedelta(0),
        minimum=datetime(2022, 6, 15, tzinfo=timezone.utc)
    ) == '2022-06-15T00:00:00Z'

    assert hyp3_floods.get_start_datetime_str(
        1,
        delta=timedelta(0)
    ) == '2022-01-01T00:00:00Z'


def test_get_end_datetime_str():
    current_time_in_ms = 1653658144647
    assert hyp3_floods.get_end_datetime_str(current_time_in_ms, timedelta(0)) == '2022-05-27T13:29:04Z'
    assert hyp3_floods.get_end_datetime_str(current_time_in_ms, timedelta(hours=1)) == '2022-05-27T14:29:04Z'
    assert hyp3_floods.get_end_datetime_str(current_time_in_ms, timedelta(hours=2)) == '2022-05-27T15:29:04Z'
    assert hyp3_floods.get_end_datetime_str(current_time_in_ms) == '2022-05-27T16:29:04Z'


def test_subscription_name_from_hazard_uuid():
    uuid = '595467f9-77f2-4036-87d3-ef9e5e4ad939'
    expected_name = 'PDC-hazard-595467f9-77f2-4036-87d3-ef9e5e4ad939'
    assert hyp3_floods.subscription_name_from_hazard_uuid(uuid) == expected_name


def test_prepare_new_subscription():
    expected = {
        'search_parameters': {
            'platform': 'S1',
            'processingLevel': 'SLC',
            'beamMode': ['IW'],
            'polarization': ['VV+VH'],
            'start': 'test-start-datetime',
            'end': 'test-end-datetime',
            'intersectsWith': 'test-aoi'
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
            'name': 'test-subscription-name'
        }
    }
    actual = hyp3_floods.prepare_new_subscription(
        'test-start-datetime', 'test-end-datetime', 'test-aoi', 'test-subscription-name'
    )
    assert actual == expected
