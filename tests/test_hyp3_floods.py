from datetime import datetime, timezone

import hyp3_floods


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


def test_datetime_from_timestamp():
    timestamp = '1639170543000'
    date_time = datetime(year=2021, month=12, day=10, hour=21, minute=9, second=3, tzinfo=timezone.utc)
    assert hyp3_floods.datetime_from_timestamp(timestamp) == date_time


def test_str_from_datetime():
    date_time = datetime(year=2021, month=12, day=10, hour=21, minute=9, second=3, tzinfo=timezone.utc)
    datetime_str = '2021-12-10T21:09:03Z'
    assert hyp3_floods.str_from_datetime(date_time) == datetime_str


def test_get_hyp3_subscription():
    # TODO use uuid instead of hazard id?
    # TODO add some buffer to start/end datetimes?
    # TODO confirm that we want to use start_Date and end_Date
    #  (there are other timestamp fields in the hazard dict)

    hazard = {
        'hazard_ID': 42,
        'start_Date': '1639170540000',
        'end_Date': '1639198800000',
        'latitude': 37.949,
        'longitude': -90.4527,
    }

    # Adapted from:
    # https://github.com/ASFHyP3/hyp3-nasa-disasters/blob/main/data_management/pdc_brazil.json
    subscription = {
        'subscription': {
            'search_parameters': {
                'platform': 'S1',
                'processingLevel': 'SLC',
                'beamMode': ['IW'],
                'polarization': ['VV+VH'],
                'start': '2021-12-10T21:09:00Z',
                'end': '2021-12-11T05:00:00Z',
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
                'name': 'PDC-hazard-42'
            }
        }
    }

    assert hyp3_floods.get_hyp3_subscription(hazard) == subscription
