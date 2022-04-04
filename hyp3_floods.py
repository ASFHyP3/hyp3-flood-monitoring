import json
import os
from datetime import datetime, timezone

import requests

# TODO make url configurable, or just use prod?
TEST_API_URL = 'https://testsentry.pdc.org'
PROD_API_URL = 'https://sentry.pdc.org'


def get_active_hazards(auth_token: str) -> list[dict]:
    url = f'{TEST_API_URL}/hp_srv/services/hazards/t/json/get_active_hazards'
    response = requests.get(url, headers={'Authorization': f'Bearer {auth_token}'})
    response.raise_for_status()
    return response.json()


def filter_hazards(hazards: list[dict]) -> list[dict]:
    # TODO should we include other hazard types?
    return [hazard for hazard in hazards if hazard['type_ID'] == 'FLOOD']


def get_aoi(hazard: dict) -> str:
    # TODO get real aoi
    return f"POINT({hazard['longitude']} {hazard['latitude']})"


def datetime_from_timestamp(timestamp: str) -> datetime:
    # TODO confirm that PDC API is using POSIX timestamps in ms
    return datetime.fromtimestamp(int(timestamp) / 1000, tz=timezone.utc)


def str_from_datetime(date_time: datetime) -> str:
    assert date_time.tzinfo == timezone.utc
    datetime_str = date_time.isoformat()
    assert datetime_str.endswith('+00:00')
    return datetime_str.removesuffix('+00:00') + 'Z'


def get_hyp3_subscription(hazard: dict) -> dict:
    start = str_from_datetime(datetime_from_timestamp(hazard['start_Date']))
    end = str_from_datetime(datetime_from_timestamp(hazard['end_Date']))
    aoi = get_aoi(hazard)
    name = f"PDC-hazard-{hazard['hazard_ID']}"
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


def main() -> None:
    auth_token = os.getenv('PDC_HAZARDS_AUTH_TOKEN')
    assert auth_token  # TODO raise appropriate exception

    hazards = get_active_hazards(auth_token)
    print(f'Hazards: {len(hazards)}')

    with open('hazards.json', 'w') as f:
        f.write(json.dumps(hazards))


if __name__ == '__main__':
    main()
