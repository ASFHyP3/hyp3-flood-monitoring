import os
from datetime import datetime, timedelta, timezone

import requests

# TODO make url configurable
PDC_URL_TEST = 'https://testsentry.pdc.org'
PDC_URL_PROD = 'https://sentry.pdc.org'

# TODO make url configurable
HYP3_URL_TEST = 'https://hyp3-test-api.asf.alaska.edu'
HYP3_URL_PROD = 'https://hyp3-api.asf.alaska.edu'


def get_active_hazards(pdc_api_url: str, auth_token: str) -> list[dict]:
    url = f'{pdc_api_url}/hp_srv/services/hazards/t/json/get_active_hazards'
    response = requests.get(url, headers={'Authorization': f'Bearer {auth_token}'})
    response.raise_for_status()
    return response.json()


def get_hyp3_api_session(username, password) -> requests.Session:
    url = 'https://urs.earthdata.nasa.gov/oauth/authorize?response_type=code&client_id=BO_n7nTIlMljdvU6kRRB3g' \
          '&redirect_uri=https://auth.asf.alaska.edu/login&app_type=401'
    session = requests.Session()
    response = session.get(url, auth=(username, password))
    response.raise_for_status()
    return session


def get_existing_subscriptions(session: requests.Session, hyp3_url: str) -> dict:
    url = f'{hyp3_url}/subscriptions'
    response = session.get(url)
    response.raise_for_status()
    return response.json()


def submit_subscription(session: requests.Session, hyp3_url: str, subscription: dict, validate_only=False) -> dict:
    url = f'{hyp3_url}/subscriptions'
    subscription['validate_only'] = validate_only
    response = session.post(url, json=subscription)
    response.raise_for_status()
    return response.json()


def filter_hazards(hazards: list[dict]) -> list[dict]:
    # TODO should we include other hazard types?
    return [hazard for hazard in hazards if hazard['type_ID'] == 'FLOOD']


def get_aoi(hazard: dict) -> str:
    # TODO get real aoi
    return f"POINT({hazard['longitude']} {hazard['latitude']})"


def datetime_from_timestamp_in_seconds(timestamp_in_seconds: int) -> datetime:
    return datetime.fromtimestamp(timestamp_in_seconds, tz=timezone.utc)


def str_from_datetime(date_time: datetime) -> str:
    assert date_time.tzinfo == timezone.utc
    datetime_str = date_time.isoformat()
    assert datetime_str.endswith('+00:00')
    return datetime_str.removesuffix('+00:00') + 'Z'


def start_datetime_str_from_timestamp_in_ms(timestamp_in_ms: int, delta: timedelta) -> str:
    return str_from_datetime(datetime_from_timestamp_in_seconds(timestamp_in_ms // 1000) - delta)


def job_name_from_hazard_uuid(uuid: str) -> str:
    return f'PDC-hazard-{uuid}'


def hazard_uuid_from_job_name(name: str) -> str:
    prefix = 'PDC-hazard-'
    assert name.startswith(prefix)
    return name.removeprefix(prefix)


def get_hyp3_subscription(hazard: dict, start_delta=timedelta(days=1)) -> dict:
    # TODO decide on appropriate default value for start_delta
    start = start_datetime_str_from_timestamp_in_ms(int(hazard['start_Date']), start_delta)
    aoi = get_aoi(hazard)
    name = job_name_from_hazard_uuid(hazard['uuid'])
    return {
        'subscription': {
            'search_parameters': {
                'platform': 'S1',
                'processingLevel': 'SLC',
                'beamMode': ['IW'],
                'polarization': ['VV+VH'],
                'start': start,
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


def get_env_var(name: str) -> str:
    val = os.getenv(name)
    assert val, f'Expected env var {name}'  # TODO raise appropriate exception
    return val


def lambda_handler(event, context) -> None:
    pdc_api_url = PDC_URL_TEST
    hyp3_url = HYP3_URL_TEST

    auth_token = get_env_var('PDC_HAZARDS_AUTH_TOKEN')
    earthdata_username = get_env_var('EARTHDATA_USERNAME')
    earthdata_password = get_env_var('EARTHDATA_PASSWORD')

    session = get_hyp3_api_session(earthdata_username, earthdata_password)

    hazards = get_active_hazards(pdc_api_url, auth_token)
    print(f'Hazards: {len(hazards)}')

    hazards = filter_hazards(hazards)
    print(f'Filtered hazards: {len(hazards)}')

    subscriptions = list(map(get_hyp3_subscription, hazards))
    for subscription in subscriptions:
        # TODO remove validate_only
        submit_subscription(session, hyp3_url, subscription, validate_only=True)
