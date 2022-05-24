import os
from datetime import date, datetime, timedelta, timezone

import requests

# TODO make url configurable
PDC_URL_TEST = 'https://testsentry.pdc.org'
PDC_URL_PROD = 'https://sentry.pdc.org'

# TODO make url configurable
HYP3_URL_TEST = 'https://hyp3-test-api.asf.alaska.edu'
HYP3_URL_PROD = 'https://hyp3-watermap.asf.alaska.edu'


class MissingEnvVarError(Exception):
    pass


class DuplicateSubscriptionNamesError(Exception):
    pass


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


def get_enabled_subscriptions(session: requests.Session, hyp3_url: str) -> dict:
    url = f'{hyp3_url}/subscriptions'
    response = session.get(url, params={'enabled': 'true'})
    response.raise_for_status()
    return response.json()


def submit_subscription(session: requests.Session, hyp3_url: str, subscription: dict, validate_only=False) -> dict:
    url = f'{hyp3_url}/subscriptions'
    subscription['validate_only'] = validate_only
    response = session.post(url, json=subscription)
    response.raise_for_status()
    return response.json()


def disable_subscription(session: requests.Session, hyp3_url: str, subscription_id: str) -> dict:
    url = f'{hyp3_url}/subscriptions/{subscription_id}'
    response = session.patch(url, json={'enabled': False})
    response.raise_for_status()
    return response.json()


def submit_subscriptions(session: requests.Session, hyp3_url: str, subscriptions: list[dict]) -> None:
    for count, subscription in enumerate(subscriptions, start=1):
        name = subscription['subscription']['job_specification']['name']
        print(f"({count}/{len(subscriptions)}) Submitting subscription: {name}")

        try:
            response = submit_subscription(session, hyp3_url, subscription)
            print(f"Got subscription ID: {response['subscription']['subscription_id']}")
        except requests.HTTPError as e:
            print('Failed to submit subscription:')
            print(e)

        print()


def disable_subscriptions(session: requests.Session, hyp3_url: str, subscription_ids: list[str]) -> None:
    for count, subscription_id in enumerate(subscription_ids, start=1):
        print(f"({count}/{len(subscription_ids)}) Disabling subscription with ID: {subscription_id}")

        try:
            disable_subscription(session, hyp3_url, subscription_id)
        except requests.HTTPError as e:
            print('Failed to disable subscription:')
            print(e)


def get_new_and_inactive_hazards(
        active_hazards: list[dict],
        enabled_subscriptions: dict) -> tuple[list[dict], list[str]]:

    hazard_uuids_to_subscription_ids = map_hazard_uuids_to_subscription_ids(enabled_subscriptions)
    subscribed_hazard_uuids = hazard_uuids_to_subscription_ids.keys()

    new_active_hazards = [hazard for hazard in active_hazards if hazard['uuid'] not in subscribed_hazard_uuids]

    active_hazard_uuids = frozenset(hazard['uuid'] for hazard in active_hazards)
    inactive_hazard_subscription_ids = [
        hazard_uuids_to_subscription_ids[uuid] for uuid in subscribed_hazard_uuids
        if uuid not in active_hazard_uuids
    ]

    return new_active_hazards, inactive_hazard_subscription_ids


def map_hazard_uuids_to_subscription_ids(subscriptions: dict) -> dict[str, str]:
    subscriptions_list = subscriptions['subscriptions']
    result = {
        hazard_uuid_from_subscription_name(sub['job_specification']['name']): sub['subscription_id']
        for sub in subscriptions_list
    }
    if len(result) != len(subscriptions_list):
        raise DuplicateSubscriptionNamesError(
            'Subscriptions list contains repeated job names. '
            'Each name should be unique and correspond to a hazard UUID. '
            'This error should never occur and indicates that something is broken.'
        )
    return result


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


def get_end_datetime_str(today: date) -> str:
    today_datetime = datetime(year=today.year, month=today.month, day=today.day, tzinfo=timezone.utc)
    end = today_datetime + timedelta(days=180)
    return str_from_datetime(end)


def subscription_name_from_hazard_uuid(uuid: str) -> str:
    return f'PDC-hazard-{uuid}'


def hazard_uuid_from_subscription_name(name: str) -> str:
    prefix = 'PDC-hazard-'
    assert name.startswith(prefix)
    return name.removeprefix(prefix)


def get_hyp3_subscription(hazard: dict, today: date, start_delta=timedelta(days=1)) -> dict:
    # TODO decide on appropriate default value for start_delta
    start = start_datetime_str_from_timestamp_in_ms(int(hazard['start_Date']), start_delta)
    end = get_end_datetime_str(today)
    aoi = get_aoi(hazard)
    name = subscription_name_from_hazard_uuid(hazard['uuid'])
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


def get_env_var(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise MissingEnvVarError(name)
    return val


def get_today() -> date:
    return datetime.utcnow().date()


def lambda_handler(event, context) -> None:
    pdc_api_url = PDC_URL_TEST
    hyp3_url = HYP3_URL_TEST

    print(f"PDC API URL: {pdc_api_url}")
    print(f"HyP3 API URL: {hyp3_url}")

    auth_token = get_env_var('PDC_HAZARDS_AUTH_TOKEN')
    earthdata_username = get_env_var('EARTHDATA_USERNAME')
    earthdata_password = get_env_var('EARTHDATA_PASSWORD')

    print(f"Earthdata user: {earthdata_username}\n")

    session = get_hyp3_api_session(earthdata_username, earthdata_password)

    print('Fetching active hazards')
    active_hazards = get_active_hazards(pdc_api_url, auth_token)
    print(f"Active hazards (before filtering): {len(active_hazards)}")

    active_hazards = filter_hazards(active_hazards)
    print(f"Active hazards (after filtering): {len(active_hazards)}\n")

    print('Fetching enabled subscriptions')
    enabled_subscriptions = get_enabled_subscriptions(session, hyp3_url)
    print(f"Enabled subscriptions: {len(enabled_subscriptions['subscriptions'])}\n")

    new_active_hazards, inactive_hazard_subscription_ids = \
        get_new_and_inactive_hazards(active_hazards, enabled_subscriptions)

    print(f"New active hazards: {len(new_active_hazards)}")
    print(f"Inactive hazards: {len(inactive_hazard_subscription_ids)}\n")

    # TODO check each existing subscription (except the ones for inactive hazards) and for any that will expire
    #  soon, update their end datetime (see get_end_datetime_str)

    today = get_today()
    new_subscriptions = [get_hyp3_subscription(hazard, today) for hazard in new_active_hazards]
    submit_subscriptions(session, hyp3_url, new_subscriptions)

    disable_subscriptions(session, hyp3_url, inactive_hazard_subscription_ids)
