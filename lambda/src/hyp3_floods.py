import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests

PDC_URL = 'https://sentry.pdc.org'

# TODO make url configurable
HYP3_URL_TEST = 'https://hyp3-test-api.asf.alaska.edu'
HYP3_URL_PROD = 'https://hyp3-watermap.asf.alaska.edu'


class MissingEnvVar(Exception):
    pass


class DuplicateSubscriptionNames(Exception):
    pass


class OutdatedAOI(Exception):
    pass


class HyP3SubscriptionsAPI:

    def __init__(self, api_url: str, username: str, password: str):
        self._url = api_url
        self._session = self._get_hyp3_api_session(username, password)

    @staticmethod
    def _get_hyp3_api_session(username: str, password: str) -> requests.Session:
        url = 'https://urs.earthdata.nasa.gov/oauth/authorize?response_type=code&client_id=BO_n7nTIlMljdvU6kRRB3g' \
              '&redirect_uri=https://auth.asf.alaska.edu/login&app_type=401'
        session = requests.Session()
        response = session.get(url, auth=(username, password))
        response.raise_for_status()
        return session

    def get_subscriptions_by_name(self, name: str) -> dict:
        url = f'{self._url}/subscriptions'
        response = self._session.get(url, params={'name': name})
        response.raise_for_status()
        return response.json()

    def submit_subscription(self, subscription: dict, validate_only=False) -> dict:
        url = f'{self._url}/subscriptions'
        payload = {'subscription': subscription, 'validate_only': validate_only}
        response = self._session.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def update_subscription(self, subscription_id: str, end: str) -> dict:
        url = f'{self._url}/subscriptions/{subscription_id}'
        response = self._session.patch(url, json={'enabled': True, 'end': end})
        response.raise_for_status()
        return response.json()


def get_active_hazards(auth_token: str) -> list[dict]:
    url = f'{PDC_URL}/hp_srv/services/hazards/t/json/get_active_hazards'
    response = requests.get(url, headers={'Authorization': f'Bearer {auth_token}'})
    response.raise_for_status()
    return response.json()


def get_existing_subscription(hyp3: HyP3SubscriptionsAPI, name: str) -> Optional[dict]:
    response = hyp3.get_subscriptions_by_name(name)
    subscriptions = response['subscriptions']
    if len(subscriptions) > 1:
        raise DuplicateSubscriptionNames(f"Got {len(subscriptions)} subscriptions with name {name} (expected 0 or 1)")
    return subscriptions[0] if subscriptions else None


def process_active_hazards(hyp3: HyP3SubscriptionsAPI, active_hazards: list[dict], now: datetime) -> None:
    # TODO tests?
    for count, hazard in enumerate(active_hazards, start=1):
        print(f"({count}/{len(active_hazards)}) Processing hazard {hazard['uuid']}")
        try:
            process_active_hazard(hyp3, hazard, now)
        except (requests.HTTPError, DuplicateSubscriptionNames, OutdatedAOI) as e:
            print(f"Error while processing hazard: {e}")


# TODO rename functions to make it clear what's fetching via network (see the get_ functions)


def process_active_hazard(hyp3: HyP3SubscriptionsAPI, hazard: dict, now: datetime) -> None:
    name = subscription_name_from_hazard_uuid(hazard['uuid'])
    print(f"Fetching existing subscription with name: {name}")
    existing_subscription = get_existing_subscription(hyp3, name)

    if existing_subscription:
        compare_aoi(existing_subscription, get_aoi(hazard))
        subscription_id = existing_subscription['subscription_id']
        print(f"Updating subscription with id: {subscription_id}")
        hyp3.update_subscription(subscription_id, get_end_datetime_str(now))
    else:
        print('No existing subscription; submitting new subscription')
        new_subscription = get_hyp3_subscription(hazard, now)
        response = hyp3.submit_subscription(new_subscription)
        subscription_id = response['subscription']['subscription_id']
        print(f"Got subscription id: {subscription_id}")


def compare_aoi(existing_subscription: dict, new_aoi: str) -> None:
    subscription_id = existing_subscription['subscription_id']
    existing_aoi = existing_subscription['search_parameters']['intersectsWith']
    if existing_aoi != new_aoi:
        raise OutdatedAOI(
            f"Subscription with id {subscription_id} has AOI {existing_aoi} but the current AOI is {new_aoi}."
            " This indicates that we need to implement a way to update subscription AOI."
        )


def filter_hazards(hazards: list[dict]) -> list[dict]:
    # TODO should we include other hazard types?
    return [hazard for hazard in hazards if hazard['type_ID'] == 'FLOOD']


def get_aoi(hazard: dict) -> str:
    # TODO get real aoi
    return f"POINT({hazard['longitude']} {hazard['latitude']})"


def str_from_datetime(date_time: datetime) -> str:
    assert date_time.tzinfo == timezone.utc
    datetime_str = date_time.isoformat()
    assert datetime_str.endswith('+00:00')
    return datetime_str.removesuffix('+00:00') + 'Z'


def get_start_datetime_str(timestamp_in_ms: int, delta: timedelta) -> str:
    return str_from_datetime(datetime.fromtimestamp(timestamp_in_ms // 1000, tz=timezone.utc) - delta)


def get_end_datetime_str(now: datetime) -> str:
    end = now + timedelta(hours=3) - timedelta(microseconds=now.microsecond)
    return str_from_datetime(end)


def subscription_name_from_hazard_uuid(uuid: str) -> str:
    return f'PDC-hazard-{uuid}'


def hazard_uuid_from_subscription_name(name: str) -> str:
    prefix = 'PDC-hazard-'
    assert name.startswith(prefix)
    return name.removeprefix(prefix)


def get_hyp3_subscription(hazard: dict, now: datetime, start_delta=timedelta(days=1)) -> dict:
    # TODO decide on appropriate default value for start_delta
    start = get_start_datetime_str(int(hazard['start_Date']), start_delta)
    end = get_end_datetime_str(now)
    aoi = get_aoi(hazard)
    name = subscription_name_from_hazard_uuid(hazard['uuid'])
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


def get_env_var(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise MissingEnvVar(name)
    return val


def get_now() -> datetime:
    return datetime.now(tz=timezone.utc)


# TODO remove extra newlines in logging
def lambda_handler(event, context) -> None:
    hyp3_url = HYP3_URL_TEST

    print(f"PDC API URL: {PDC_URL}")
    print(f"HyP3 API URL: {hyp3_url}")

    auth_token = get_env_var('PDC_HAZARDS_AUTH_TOKEN')
    earthdata_username = get_env_var('EARTHDATA_USERNAME')
    earthdata_password = get_env_var('EARTHDATA_PASSWORD')

    print(f"Earthdata user: {earthdata_username}\n")

    hyp3 = HyP3SubscriptionsAPI(hyp3_url, earthdata_username, earthdata_password)

    print('Fetching active hazards')
    active_hazards = get_active_hazards(auth_token)
    print(f"Active hazards (before filtering): {len(active_hazards)}")

    active_hazards = filter_hazards(active_hazards)
    print(f"Active hazards (after filtering): {len(active_hazards)}\n")

    # TODO refactor to only call get_end_datetime_str here?
    process_active_hazards(hyp3, active_hazards, get_now())

    # TODO finish lambda handler, remove unused functions and tests, update lambda handler tests
