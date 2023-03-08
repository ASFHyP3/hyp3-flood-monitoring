import argparse
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Optional

import requests

PDC_URL = 'https://sentry.pdc.org'

# This value was chosen arbitrarily.
HAZARD_END_DATE_DELTA = timedelta(hours=3)

# Below constants are documented at
# https://github.com/ASFHyP3/hyp3-flood-monitoring#important-constants

HAZARD_START_DATE_DELTA = timedelta(hours=1)

HAZARD_START_DATE_MINIMUM = datetime(2022, 1, 1, tzinfo=timezone.utc)


class MissingEnvVar(Exception):
    pass


class DuplicateSubscriptionNames(Exception):
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

    def update_subscription(self, subscription_id: str, **kwargs) -> dict:
        url = f'{self._url}/subscriptions/{subscription_id}'
        response = self._session.patch(url, json=kwargs)
        response.raise_for_status()
        return response.json()


def get_aoi(pdc_auth_token: str, hazard_id: int) -> str:
    url = f'{PDC_URL}/hp_srv/services/hazard/{hazard_id}/alertGeography'
    response = requests.get(url, headers={'Authorization': f'Bearer {pdc_auth_token}'})
    response.raise_for_status()
    return response.json()['wkt']['text']


def get_active_hazards(pdc_auth_token: str) -> list[dict]:
    url = f'{PDC_URL}/hp_srv/services/hazards/t/json/get_active_hazards'
    response = requests.get(url, headers={'Authorization': f'Bearer {pdc_auth_token}'})
    response.raise_for_status()
    return response.json()


def filter_hazards(hazards: list[dict], current_time_in_ms: int) -> list[dict]:
    return [hazard for hazard in hazards if is_valid_hazard(hazard, current_time_in_ms)]


def is_valid_hazard(hazard: dict, current_time_in_ms: int) -> bool:
    return (
        hazard['type_ID'] == 'FLOOD'
        and hazard['category_ID'] == 'EVENT'
        and hazard['severity_ID'] == 'WARNING'
        and int(hazard['start_Date']) <= current_time_in_ms
    )


def get_existing_subscription(hyp3: HyP3SubscriptionsAPI, name: str) -> Optional[dict]:
    response = hyp3.get_subscriptions_by_name(name)
    subscriptions = response['subscriptions']
    if len(subscriptions) > 1:
        raise DuplicateSubscriptionNames(f'Got {len(subscriptions)} subscriptions with name {name} (expected 0 or 1)')
    return subscriptions[0] if subscriptions else None


def process_active_hazards(
        pdc_auth_token: str,
        hyp3: HyP3SubscriptionsAPI,
        active_hazards: list[dict],
        end: str,
        dry_run: bool) -> None:
    for count, hazard in enumerate(active_hazards, start=1):
        print(f'({count}/{len(active_hazards)}) Processing hazard {hazard["uuid"]}')
        try:
            process_active_hazard(pdc_auth_token, hyp3, hazard, end, dry_run=dry_run)
        except (requests.HTTPError, DuplicateSubscriptionNames) as e:
            print(f'Error while processing hazard {hazard["uuid"]}: {e}')


def process_active_hazard(
        pdc_auth_token: str,
        hyp3: HyP3SubscriptionsAPI,
        hazard: dict,
        end: str,
        dry_run: bool) -> None:
    name = subscription_name_from_hazard_uuid(hazard['uuid'])
    start = get_start_datetime_str(int(hazard['start_Date']))

    print(f'Fetching AOI for hazard ID: {hazard["hazard_ID"]}')
    aoi = get_aoi(pdc_auth_token, hazard['hazard_ID'])

    print(f'Fetching existing subscription with name: {name}')
    existing_subscription = get_existing_subscription(hyp3, name)

    if not existing_subscription:
        print(f'No existing subscription; submitting new subscription with name: {name}')
        new_subscription = prepare_new_subscription(start, end, aoi, name)
        response = hyp3.submit_subscription(new_subscription, validate_only=dry_run)
        subscription_id = response['subscription']['subscription_id']
        print(f'Got subscription id: {subscription_id}')
    else:
        log_updates(existing_subscription, start, aoi)
        if not dry_run:
            hyp3.update_subscription(
                subscription_id=existing_subscription['subscription_id'],
                start=start,
                end=end,
                intersectsWith=aoi,
                enabled=True,
            )


def log_updates(existing_subscription: dict, new_start: str, new_aoi: str) -> None:
    subscription_id = existing_subscription['subscription_id']
    print(f'Updating subscription with id: {subscription_id}')

    existing_start = existing_subscription['search_parameters']['start']
    existing_aoi = existing_subscription['search_parameters']['intersectsWith']

    if existing_start != new_start:
        print(f'Updating start datetime for subscription {subscription_id} from {existing_start} to {new_start}')

    if existing_aoi != new_aoi:
        print(f'Updating AOI for subscription {subscription_id} from {existing_aoi} to {new_aoi}')


def str_from_datetime(date_time: datetime) -> str:
    assert date_time.tzinfo == timezone.utc
    datetime_str = date_time.isoformat()
    assert datetime_str.endswith('+00:00')
    return datetime_str.removesuffix('+00:00') + 'Z'


def get_start_datetime_str(
        timestamp_in_ms: int,
        delta: timedelta = HAZARD_START_DATE_DELTA,
        minimum: datetime = HAZARD_START_DATE_MINIMUM) -> str:
    start_datetime = max(
        datetime.fromtimestamp(timestamp_in_ms // 1000, tz=timezone.utc) - delta,
        minimum
    )
    return str_from_datetime(start_datetime)


def get_end_datetime_str(current_time_in_ms: int, delta: timedelta = HAZARD_END_DATE_DELTA) -> str:
    return str_from_datetime(datetime.fromtimestamp(current_time_in_ms // 1000, tz=timezone.utc) + delta)


def subscription_name_from_hazard_uuid(uuid: str) -> str:
    return f'PDC-hazard-{uuid}'


def prepare_new_subscription(start: str, end: str, aoi: str, name: str) -> dict:
    # Adapted from:
    # https://github.com/ASFHyP3/hyp3-nasa-disasters/blob/main/data_management/pdc_brazil.json
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


def get_current_time_in_ms() -> int:
    return int(time.time() * 1000)


def lambda_handler(event, context) -> None:
    main(dry_run=False)


def main(dry_run: bool) -> None:
    if dry_run:
        print('(DRY RUN)')

    pdc_auth_token = get_env_var('PDC_HAZARDS_AUTH_TOKEN')
    hyp3_url = get_env_var('HYP3_URL')
    earthdata_username = get_env_var('EARTHDATA_USERNAME')
    earthdata_password = get_env_var('EARTHDATA_PASSWORD')

    print(f'PDC API URL: {PDC_URL}')
    print(f'HyP3 API URL: {hyp3_url}')
    print(f'Earthdata user: {earthdata_username}')

    hyp3 = HyP3SubscriptionsAPI(hyp3_url, earthdata_username, earthdata_password)

    print('Fetching active hazards')
    active_hazards = get_active_hazards(pdc_auth_token)
    print(f'Active hazards (before filtering): {len(active_hazards)}')

    current_time_in_ms = get_current_time_in_ms()
    active_hazards = filter_hazards(active_hazards, current_time_in_ms)
    print(f'Active hazards (after filtering): {len(active_hazards)}')

    end = get_end_datetime_str(current_time_in_ms)
    process_active_hazards(pdc_auth_token, hyp3, active_hazards, end, dry_run=dry_run)


if __name__ == '__main__':
    from dotenv import load_dotenv

    parser = argparse.ArgumentParser()
    parser.add_argument('dotenv_path')
    parser.add_argument('--no-dry-run', action='store_true')
    args = parser.parse_args()

    load_dotenv(dotenv_path=args.dotenv_path)
    main(dry_run=(not args.no_dry_run))
