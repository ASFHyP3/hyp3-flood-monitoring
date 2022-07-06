import argparse
import os

from dotenv import load_dotenv

import hyp3_floods
import _util
import _logs


def count_updated_subscriptions(subscriptions: list[dict]) -> tuple[str, int]:
    current_end = max(subscription['search_parameters']['end'] for subscription in subscriptions)
    return (
        current_end,
        sum(1 for subscription in subscriptions if subscription['search_parameters']['end'] == current_end)
    )


def main() -> None:
    hyp3_url = hyp3_floods.get_env_var('HYP3_URL')
    earthdata_username = hyp3_floods.get_env_var('EARTHDATA_USERNAME')
    earthdata_password = hyp3_floods.get_env_var('EARTHDATA_PASSWORD')

    session = hyp3_floods.HyP3SubscriptionsAPI._get_hyp3_api_session(earthdata_username, earthdata_password)
    subscriptions = _util.get_subscriptions(session, hyp3_url)['subscriptions']

    print(f'Total subscriptions (from logs):     {_logs.get_expected_subscriptions_count()}')
    print(f'Total subscriptions (from HyP3 API): {len(subscriptions)}\n')

    active_hazards_timestamp, active_hazards_count = _logs.get_active_hazards_count()
    updated_subscriptions_end, updated_subscriptions_count = count_updated_subscriptions(subscriptions)

    print(f'Active hazards (from logs):               {active_hazards_count}')
    print(f'Up-to-date subscriptions (from HyP3 API): {updated_subscriptions_count}\n')

    print(f'Active hazards log timestamp:          {active_hazards_timestamp}')
    print(f'Up-to-date subscriptions end datetime: {updated_subscriptions_end}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('dotenv_path')
    args = parser.parse_args()

    load_dotenv(dotenv_path=args.dotenv_path)
    main()
