import argparse

from dotenv import load_dotenv

import hyp3_floods
import _util
import _logs


def count_updated_subscriptions(subscriptions: list[dict]) -> tuple[str, int]:
    current_end = max(subscription['search_parameters']['end'] for subscription in subscriptions)
    return (
        current_end,
        sum(subscription['search_parameters']['end'] == current_end for subscription in subscriptions)
    )


def main() -> None:
    hyp3_url = hyp3_floods.get_env_var('HYP3_URL')
    earthdata_username = hyp3_floods.get_env_var('EARTHDATA_USERNAME')
    earthdata_password = hyp3_floods.get_env_var('EARTHDATA_PASSWORD')

    print(f'HyP3 API URL: {hyp3_url}')
    print(f'Earthdata user: {earthdata_username}\n')

    if 'hyp3-test-api' in hyp3_url:
        log_group = '/aws/lambda/hyp3-flood-monitoring-test-Lambda-XUnL4S4ZZ2Cn'
    elif 'hyp3-watermap' in hyp3_url:
        log_group = '/aws/lambda/hyp3-flood-monitoring-Lambda-q7JXd48mgEhC'
    else:
        raise ValueError('HyP3 URL not recognized')

    session = hyp3_floods.HyP3SubscriptionsAPI._get_hyp3_api_session(earthdata_username, earthdata_password)
    subscriptions = _util.get_subscriptions(session, hyp3_url)['subscriptions']

    active_hazards_timestamp, active_hazards_count = _logs.get_active_hazards_count(log_group)
    updated_subscriptions_end, updated_subscriptions_count = count_updated_subscriptions(subscriptions)

    print(
        'Up-to-date subscriptions are those subscriptions with the\n'
        'most recent end datetime among the list of HyP3\n'
        f'subscriptions for the {earthdata_username} user.\n\n'

        'The number of up-to-date subscriptions should equal the\n'
        'number of active hazards:\n'
    )

    print(f'    Active hazards (from logs):               {active_hazards_count}')
    print(f'    Up-to-date subscriptions (from HyP3 API): {updated_subscriptions_count}\n')

    print(
        'Furthermore, all up-to-date subscriptions should have an end\n'
        f'datetime approximately {hyp3_floods.HAZARD_END_DATE_DELTA} ahead of the time of the most\n'
        'recent flood monitoring Lambda execution:\n'
    )

    print(f'    Most recent Lambda execution, approximate (from logs): {active_hazards_timestamp}')
    print(f'    Up-to-date subscription end datetime (from HyP3 API):  {updated_subscriptions_end}\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('dotenv_path')
    args = parser.parse_args()

    load_dotenv(dotenv_path=args.dotenv_path)
    main()
