import time

import boto3
import requests

import hyp3_floods

LOG_GROUP = '/aws/lambda/hyp3-flood-monitoring-test-Lambda-XUnL4S4ZZ2Cn'


def get_subscriptions(session: requests.Session) -> dict:
    url = f'{hyp3_floods.HYP3_URL_TEST}/subscriptions'
    response = session.get(url)
    response.raise_for_status()
    return response.json()


def get_query_results(client, **kwargs) -> dict:
    query_id = client.start_query(**kwargs)['queryId']

    while (results := client.get_query_results(queryId=query_id))['status'] != 'Complete':
        time.sleep(0.5)

    return results


def get_expected_subscriptions_count(client) -> int:
    results = get_query_results(
        client,
        logGroupName=LOG_GROUP,
        startTime=0,
        endTime=int(time.time() + 3600),
        queryString='fields @message | filter @message like /Got subscription id/ | stats count()'
    )

    count = int(results['results'][0][0]['value'])
    assert count == results['statistics']['recordsMatched']
    return count


def get_active_hazards_count(client) -> tuple[str, int]:
    fields = get_query_results(
        client,
        logGroupName=LOG_GROUP,
        startTime=int(time.time() - 3600),
        endTime=int(time.time() + 3600),
        queryString=(
            'fields @timestamp, @message | sort @timestamp desc | limit 1 | filter @message like /after filtering/'
        )
    )['results'][0]

    assert fields[0]['field'] == '@timestamp'
    timestamp = fields[0]['value']

    assert fields[1]['field'] == '@message'
    message = fields[1]['value'].strip().split(':')

    assert message[0] == 'Active hazards (after filtering)'
    count = int(message[1])

    return timestamp, count


def count_updated_subscriptions(subscriptions: list[dict]) -> tuple[str, int]:
    current_end = max(subscription['search_parameters']['end'] for subscription in subscriptions)
    return (
        current_end,
        sum(1 for subscription in subscriptions if subscription['search_parameters']['end'] == current_end)
    )


def main() -> None:
    earthdata_username = hyp3_floods.get_env_var('EARTHDATA_USERNAME')
    earthdata_password = hyp3_floods.get_env_var('EARTHDATA_PASSWORD')

    session = hyp3_floods.HyP3SubscriptionsAPI._get_hyp3_api_session(earthdata_username, earthdata_password)
    subscriptions = get_subscriptions(session)['subscriptions']

    client = boto3.client('logs')

    print(f'Total subscriptions (from logs):     {get_expected_subscriptions_count(client)}')
    print(f'Total subscriptions (from HyP3 API): {len(subscriptions)}\n')

    active_hazards_timestamp, active_hazards_count = get_active_hazards_count(client)
    updated_subscriptions_end, updated_subscriptions_count = count_updated_subscriptions(subscriptions)

    print(f'Active hazards (from logs):               {active_hazards_count}')
    print(f'Up-to-date subscriptions (from HyP3 API): {updated_subscriptions_count}\n')

    print(f'Active hazards log timestamp:          {active_hazards_timestamp}')
    print(f'Up-to-date subscriptions end datetime: {updated_subscriptions_end}')


if __name__ == '__main__':
    main()
