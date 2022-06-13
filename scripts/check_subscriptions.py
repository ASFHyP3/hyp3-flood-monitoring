import time

import boto3
import requests

import hyp3_floods


def get_subscriptions(session: requests.Session) -> dict:
    url = f'{hyp3_floods.HYP3_URL_TEST}/subscriptions'
    response = session.get(url)
    response.raise_for_status()
    return response.json()


def get_expected_subscriptions_count() -> int:
    client = boto3.client('logs')

    query_id = client.start_query(
        logGroupName='/aws/lambda/hyp3-flood-monitoring-test-Lambda-XUnL4S4ZZ2Cn',
        startTime=0,
        endTime=int(time.time() + 3600),
        queryString='fields @message | filter @message like /Got subscription id/ | stats count()'
    )['queryId']

    while (results := client.get_query_results(queryId=query_id))['status'] != 'Complete':
        time.sleep(0.5)

    count = int(results['results'][0][0]['value'])
    assert count == results['statistics']['recordsMatched']
    return count


def main() -> None:
    earthdata_username = hyp3_floods.get_env_var('EARTHDATA_USERNAME')
    earthdata_password = hyp3_floods.get_env_var('EARTHDATA_PASSWORD')

    session = hyp3_floods.HyP3SubscriptionsAPI._get_hyp3_api_session(earthdata_username, earthdata_password)
    subscriptions = get_subscriptions(session)['subscriptions']

    print(f'Expected subscriptions count: {get_expected_subscriptions_count()}')
    print(f'Actual subscriptions count: {len(subscriptions)}')


if __name__ == '__main__':
    main()
