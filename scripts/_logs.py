import time
from datetime import datetime, timezone

import boto3


LOG_GROUP = '/aws/lambda/hyp3-flood-monitoring-test-Lambda-XUnL4S4ZZ2Cn'

CLIENT = boto3.client('logs')

# Datetime when we last deleted jobs and subscriptions.
# https://asfdaac.atlassian.net/browse/TOOL-656?focusedCommentId=70170
START_TIME = 1657314000
START_DATETIME = datetime.fromtimestamp(START_TIME, tz=timezone.utc)
assert START_DATETIME \
       == datetime(year=2022, month=7, day=8, hour=21, minute=0, second=0, microsecond=0, tzinfo=timezone.utc), \
       START_DATETIME


def get_query_results(**kwargs) -> dict:
    query_id = CLIENT.start_query(**kwargs)['queryId']

    while (results := CLIENT.get_query_results(queryId=query_id))['status'] != 'Complete':
        time.sleep(0.5)

    return results


def get_expected_subscriptions_count() -> int:
    results = get_query_results(
        logGroupName=LOG_GROUP,
        startTime=START_TIME,
        endTime=int(time.time() + 3600),
        queryString='fields @message | filter @message like /Got subscription id/ | stats count()'
    )

    count = int(results['results'][0][0]['value'])
    assert count == results['statistics']['recordsMatched']
    return count


def get_active_hazards_count() -> tuple[str, int]:
    fields = get_query_results(
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


def get_updated_aoi_count() -> int:
    results = get_query_results(
        logGroupName=LOG_GROUP,
        startTime=START_TIME,
        endTime=int(time.time() + 3600),
        queryString='fields @message | filter @message like /Updating AOI for subscription/ | stats count()'
    )

    count = int(results['results'][0][0]['value'])
    assert count == results['statistics']['recordsMatched']
    return count
