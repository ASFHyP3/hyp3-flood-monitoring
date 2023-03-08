import time

import boto3


CLIENT = boto3.client('logs')


def get_query_results(**kwargs) -> dict:
    query_id = CLIENT.start_query(**kwargs)['queryId']

    while (results := CLIENT.get_query_results(queryId=query_id))['status'] != 'Complete':
        time.sleep(0.5)

    return results


def get_active_hazards_count(log_group: str) -> tuple[str, int]:
    fields = get_query_results(
        logGroupName=log_group,
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
