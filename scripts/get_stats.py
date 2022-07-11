import argparse
import urllib.parse
from collections import namedtuple
from datetime import datetime, timezone

import boto3
from hyp3_sdk import HyP3

import hyp3_floods
import _util
import _logs


TARGET_BUCKET = 'hyp3-flood-monitoring-stats'

FIELDS = ('id', 'name', 'aoi', 'creation_date', 'start', 'end', 'delta', 'jobs', 'enabled')

Row = namedtuple('Row', FIELDS)


def get_subscription_stats(subscriptions: list[dict], job_subscription_ids: list[str]) -> list[Row]:
    rows = []
    for subscription in subscriptions:
        start = subscription['search_parameters']['start']
        end = subscription['search_parameters']['end']
        rows.append(Row(
            id=subscription['subscription_id'],
            name=subscription['job_specification']['name'],
            aoi=subscription['search_parameters']['intersectsWith'],
            creation_date=subscription['creation_date'],
            start=start,
            end=end,
            delta=parse_datetime(end) - parse_datetime(start),
            jobs=sum(job_sub_id == subscription['subscription_id'] for job_sub_id in job_subscription_ids),
            enabled=subscription['enabled'],
        ))
    rows.sort(key=lambda row: row.creation_date)
    return rows


def parse_datetime(datetime_str: str) -> datetime:
    return datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%SZ')


def get_summary(
        rows: list[Row],
        now: str,
        job_count: int,
        active_hazards_count: int,
        active_hazards_timestamp: str,
        aoi_changes_count: int) -> str:

    enabled_with_jobs = sum(row.enabled and row.jobs > 0 for row in rows)
    enabled_without_jobs = sum(row.enabled and row.jobs == 0 for row in rows)

    disabled_with_jobs = sum(not row.enabled and row.jobs > 0 for row in rows)
    disabled_without_jobs = sum(not row.enabled and row.jobs == 0 for row in rows)

    assert sum([enabled_with_jobs, enabled_without_jobs, disabled_with_jobs, disabled_without_jobs]) == len(rows)

    return '\n'.join([
        f'Current timestamp: {now}\n',

        f'Active hazards: {active_hazards_count}',
        f'  - Log message timestamp: {active_hazards_timestamp}\n',

        f'Summary from {_logs.START_DATETIME.isoformat()} to present:\n',

        f'Jobs: {job_count}\n',

        f'Subscriptions: {len(rows)}',
        f'  - Active subscriptions with at least one job: {enabled_with_jobs}',
        f'  - Active subscriptions with no jobs: {enabled_without_jobs}',
        f'  - Expired subscriptions with at least one job: {disabled_with_jobs}',
        f'  - Expired subscriptions with no jobs: {disabled_without_jobs}\n',

        f'AOI changes: {aoi_changes_count}\n',

        ('Note that the number of active subscriptions is greater than the number of active hazards, '
         'as a subscription remains active for a few days after the corresponding hazard expires, in case '
         'any new data becomes available that was acquired during the hazard\'s lifetime.\n'),
    ])


def write_csv(rows: list[tuple], path: str) -> None:
    csv = '\n'.join(','.join(f'"{field}"' for field in row) for row in rows)

    with open(path, 'w') as f:
        f.write(csv)

    print(f'Wrote {path}')


def write_summary(summary: str, path: str) -> None:
    with open(path, 'w') as f:
        f.write(summary)

    print(f'Wrote {path}')


def publish_sns_message(topic_arn: str, msg: str) -> dict:
    sns = boto3.client('sns')
    return sns.publish(
        TopicArn=topic_arn,
        Message=msg,
        Subject='HyP3/PDC flood monitoring stats',
    )


def main(upload: bool) -> None:
    hyp3_url = hyp3_floods.get_env_var('HYP3_URL')
    earthdata_username = hyp3_floods.get_env_var('EARTHDATA_USERNAME')
    earthdata_password = hyp3_floods.get_env_var('EARTHDATA_PASSWORD')
    topic_arn = hyp3_floods.get_env_var('SNS_TOPIC_ARN')

    session = hyp3_floods.HyP3SubscriptionsAPI._get_hyp3_api_session(earthdata_username, earthdata_password)
    hyp3 = HyP3(hyp3_url, earthdata_username, earthdata_password)

    print('Fetching subscriptions')
    subscriptions = _util.get_subscriptions(session, hyp3_url)['subscriptions']

    print('Fetching jobs')
    jobs = hyp3.find_jobs()

    job_subscription_ids = [job.subscription_id for job in jobs]
    subscription_ids = frozenset(subscription['subscription_id'] for subscription in subscriptions)
    assert frozenset(job_subscription_ids).issubset(subscription_ids)

    rows = get_subscription_stats(subscriptions, job_subscription_ids)

    print('Querying logs')
    active_hazards_timestamp, active_hazards_count = _logs.get_active_hazards_count()
    aoi_changes_count = _logs.get_updated_aoi_count()

    now = datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()
    summary = get_summary(
        rows,
        now=now,
        job_count=len(jobs),
        active_hazards_count=active_hazards_count,
        active_hazards_timestamp=active_hazards_timestamp,
        aoi_changes_count=aoi_changes_count
    )

    csv_name = f'subscription-stats-{now}.csv'
    summary_name = f'subscription-stats-summary-{now}.txt'

    write_csv([FIELDS, *rows], csv_name)
    write_summary(summary, summary_name)

    if upload:
        print('Uploading to S3')
        s3 = boto3.resource('s3')
        s3.Bucket(TARGET_BUCKET).upload_file(Filename=csv_name, Key=f'{now}/{csv_name}')
        s3.Bucket(TARGET_BUCKET).upload_file(Filename=summary_name, Key=f'{now}/{summary_name}')

        download_link = lambda filename: \
            f'https://{TARGET_BUCKET}.s3.us-west-2.amazonaws.com{urllib.parse.quote(f"/{now}/{filename}")}'  # noqa: E731 E501
        msg = (
            f'Subscription stats (CSV):\n{download_link(csv_name)}\n\n'
            f'Summary (text):\n{download_link(summary_name)}\n\n'
            f'{"-" * 50}\n\n{summary}'
        )

        print('Publishing SNS message')
        response = publish_sns_message(topic_arn, msg)
        print(response)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--env')
    parser.add_argument('--upload', action='store_true')
    args = parser.parse_args()

    if args.env is not None:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=args.env)

    main(args.upload)
