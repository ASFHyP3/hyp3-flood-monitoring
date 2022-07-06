import argparse
from collections import namedtuple
from datetime import datetime

from dotenv import load_dotenv
from hyp3_sdk import HyP3

import hyp3_floods
import _util
import _logs


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


def get_summary(rows: list[Row], job_count: int, active_hazard_count: int, aoi_changes_count: int) -> str:
    # TODO more info: active hazards, aoi changes, etc.
    # TODO improve readability

    enabled_with_jobs = sum(row.enabled and row.jobs > 0 for row in rows)
    enabled_without_jobs = sum(row.enabled and row.jobs == 0 for row in rows)

    disabled_with_jobs = sum(not row.enabled and row.jobs > 0 for row in rows)
    disabled_without_jobs = sum(not row.enabled and row.jobs == 0 for row in rows)

    assert sum([enabled_with_jobs, enabled_without_jobs, disabled_with_jobs, disabled_without_jobs]) == len(rows)

    return '\n'.join([
        'Test system was deployed on 2022-06-06',
        f'Current active hazards: {active_hazard_count}',
        f'Total number of AOI changes: {aoi_changes_count}',
        '  - Note that we started logging AOI changes on 2022-06-15',
        f'Jobs: {job_count}',
        f'Subscriptions: {len(rows)}',
        f'Enabled subscriptions with at least one job: {enabled_with_jobs}',
        f'Enabled subscriptions with no jobs: {enabled_without_jobs}',
        f'Disabled subscriptions with at least one job: {disabled_with_jobs}',
        f'Disabled subscriptions with no jobs: {disabled_without_jobs}',
        ''
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


def parse_datetime(datetime_str: str) -> datetime:
    return datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%SZ')


def main() -> None:
    hyp3_url = hyp3_floods.get_env_var('HYP3_URL')
    earthdata_username = hyp3_floods.get_env_var('EARTHDATA_USERNAME')
    earthdata_password = hyp3_floods.get_env_var('EARTHDATA_PASSWORD')

    session = hyp3_floods.HyP3SubscriptionsAPI._get_hyp3_api_session(earthdata_username, earthdata_password)
    hyp3 = HyP3(hyp3_url, earthdata_username, earthdata_password)

    subscriptions = _util.get_subscriptions(session, hyp3_url)['subscriptions']
    jobs = hyp3.find_jobs()

    job_subscription_ids = [job.subscription_id for job in jobs]
    subscription_ids = frozenset(subscription['subscription_id'] for subscription in subscriptions)
    assert frozenset(job_subscription_ids).issubset(subscription_ids)

    _, active_hazard_count = _logs.get_active_hazards_count()
    aoi_changes_count = _logs.get_updated_aoi_count()

    rows = get_subscription_stats(subscriptions, job_subscription_ids)
    summary = get_summary(
        rows,
        job_count=len(jobs),
        active_hazard_count=active_hazard_count,
        aoi_changes_count=aoi_changes_count
    )

    write_csv([FIELDS, *rows], 'subscription-stats.csv')
    write_summary(summary, 'subscription-stats-summary.txt')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('dotenv_path')
    args = parser.parse_args()

    load_dotenv(dotenv_path=args.dotenv_path)
    main()
