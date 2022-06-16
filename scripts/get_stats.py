from datetime import datetime

import hyp3_floods
import _util


def get_jobless_subscriptions(subscriptions: list[dict], job_subscription_ids: frozenset[str]) -> list[dict]:
    return [
        subscription for subscription in subscriptions
        if not subscription['enabled']
        and subscription['subscription_id'] not in job_subscription_ids
    ]


def write_subscription_stats(subscriptions: list[dict], csv_path: str) -> None:
    rows = [('id', 'name', 'aoi', 'start', 'end', 'delta')]
    for subscription in subscriptions:
        start = subscription['search_parameters']['start']
        end = subscription['search_parameters']['end']
        rows.append((
            subscription['subscription_id'],
            subscription['job_specification']['name'],
            subscription['search_parameters']['intersectsWith'],
            start,
            end,
            parse_datetime(end) - parse_datetime(start),
        ))
    csv = '\n'.join(','.join(f'"{field}"' for field in row) for row in rows)

    with open(csv_path, 'w') as f:
        f.write(csv)

    print(f'Wrote {csv_path}')


def parse_datetime(datetime_str: str) -> datetime:
    return datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%SZ')


def main() -> None:
    earthdata_username = hyp3_floods.get_env_var('EARTHDATA_USERNAME')
    earthdata_password = hyp3_floods.get_env_var('EARTHDATA_PASSWORD')

    session = hyp3_floods.HyP3SubscriptionsAPI._get_hyp3_api_session(earthdata_username, earthdata_password)

    subscriptions = _util.get_subscriptions(session)['subscriptions']

    # start value is approximate time when rewritten lambda handler was deployed,
    # see https://asfdaac.atlassian.net/browse/TOOL-591
    jobs = _util.get_jobs(session, start='2022-06-07T00:30:00Z')['jobs']
    print(f'Jobs: {len(jobs)}')

    job_subscription_ids = frozenset(job['subscription_id'] for job in jobs)
    subscription_ids = frozenset(subscription['subscription_id'] for subscription in subscriptions)
    assert job_subscription_ids.issubset(subscription_ids)

    print(f'{len(job_subscription_ids)} / {len(subscriptions)} subscriptions have at least one job')

    jobless_subscriptions = get_jobless_subscriptions(subscriptions, job_subscription_ids)

    print(f'{len(jobless_subscriptions)} / {len(subscriptions)} subscriptions are disabled and jobless')

    write_subscription_stats(jobless_subscriptions, 'jobless-subscription-stats.csv')
    write_subscription_stats(subscriptions, 'subscription-stats.csv')


if __name__ == '__main__':
    main()
