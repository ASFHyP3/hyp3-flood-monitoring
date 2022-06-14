import hyp3_floods
import _util


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


if __name__ == '__main__':
    main()
