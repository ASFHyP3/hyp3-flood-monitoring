import hyp3_floods
import _util

# TODO query for jobs/subs since rewritten lambda handler


def main() -> None:
    earthdata_username = hyp3_floods.get_env_var('EARTHDATA_USERNAME')
    earthdata_password = hyp3_floods.get_env_var('EARTHDATA_PASSWORD')

    session = hyp3_floods.HyP3SubscriptionsAPI._get_hyp3_api_session(earthdata_username, earthdata_password)

    subscriptions = _util.get_subscriptions(session)['subscriptions']
    jobs = _util.get_jobs(session)['jobs']

    job_subscription_ids = frozenset(job['subscription_id'] for job in jobs)
    count = sum(1 for subscription in subscriptions if subscription['subscription_id'] in job_subscription_ids)

    print(f'{count} / {len(subscriptions)} subscriptions have at least one job')


if __name__ == '__main__':
    main()
