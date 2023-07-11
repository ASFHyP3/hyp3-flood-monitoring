import argparse

from dotenv import load_dotenv

import hyp3_floods
import _util


def main():
    hyp3_url = hyp3_floods.get_env_var('HYP3_URL')
    earthdata_username = hyp3_floods.get_env_var('EARTHDATA_USERNAME')
    earthdata_password = hyp3_floods.get_env_var('EARTHDATA_PASSWORD')

    print(f'HyP3 API URL: {hyp3_url}')
    print(f'Earthdata user: {earthdata_username}\n')

    assert 'hyp3-pdc' in hyp3_url

    hyp3 = hyp3_floods.HyP3SubscriptionsAPI(hyp3_url, earthdata_username, earthdata_password)
    subscriptions = _util.get_subscriptions(hyp3._session, hyp3_url)['subscriptions']

    enabled_subscriptions = [subscription for subscription in subscriptions if subscription['enabled']]
    for count, subscription in enumerate(enabled_subscriptions, start=1):
        print(f'{count}/{len(enabled_subscriptions)} Disabling subscription {subscription["subscription_id"]}')
        hyp3.update_subscription(subscription['subscription_id'], enabled=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('dotenv_path')
    args = parser.parse_args()

    load_dotenv(dotenv_path=args.dotenv_path)
    main()
