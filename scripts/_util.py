import requests

from hyp3_floods import HYP3_URL_TEST


def get_subscriptions(session: requests.Session) -> dict:
    url = f'{HYP3_URL_TEST}/subscriptions'
    response = session.get(url)
    response.raise_for_status()
    return response.json()
