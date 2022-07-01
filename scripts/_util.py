import requests


# TODO handle pagination


def get_jobs(session: requests.Session, hyp3_url: str) -> dict:
    url = f'{hyp3_url}/jobs'
    response = session.get(url)
    response.raise_for_status()
    return response.json()


def get_subscriptions(session: requests.Session, hyp3_url: str) -> dict:
    url = f'{hyp3_url}/subscriptions'
    response = session.get(url)
    response.raise_for_status()
    return response.json()
