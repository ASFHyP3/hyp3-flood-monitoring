import json
import os

import requests

# TODO make url configurable, or just use prod?
TEST_API_URL = 'https://testsentry.pdc.org'
PROD_API_URL = 'https://sentry.pdc.org'


def get_active_hazards(auth_token: str) -> list[dict]:
    url = f'{TEST_API_URL}/hp_srv/services/hazards/t/json/get_active_hazards'
    response = requests.get(url, headers={'Authorization': f'Bearer {auth_token}'})
    response.raise_for_status()
    return response.json()


def main() -> None:
    auth_token = os.getenv('PDC_HAZARDS_AUTH_TOKEN')
    assert auth_token  # TODO raise appropriate exception

    hazards = get_active_hazards(auth_token)
    print(f'Hazards: {len(hazards)}')

    with open('hazards.json', 'w') as f:
        f.write(json.dumps(hazards))


if __name__ == '__main__':
    main()
