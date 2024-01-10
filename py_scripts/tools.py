import requests

def get_information(endpoint, base_url='https://api-web.nhle.com'):
    base_url = f'{base_url}'
    endpoint = f'{endpoint}'
    full_url = f'{base_url}{endpoint}'

    response = requests.get(full_url)

    if response.status_code == 200:
        player_data = response.json()
        return player_data
    else:
        print(f'Error: Unable to fetch data. Status code: {response.status_code}')