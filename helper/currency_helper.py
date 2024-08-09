import requests

from app.db.session_metricseo import redis_session
from helper.redis_helper import cache_get, cache_set


URL = 'https://free.currconv.com'
API_KEY = '980105b1d1c9268ba388'
API_VERSION = 'v7'


def get_currency_country(country):
    global URL, API_KEY, API_VERSION
    cache_key = 'currency_data'
    currency_data = cache_get(redis_session=redis_session, key=cache_key)

    if currency_data is None:
        response = requests.get(url=f'''{URL}/api/{API_VERSION}/countries?apiKey={API_KEY}''')
        currency_data = dict()
        if response.status_code == 200 and response.json() is not None and response.json().get('results') is not None:
            for key in response.json().get('results'):
                item = response.json().get('results').get(key)
                k = item.get('id').casefold()
                v = item.get('currencyId')
                currency_data[k] = v
            cache_set(redis_session=redis_session, key=cache_key, value=currency_data, expired_second=24 * 3600)

    return currency_data.get(country)


def get_exchange_rate(from_country, to_country, value=1):
    global URL, API_KEY, API_VERSION

    from_unit = get_currency_country(country=from_country)
    to_unit = get_currency_country(country=to_country)
    cache_key = f'currency_data_{from_unit}_{to_unit}'
    exchanges_rate = cache_get(redis_session=redis_session, key=cache_key)

    if exchanges_rate is None:
        query = f'{from_unit}_{to_unit}'
        response = requests.get(url=f'''{URL}/api/{API_VERSION}/convert?q={query}&compact=ultra&apiKey={API_KEY}''')
        if response.status_code == 200 and response.json() is not None and response.json().get(query) is not None:
            exchanges_rate = float(response.json().get(query))
            cache_set(redis_session=redis_session, key=cache_key, value=exchanges_rate, expired_second=24 * 3600)
    else:
        exchanges_rate = float(exchanges_rate)

    return value * exchanges_rate


def get_exchange_rate_to_vnd(from_unit):
    exchange_rates = {
        "VND": 1.00000,
        "IDR": 1.575181,
        "MXN": 1145.213607,
        "MYR": 5579.323434,
        "PHP": 475.175823,
        "SGD": 17198.31084,
        "TWD": 810.068209,
        "THB": 736.62,
        "BRL": 4169.41,
        "COP": 6.12,
        "CLP": 30.86,
    }
    return exchange_rates.get(from_unit)


if __name__ == '__main__':
    print(get_exchange_rate(from_country='us', to_country='vn'))
