import requests

from helper.error_helper import log_error


PLATFORM_MAP = {
    "shopee": 1,
    "lazada": 2,
    "tiki": 3,
    "tiktok": 8
}


def get_metric_gift_product_filter(platform: str) -> list:
    try:
        endpoint = f"https://apiv3.metric.vn/metric_blacklist/gift_product/"
        params = {"platform": PLATFORM_MAP.get(platform)} if platform else None  # Query parameters
        headers = {"Content-Type": "application/json"}
        response = requests.get(endpoint, params=params, headers=headers)
        response.raise_for_status()
        response_json = response.json()
        data = response_json.get('data')
        return [item.get('keyword') for item in data] if response_json.get("status") == "success" else []
    except Exception as e:
        log_error(e)
        return []


def get_metric_blacklist_keyword_filter(platform: str) -> list:
    try:
        endpoint = f"https://apiv3.metric.vn/metric_blacklist/blacklist_keyword/"
        params = {"platformId": PLATFORM_MAP.get(platform)} if platform else None
        headers = {"Content-Type": "application/json"}

        response = requests.get(endpoint, params=params, headers=headers)
        response.raise_for_status()
        response_json = response.json()
        data = response_json.get('data')
        return data if response_json.get("status") == "success" else []
    except Exception as e:
        log_error(e)
        return []
