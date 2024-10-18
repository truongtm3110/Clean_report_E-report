import requests
from helper.error_helper import log_error


def get_limit_range_all_platforms():
    endpoint = 'https://apiv3.metric.vn/market/search_basic?platformId=1&platformId=2&platformId=3&platformId=4&platformId=8'
    try:
        response = requests.get(endpoint, timeout=120)
        response_status = response.status_code
        if response_status == 200:
            response_json = response.json()
            data = response_json.get('data')
            platforms = data.get('platforms')
            result = {}
            for platform in platforms:
                if platform.get('platform_id') == 1:
                    result['shopee'] = {
                        'start_date': '20210801',
                        'end_date': platform.get('current_date')
                    }
                elif platform.get('platform_id') == 2:
                    result['lazada'] = {
                        'start_date': '20210801',
                        'end_date': platform.get('current_date')
                    }
                elif platform.get('platform_id') == 3:
                    result['tiki'] = {
                        'start_date': '20210801',
                        'end_date': platform.get('current_date')
                    }
                elif platform.get('platform_id') == 4:
                    result['sendo'] = {
                        'start_date': '20210801',
                        'end_date': platform.get('current_date')
                    }
                elif platform.get('platform_id') == 8:
                    result['tiktok'] = {
                        'start_date': '20220915',
                        'end_date': platform.get('current_date')
                    }
            return result
    except Exception as e:
        log_error(e)
    return None


if __name__ == '__main__':
    limit_range_all_platforms = get_limit_range_all_platforms()
    print(limit_range_all_platforms)