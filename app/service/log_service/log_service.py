from datetime import datetime

import requests

from helper.error_helper import log_error


def push_log(data, user_id: int = None, endpoint: str = None):
    log_url = "https://log.beecost.com/t_common/log/common"
    try:
        data = {
            "source": "market_api_platform",
            "client_id": "market_api_platform",
            "action": "market_api_platform_log",
            "action_meta": {
                "user_id": user_id,
                "endpoint": endpoint,
                "data": data
            },
            "updated_at": int(datetime.now().timestamp())
        }
        response = requests.post(log_url, json=data, timeout=2)
        if response.status_code == 200:
            return True
    except Exception as e:
        log_error(e)
        return False
    return False
