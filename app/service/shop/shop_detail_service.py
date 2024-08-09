import asyncio

import aiohttp

from app.core.config import settings
from helper.error_helper import log_error
from helper.logger_helper import LoggerSimple
from helper.user_agent_helper import get_agent_metric

logger = LoggerSimple(name=__name__).logger


async def get_shop_detail(shop_base_id: str):
    url_api_shop_detail_internal = f'{settings.URL_API_BEECOST_INTERNAL}/shop/detailrt?shop_base_id={shop_base_id}'
    response_data = None
    try:
        async with aiohttp.ClientSession() as session:
            headers = {
                "user-agent": get_agent_metric(),
            }
            response = await session.get(url_api_shop_detail_internal, headers=headers)
            response_data = await response.json()
    except Exception as e:
        log_error(e)
    return response_data


async def get_shop_base_id_by_shop_url(url_shop):
    try:
        async with aiohttp.ClientSession() as session:
            headers = {
                "user-agent": get_agent_metric(),
            }
            response = await session.get("https://apiv2.beecost.vn/shop/detail_by_url", params={
                "url": url_shop
            }, headers=headers)
            # logger.info(response.status)
            if response.status == 200:
                response_data = await response.json()
                shop_base = response_data.get("data", {}).get("shop_base", {})
                if shop_base is not None:
                    return shop_base.get("shop_base_id")
    except Exception as e:
        log_error(e)
    return None


if __name__ == '__main__':
    # shop_base_id = '1__167961044'
    url_shop = 'https://shopee.vn/shoptida_flagship_store'
    asyncio.run(
        # get_shop_detail(shop_base_id=shop_base_id)
        get_shop_base_id_by_shop_url(url_shop=url_shop)
    )
