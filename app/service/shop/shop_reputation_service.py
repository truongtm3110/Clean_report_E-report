import asyncio
import datetime

from elasticsearch import Elasticsearch

from app.service.shop.shop_detail_service import get_shop_detail
from app.service.extractor.extract_information import extract_information_from_text
from helper.datetime_helper import convert_str_to_datetime
from helper.elasticsearch_helper import search_v2
# from helper.elasticsearch_async_helper import search_v2
from helper.logger_helper import LoggerSimple
from helper.type_helper import cast_int

logger = LoggerSimple(name=__name__).logger
import urllib3

urllib3.disable_warnings()

config_score_reputation = {
    'score_review_total': [
        {'lt': 500, 'score': 0},
        {'gte': 500, 'lt': 1_000, 'score': 1},
        {'gte': 1000, 'lt': 2_000, 'score': 2},
        {'gte': 2000, 'lt': 5_000, 'score': 3},
        {'gte': 5000, 'lt': 10_000, 'score': 4},
        {'gte': 10000, 'score': 5}
    ],
    'score_year_old': [
        {'lt': 1, 'score': 0},
        {'gte': 1, 'lt': 2, 'score': 1},
        {'gte': 2, 'lt': 3, 'score': 2},
        {'gte': 3, 'lt': 4, 'score': 3},
        {'gte': 4, 'lt': 5, 'score': 4},
        {'gte': 5, 'score': 5}
    ],
    'score_revenue_monthly': [
        {'lt': 10_000_000, 'score': 0},
        {'gte': 10_000_000, 'lt': 30_000_000, 'score': 1},
        {'gte': 30_000_000, 'lt': 50_000_000, 'score': 2},
        {'gte': 50_000_000, 'lt': 100_000_000, 'score': 3},
        {'gte': 100_000_000, 'lt': 500_000_000, 'score': 4},
        {'gte': 50_000_000, 'score': 5},
    ],
    'score_review_avg': [
        {'lt': 3.55, 'score': 0},
        {'gte': 3.55, 'lt': 4.65, 'score': 1},
        {'gte': 4.65, 'lt': 4.75, 'score': 2},
        {'gte': 4.75, 'lt': 4.85, 'score': 3},
        {'gte': 4.85, 'lt': 4.95, 'score': 4},
        {'gte': 4.95, 'score': 5},
    ],
    'score_response_rate': [
        {'lt': 60, 'score': 0},
        {'gte': 60, 'lt': 70, 'score': 1},
        {'gte': 70, 'lt': 85, 'score': 2},
        {'gte': 85, 'lt': 90, 'score': 3},
        {'gte': 90, 'lt': 95, 'score': 4},
        {'gte': 95, 'score': 5},
    ],
}


def _get_score_number(number, score_type):
    if number is None:
        return None
    lst_score_obj = config_score_reputation.get(score_type)
    for score_obj in lst_score_obj:
        gte = score_obj.get('gte')
        lt = score_obj.get('lt')
        if (gte is None or (gte is not None and number >= gte)) and (lt is None or (lt is not None and number < lt)):
            score = score_obj.get('score')
            return score
    return None


async def calculate_shop_reputation(shop_base_id):
    shop_reputation = None
    score_review_total = None
    score_year_old = None
    score_revenue_monthly = None
    score_review_avg = None
    score_response_rate = None

    shop_detail_response = await get_shop_detail(shop_base_id=shop_base_id)
    shop_metric = await get_shop_info_metric(shop_base_id=shop_base_id)
    shop_detail = shop_detail_response.get('data').get('shop_base')
    # logger.info(shop_detail)
    revenue_monthly = shop_metric.get('revenue_180d') / 6
    platform_id = shop_base_id.split('__')[0]
    score_revenue_monthly = _get_score_number(number=revenue_monthly, score_type='score_revenue_monthly')

    total_review = 0
    if shop_detail.get('rating_normal'):
        total_review += shop_detail.get('rating_normal')
    if shop_detail.get('rating_bad'):
        total_review += shop_detail.get('rating_bad')
    if shop_detail.get('rating_good'):
        total_review += shop_detail.get('rating_good')

    score_review_total = _get_score_number(number=total_review, score_type='score_review_total')

    created_at_platform = convert_str_to_datetime(str=shop_detail.get('created_at_platform'),
                                                  format='%Y-%m-%dT%H:%M:%S%f%z')
    if created_at_platform:
        created_at_platform = created_at_platform.replace(tzinfo=None)

    if created_at_platform:
        delta = datetime.datetime.now() - created_at_platform
        shop_year_old = delta.days / 365
        score_year_old = _get_score_number(number=shop_year_old, score_type='score_year_old')

    rating_avg = shop_detail.get('rating_avg')
    score_review_avg = _get_score_number(number=rating_avg, score_type='score_review_avg')

    response_rate = shop_detail.get('response_rate')
    score_response_rate = _get_score_number(number=response_rate, score_type='score_response_rate')
    # logger.info(shop_detail)
    extra_info = {
        'url_image': shop_detail.get('url_image'),
        'shop_location': shop_detail.get('shop_location'),
        'name': shop_detail.get('name'),
        'username': shop_detail.get('username'),
        'shop_base_id': shop_detail.get('shop_base_id'),
        'is_official_shop': shop_detail.get('is_official_shop'),
        'total_review': total_review,
        'created_at_platform': created_at_platform,
        'rating_avg': rating_avg,
        'response_rate': response_rate,
    }
    result_extraction = extract_information_from_text(text=shop_detail.get('description'))
    if result_extraction:
        extra_info = {
            **extra_info,
            'lst_phone': result_extraction.get('lst_phone'),
            'lst_email': result_extraction.get('lst_email'),
            'lst_fb': result_extraction.get('lst_fb'),
            'lst_url': result_extraction.get('lst_url'),
        }

    return {
        'shop_reputation': {
            'score_review_total': score_review_total,
            'score_year_old': score_year_old,
            'score_revenue_monthly': score_revenue_monthly,
            'score_review_avg': score_review_avg,
            'score_response_rate': score_response_rate,
        },
        'extra_info': extra_info
    }


async def get_shop_info_metric(shop_base_id):
    """
    Lấy thông tin doanh số shop
    :param shop_base_id:
    :return:
    """
    revenue_180d = None

    es_config = ['https://linhnguyen:KtT3yRER8XPflbR@sv6.beecost.net:9200']
    # es_session = AsyncElasticsearch(es_config, verify_certs=False, request_timeout=20, ssl_show_warn=False)
    es_session = Elasticsearch(es_config, verify_certs=False, request_timeout=20, ssl_show_warn=False)
    query_es = {
        "query": {
            "bool": {
                "must": [
                    {
                        "bool": {
                            "should": [
                                {
                                    "script": {
                                        "script": {
                                            "source": "if (doc['rating_count'].size() <= 0 || doc['order_count'].size() <= 0){return false;}double f1 = doc['rating_count'].value * 20.0;double f2 = doc['order_count'].value *1.0;return f1>f2;",
                                            "lang": "painless"
                                        },
                                        "boost": 1
                                    }
                                }
                            ],
                            "adjust_pure_negative": True,
                            "boost": 1
                        }
                    },
                    {
                        "range": {
                            "order_revenue_180d": {
                                "gt": 0
                            }
                        }
                    },
                    {
                        "match_phrase": {
                            "shop_base_id": shop_base_id
                        }
                    }
                ]
            }
        },
        "size": 0,
        "aggs": {
            "revenue_180d": {
                "sum": {
                    "field": "order_revenue_180d"
                }
            }
        }
    }
    index_name = None
    platform_id = shop_base_id.split('__')[0]
    if platform_id == '1':
        index_name = 'market_current__shopee_vn'
    if platform_id == '2':
        index_name = 'market_current__lazada_vn'
    if platform_id == '3':
        index_name = 'market_current__tiki_vn'
    if platform_id == '4':
        index_name = 'market_current__sendo_vn'
    results, meta = search_v2(es=es_session,
                              index_name=index_name,
                              query=query_es)
    if meta and meta.get('aggregations') and meta.get('aggregations').get('revenue_180d'):
        revenue_180d = cast_int(meta.get('aggregations').get('revenue_180d').get('value'))

    return {
        'revenue_180d': revenue_180d
    }


async def example():
    shop_base_id = '1__236538249'
    shop_reputation = await calculate_shop_reputation(shop_base_id=shop_base_id)
    logger.info(shop_reputation)


if __name__ == '__main__':
    asyncio.run(
        example()
    )
