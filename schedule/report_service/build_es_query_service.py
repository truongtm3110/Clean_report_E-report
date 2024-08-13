import asyncio
import json
from datetime import datetime
from typing import Any, List, Optional

import requests
from dateutil.relativedelta import relativedelta
from pydantic import BaseModel

from helper.datetime_helper import convert_str_to_datetime


class Range(BaseModel):
    begin: Any = None
    end: Any = None


class FilterReport(BaseModel):
    lst_platform_id: List[Optional[int]] | None = None

    lst_category_base_id: List[str] | None = None

    lst_bee_category_base_id: List[str] | None = None

    lst_keyword_required: List[str] | None = None

    is_smart_queries: bool = False
    lst_keyword: List[str] | None = None
    or_lst_brand: List[str] | None = None

    lst_keyword_exclude: List[str] | None = None
    is_remove_fake_sale: bool = False

    composite_compare_lst: List[List[dict]] | None = None

    duration_day: int | None = None
    locations: List[str] | None = None
    official_types: List[int] = [1, 0]

    price_range: Range | None = None
    rating_count_range: Range | None = None
    platform_created_at_range: Range | None = None

    order_count_range: Range | None = None
    order_count_7d_range: Range | None = None
    order_count_30d_range: Range | None = None
    order_count_90d_range: Range | None = None
    order_count_180d_range: Range | None = None

    revenue_range: Range | None = None
    order_revenue_7d_range: Range | None = None
    order_revenue_30d_range: Range | None = None
    order_revenue_90d_range: Range | None = None
    order_revenue_180d_range: Range | None = None

    def to_dict(self):
        return json.loads(self.json())


async def build_query_es_from_api(filter_report: FilterReport, start_date, end_date):
    check_exist_field = "order_count_custom_range" \
        if not filter_report.duration_day \
        else f'order_count_{filter_report.duration_day}d_range'
    body = {
        "start_date": start_date,
        "end_date": end_date,
        "include_query": {
            "start_date": start_date,
            "end_date": end_date,
            "platforms": filter_report.lst_platform_id,
            "is_remove_bad_product": True,
            "official_types": filter_report.official_types,
            "locations": filter_report.locations,
            "brands": filter_report.or_lst_brand,
            check_exist_field: {
                "from": 1,
                "is_default_from": True
            }
        },
        "exclude_query": {
            "queries": filter_report.lst_keyword_exclude
        },
    }
    queries = []

    if filter_report.lst_keyword and len(filter_report.lst_keyword) > 0:
        queries += filter_report.lst_keyword

    if filter_report.lst_keyword_required and len(filter_report.lst_keyword_required) > 0:
        for kw in filter_report.lst_keyword_required:
            queries.append(f'+{kw}')

    if len(queries) > 0:
        body['include_query']['queries'] = queries

    if filter_report.lst_category_base_id and len(filter_report.lst_category_base_id) > 0:
        body['include_query']['categories'] = filter_report.lst_category_base_id

    if filter_report.lst_bee_category_base_id and len(filter_report.lst_bee_category_base_id) > 0:
        body['include_query']['bee_categories'] = filter_report.lst_bee_category_base_id

    body['include_query']['composite_compare_lst'] = []
    if filter_report.is_remove_fake_sale \
            and (not filter_report.composite_compare_lst or len(filter_report.composite_compare_lst) == 0):
        body['include_query']['composite_compare_lst'] = [
            [
                {
                    "first_field": "rating_count",
                    "second_field": "order_count",
                    "first_rate": 1,
                    "second_rate": 0.02,
                    "compare_condition": ">",
                    "is_default": True
                }
            ]
        ]
    if filter_report.composite_compare_lst and len(filter_report.composite_compare_lst) > 0:
        for composite_compare in filter_report.composite_compare_lst:
            body['include_query']['composite_compare_lst'].append([
                {
                    "first_field": composite_compare[0].get('first_field'),
                    "second_field": composite_compare[0].get('second_field'),
                    "first_rate": composite_compare[0].get('first_rate'),
                    "second_rate": composite_compare[0].get('second_rate'),
                    "compare_condition": composite_compare[0].get('compare_condition'),
                }
            ])

    range_fields = [
        'price_range',
        'rating_count_range',
        'platform_created_at_range',
        'order_count_range',
        'order_count_7d_range',
        'order_count_30d_range',
        'order_count_90d_range',
        'order_count_180d_range',
        'revenue_range',
        'order_revenue_7d_range',
        'order_revenue_30d_range',
        'order_revenue_90d_range',
        'order_revenue_180d_range',
    ]
    for field in range_fields:
        if getattr(filter_report, field):
            body['include_query'][field] = {
                "from": getattr(getattr(filter_report, field), 'begin'),
                "to": getattr(getattr(filter_report, field), 'end'),
            }

    if filter_report.is_smart_queries:
        body['include_query']['is_smart_queries'] = filter_report.is_smart_queries

    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json;charset=UTF-8',
        'Cookie': '_fbp=fb.1.1651219381022.473108228; _lfa=LF1.1.7725ab9e412e13b2.1648708381170; _tt_enable_cookie=1; _ttp=1445b897-2f13-4fd7-9377-504ccacc5f88; __cuid=a0db116612784989be3a9218e807cdad; amp_fef1e8=814bc983-759a-4c1f-8bcf-ccbbade037d5R...1geqnfiep.1geqnj6so.10.6.16; _gid=GA1.2.965210760.1666151850; _clck=f7m4af|1|f5v|0; _ga_D94S7754SL=GS1.1.1666239146.84.0.1666239146.0.0.0; _ga=GA1.2.1673475084.1651219381; _clsk=r1cs35|1666240171666|2|1|j.clarity.ms/collect; _gat=1',
        'Origin': 'https://metric.vn',
        'PlatformId': '1',
        'Pragma': 'no-cache',
        'Referer': 'https://metric.vn/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'Token': 'tuantm.0f9e8ce1611e8ca9b8522e169cf51f74.2b7db204e15ce6e90d331c09dac63dab',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36',
        'VisitorId': '48472__4339d1da882f151f2ac6aedb3f6b6a78',
        'sec-ch-ua': '"Chromium";v="106", "Google Chrome";v="106", "Not;A=Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Linux"'
    }
    response = requests.post(url='https://apiv3.metric.vn/market/search_basic?include_raw_search_query=1',
                             headers=headers, json=body)
    query_from_api = response.json().get('data').get('raw_search_body').get('query')
    return query_from_api


def get_aggs_runtime_mapping_es(filter_report: FilterReport, start_date: str, end_date: str):
    start_at = convert_str_to_datetime(str=start_date, format='%Y%m%d') - relativedelta(days=1)
    end_at = convert_str_to_datetime(str=end_date, format='%Y%m%d')

    duration_day = (end_at - start_at).days if not filter_report.duration_day else filter_report.duration_day

    start_date_adjacent = (start_at - relativedelta(days=duration_day)).strftime('%Y%m%d')
    end_date_adjacent = (end_at - relativedelta(days=duration_day)).strftime('%Y%m%d')

    start_date = start_at.strftime('%Y%m%d')

    lst_aggs = {
        "revenue": {
            "sum": {
                "field": "order_revenue_custom"
            }
        },
        "sale": {
            "sum": {
                "field": "order_count_custom"
            }
        },
        "shop": {
            "cardinality": {
                "field": "shop_base_id.keyword",
                "precision_threshold": 40000
            }
        },
        "brand": {
            "cardinality": {
                "field": "bee_brand.keyword",
                "precision_threshold": 40000
            }
        },
        "product": {
            "cardinality": {
                "field": "product_base_id.keyword"
            }
        },

        "category": {
            "terms": {
                "field": "categories.id.keyword",
                "size": 100,
                "order": {
                    "revenue": "desc"
                }
            },
            "aggs": {
                "revenue": {
                    "sum": {
                        "field": "order_revenue_custom"
                    }
                }
            }
        },
        "bee_category": {
            "terms": {
                "field": "bee_categories.id.keyword",
                "size": 100,
                "order": {
                    "revenue": "desc"
                }
            },
            "aggs": {
                "revenue": {
                    "sum": {
                        "field": "order_revenue_custom"
                    }
                }
            }
        },
        "by_marketplace": {
            "terms": {
                "field": "platform_id",
                "size": 10,
                "min_doc_count": 1,
                "shard_min_doc_count": 0,
                "show_term_doc_count_error": False,
                "order": {
                    "revenue": "desc"
                }
            },
            "aggs": {
                "revenue": {
                    "sum": {
                        "field": "order_revenue_custom"
                    }
                },
                "sale": {
                    "sum": {
                        "field": "order_count_custom"
                    }
                }
            }
        },
        "by_price_range": {
            "terms": {
                "field": "price_ranges.keyword",
                "size": 100,
                "shard_size": 100,
                "min_doc_count": 1,
                "shard_min_doc_count": 0,
                "show_term_doc_count_error": False
            },
            "aggs": {
                "revenue": {
                    "sum": {
                        "field": "order_revenue_custom"
                    }
                },
                "sale": {
                    "sum": {
                        "field": "order_count_custom"
                    }
                },
                "platform": {
                    "terms": {
                        "field": "platform_id",
                        "size": 100
                    },
                    "aggs": {
                        "revenue": {
                            "sum": {
                                "field": "order_revenue_custom"
                            }
                        },
                        "sale": {
                            "sum": {
                                "field": "order_count_custom"
                            }
                        }
                    }
                }
            }
        },
        "by_shop_ratio": {
            "terms": {
                "field": "official_type",
                "size": 10,
                "shard_size": 100,
                "min_doc_count": 1,
                "shard_min_doc_count": 0,
                "show_term_doc_count_error": False
            },
            "aggs": {
                "revenue": {
                    "sum": {
                        "field": "order_revenue_custom"
                    }
                },
                "sale": {
                    "sum": {
                        "field": "order_count_custom"
                    }
                },
                "shop": {
                    "cardinality": {
                        "field": "shop_base_id.keyword",
                        "precision_threshold": 40000
                    }
                },
                "product": {
                    "cardinality": {
                        "field": "product_base_id.keyword"
                    }
                }
            }
        },
        "lst_top_shop": {
            "terms": {
                "field": "shop_base_id.keyword",
                "size": 20,
                "shard_size": 100000,
                "min_doc_count": 1,
                "shard_min_doc_count": 0,
                "show_term_doc_count_error": False,
                "order": {
                    "revenue": "desc"
                }
            },
            "aggs": {
                "revenue": {
                    "sum": {
                        "field": "order_revenue_custom"
                    }
                },
                "sale": {
                    "sum": {
                        "field": "order_count_custom"
                    }
                },
                "product": {
                    "cardinality": {
                        "field": "product_base_id.keyword"
                    }
                }
            }
        },
        "lst_top_brand_revenue": {
            "terms": {
                "field": "bee_brand.keyword",
                "size": 21,
                "shard_size": 100000,
                "min_doc_count": 1,
                "shard_min_doc_count": 0,
                "show_term_doc_count_error": False,
                "missing": "N/A",
                "order": {
                    "revenue": "desc"
                }
            },
            "aggs": {
                "revenue": {
                    "sum": {
                        "field": "order_revenue_custom"
                    }
                },
                "sale": {
                    "sum": {
                        "field": "order_count_custom"
                    }
                },
                "shop": {
                    "cardinality": {
                        "field": "shop_base_id.keyword",
                        "precision_threshold": 40000
                    }
                },
                "product": {
                    "cardinality": {
                        "field": "product_base_id.keyword"
                    }
                }
            }
        },
        "lst_top_brand_sale": {
            "terms": {
                "field": "bee_brand.keyword",
                "size": 10,
                "shard_size": 100000,
                "min_doc_count": 1,
                "shard_min_doc_count": 0,
                "show_term_doc_count_error": False,
                "order": {
                    "sale": "desc"
                }
            },
            "aggs": {
                "sale": {
                    "sum": {
                        "field": "order_count_custom"
                    }
                }
            }
        },
    }
    lst_runtime_mapping = {
        "order_count_custom": {
            "type": "long",
            "script": {
                "source": "long a = 0; long b = 0; if(doc.containsKey('order_history_" + end_date + "') && doc['order_history_" + end_date + "'].size() != 0){ a = doc['order_history_" + end_date + "'].value } if(doc.containsKey('order_history_" + start_date + "') && doc['order_history_" + start_date + "'].size() != 0){ b = doc['order_history_" + start_date + "'].value } emit(a-b)",
                "lang": "painless"
            }
        },
        "order_revenue_custom": {
            "type": "long",
            "script": {
                "source": "long a = 0; long b = 0; if(doc.containsKey('revenue_history_" + end_date + "') && doc['revenue_history_" + end_date + "'].size() != 0){ a = doc['revenue_history_" + end_date + "'].value } if(doc.containsKey('revenue_history_" + start_date + "') && doc['revenue_history_" + start_date + "'].size() != 0){ b = doc['revenue_history_" + start_date + "'].value } emit(a-b)",
                "lang": "painless"
            }
        },
        "order_count_30d_adjacent": {
            "type": "long",
            "script": {
                "source": "long a = 0; long b = 0; if(doc.containsKey('order_history_" + end_date_adjacent + "') && doc['order_history_" + end_date_adjacent + "'].size() != 0){ a = doc['order_history_" + end_date_adjacent + "'].value } if(doc.containsKey('order_history_" + start_date_adjacent + "') && doc['order_history_" + start_date_adjacent + "'].size() != 0){ b = doc['order_history_" + start_date_adjacent + "'].value } emit(a-b)",
                "lang": "painless"
            }
        },
        "order_revenue_30d_adjacent": {
            "type": "long",
            "script": {
                "source": "long a = 0; long b = 0; if(doc.containsKey('revenue_history_" + end_date_adjacent + "') && doc['revenue_history_" + end_date_adjacent + "'].size() != 0){ a = doc['revenue_history_" + end_date_adjacent + "'].value } if(doc.containsKey('revenue_history_" + start_date_adjacent + "') && doc['revenue_history_" + start_date_adjacent + "'].size() != 0){ b = doc['revenue_history_" + start_date_adjacent + "'].value } emit(a-b)",
                "lang": "painless"
            }
        }
    }

    return lst_aggs, lst_runtime_mapping


async def build_es_query_from_filter_report(filter_report: FilterReport, start_date, end_date, size_product=75):
    query = await build_query_es_from_api(filter_report, start_date, end_date)
    fields = [
        "order_count_custom",
        "order_revenue_custom",
        "order_count_custom_adjacent",
        "order_revenue_custom_adjacent",
        "review_count_custom"
    ]
    _source = [
        "product_base_id",
        "shop_base_id",
        "product_name",
        "url_thumbnail",
        "official_type",
        "bee_brand",
        "shop_platform_name",
        "shop_url",
        "price",
        "order_count",
        "rating_avg",
        "rating_count",
        "revenue",
        "platform_created_at",
        "price_min",
        "price_max",
        "order_count_30d",
        "order_revenue_30d",
        "order_count_custom_adjacent",
        "order_revenue_custom_adjacent",
        "price_updated_at"
    ]

    lst_aggs, lst_runtime_mapping = get_aggs_runtime_mapping_es(filter_report, start_date, end_date)

    query_es_default = {
        'query': query,
        'runtime_mappings': lst_runtime_mapping,
        'fields': fields,
        '_source': _source,
        'aggs': lst_aggs,
        "size": size_product,
        "sort": [
            {
                "order_revenue_30d": {
                    "order": "desc"
                }
            }
        ],
    }

    three_month_ago_ts = int((datetime.now() - relativedelta(months=3)).timestamp()) * 1000
    query_new_product = json.loads(json.dumps(query))
    query_new_product['bool']['must'].append({
        "range": {
            "platform_created_at": {
                "gte": three_month_ago_ts
            }
        }
    })
    query_es_new_product = {
        'query': query_new_product,
        'runtime_mappings': lst_runtime_mapping,
        'aggs': lst_aggs,
        'fields': fields,
        '_source': _source,
        "size": size_product,
        "sort": [
            {
                "order_revenue_30d": {
                    "order": "desc"
                }
            }
        ],
    }

    query_es_top_revenue_custom = {
        'query': query,
        'runtime_mappings': lst_runtime_mapping,
        'fields': fields,
        '_source': ['product_name', 'price'],
        "size": size_product,
        "sort": [
            {
                "order_revenue_custom": {
                    "order": "desc"
                }
            }
        ],
    }

    query_es_top_1000_product = {
        'query': query,
        'runtime_mappings': lst_runtime_mapping,
        '_source': ['product_base_id'],
        "size": 1000,
        "sort": [
            {
                "order_revenue_30d": {
                    "order": "desc"
                }
            }
        ],
    }

    return {
        'query_es_default': query_es_default,
        'query_es_new_product': query_es_new_product,
        'query_es_top_revenue_custom': query_es_top_revenue_custom,
        'query_es_top_1000_product': query_es_top_1000_product,
    }


async def run():
    filter_report = FilterReport(
        lst_platform_id=[1, 2, 3],
        price_range=Range(begin=100_000, end=500_000),
        lst_category_base_id=['1__100717'],
        lst_bee_category_base_id=['c1144728673'],
        lst_keyword_required=['nồi', 'bếp'],
        lst_keyword_exclude=['rẻ'],
        lst_keyword=['nồi chiên', 'chảo inox'],
        is_smart_queries=True
    )
    query_es_from_filter_report = await build_es_query_from_filter_report(filter_report=filter_report,
                                                                          start_date='20240101',
                                                                          end_date='20241231')

    print(json.dumps(query_es_from_filter_report.get('query_es_default'), ensure_ascii=False))


if __name__ == '__main__':
    asyncio.run(run())
