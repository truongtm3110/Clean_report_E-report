import asyncio
import json
import math
import os
from datetime import datetime
from typing import AsyncGenerator

import pandas as pd
from dateutil.relativedelta import relativedelta
from pandas import Series

from app.db.session_es_metric import get_es_metric_session
from helper.elasticsearch7_helper import search_v2
from helper.logger_helper import LoggerSimple
from helper.text_hash_helper import text_to_hash_md5
from schedule.report_service.build_es_query_service import build_query_es_from_api, get_aggs_runtime_mapping_es, \
    FilterReport, Range
from schedule.report_service.transform_es_response_service import _tranform_aggs

logger = LoggerSimple(name=__name__).logger

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


async def _build_es_query_from_filter_report(filter_report: FilterReport, start_date, end_date, size_product=75):
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


def load_query_dataframe(file_path: str, sheet_name: str = None) -> pd.DataFrame:
    if file_path.endswith('.csv'):
        return pd.read_csv(file_path)
    elif file_path.endswith('.xlsx'):
        return pd.read_excel(file_path, sheet_name=sheet_name)


async def fetch_data_keyword(row):
    # async for session_metric in get_async_session_metric():
    lst_keyword = row['Từ khóa'].split(',') if isinstance(row['Từ khóa'], str) else []
    lst_exclude_keyword = []
    if type(row['Từ khóa loại trừ']) is str:
        lst_exclude_keyword = row['Từ khóa loại trừ'].split(',')

    lst_keyword_required = []
    if type(row['Từ khóa cộng']) is str:
        lst_keyword_required = row['Từ khóa cộng'].split(',')

    is_smart_queries = True if row['Chế độ tìm'] == 'Tìm thông minh' else False
    price_min = row['Giá min'] if not math.isnan(row['Giá min']) else None
    price_max = row['Giá max'] if not math.isnan(row['Giá max']) else None
    lst_categories = get_categories_from_row(row)

    price_range = None
    if price_min or price_max:
        price_range = Range(
            begin=price_min,
            end=price_max
        )

    filter_report = FilterReport(
        lst_platform_id=[1, 2, 3],
        lst_keyword_exclude=lst_exclude_keyword,
        lst_keyword_required=lst_keyword_required,
        lst_keyword=lst_keyword,
        is_smart_queries=is_smart_queries,
        lst_category_base_id=lst_categories,
        price_range=price_range
    )

    lst_query_es = await _build_es_query_from_filter_report(
        filter_report,
        '20230701',
        '20240630',
        size_product=2_000
    )

    query_es_default = lst_query_es.get('query_es_default')
    index_name = 'market_current__shopee_vn,market_current__lazada_vn,market_current__tiki_vn'
    es_session = get_es_metric_session(request_timeout=20)

    # print(json.dumps(query_es_default, ensure_ascii=False))
    # step 4: query elasticsearch
    lst_product_revenue_30d_raw, aggs = search_v2(
        es=es_session,
        query=query_es_default,
        index_name=index_name,
    )

    result_analytic_report = await _tranform_aggs(aggs=aggs)

    by_overview = result_analytic_report.by_overview

    revenue = by_overview.revenue
    sale = by_overview.sale
    product = by_overview.product
    shop = by_overview.shop

    lst_bee_category = result_analytic_report.by_category.lst_bee_category
    # by_brand = result_analytic_report.by_brand.lst_top_brand_revenue
    # by_shop = result_analytic_report.by_shop.lst_top_shop

    top_10_product = lst_product_revenue_30d_raw[:10]
    middle_10_product = lst_product_revenue_30d_raw[
                        len(lst_product_revenue_30d_raw) // 2 - 5: len(lst_product_revenue_30d_raw) // 2 + 5]
    bottom_10_product = lst_product_revenue_30d_raw[-10:]

    return {
        'revenue': revenue,
        'sale': sale,
        'product': product,
        'shop': shop,
        'lst_bee_category': lst_bee_category,
        # 'by_brand': by_brand,
        # 'by_shop': by_shop,
        'top_10_product': top_10_product,
        'middle_10_product': middle_10_product,
        'bottom_10_product': bottom_10_product,
    }


def find_category_id_by_label_path(label_path: str, categories_tree: dict):
    """
    This function is used to find the category id by the label path
    :param label_path: the label path Example: 'Mẹ & Bé/Đồ dùng ăn dặm cho bé/Bình sữa'
    :param categories_tree: the categories tree
    :return: the category id
    """
    label_lst = [label.strip() for label in label_path.split('/')]
    for category in categories_tree:
        if category['label'].strip().lower() == label_lst[0].lower():
            if len(label_lst) == 1:
                return category['value']
            else:
                return find_category_id_by_label_path('/'.join(label_lst[1:]), category['children'])
    return None


def load_category_tree(platfrom: str = 'shopee') -> dict | None:
    with open(f'{ROOT_DIR}/categories_data/{platfrom}_categories.json', 'r',
              encoding='utf-8') as f:
        category_tree = json.load(f)
    return category_tree


def get_categories_from_row(row: Series, platform: str = 'shopee'):
    column_name = f'Danh mục {platform.capitalize()}'
    category_cell_value = row[column_name]
    categories_tree = load_category_tree(platform)
    if type(category_cell_value) is not str:
        return 'skip'
    if category_cell_value == 'Tất cả':
        return [category['value'] for category in categories_tree]
    elif category_cell_value.strip() == 'Không lấy dữ liệu':
        return 'skip'
    else:
        lst_category = []
        lst_name = category_cell_value.split('\n')
        print("lst_name:", lst_name, len(lst_name))
        for name in lst_name:
            category_id = find_category_id_by_label_path(name.strip(), categories_tree)
            print(category_id)
            if category_id:
                lst_category.append(category_id)
        return lst_category


async def run():
    input_file_path = f'{ROOT_DIR}/Danh sách báo cáo e-report.xlsx'

    df = load_query_dataframe(input_file_path, 'Sheet1')

    for index, row in df.iterrows():
        start_time = datetime.now()

        filter_columns = [
            'Từ khóa',
            'Danh mục Shopee',
            'Danh mục Lazada',
            'Danh mục Tiki',
            'Danh mục Tiktok',
            'Từ khóa loại trừ',
            'Từ khóa cộng',
            'Chế độ tìm',
            'Giá min',
            'Giá max',
        ]
        filter_as_str = ''
        for col in filter_columns:
            filter_as_str += f"{row[col]}"

        key_filter_report = text_to_hash_md5(filter_as_str)
        key_response_report = row['Key']
        # print('key', key_filter_report, key_response_report)
        # if key_filter_report == key_response_report:
        #     print(f"- IGNORE Bộ lọc không thay đổi, bỏ qua {row['Từ khóa']} \n")
        #     continue

        print(f"- START query từ khóa: {row['Từ khóa']} {index + 1}/{len(df)}")
        query_data_num = row['Lần query data']
        if math.isnan(query_data_num):
            query_data_num = 0

        report_response = await fetch_data_keyword(row)
        revenue = report_response.get('revenue')
        sale = report_response.get('sale')
        product = report_response.get('product')
        shop = report_response.get('shop')
        lst_bee_category = report_response.get('lst_bee_category')[:10]
        # by_brand = report_response.get('by_brand')[:10]
        # by_shop = report_response.get('by_shop')[:10]
        top_10_product = report_response.get('top_10_product')
        middle_10_product = report_response.get('middle_10_product')
        bottom_10_product = report_response.get('bottom_10_product')

        row['Lần query data'] = query_data_num + 1
        row['Key'] = key_filter_report
        row['Doanh số'] = revenue
        row['Sản lượng'] = sale
        row['Sản phẩm có lượt bán'] = product
        row['Số shop'] = shop
        category_str = ''
        for bee_category in lst_bee_category:
            category_str += f"{bee_category.name}\n"

        row['Ngành hàng'] = category_str[:-1]

        lst_product_name_str = ''
        for product in top_10_product:
            lst_product_name_str += f"{product.get('product_name')}\n"
        for product in middle_10_product:
            lst_product_name_str += f"{product.get('product_name')}\n"
        for product in bottom_10_product:
            lst_product_name_str += f"{product.get('product_name')}\n"

        row['Product name'] = lst_product_name_str[:-1]

        df.loc[index] = row

        df.to_excel(input_file_path, index=False)

        print(f"DONE {index + 1}/{len(df)} trong {datetime.now() - start_time} \n")


if __name__ == '__main__':
    asyncio.run(run())
