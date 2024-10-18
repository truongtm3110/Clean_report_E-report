import json
import math
import os
import random
import string
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import pandas as pd
import requests
from pandas import Series
from helpers.calendar_helpers import get_all_month_between, get_end_field_and_start_field
from helpers.dataframe_helper import split_dataframe_into_chunks, combine_list_dataframe
from helpers.error_helper import log_error
from helpers.logger_helper import LoggerSimple
from helpers.metric_helper import get_limit_range_all_platforms
from utils.exclude_utils import get_metric_blacklist_keyword_filter, get_metric_gift_product_filter

logger = LoggerSimple(name=__name__).logger

PLATFORMS_DATE_LIMIT_CONFIG = []


def generate_random_string(n: int = 10):
    return ''.join(random.SystemRandom().choice(string.ascii_uppercase + string.digits) for _ in range(n))


def load_sample_query(platform: str = 'shopee') -> dict | None:
    with open(f'../sample_data/sample_query__{platform}.json', 'r', encoding='utf-8') as f:
        search_body = json.load(f)
    gift_products = get_metric_gift_product_filter(platform)
    blacklist_keywords = get_metric_blacklist_keyword_filter(platform)
    exclude_query = search_body.get("exclude_query")
    return {
        **search_body,
        "exclude_query": {
            **exclude_query,
            "queries": [
                *gift_products,
                *blacklist_keywords
            ],
        }
    }


def load_category_tree(platfrom: str = 'shopee') -> dict | None:
    with open(f'../categories_data/{platfrom}_categories.json', 'r', encoding='utf-8') as f:
        category_tree = json.load(f)
    return category_tree


def load_query_dataframe(file_path: str, sheet_name: str = None) -> pd.DataFrame:
    if file_path.endswith('.csv'):
        return pd.read_csv(file_path)
    elif file_path.endswith('.xlsx'):
        return pd.read_excel(file_path, sheet_name=sheet_name)


def build_request_headers():
    return {
        "Origin": "https://metric.vn",
        "Token": "tuantm.0f9e8ce1611e8ca9b8522e169cf51f74.2b7db204e15ce6e90d331c09dac63dab",
        "PlatformId": "1",
        "VisitorId": "5eb7533601739161e1a96fc455b88229",
    }


def build_request_body(platform: str,
                       keywords: list[str],
                       must_include_keywords: list[str],
                       exclude_keywords: list[str],
                       categories: list[str],
                       remove_bad_product_coefficient: str | float,
                       not_include_deleted_product: bool,
                       is_smart_search: bool,
                       min_price: int = None,
                       max_price: int = None,
                       start_date: str = None,
                       end_date: str = None,
                       duration_days: int = None
                       ) -> dict | None:
    if platform == 'tiktok':
        start_date_obj = datetime.strptime(start_date, '%Y%m%d')
        tiktok_first_date = datetime(2022, 9, 1)
        if start_date_obj < tiktok_first_date:
            start_date = tiktok_first_date.strftime('%Y%m%d')
    query = load_sample_query(platform)
    query['include_query']['queries'] = [*keywords, *[f'+{keyword}' for keyword in must_include_keywords]]
    query['exclude_query']['queries'].extend(exclude_keywords)
    query['include_query']['categories'] = categories

    if not_include_deleted_product == 'Không':
        query['notIncludeDeletedProduct'] = False
    else:
        query['notIncludeDeletedProduct'] = True
    query['include_query']['is_smart_queries'] = is_smart_search

    if remove_bad_product_coefficient:
        query['include_query']['is_remove_bad_product'] = True
        if isinstance(remove_bad_product_coefficient, float):
            composite_compare_lst = query['include_query']["composite_compare_lst"]
            has_rating_count_order_count = False
            for compare in composite_compare_lst:
                if type(compare) is list and len(compare) > 0:
                    compare = compare[0]
                if compare['first_field'] == 'rating_count' and compare['second_field'] == 'order_count':
                    has_rating_count_order_count = True
                    compare['first_rate'] = 1
                    compare['second_rate'] = remove_bad_product_coefficient
                    break
            if not has_rating_count_order_count:
                composite_compare_lst.append({
                    "first_field": "rating_count",
                    "second_field": "order_count",
                    "first_rate": 1,
                    "second_rate": remove_bad_product_coefficient,
                    "compare_condition": ">",
                })

        elif remove_bad_product_coefficient == 'Không':
            query['include_query']['is_remove_bad_product'] = False
            composite_compare_lst: list = query['include_query']["composite_compare_lst"]
            has_rating_count_order_count = False
            index_of_bad_product_filter = None
            for index, compare in enumerate(composite_compare_lst):
                if type(compare) is list and len(compare) > 0:
                    compare = compare[0]
                if compare['first_field'] == 'rating_count' and compare['second_field'] == 'order_count':
                    has_rating_count_order_count = True
                    index_of_bad_product_filter = index
                    break
            print(has_rating_count_order_count)
            if has_rating_count_order_count:
                composite_compare_lst.pop(index_of_bad_product_filter)

    if min_price or max_price:
        query['include_query']['price_range'] = {
            "from": min_price,
            "to": max_price
        }
    date_range_limit = PLATFORMS_DATE_LIMIT_CONFIG.get(platform)
    query_start_date = start_date
    query_end_date = end_date
    if start_date < date_range_limit['start_date']:
        query_start_date = date_range_limit['start_date']
    if end_date > date_range_limit['end_date']:
        query_end_date = date_range_limit['end_date']
    query['include_query']['start_date'] = query_start_date
    query['include_query']['end_date'] = query_end_date
    if duration_days in [7, 30, 90, 180]:
        query['include_query']['durationDay'] = duration_days
        query['include_query'][f'order_count_{duration_days}d_range'] = {
            "from": 1,
            "is_default_from": True
        }
        query['fields'] = [
            "product_base_id",
            "product_name",
            "url_thumbnail",
            "price",
            "price_min",
            "price_max",
            "official_type",
            "rating_avg",
            "rating_count",
            "order_count",
            "price_updated_at",
            "revenue",
            "platform_created_at",
            "shop_platform_name",
            "shop_url",
            "brand",
            "categories",
            f"order_count_{duration_days}d",
            f"order_revenue_{duration_days}d",
            f"order_count_{duration_days}d_previous",
            f"order_revenue_{duration_days}d_previous",
            f"review_count_{duration_days}d",
            f"review_count_{duration_days}d_previous"
        ]
        query['statistics'] = [
            "shop_total",
            "product_total",
            "official_type",
            f"order_total_{duration_days}d",
            f"review_total_{duration_days}d",
            f"revenue_total_{duration_days}d",
            f"revenue__by__categories_{duration_days}d",
            f"revenue__by__brands_{duration_days}d",
            f"revenue__by__price_range_{duration_days}d",
            f"revenue__by__location_{duration_days}d",
            f"revenue__by__shop_type_{duration_days}d"
        ]
        query['sort_by'] = f"order_revenue_{duration_days}d__desc"
    else:
        query['start_date'] = start_date
        query['end_date'] = end_date
        query['include_query'][f'order_count_custom_range'] = {
            "from": 1,
            "is_default_from": True
        }
        query['fields'] = [
            "product_base_id",
            "product_name",
            "url_thumbnail",
            "price",
            "price_min",
            "price_max",
            "official_type",
            "rating_avg",
            "rating_count",
            "order_count",
            "price_updated_at",
            "revenue",
            "platform_created_at",
            "shop_platform_name",
            "shop_url",
            "brand",
            "categories",
            "order_count_custom",
            "order_revenue_custom",
            "order_count_custom_previous",
            "order_revenue_custom_previous",
            "review_count_custom",
            "review_count_custom_previous"
        ]
        query['statistics'] = [
            "shop_total",
            "product_total",
            "official_type",
            "order_total_custom",
            "revenue_total_custom",
            "review_total_custom",
            "revenue__by__categories_custom",
            "revenue__by__brands_custom",
            "revenue__by__price_range_custom",
            "revenue__by__location_custom",
            "revenue__by__shop_type_custom"
        ]
        del query['durationDay']
        query['sort_by'] = f"order_revenue_custom__desc"
    return query


def add_aggregations_to_raw_query(raw_query: dict, platform: str = 'shopee'):
    month_start = '202201'
    current_month = datetime.now().month
    current_year = datetime.now().year
    previous_month = current_month - 1
    print("current_month", current_month)
    print("current_year", current_year)

    month_end = f"{current_year}{str(previous_month).zfill(2)}"
    if platform == 'tiktok':
        month_start = '202209'
    lst_months = get_all_month_between(month_start, month_end)
    field_types = ['revenue', 'sale']
    for month in lst_months:
        for field_type in field_types:
            start_field, end_field = get_end_field_and_start_field(month, field_type)

            # hard code
            if '202409' in end_field and 'revenue' in end_field:
                end_field = 'revenue_history_20240926'
            elif '202409' in end_field and 'order' in end_field:
                end_field = "order_history_20240926"
            ###

            field_name = f"{field_type}_{month}"
            if 'runtime_mappings' not in raw_query:
                raw_query['runtime_mappings'] = {}
            raw_query['runtime_mappings'][field_name] = {
                "type": "double",
                "script": {
                    "source": "double c = 0; double d = 0; if (doc['" + end_field +
                              "'].size() > 0){ c= doc['" + end_field +
                              "'].value;} if (doc['" + start_field +
                              "'].size() > 0){d= doc['" + start_field + "'].value;} emit(c-d);"
                }
            }
            raw_query['fields'].append({
                "field": field_name,
            })
            raw_query['aggregations'][field_name] = {
                "sum": {
                    "field": field_name
                }
            }
    return raw_query


def add_source_to_query(raw_query: dict, platform: str = 'shopee'):
    if raw_query is None:
        return None
    raw_query['_source']['includes'].append("shop_base_id")
    raw_query['_source']['includes'].append("platform_id")
    return raw_query


def get_raw_elasticsearch_query(search_body: dict, pgn: str, platform: str = 'shopee', output_file_name: str = None):
    """
    This function is used to get the raw elasticsearch query from the bee_market/views/search/insight_search_view_set.py
    :return: the raw elasticsearch query
    """
    retry_time = 3
    retry_count = 0
    endpoint = 'https://apiv3.metric.vn/market/search_basic?include_raw_search_query=1'
    while retry_count < retry_time:
        try:
            response = requests.post(endpoint,
                                     json=search_body,
                                     headers=build_request_headers(),
                                     timeout=120)
            response_status = response.status_code
            print(
                f"curl -X POST -H 'Content-Type: application/json' -d '{json.dumps(search_body, ensure_ascii=False)}' '{endpoint}'")
            # print(f"status code: {response.json()}")
            if response_status == 200:
                print("connect success")
                response_json = response.json()
                data = response_json.get('data')
                raw_search_body = data.get('raw_search_body')
                if not raw_search_body:
                    return None
                els_query = add_aggregations_to_raw_query(raw_search_body, platform)
                els_query = add_source_to_query(els_query, platform)
                els_query["size"] = 60_000
                if not els_query:
                    return None
                els_query_dumps = json.dumps(els_query, ensure_ascii=False)
                file_name = f"{pgn}__{platform}__{output_file_name}"
                with open(f'{folder_output}/{file_name}.json', 'w', encoding='utf-8') as f:
                    f.write(els_query_dumps)
                return file_name
            else:
                print('connect error')
                retry_count += 1
        except Exception as e:
            log_error(e)
            retry_count += 1
    if retry_count == retry_time:
        return None


def compose_response_data(response_data: dict, duration_days: int):
    """
    This function is used to compose the response data
    :param response_data: the response data
    :return: the composed response data
    """
    if not response_data:
        return None
    products = response_data.get('products')
    aggregations = response_data.get('aggregations')
    revenue_total = 0
    order_total = 0
    product_total = 0
    shop_total = 0
    duration_code = f"{duration_days}D" if duration_days in [7, 30, 90, 180] else 'CUSTOM'
    for aggregation in aggregations:
        if 'aggregation' in aggregation and aggregation['aggregation'] == 'SHOP_TOTAL':
            shop_total = aggregation['entity']['value']
        if 'aggregation' in aggregation and aggregation['aggregation'] == f'PRODUCT_TOTAL':
            product_total = aggregation['entity']['value']
        if 'aggregation' in aggregation and aggregation['aggregation'] == f'REVENUE_TOTAL_{duration_code}':
            revenue_total = aggregation['entity']['value']
        if 'aggregation' in aggregation and aggregation['aggregation'] == f'ORDER_TOTAL_{duration_code}':
            order_total = aggregation['entity']['value']
    if products is not None:
        product_name_lst = [f"{product['product_base_id']},{product['product_name']}" for product in products]
        product_name_lst_text = '\n\t'.join(product_name_lst)
    else:
        product_name_lst_text = 'Empty'
    return (f"Doanh số: {revenue_total}\n"
            f"Số sản phẩm đã bán: {order_total}\n"
            f"Số sản phẩm có lượt bán: {product_total}\n"
            f"Số shop có lượt bán: {shop_total}\n"
            f"Danh sách sản phẩm: \n\t{product_name_lst_text}\n")


def get_response_data(search_body: dict, duration_days: int):
    """
    This function is used to get the response data from the bee_market/views/search/insight_search_view_set.py
    :return: the response data
    """
    retry_time = 3
    retry_count = 0
    endpoint = 'https://apiv3.metric.vn/market/search_basic'
    while retry_count < retry_time:
        try:
            response = requests.post(endpoint,
                                     json=search_body,
                                     headers=build_request_headers(),
                                     timeout=120)
            response_status = response.status_code
            if response_status == 200:
                response_json = response.json()
                return compose_response_data(response_json.get('data'), duration_days)
            else:
                retry_count += 1
        except Exception as e:
            log_error(e)
            retry_count += 1
    if retry_count == retry_time:
        return None


def get_keywords_from_row(row: Series):
    """
    This function is used to get the keywords from a row of the dataframe
    :param row: the row of the dataframe
    :return: the keywords
    """

    def handle_keywords_field(field_name: str):
        set_output_kw = set()
        if type(row[field_name]) is str:
            list_tmp_kw = row[field_name].split(',')
        else:
            list_tmp_kw = []
        for kw in list_tmp_kw:
            set_output_kw.update(set([sub_kw.strip() for sub_kw in kw.split(';')]))
        return list(set_output_kw)

    keywords = handle_keywords_field('Từ khóa')
    must_include_keywords = handle_keywords_field('Từ khóa +')
    exclude_keywords = handle_keywords_field('Từ khóa -')
    return keywords, must_include_keywords, exclude_keywords


def get_categories_from_row(row: Series, platform: str = 'shopee'):
    """
    This function is used to get the categories from a row of the dataframe
    :param platform:
    :param row: the row of the dataframe
    :return: the categories
    """
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
            # print(category_id)
            if category_id:
                lst_category.append(category_id)
        return lst_category


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


def get_date_range_from_row(row: Series):
    """
    This function is used to get the date range from a row of the dataframe
    :param row: the row of the dataframe
    :return: the date range
    """
    date_range = row['Khoảng thời gian phân tích']
    if type(date_range) is not str:
        return None, None, None
    start_date, end_date = date_range.split('-')
    start_date = start_date.strip()
    end_date = end_date.strip()
    start_date = datetime.strptime(start_date, '%d/%m/%Y').strftime('%Y%m%d')
    end_date = datetime.strptime(end_date, '%d/%m/%Y').strftime('%Y%m%d')
    duration_days = (datetime.strptime(end_date, '%Y%m%d') - datetime.strptime(start_date, '%Y%m%d')).days + 1
    return start_date, end_date, duration_days


def retrieve_elasticsearch_query(dataframe: pd.DataFrame, output_file_name: str):
    columns = dataframe.columns
    dataframe['Shopee Elasticsearch Query'] = None
    dataframe['Lazada Elasticsearch Query'] = None
    dataframe['Tiki Elasticsearch Query'] = None
    if 'Danh mục Sendo' in columns:
        dataframe['Sendo Elasticsearch Query'] = None
    dataframe['Tiktok Elasticsearch Query'] = None
    dataframe['Shopee Elasticsearch Result'] = None
    dataframe['Lazada Elasticsearch Result'] = None
    dataframe['Tiki Elasticsearch Result'] = None
    if 'Danh mục Sendo' in columns:
        dataframe['Sendo Elasticsearch Result'] = None
    dataframe['Tiktok Elasticsearch Result'] = None
    for idx, row in dataframe.iterrows():
        group_name = row['Nhóm hàng'] if isinstance(row['Nhóm hàng'], str) else generate_random_string(5)
        # print('group_name', group_name)
        keywords, must_include_keywords, exclude_keywords = get_keywords_from_row(row)
        shopee_categories = get_categories_from_row(row, 'shopee')
        tiki_categories = get_categories_from_row(row, 'tiki')
        lazada_categories = get_categories_from_row(row, 'lazada')
        if 'Danh mục Sendo' in columns:
            sendo_categories = get_categories_from_row(row, 'sendo')
        else:
            sendo_categories = 'skip'
        tiktok_categories = get_categories_from_row(row, 'tiktok')
        remove_bad_product_coefficient = row['Lọc sản phẩm ảo']
        not_include_deleted_product = row['Lọc sản phẩm đã xóa']
        search_mode = row['Chế độ tìm'] if isinstance(row['Chế độ tìm'], str) else None
        is_smart_search = True if search_mode == 'Tìm thông minh' else False
        min_price = row['Giá min'] if not math.isnan(row['Giá min']) else None
        max_price = row['Giá max'] if not math.isnan(row['Giá max']) else None
        start_date, end_date, duration_days = get_date_range_from_row(row)
        if shopee_categories != 'skip':
            shopee_query = build_request_body('shopee',
                                              keywords,
                                              must_include_keywords,
                                              exclude_keywords,
                                              shopee_categories,
                                              remove_bad_product_coefficient,
                                              not_include_deleted_product,
                                              is_smart_search,
                                              min_price,
                                              max_price,
                                              start_date,
                                              end_date,
                                              duration_days)
            print("shopee_query::", json.dumps(shopee_query, ensure_ascii=False))
            shopee_raw_query = get_raw_elasticsearch_query(shopee_query, group_name, 'shopee',
                                                           output_file_name=output_file_name)
            shopee_result_data = get_response_data(shopee_query, duration_days)
            dataframe.at[idx, 'Shopee Elasticsearch Query'] = shopee_raw_query
            dataframe.at[idx, 'Shopee Elasticsearch Result'] = shopee_result_data
        if lazada_categories != 'skip':
            lazada_query = build_request_body('lazada',
                                              keywords,
                                              must_include_keywords,
                                              exclude_keywords,
                                              lazada_categories,
                                              remove_bad_product_coefficient,
                                              not_include_deleted_product,
                                              is_smart_search,
                                              min_price,
                                              max_price,
                                              start_date,
                                              end_date,
                                              duration_days)
            lazada_raw_query = get_raw_elasticsearch_query(lazada_query, group_name, 'lazada',
                                                           output_file_name=output_file_name)
            dataframe.at[idx, 'Lazada Elasticsearch Query'] = lazada_raw_query
            lazada_result_data = get_response_data(lazada_query, duration_days)
            dataframe.at[idx, 'Lazada Elasticsearch Result'] = lazada_result_data
        if tiki_categories != 'skip':
            tiki_query = build_request_body('tiki',
                                            keywords,
                                            must_include_keywords,
                                            exclude_keywords,
                                            tiki_categories,
                                            remove_bad_product_coefficient,
                                            not_include_deleted_product,
                                            is_smart_search,
                                            min_price,
                                            max_price,
                                            start_date,
                                            end_date,
                                            duration_days)
            tiki_raw_query = get_raw_elasticsearch_query(tiki_query, group_name, 'tiki',
                                                         output_file_name=output_file_name)
            tiki_result_data = get_response_data(tiki_query, duration_days)
            dataframe.at[idx, 'Tiki Elasticsearch Query'] = tiki_raw_query
            dataframe.at[idx, 'Tiki Elasticsearch Result'] = tiki_result_data
        if sendo_categories != 'skip':
            sendo_query = build_request_body('sendo',
                                             keywords,
                                             must_include_keywords,
                                             exclude_keywords,
                                             sendo_categories,
                                             remove_bad_product_coefficient,
                                             not_include_deleted_product,
                                             is_smart_search,
                                             min_price,
                                             max_price,
                                             start_date,
                                             end_date,
                                             duration_days)
            sendo_raw_query = get_raw_elasticsearch_query(sendo_query, group_name, 'sendo',
                                                          output_file_name=output_file_name)
            sendo_result_data = get_response_data(sendo_query, duration_days)
            dataframe.at[idx, 'Sendo Elasticsearch Query'] = sendo_raw_query
            dataframe.at[idx, 'Sendo Elasticsearch Result'] = sendo_result_data
        if tiktok_categories != 'skip':
            tiktok_query = build_request_body('tiktok',
                                              keywords,
                                              must_include_keywords,
                                              exclude_keywords,
                                              tiktok_categories,
                                              remove_bad_product_coefficient,
                                              not_include_deleted_product,
                                              is_smart_search,
                                              min_price,
                                              max_price,
                                              start_date,
                                              end_date,
                                              duration_days)
            tiktok_raw_query = get_raw_elasticsearch_query(tiktok_query, group_name, 'tiktok',
                                                           output_file_name=output_file_name)
            print("tiktok_raw_query")
            print(json.dumps(tiktok_query, ensure_ascii=False))
            tiktok_result_data = get_response_data(tiktok_query, duration_days)
            dataframe.at[idx, 'Tiktok Elasticsearch Query'] = tiktok_raw_query
            dataframe.at[idx, 'Tiktok Elasticsearch Result'] = tiktok_result_data
    logger.info(f"Handle {dataframe.shape[0]} rows")
    dataframe['Shopee Elasticsearch Query'] = dataframe['Shopee Elasticsearch Query'].astype('object')
    dataframe['Lazada Elasticsearch Query'] = dataframe['Lazada Elasticsearch Query'].astype('object')
    dataframe['Tiki Elasticsearch Query'] = dataframe['Tiki Elasticsearch Query'].astype('object')
    if 'Danh mục Sendo' in columns:
        dataframe['Sendo Elasticsearch Query'] = dataframe['Sendo Elasticsearch Query'].astype('object')
    dataframe['Tiktok Elasticsearch Query'] = dataframe['Tiktok Elasticsearch Query'].astype('object')
    dataframe['Shopee Elasticsearch Result'] = dataframe['Shopee Elasticsearch Result'].astype('object')
    dataframe['Lazada Elasticsearch Result'] = dataframe['Lazada Elasticsearch Result'].astype('object')
    dataframe['Tiki Elasticsearch Result'] = dataframe['Tiki Elasticsearch Result'].astype('object')
    if 'Danh mục Sendo' in columns:
        dataframe['Sendo Elasticsearch Result'] = dataframe['Sendo Elasticsearch Result'].astype('object')
    dataframe['Tiktok Elasticsearch Result'] = dataframe['Tiktok Elasticsearch Result'].astype('object')


if __name__ == '__main__':
    PLATFORMS_DATE_LIMIT_CONFIG = get_limit_range_all_platforms()

    folder_name = 'sunscream_26092024_data_coding'
    sheet_name = 'Kem chống nắng'

    folder_output = f"C:\\Users\\Admin\\PycharmProjects\\export_data_metric\\da_tool\\da_tool\\data\\output\\json_query\\{folder_name}"

    if not os.path.exists(folder_output):
        os.makedirs(folder_output)
    input_file_path = r"C:\Users\Admin\Downloads\Bộ lọc báo cáo nhóm hàng.xlsx"
    time_str = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    output_file_name = f'{folder_name}_{time_str}'
    output_file_path = f'{folder_output}/{output_file_name}.xlsx'
    df = load_query_dataframe(input_file_path, sheet_name)
    lst_dataframes = split_dataframe_into_chunks(df, 2)

    with ThreadPoolExecutor(max_workers=10) as executor:
        for chunk in lst_dataframes:
            executor.submit(retrieve_elasticsearch_query, chunk, output_file_name)
    new_dataframe = combine_list_dataframe(lst_dataframes)

    with pd.ExcelWriter(output_file_path, engine='xlsxwriter',
                        engine_kwargs={'options': {'strings_to_urls': False}}) as writer:
        new_dataframe.to_excel(writer, index=False)