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
from helper.calendar_helpers import get_all_month_between, get_end_field_and_start_field
from helper.dataframe_helper import split_dataframe_into_chunks, combine_list_dataframe
from helper.error_helper import log_error
from helper.logger_helper import LoggerSimple
from helper.metric_helper import get_limit_range_all_platforms
from utils.exclude_utils import get_metric_blacklist_keyword_filter, get_metric_gift_product_filter
from retrive_elas_query import load_query_dataframe
from retrive_elas_query import retrieve_elasticsearch_query
import warnings
import json
import os.path
from hashlib import md5
from elasticsearch import Elasticsearch
import warnings
import pandas as pd
from main_export_new import get_product_url

logger = LoggerSimple(name=__name__).logger

if __name__ == '__main__':
    PLATFORMS_DATE_LIMIT_CONFIG = get_limit_range_all_platforms()

    folder_name = 'xit_khoang'
    sheet_name = 'Xịt Khoáng'
    time_str = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")

    suffix_file_name = f'{time_str}'
    output_file_path = f"C:\\Users\\Admin\\PycharmProjects\\Clean_report_E-report\\data_raw_cate\\{folder_name}"
    output_file_folder_ = f"C:\\Users\\Admin\\PycharmProjects\\Clean_report_E-report\\data_raw_cate"

    if os.path.exists(output_file_path):
        os.makedirs(output_file_path)

    input_file_get_filter_path = r"C:\Users\Admin\Downloads\Bộ lọc báo cáo nhóm hàng.xlsx"
    # input_file_get_filter_path = r"C:\Users\Admin\Downloads\Plan Q4_2024 - Data Coding.xlsx"

    df = load_query_dataframe(input_file_get_filter_path, sheet_name)
    lst_dataframes = split_dataframe_into_chunks(df, 2)

    with ThreadPoolExecutor(max_workers=10) as executor:
        for chunk in lst_dataframes:
            executor.submit(retrieve_elasticsearch_query, chunk, suffix_file_name)
    new_dataframe = combine_list_dataframe(lst_dataframes)

    for filename in os.listdir(output_file_folder_):
        if filename.endswith('.json'):
            file_path = os.path.join(output_file_folder_, filename)
            input_query_file = file_path
            name_platform = filename.split('__')[1]
            output_file = f'{output_file_folder_}\\{filename.replace(".json", ".xlsx")}'
            index_name = f'market_current__{name_platform}_vn,market_current_die__{name_platform}_vn'

            # index_name = 'market_current__shopee_vn,market_current_die__shopee_vn'
            is_replace_raw_output = True

            # http://<username>:<password>@sv6.beecost.net:9200
            es = Elasticsearch(['https://thangtm:b8Cv5Ey5qpMjMJaVu7UqJ@sv6.beecost.net:9200'],
                               request_timeout=10000,
                               verify_certs=False
                               )

            with open(input_query_file, 'rt', encoding='utf-8') as f:
                search_body = f.read()
                search_body = search_body.replace('\n', ' ').replace('"""', '"')
                search_body_hash = md5(search_body.encode('utf-8')).hexdigest().lower()
                raw_output_file = f'{search_body_hash}.json'
                print("search_body", "ok")

                if os.path.exists(raw_output_file) and not is_replace_raw_output:
                    with open(raw_output_file, 'rt', encoding='utf-8') as f:
                        result = json.loads(f.read())

                else:
                    search_body_json = json.loads(search_body)
                    result = es.search(index=index_name,
                                       body=json.loads(search_body),
                                       request_timeout=120
                                       ).body

                    with open(raw_output_file, 'wt', encoding='utf-8') as f:
                        f.write(json.dumps(result, ensure_ascii=False))

                # print('exported', result)
                response = result
                output_product_lst = []
                for hit in response['hits']['hits']:
                    output_product_lst.append(
                        {
                            "product_base_id": hit['_source'].get('product_base_id'),
                            "product_name": hit['_source'].get('product_name'),
                            "platform_id": hit['_source'].get('platform_id'),
                            "price": hit['_source'].get('price'),
                            "price_min": hit['_source'].get('price_min'),
                            "price_max": hit['_source'].get('price_max'),
                            "official_type": hit['_source'].get('official_type'),
                            "url_thumbnail": hit['_source'].get('url_thumbnail'),
                            "shop_base_id": hit['_source'].get('shop_base_id'),
                            "price_ranges": hit['_source'].get('price_ranges'),
                            "brand": hit['_source'].get('brand'),
                            "shop_platform_name": hit['_source'].get('shop_platform_name'),
                            "category": hit['_source'].get('category_base_id'),
                            "order_count": hit['_source'].get('order_count'),
                            "order_revenue_custom": hit['fields'].get('order_revenue_custom')[0],
                            "order_count_custom": hit['fields'].get('order_count_custom')[0],
                            # "revenue_202201": hit['fields'].get('revenue_202201')[0],
                            # "revenue_202202": hit['fields'].get('revenue_202202')[0],
                            # "revenue_202203": hit['fields'].get('revenue_202203')[0],
                            # "revenue_202204": hit['fields'].get('revenue_202204')[0],
                            # "revenue_202205": hit['fields'].get('revenue_202205')[0],
                            # "revenue_202206": hit['fields'].get('revenue_202206')[0],
                            # "revenue_202207": hit['fields'].get('revenue_202207')[0],
                            # "revenue_202208": hit['fields'].get('revenue_202208')[0],
                            # "revenue_202209": hit['fields'].get('revenue_202209')[0],
                            "revenue_202210": hit['fields'].get('revenue_202210')[0],
                            "revenue_202211": hit['fields'].get('revenue_202211')[0],
                            "revenue_202212": hit['fields'].get('revenue_202212')[0],
                            "revenue_202301": hit['fields'].get('revenue_202301')[0],
                            "revenue_202302": hit['fields'].get('revenue_202302')[0],
                            "revenue_202303": hit['fields'].get('revenue_202303')[0],
                            "revenue_202304": hit['fields'].get('revenue_202304')[0],
                            "revenue_202305": hit['fields'].get('revenue_202305')[0],
                            "revenue_202306": hit['fields'].get('revenue_202306')[0],
                            "revenue_202307": hit['fields'].get('revenue_202307')[0],
                            "revenue_202308": hit['fields'].get('revenue_202308')[0],
                            "revenue_202309": hit['fields'].get('revenue_202309')[0],
                            "revenue_202310": hit['fields'].get('revenue_202310')[0],
                            "revenue_202311": hit['fields'].get('revenue_202311')[0],
                            "revenue_202312": hit['fields'].get('revenue_202312')[0],
                            "revenue_202401": hit['fields'].get('revenue_202401')[0],
                            "revenue_202402": hit['fields'].get('revenue_202402')[0],
                            "revenue_202403": hit['fields'].get('revenue_202403')[0],
                            "revenue_202404": hit['fields'].get('revenue_202404')[0],
                            "revenue_202405": hit['fields'].get('revenue_202405')[0],
                            "revenue_202406": hit['fields'].get('revenue_202406')[0],
                            "revenue_202407": hit['fields'].get('revenue_202407')[0],
                            "revenue_202408": hit['fields'].get('revenue_202408')[0],
                            "revenue_202409": hit['fields'].get('revenue_202409')[0],
                            # "sale_202201": hit['fields'].get('sale_202201')[0],
                            # "sale_202202": hit['fields'].get('sale_202202')[0],
                            # "sale_202203": hit['fields'].get('sale_202203')[0],
                            # "sale_202204": hit['fields'].get('sale_202204')[0],
                            # "sale_202205": hit['fields'].get('sale_202205')[0],
                            # "sale_202206": hit['fields'].get('sale_202206')[0],
                            # "sale_202207": hit['fields'].get('sale_202207')[0],
                            # "sale_202208": hit['fields'].get('sale_202208')[0],
                            # "sale_202209": hit['fields'].get('sale_202209')[0],
                            "sale_202210": hit['fields'].get('sale_202210')[0],
                            "sale_202211": hit['fields'].get('sale_202211')[0],
                            "sale_202212": hit['fields'].get('sale_202212')[0],
                            "sale_202301": hit['fields'].get('sale_202301')[0],
                            "sale_202302": hit['fields'].get('sale_202302')[0],
                            "sale_202303": hit['fields'].get('sale_202303')[0],
                            "sale_202304": hit['fields'].get('sale_202304')[0],
                            "sale_202305": hit['fields'].get('sale_202305')[0],
                            "sale_202306": hit['fields'].get('sale_202306')[0],
                            "sale_202307": hit['fields'].get('sale_202307')[0],
                            "sale_202308": hit['fields'].get('sale_202308')[0],
                            "sale_202309": hit['fields'].get('sale_202309')[0],
                            "sale_202310": hit['fields'].get('sale_202310')[0],
                            "sale_202311": hit['fields'].get('sale_202311')[0],
                            "sale_202312": hit['fields'].get('sale_202312')[0],
                            "sale_202401": hit['fields'].get('sale_202401')[0],
                            "sale_202402": hit['fields'].get('sale_202402')[0],
                            "sale_202403": hit['fields'].get('sale_202403')[0],
                            "sale_202404": hit['fields'].get('sale_202404')[0],
                            "sale_202405": hit['fields'].get('sale_202405')[0],
                            "sale_202406": hit['fields'].get('sale_202406')[0],
                            "sale_202407": hit['fields'].get('sale_202407')[0],
                            "sale_202408": hit['fields'].get('sale_202408')[0],
                            "sale_202409": hit['fields'].get('sale_202409')[0],
                            'url': str(get_product_url(hit['_source'].get('product_base_id')))
                        }
                    )

                df = pd.DataFrame(output_product_lst)
                # df.to_excel(output_file, index=False, engine='xlsxwriter')
                with pd.ExcelWriter(output_file, engine='xlsxwriter',
                                    engine_kwargs={'options': {'strings_to_urls': False}}) as writer:
                    df.to_excel(writer, index=False)

    files = os.listdir(output_file_folder_)  # Lấy danh sách các tệp tin trong thư mục
    df = None
    total = 0

    for file in files:
        if file.endswith('.xlsx') or file.endswith('.xls'):  # Chỉ xử lý các tệp có đuôi .xlsx hoặc .xls
            file_path = os.path.join(output_file_folder_, file)
            df_file = pd.read_excel(file_path)
            print(file, df_file.shape[0])
            total += df_file.shape[0]

            if df is None:
                df = df_file
            else:
                df = pd.concat([df, df_file], ignore_index=True)

    # Loại bỏ các bản ghi trùng lặp theo cột 'product_base_id'
    df1 = df.drop_duplicates(subset=['product_base_id'], keep='first')
    print("total::", total)
    print("output_size::", df1.shape)

    writer = pd.ExcelWriter(os.path.join(output_file_folder_, f'{folder_name}_update_to_30092024.xlsx'), engine='xlsxwriter',
                            engine_kwargs={'options': {'strings_to_urls': False}})

    df1.to_excel(writer, index=False)

    writer.close()