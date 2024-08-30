import json
import os
import re
from datetime import datetime

import math
import pandas as pd
from pandas import Series

from app.constant.constant_metric import get_map_category_obj
from helper.logger_helper import LoggerSimple
from helper.text_hash_helper import text_to_hash_md5
from schedule.report_service.build_es_query_service import FilterReport, Range

clickhouse_config = {
    'Host': 'sv7.beecost.net',
    'Port': '8124',
    'User': 'minhtien',
    'Password': 't5W54BybGTZxLPzy',
}

logger = LoggerSimple(name=__name__).logger

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


def sanitize_excel_string(value: str) -> str:
    """
    Sanitize a string to be safely used in Excel cells.
    """
    if isinstance(value, str):
        return re.sub(r"[\000-\010]|[\013-\014]|[\016-\037]", "", value)
    return value


def format_text_currency(price, min_value=1_000):
    if not price:
        return price

    price = float(price)

    def round_and_format(price, divisor, unit):
        rounded_price = '{:,.0f}'.format(round(price / divisor, 2))
        return f"{rounded_price}{unit}"

    if price > 1_000_000_000 and price % 1_000_000_000 != 0:
        return round_and_format(price, 1_000_000_000, " tỷ")

    if price > 1_000_000 and price % 1_000_000 != 0 and price >= min_value:
        return round_and_format(price, 1_000_000, " triệu")

    if price > 1000 and price % 1000 != 0 and price >= min_value:
        return round_and_format(price, 1000, " nghìn")

    return round_and_format(price, 1, "")


def load_query_dataframe(file_path: str, sheet_name: str = None) -> pd.DataFrame:
    if file_path.endswith('.csv'):
        return pd.read_csv(file_path)
    elif file_path.endswith('.xlsx'):
        return pd.read_excel(file_path, sheet_name=sheet_name)


def build_clickhouse_query(filer_report):
    where_query = ""

    # lst_category_base_id = filer_report.lst_category_base_id
    # if lst_category_base_id:
    #     where_query += f" AND ("
    #     where_query += f" categories__id_1 IN {tuple(lst_category_base_id)} "
    #     where_query += f" OR categories__id_2 IN {tuple(lst_category_base_id)} "
    #     where_query += f" OR categories__id_3 IN {tuple(lst_category_base_id)} "
    #     where_query += f" OR categories__id_4 IN {tuple(lst_category_base_id)} "
    #     where_query += f" OR categories__id_5 IN {tuple(lst_category_base_id)} "
    #     where_query += f")"

    lst_shopee_categories = filer_report.lst_shopee_categories
    if lst_shopee_categories:
        where_query += f" OR (platform_id = 1"
        where_query += f" AND ("
        where_query += f" categories__id_1 IN {tuple(lst_shopee_categories)} "
        where_query += f" OR categories__id_2 IN {tuple(lst_shopee_categories)} "
        where_query += f"))"
    else:
        where_query += f" OR (platform_id = 1)"

    lst_lazada_categories = filer_report.lst_lazada_categories
    if lst_lazada_categories:
        where_query += f" OR (platform_id = 2"
        where_query += f" AND ("
        where_query += f" categories__id_1 IN {tuple(lst_lazada_categories)} "
        where_query += f" OR categories__id_2 IN {tuple(lst_lazada_categories)} "
        where_query += f"))"
    else:
        where_query += f" OR (platform_id = 2)"

    lst_tiki_categories = filer_report.lst_tiki_categories
    if lst_tiki_categories:
        where_query += f" OR (platform_id = 3"
        where_query += f" AND ("
        where_query += f" categories__id_1 IN {tuple(lst_tiki_categories)} "
        where_query += f" OR categories__id_2 IN {tuple(lst_tiki_categories)} "
        where_query += f"))"
    else:
        where_query += f" OR (platform_id = 3)"

    lst_tiktok_categories = filer_report.lst_tiktok_categories
    if lst_tiktok_categories:
        where_query += f" OR (platform_id = 8"
        where_query += f" AND ("
        where_query += f" categories__id_1 IN {tuple(lst_tiktok_categories)} "
        where_query += f" OR categories__id_2 IN {tuple(lst_tiktok_categories)} "
        where_query += f"))"
    else:
        where_query += f" OR (platform_id = 8)"

    is_split_keyword = filer_report.is_smart_queries
    if filer_report.lst_keyword:
        where_query += f" AND ("
        for keyword in filer_report.lst_keyword:
            where_query += f"("
            if is_split_keyword:
                lst_sub_keyword = keyword.split(' ')
            else:
                lst_sub_keyword = [keyword]

            lst_keyword_query = [f"product_name ILIKE '%{sub_keyword}%'" for sub_keyword in lst_sub_keyword]
            where_query += f" {' AND '.join(lst_keyword_query)}"
            where_query += f") OR"
        where_query = where_query[:-2]
        where_query += f")"

    lst_keyword_exclude = filer_report.lst_keyword_exclude
    if lst_keyword_exclude:
        lst_keyword_exclude_query = [f"product_name NOT ILIKE '%{keyword}%'" for keyword in lst_keyword_exclude]
        where_query += f" AND {' AND '.join(lst_keyword_exclude_query)}"

    lst_keyword_required = filer_report.lst_keyword_required
    if lst_keyword_required:
        where_query += f" AND ("
        for keyword in lst_keyword_required:
            lst_keyword_query = [f"product_name ILIKE '%{sub_keyword}%'" for sub_keyword in [keyword]]
            where_query += f" {' AND '.join(lst_keyword_query)} AND"
        where_query = where_query[:-3]
        where_query += f")"

    price_range = filer_report.price_range
    if price_range:
        where_query += f" AND price >= {price_range.begin} AND price <= {price_range.end}"

    if where_query.startswith(" AND "):
        where_query = where_query[5:]

    if where_query.startswith(" OR "):
        where_query = where_query[4:]
    # print(where_query)
    return where_query


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
        return []
    if category_cell_value == 'Tất cả':
        return []
        return [category['value'] for category in categories_tree]
    elif category_cell_value.strip() == 'Không lấy dữ liệu':
        return []
    else:
        lst_category = []
        lst_name = category_cell_value.split('\n')
        for name in lst_name:
            category_id = find_category_id_by_label_path(name.strip(), categories_tree)
            if category_id:
                lst_category.append(category_id)
        return lst_category


def build_multiple_row_data_query(index, df_batch, start_date, end_date):
    has_any_filter_change = False
    for idx, row in df_batch.iterrows():
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

        has_change_filter = key_filter_report != key_response_report

        if has_change_filter:
            has_any_filter_change = True
            break
    if not has_any_filter_change:
        return None
    aggs_query = "SELECT"
    for idx, row in df_batch.iterrows():
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

        # has_change_filter = True
        has_change_filter = key_filter_report != key_response_report

        lst_keyword = []
        if type(row['Từ khóa']) is str:
            lst_keyword = [keyword.strip() for keyword in row['Từ khóa'].split(',')]

        lst_exclude_keyword = []
        if type(row['Từ khóa loại trừ']) is str:
            lst_exclude_keyword = [keyword.strip() for keyword in row['Từ khóa loại trừ'].split(',')]

        lst_keyword_required = []
        if type(row['Từ khóa cộng']) is str:
            lst_keyword_required = [keyword.strip() for keyword in row['Từ khóa cộng'].split(',')]

        is_smart_queries = True if row['Chế độ tìm'] == 'Tìm thông minh' else False
        price_min = row['Giá min'] if not math.isnan(row['Giá min']) else None
        price_max = row['Giá max'] if not math.isnan(row['Giá max']) else None

        lst_shopee_categories = get_categories_from_row(row, 'shopee')
        lst_lazada_categories = get_categories_from_row(row, 'lazada')
        lst_tiki_categories = get_categories_from_row(row, 'tiki')
        lst_tiktok_categories = get_categories_from_row(row, 'tiktok')

        price_range = None
        if price_min or price_max:
            price_range = Range(
                begin=price_min,
                end=price_max
            )

        filter_report = FilterReport(
            lst_platform_id=[1, 2, 3, 8],
            lst_keyword_exclude=lst_exclude_keyword,
            lst_keyword_required=lst_keyword_required,
            lst_keyword=lst_keyword,
            is_smart_queries=is_smart_queries,
            lst_category_base_id=lst_shopee_categories + lst_lazada_categories + lst_tiki_categories + lst_tiktok_categories,
            price_range=price_range,
            is_remove_fake_sale=True,
            lst_shopee_categories=lst_shopee_categories,
            lst_lazada_categories=lst_lazada_categories,
            lst_tiki_categories=lst_tiki_categories,
            lst_tiktok_categories=lst_tiktok_categories,
        )

        where_query = build_clickhouse_query(filter_report)
        if has_change_filter:
            aggs_query += f"""
                (sumIf(revenue_custom, {where_query}),
                sumIf(order_custom, {where_query}),
                countIf(distinct product_base_id, {where_query}),
                countIf(distinct shop_base_id, {where_query}),
                topKWeightedIf(30, 3, 'counts')(platform, revenue_custom, {where_query}),
                topKWeightedIf(30, 3, 'counts')(categories__id_2, revenue_custom, {where_query}),
                topKWeightedIf(500, 3, 'counts')(product_name, revenue_custom, {where_query}),
                topKWeightedIf(30, 3, 'counts')(categories__id_1, revenue_custom, {where_query})) as report_{idx},"""
        else:
            aggs_query += f"""
                ('', '', '', '', '', '', '') as report_{idx},"""

    # print(where_query)
    start_date = datetime.strptime(start_date, '%Y%m%d').strftime('%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y%m%d').strftime('%Y-%m-%d')
    aggs_query = aggs_query[:-1]
    aggs_query += f"""
            FROM (
                SELECT *,
                    arraySum(x -> if(x.1 between '{start_date}' and '{end_date}', x.3, 0),
                             order_revenue_arr) AS revenue_custom,
                    arraySum(x -> if(x.1 between '{start_date}' and '{end_date}', x.2, 0),
                             order_revenue_arr) AS order_custom
                FROM analytics.products
            ) p
            WHERE platform_id IN (1, 2, 3, 8) 
                AND order_custom >= 1 
                AND (rating_count * 1.0 > order_count * 0.03)
                AND NOT (product_name ILIKE '%[QT_Pampers]%' OR product_name ILIKE '%GIFT_Nước%' OR
                   product_name ILIKE '%GIFT_Tinh%' OR product_name ILIKE '%GIFT_%' OR product_name ILIKE '%GIFT_Gel%' OR
                   product_name ILIKE '%GIFT_Sữa%' OR product_name ILIKE '%GIFT_TÚI%' OR product_name ILIKE '%GIFT_Combo%' OR
                   product_name ILIKE '%GIFT_Tinh chất (Serum)%' OR product_name ILIKE '%GIFT_[Phiên bản lễ hội 2022] Túi%' OR
                   product_name ILIKE '%GIFT_ Combo%' OR product_name ILIKE '%GIFT_Kem%' OR product_name ILIKE '%Gift_Bộ%' OR
                   product_name ILIKE '%[MKB Gift]%' OR product_name ILIKE '%[QT_Huggies]%' OR
                   product_name ILIKE '%(QT_Huggies)%' OR product_name ILIKE '%không bán%' OR
                   product_name ILIKE '%[HC GIFT]%' OR product_name ILIKE '%[Gift]%' OR product_name ILIKE '%[Quà tặng]%' OR
                   product_name ILIKE '%[Hàng tặng không bán]%' OR product_name ILIKE '%[HB GIFT]%' OR
                   product_name ILIKE '%[Quà tặng không bán]%' OR product_name ILIKE '%quà tặng không bán%')
               AND product_base_id NOT IN
                  ('1__24957347281__14063555', '1__22222367403__266619909', '1__23722365583__266619909', '1__11141784570__37147137',
                   '1__21482936482__233692311', '1__17651456174__147191228', '1__25106186302__27495839',
                   '1__22978629888__1041184627', '1__20195900981__1041184627', '1__21491369435__1041184627',
                   '1__18293309371__1041184627', '1__23653135954__1041184627', '1__22075364950__1041184627',
                   '1__24958033319__1041184627', '1__21582424436__1041184627', '1__24058608585__1041184627',
                   '1__18682436952__1041184627', '1__22487271251__1041184627', '1__22759580760__1041184627',
                   '1__18783598787__1041184627', '1__22353137296__1041184627', '1__24005630082__1041184627',
                   '1__14399401581__1041184627', '1__21286543196__1041184627', '1__25366405828__1041184627',
                   '1__20595900951__1041184627', '1__12881879616__341418380', '1__22718228347__78546729',
                   '1__20283585269__1041184627', '1__17093392925__948819478', '1__19674370457__948819478',
                   '1__20816632520__37251700', '1__18293673509__1041184627', '1__18981035578__102374043',
                   '1__24501797634__1017203611', '1__15698792132__78546729', '1__22744159326__78546729', '1__19188744016__78546729',
                   '1__22126709725__78546729', '1__22836017144__78546729', '1__18877323592__78546729', '1__22856676638__90366612',
                   '1__23418231110__78546729', '1__21273342308__954659720', '1__23241302273__180651803',
                   '1__22967804703__944545485', '1__21476491960__180651803', '1__22228577909__182053498',
                   '1__23024413633__954659720', '1__23630395242__182053498', '1__22448104536__391379978',
                   '1__18990756057__391379978', '1__17298284924__391379978', '1__15510206761__487013605',
                   '1__19489251460__300458619', '1__10815055198__37251700', '1__23533338149__78546729', '1__22842437672__78546729',
                   '1__23118220623__78546729', '1__23529023038__78546729', '1__22035520048__78546729', '1__16493661986__78546729',
                   '1__23326697557__78546729', '1__13498828558__78546729', '1__19371170617__78546729', '1__21087297107__1041184627',
                   '1__25603574145__1041184627', '1__23053544950__989774266', '1__23570023664__76421217',
                   '1__23640709144__530333613', '1__21249461418__37251700', '1__5649906977__37147137', '1__8054727060__37147137',
                   '1__8933594845__37147137', '1__11221656422__37147137', '1__18935483365__122583961', '1__21448128807__122583961',
                   '1__17595131274__122583961', '1__17349788726__122583961', '1__17073730664__122583961',
                   '1__17462767801__122583961', '1__19416740522__37251700', '1__22731830626__320642539', '1__23616219555__27495839',
                   '1__10590701768__37251700', '1__9739323899__37251700', '1__2343883738__697821129')
              AND shop_platform_id NOT IN (391379079, 391379978, 521935626, 266485724, 451610579, 1099988375, 400054588, 68410045)
              AND is_deleted = false
        """

    return aggs_query


def run():
    input_file_path = f'{ROOT_DIR}/eReport -TTN-clean_cate_1.xlsx'
    # input_file_path = f'/Users/tienbm/Downloads/danh sách báo cáo thời trang nữ (1).xlsx'
    # input_file_path = f'/Users/tienbm/Downloads/output_file_part_2 (2).xlsx'
    df = load_query_dataframe(input_file_path, 'Sheet1')
    pd.options.mode.copy_on_write = True

    batch_size = 50

    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=clickhouse_config['Host'],
        port=clickhouse_config['Port'],
        user=clickhouse_config['User'],
        password=clickhouse_config['Password']
    )

    start_date = '20230701'
    end_date = '20240630'

    for i in range(0, len(df), batch_size):
        start_time = datetime.now()
        df_batch = df[i:i + batch_size]

        query = build_multiple_row_data_query(i, df_batch, start_date, end_date)
        if not query:
            continue

        # print(query)
        # exit()
        aggs = client.query(query)

        result = aggs.result_rows[0]

        for idx, result_row in zip(range(i, i + batch_size), result):
            # print('keyword', df.loc[idx, 'name'])
            if not result_row:
                # print(f"Row {idx} is empty")
                continue
            revenue_total = result_row[0]
            if not revenue_total:
                continue
            order_total = result_row[1]
            product_total = result_row[2]
            shop_total = result_row[3]
            revenue_by_platform = result_row[4]
            revenue_by_categories__id_2 = result_row[5]
            lst_product = result_row[6]
            revenue_by_categories__id_1 = result_row[7]

            top_10_product = [p.get('item') for p in lst_product[:30]]
            # middle_10_product = [p.get('item') for p in
            #                      lst_product[len(lst_product) // 2 - 5: len(lst_product) // 2 + 5]]
            # bottom_10_product = [p.get('item') for p in lst_product[-10:]]

            revenue_by_market_place = ''
            shopee_revenue = 0
            lazada_revenue = 0
            tiki_revenue = 0
            tiktok_revenue = 0
            for item in revenue_by_platform:
                name = item.get('item')
                revenue = item.get('count')
                if name == 'shopee':
                    shopee_revenue = revenue
                if name == 'lazada':
                    lazada_revenue = revenue
                if name == 'tiki':
                    tiki_revenue = revenue
                if name == 'tiktok':
                    tiktok_revenue = revenue
                ratio_revenue = round((revenue / revenue_total) * 100, 2)
                revenue_by_market_place += f"{name} - {format_text_currency(revenue)} - {ratio_revenue}%\n"
            revenue_by_market_place = revenue_by_market_place[:-1]

            lst_shopee_category = [cate for cate in revenue_by_categories__id_2 if
                                   cate.get('item', '').startswith('1__')]
            lst_lazada_category = [cate for cate in revenue_by_categories__id_2 if
                                   cate.get('item', '').startswith('2__')]
            lst_tiki_category = [cate for cate in revenue_by_categories__id_2 if cate.get('item', '').startswith('3__')]
            lst_tiktok_category = [cate for cate in revenue_by_categories__id_2 if
                                   cate.get('item', '').startswith('8__')]

            lst_shopee_category += [cate for cate in revenue_by_categories__id_1 if
                                    cate.get('item', '').startswith('1__')]
            lst_lazada_category += [cate for cate in revenue_by_categories__id_1 if
                                    cate.get('item', '').startswith('2__')]
            lst_tiki_category += [cate for cate in revenue_by_categories__id_1 if
                                  cate.get('item', '').startswith('3__')]
            lst_tiktok_category += [cate for cate in revenue_by_categories__id_1 if
                                    cate.get('item', '').startswith('8__')]

            shopee_category_str = ''
            lazada_category_str = ''
            tiki_category_str = ''
            tiktok_category_str = ''
            map_category_obj = get_map_category_obj()

            for cate in lst_shopee_category:
                category_id = cate.get('item')
                revenue = cate.get('count')
                category = map_category_obj.get(category_id)
                if not category:
                    continue
                ratio_revenue = round((revenue / shopee_revenue) * 100, 2)
                if ratio_revenue < 1:
                    continue
                if category.get('label') == 'Chưa phân loại':
                    shopee_category_str += f"{category.get('label')} - {format_text_currency(revenue)} - {ratio_revenue}%\n"
                if category.get('level') == 2:
                    shopee_category_str += f"{category.get('parent_name')}/{category.get('label')} - {format_text_currency(revenue)} - {ratio_revenue}%\n"
            shopee_category_str = shopee_category_str[:-1]

            for cate in lst_lazada_category:
                category_id = cate.get('item')
                revenue = cate.get('count')
                category = map_category_obj.get(category_id)
                if not category:
                    continue
                ratio_revenue = round((revenue / lazada_revenue) * 100, 2)
                if ratio_revenue < 1:
                    continue
                if category.get('label') == 'Chưa phân loại':
                    lazada_category_str += f"{category.get('label')} - {format_text_currency(revenue)} - {ratio_revenue}%\n"
                if category.get('level') == 2:
                    lazada_category_str += f"{category.get('parent_name')}/{category.get('label')} - {format_text_currency(revenue)} - {ratio_revenue}%\n"
            lazada_category_str = lazada_category_str[:-1]

            for cate in lst_tiki_category:
                category_id = cate.get('item')
                revenue = cate.get('count')
                category = map_category_obj.get(category_id)
                if not category:
                    continue
                ratio_revenue = round((revenue / tiki_revenue) * 100, 2)
                if ratio_revenue < 1:
                    continue
                if category.get('label') == 'Chưa phân loại':
                    tiki_category_str += f"{category.get('label')} - {format_text_currency(revenue)} - {ratio_revenue}%\n"
                if category.get('level') == 2:
                    tiki_category_str += f"{category.get('parent_name')}/{category.get('label')} - {format_text_currency(revenue)} - {ratio_revenue}%\n"
            tiki_category_str = tiki_category_str[:-1]

            for cate in lst_tiktok_category:
                category_id = cate.get('item')
                category = map_category_obj.get(category_id)
                if not category:
                    continue
                if category.get('level') != 1:
                    continue

                revenue = cate.get('count')
                ratio_revenue = round((revenue / tiktok_revenue) * 100, 2)
                tiktok_category_str += f"{category.get('label')} - {format_text_currency(revenue)} - {ratio_revenue}%\n"
            tiktok_category_str = tiktok_category_str[:-1]

            lst_product_name_str = ''
            # for product in top_10_product + middle_10_product + bottom_10_product:
            for product in top_10_product:
                lst_product_name_str += f"{product}\n"

            lst_product_name_str = lst_product_name_str[:-1]

            row = df_batch.loc[idx]
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
            df.loc[idx, 'Lần query data'] = row['Lần query data'] + 1 if 'Lần query data' in row else 1
            df.loc[idx, 'Key'] = key_filter_report
            df.loc[idx, 'Doanh số từng sàn'] = revenue_by_market_place
            df.loc[idx, 'Doanh số'] = format_text_currency(revenue_total)
            df.loc[idx, 'Sản lượng'] = format_text_currency(order_total)
            df.loc[idx, 'Sản phẩm có lượt bán'] = format_text_currency(product_total)
            df.loc[idx, 'Số shop'] = format_text_currency(shop_total)
            df.loc[idx, 'Ngành hàng Shopee'] = shopee_category_str
            df.loc[idx, 'Ngành hàng Lazada'] = lazada_category_str
            df.loc[idx, 'Ngành hàng Tiki'] = tiki_category_str
            df.loc[idx, 'Ngành hàng Tiktok'] = tiktok_category_str
            df.loc[idx, 'Product name'] = lst_product_name_str[:-1]

        print(f"Time to process batch {i + 1}-{i + batch_size}: {datetime.now() - start_time}")
        df = df.map(sanitize_excel_string)
        df.to_excel(input_file_path, index=False)
        # exit()


if __name__ == '__main__':
    run()
