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
from run_data_for_updating_filter_report_manually import load_category_tree, find_category_id_by_label_path
from schedule.report_service.build_es_query_service import build_query_es_from_api, get_aggs_runtime_mapping_es, \
    FilterReport, Range
from schedule.report_service.transform_es_response_service import _tranform_aggs


logger = LoggerSimple(name=__name__).logger
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

default_exclude_keyword = [
    "https://shopee.vn/product/266619909/22222367403",
    "https://shopee.vn/product/266619909/23722365583",
    "https://shopee.vn/product/37147137/11141784570",
    "https://shopee.vn/product/233692311/21482936482",
    "https://shopee.vn/product/147191228/17651456174",
    "https://shopee.vn/product/27495839/25106186302",
    "[QT_Pampers]",
    "https://shopee.vn/gifttui-tote-kho-lon-co-2-tui-truoc-phoi-vang-i.1041184627.22978629888",
    "https://shopee.vn/giftcombo-12-mat-na-dat-set-rare-earth-deep-pore-cleansing-masque-5ml-i.1041184627.20195900981",
    "https://shopee.vn/giftkem-duong-am-kiehls-ultra-facial-cream-14ml-i.1041184627.21491369435",
    "https://shopee.vn/giftbo-doi-nuoc-can-bang-hoa-cuc-kiehls-calendula-herbal-extract-alcohol-free-toner-40ml-i.1041184627.18293309371",
    "https://shopee.vn/giftbo-2-kem-duong-am-7ml-i.1041184627.23653135954",
    "https://shopee.vn/giftsua-rua-mat-hoa-cuc-kiehls-calendula-deep-cleansing-foaming-face-wash-30ml-i.1041184627.22075364950",
    "https://shopee.vn/giftcombo-4-duong-chat-serum-lam-sang-da-mo-tham-mun-15ml-mau-thu-goi-giay-i.1041184627.24958033319",
    "https://shopee.vn/giftgel-duong-am-kiehls-ultra-facial-oil-free-gel-cream-14ml-i.1041184627.21582424436",
    "https://shopee.vn/giftcombo-12-gel-duong-am-kiehls-ultra-facial-oil-free-gel-cream-3ml-sachet-i.1041184627.24058608585",
    "https://shopee.vn/giftkem-duong-am-kiehls-ultra-facial-cream-7ml-i.1041184627.18682436952",
    "https://shopee.vn/giftcombo-2-tinh-chat-retinol-micro-dose-giup-tai-tao-giup-da-san-chac-dan-hoi-10ml-i.1041184627.22487271251",
    "https://shopee.vn/giftcombo-7-mat-na-dat-set-rare-earth-deep-pore-cleansing-masque-5ml-i.1041184627.22759580760",
    "https://shopee.vn/giftcombo-2-kem-duong-am-kiehls-ultra-facial-cream-14ml-i.1041184627.18783598787",
    "https://shopee.vn/gifttinh-chat-serum-lam-sang-da-mo-tham-mun-kiehls-clearly-corrective-dark-spot-solution-4ml-i.1041184627.22353137296",
    "https://shopee.vn/giftcombo-4-gel-duong-am-kiehls-ultra-facial-oil-free-gel-cream-3ml-sachet-i.1041184627.24005630082",
    "https://shopee.vn/giftkem-duong-phuc-hoi-ban-dem-14ml-kiehls-mid-recovery-cream-i.1041184627.14399401581",
    "https://shopee.vn/giftcombo-6-mat-na-dat-set-rare-earth-deep-pore-cleansing-masque-5ml-i.1041184627.21286543196",
    "https://shopee.vn/giftnuoc-can-bang-hoa-cuc-kiehls-calendula-herbal-extract-alcohol-free-toner-75ml-i.1041184627.25366405828",
    "https://shopee.vn/giftcombo-4-mat-na-dat-set-rare-earth-deep-pore-cleansing-masque-5ml-i.1041184627.20595900951",
    "GIFT_Nước",
    "GIFT_Tinh",
    "GIFT_",
    "GIFT_Gel",
    "GIFT_Sữa",
    "GIFT_TÚI",
    "GIFT_Combo",
    "GIFT_Tinh chất (Serum)",
    "GIFT_[Phiên bản lễ hội 2022] Túi",
    "GIFT_ Combo",
    "GIFT_Kem",
    "Gift_Bộ",
    "https://shopee.vn/hang-tangdau-tay-trang-espoir-all-makeup-deep-cleansing-oil-i.341418380.12881879616",
    "https://shopee.vn/giftnuoc-tay-trang-thanh-loc-va-lam-sach-sau-neutrogena-deep-clean-dung-tich-400ml-combo-2-kem-duong-i.78546729.22718228347",
    "https://shopee.vn/giftdau-tay-trang-kiehls-midnight-recovery-botanical-cleansing-oil-40ml-i.1041184627.20283585269",
    "https://shopee.vn/-GIFT-H%C3%80NG-T%E1%BA%B6NG-KH%C3%94NG-B%C3%81N-S%C3%A1p-t%E1%BA%A9y-trang-BANILA-CO-Clean-It-Zero-Cleansing-Balm-Pore-Clarifying-Blister-(3ml)-i.948819478.17093392925?sp_atk=a0de330f-e238-41de-a055-08349b5ca99a&xptdk=a0de330f-e238-41de-a055-08349b5ca99a",
    "https://shopee.vn/-GIFT-H%C3%80NG-T%E1%BA%B6NG-KH%C3%94NG-B%C3%81N-S%C3%A1p-t%E1%BA%A9y-trang-Clean-It-Zero-Original-Miniature-(7ml)-i.948819478.19674370457",
    "https://shopee.vn/vi-cam-tay-tien-loi-la-roche-posay-i.37251700.20816632520",
    "https://shopee.vn/product/1041184627/18293673509",
    "https://shopee.vn/sua-tieu-duong-diasure-850g-ban-moi-date-moi-i.102374043.18981035578",
    "https://shopee.vn/giftnuoc-duong-da-ngan-ngua-lao-hoa-elixir-bouncing-moisture-lotion-2-18ml-i.1017203611.24501797634",
    "https://shopee.vn/giftcombo-2-sua-tam-goi-toan-than-johnsons-top-to-toe-200ml-sua-tam-goi-toan-than-johnsons-top-to-to-i.78546729.15698792132",
    "https://shopee.vn/giftcombo-sua-tam-goi-toan-than-johnsons-ttt-100ml-nuoc-hoa-ban-mai-50ml-2-kem-duong-aveeno-14g-i.78546729.22744159326",
    "https://shopee.vn/giftphan-thom-cho-be-huong-hoa-johnsons-baby-powder-200g-sua-tam-goi-toan-than-johnsons-top-to-toe-1-i.78546729.19188744016",
    "https://shopee.vn/giftcombo-2-sua-tam-goi-toan-than-mem-min-johnson-baby-bath-cotton-touch-200ml-i.78546729.22126709725",
    "https://shopee.vn/giftphan-thom-cho-be-huong-hoa-johnsons-baby-powder-200g-combo-2-sua-tam-goi-toan-than-mem-min-johns-i.78546729.22836017144",
    "https://shopee.vn/giftcombo-3-sua-tam-goi-toan-than-johnsons-top-to-toe-200ml-i.78546729.18877323592",
    "https://shopee.vn/qtpampers-thau-rua-mat-notoro-21-cm-i.90366612.22856676638",
    "https://shopee.vn/giftphan-thom-cho-be-huong-hoa-johnsons-baby-powder-200g-sua-tam-goi-toan-than-johnsons-top-to-toe-1-i.78546729.23418231110?is_from_login=True",
    "https://shopee.vn/may-laser-xoa-xam-tri-nam-k4-i.954659720.21273342308",
    "https://shopee.vn/may-giam-beo-ret-rf-slimming-dot-mo-da-tang-bh-24-thang-i.180651803.23241302273",
    "https://shopee.vn/may-giam-beo-dot-mo-da-tang-giam-beo-cong-nghe-ret-rf-slimming-dung-cu-lam-thon-gon-co-the-dung-tron-i.944545485.22967804703",
    "https://shopee.vn/bh-24-thang-may-hera-2in1-triet-long-xoa-xam-i.180651803.21476491960",
    "https://shopee.vn/may-triet-long-xoa-xam-diode-laser-soprano-titanium-mau-2023-i.182053498.22228577909",
    "https://shopee.vn/may-triet-long-xoa-xam-dpl-korea-busan-i.954659720.23024413633",
    "https://shopee.vn/may-xoa-xam-xoa-nam-tan-nhang-may-laser-ruikd-vua-cua-laser-i.182053498.23630395242",
    "https://shopee.vn/phi-thuyen-tam-trang-hong-ngoai-phi-thuyen-giam-beo-san-go-hang-chinh-hang-i.391379978.22448104536",
    "https://shopee.vn/phi-thuyen-tam-trang-giam-beo-hong-ngoai-collagen-phi-thuyen-tam-trang-collagen-i.391379978.18990756057",
    "https://shopee.vn/may-laser-picosure-nano-may-truc-khuyu-xoa-xam-xoa-nam-tan-nhang-may-laser-xoa-xam-truc-khuyu-i.391379978.17298284924",
    "https://shopee.vn/giftdau-goi-giup-toc-chac-khoe-giam-dut-gay-va-dai-hon-loreal-professionnel-serie-expert-pro-longer--i.487013605.15510206761",
    "https://shopee.vn/hbgift-bo-guong-luoc-uriage-i.300458619.19489251460",
    "https://shopee.vn/bo-gel-rua-mat-cho-da-dau-la-roche-posay-effaclar-purifying-foaming-gel-50ml-i.37251700.10815055198",
    "https://shopee.vn/giftcombo-10-sua-tam-goi-toan-than-johnsons-top-to-toe-100ml-i.78546729.23533338149",
    "https://shopee.vn/giftsua-tam-goi-toan-than-johnsons-top-to-toe-500ml-khan-uot-bobby-care-khong-huong-80-mieng-i.78546729.22842437672",
    "https://shopee.vn/giftsua-tam-johnsons-chua-sua-va-gao-dung-tich-500ml-sua-tam-goi-toan-than-mem-min-johnsons-baby-top-i.78546729.23118220623",
    "https://shopee.vn/giftcombo-2-sua-tam-goi-toan-than-mem-min-johnsons-cottontouch-top-to-toe-bath-500ml-i.78546729.23529023038",
    "https://shopee.vn/giftsua-tam-goi-toan-than-johnsons-top-to-toe-200ml-combo-2-sua-tam-goi-toan-than-mem-min-johnsons-c-i.78546729.22035520048",
    "https://shopee.vn/giftsua-tam-goi-toan-than-johnsons-top-to-toe-200ml-sua-tam-goi-toan-than-johnsons-top-to-toe-100ml-i.78546729.16493661986",
    "https://shopee.vn/giftcombo-2-sua-tam-goi-johnson-baby-bath-cotton-touch-200ml-sua-tam-goi-johnsons-cottontouch-top-to-i.78546729.23326697557",
    "https://shopee.vn/giftsua-tam-goi-toan-than-johnsons-top-to-toe-500ml-sua-tam-goi-toan-than-johnsons-top-to-toe-100ml--i.78546729.13498828558",
    "https://shopee.vn/giftsua-tam-goi-toan-than-mem-min-johnson-baby-bath-cotton-touch-200ml-sua-tam-goi-toan-than-johnson-i.78546729.19371170617",
    "https://shopee.vn/giftcombo-5-gel-duong-am-kiehls-ultra-facial-oil-free-gel-cream-3ml-sachet-i.1041184627.21087297107",
    "https://shopee.vn/gift-combo-8-kem-duong-ultra-facial-cream-3ml-i.1041184627.25603574145",
    "https://shopee.vn/qt-bobby-ghe-nhua-xep-duy-tan-cao-cap-cho-be-i.989774266.23053544950",
    "https://shopee.vn/km-chuong-trinh-qua-huggies-i.76421217.23570023664",
    "https://shopee.vn/chi-tang-1-hop-tren-1-don-hang-hop-dung-san-pham-skinc-i.530333613.23640709144",
    "https://shopee.vn/bo-doi-gel-rua-mat-tao-bot-la-roche-posay-effaclar-purifying-gel-15ml-i.37251700.21249461418",
    "https://shopee.vn/product/37147137/5649906977",
    "https://shopee.vn/product/37147137/8054727060",
    "https://shopee.vn/product/37147137/8933594845",
    "https://shopee.vn/product/37147137/11221656422",
    "https://shopee.vn/bao-nguyen-may-laser-co2-fractionallasertruc-khuyuchuyen-dieu-tri-seo-rocat-mun-thitmun-ruoitre-hoa--i.122583961.18935483365",
    "https://shopee.vn/bao-nguyen-may-triet-long-diode-laser-soprano-titanium-bao-hanh-12-thang-mau-2023-10-thanh-50-trieu--i.122583961.21448128807",
    "https://shopee.vn/bao-nguyen-may-triet-long-diode-808-may-2-in-1-10-thanh-50-trieu-xunglaser-xoa-nam-tan-nhang-mau-202-i.122583961.17595131274",
    "https://shopee.vn/bao-nguyen-may-triet-long-diode-808-nm-2in1-cong-nghe-cao-laser-va-triet-long-may-loai-1-bao-hanh-12-i.122583961.17349788726",
    "https://shopee.vn/bao-nguyen-may-triet-long-diode-laser-1-tay-cam-triet-long-mau-2023-bao-hanh-12-thang-i.122583961.17073730664",
    "https://shopee.vn/bao-nguyen-may-xoa-xam-laser-q12-chuyen-xoa-xamnamtan-nhangxoa-moi-may-mi-xoa-xam-tato-bao-hanh-12-t-i.122583961.17462767801",
    "https://shopee.vn/balo-xach-tay-hinh-chu-nhat-la-roche-posay-i.37251700.19416740522",
    "[MKB Gift]",
    "[QT_Huggies]",
    "https://shopee.vn/balo-bioamicus-cao-cap-cho-be-i.320642539.22731830626",
    "(QT_Huggies)",
    "https://shopee.vn/qtmu-huou-cao-co-bioamicus-mau-xanh-cho-be-i.27495839.23616219555",
    "https://shopee.vn/product/37251700/10590701768",
    "https://shopee.vn/product/37251700/9739323899",
    "https://shopee.vn/que-go-de-thoa-kem-tay-long-va-loai-bo-long-1-que-i.697821129.2343883738",
    "không bán",
    "[HC GIFT]",
    "[Gift]",
    "[Quà tặng]",
    "[Hàng tặng không bán]",
    "[HB GIFT]",
    "[Quà tặng không bán]",
    "quà tặng không bán",
    "https://shopee.vn/dogothangtailoc",
    "https://shopee.vn/thietbispaphuxuyen",
    "https://shopee.vn/shop/400054588",
    "https://shopee.vn/shop/391379079",
    "https://shopee.vn/thietbispa24h?",
    "https://shopee.vn/shop/521935626",
    "https://shopee.vn/shop/68410045",
    "https://shopee.vn/shop/266485724",
]


async def _build_es_query_from_filter_report(filter_report: FilterReport, start_date, end_date, size_product=75):
    query = await build_query_es_from_api(filter_report, start_date, end_date)
    fields = ["order_count_custom", "order_revenue_custom", "order_count_custom_adjacent", "order_revenue_custom_adjacent", "review_count_custom"]
    _source = ["product_base_id", "shop_base_id", "product_name", "url_thumbnail", "official_type", "bee_brand", "shop_platform_name", "shop_url", "price", "order_count", "rating_avg", "rating_count", "revenue", "platform_created_at", "price_min", "price_max", "order_count_30d", "order_revenue_30d", "order_count_custom_adjacent", "order_revenue_custom_adjacent", "price_updated_at"]
    lst_aggs, lst_runtime_mapping = get_aggs_runtime_mapping_es(filter_report, start_date, end_date)
    base_query = {
        'query': query,
        'runtime_mappings': lst_runtime_mapping,
        'fields': fields,
        '_source': _source,
        'aggs': lst_aggs,
        "size": size_product,
        "sort": [{"order_revenue_30d": {"order": "desc"}}],
    }
    three_month_ago_ts = int((datetime.now() - relativedelta(months=3)).timestamp()) * 1000
    query_new_product = json.loads(json.dumps(query))
    query_new_product['bool']['must'].append({"range": {"platform_created_at": {"gte": three_month_ago_ts}}})
    return {
        'query_es_default': base_query,
        'query_es_new_product': {**base_query, 'query': query_new_product},
        'query_es_top_revenue_custom': {**base_query, '_source': ['product_name', 'price'], "sort": [{"order_revenue_custom": {"order": "desc"}}]},
        'query_es_top_1000_product': {**base_query, '_source': ['product_base_id'], "size": 1000},
    }

def load_query_dataframe(file_path: str, sheet_name: str = None) -> pd.DataFrame:
    if file_path.endswith('.csv'):
        return pd.read_csv(file_path)
    elif file_path.endswith('.xlsx'):
        return pd.read_excel(file_path, sheet_name=sheet_name)

def get_categories_from_row(row: Series, platform: str = 'shopee'):
    column_name = f'Danh mục {platform.capitalize()}'
    category_cell_value = row[column_name]
    categories_tree = load_category_tree(platform)
    if type(category_cell_value) is not str:
        return []
    if category_cell_value == 'Tất cả':
        return [category['value'] for category in categories_tree]
    elif category_cell_value.strip() == 'Không lấy dữ liệu':
        return []
    else:
        lst_category = []
        lst_name = category_cell_value.split('\n')
        # print("lst_name:", lst_name, len(lst_name))
        for name in lst_name:
            category_id = find_category_id_by_label_path(name.strip(), categories_tree)
            # print(category_id)
            if category_id:
                lst_category.append(category_id)
        return lst_category


async def fetch_data_keyword(row):
    lst_keyword = row['Từ khóa'].split(',') if isinstance(row['Từ khóa'], str) else []
    lst_exclude_keyword = row['Từ khóa loại trừ'].split(',') if isinstance(row['Từ khóa loại trừ'], str) else default_exclude_keyword
    lst_keyword_required = row['Từ khóa cộng'].split(',') if isinstance(row['Từ khóa cộng'], str) else []
    price_min = row['Giá min'] if not math.isnan(row['Giá min']) else None
    price_max = row['Giá max'] if not math.isnan(row['Giá max']) else None
    price_range = Range(begin=price_min, end=price_max) if price_min or price_max else None
    lst_categories = get_categories_from_row(row, 'shopee') + get_categories_from_row(row, 'lazada') + get_categories_from_row(row, 'tiki') + get_categories_from_row(row, 'tiktok')
    filter_report = FilterReport(lst_platform_id=[1, 2, 3, 8], lst_keyword_exclude=lst_exclude_keyword, lst_keyword_required=lst_keyword_required, lst_keyword=lst_keyword, is_smart_queries=row['Chế độ tìm'] == 'Tìm thông minh', lst_category_base_id=lst_categories, price_range=price_range, is_remove_fake_sale=True)
    lst_query_es = await _build_es_query_from_filter_report(filter_report, '20230701', '20240630', size_product=2_000)
    query_es_default = lst_query_es.get('query_es_default')
    index_name = 'market_current__shopee_vn,market_current__lazada_vn,market_current__tiki_vn,market_current__tiktok_vn'
    es_session = get_es_metric_session(request_timeout=20)
    lst_product_revenue_30d_raw, aggs = search_v2(es=es_session, query=query_es_default, index_name=index_name)
    return await _tranform_aggs(aggs=aggs)

async def run():
    input_file_path = f'{ROOT_DIR}/Thời trang nữ - Copy.xlsx'
    df = load_query_dataframe(input_file_path, 'Sheet1')

    for index, row in df.iterrows():
        start_time = datetime.now()

        filter_as_str = ''.join(str(row[col]) for col in ['Từ khóa', 'Danh mục Shopee', 'Danh mục Lazada', 'Danh mục Tiki', 'Danh mục Tiktok', 'Từ khóa loại trừ', 'Từ khóa cộng', 'Chế độ tìm', 'Giá min', 'Giá max'])
        key_filter_report = text_to_hash_md5(filter_as_str)
        if key_filter_report == row['Key']:
            continue

        report_response = await fetch_data_keyword(row)
        by_marketplace = report_response.by_marketplace
        by_category = report_response.by_category

        row['Key'] = key_filter_report
        row['Lần query data'] = (row['Lần query data'] if not math.isnan(row['Lần query data']) else 0) + 1
        row['Doanh số từng sàn'] = '\n'.join(f"{item.name} - {round(item.ratio_revenue * 100, 2)}%" for item in by_marketplace.lst_marketplace)
        row['Doanh số'] = report_response.get('revenue')
        row['Sản phẩm có lượt bán'] = report_response.get('product')
        row['Ngành hàng Shopee'] = '\n'.join(f"{category.parent_name}/{category.name} - {round(category.ratio_revenue * 100, 2)}%" for category in by_category.lst_shopee_category if category.level == 2)
        row['Ngành hàng Lazada'] = '\n'.join(f"{category.parent_name}/{category.name} - {round(category.ratio_revenue * 100, 2)}%" for category in by_category.lst_lazada_category if category.level == 2)
        row['Ngành hàng Tiki'] = '\n'.join(f"{category.parent_name}/{category.name} - {round(category.ratio_revenue * 100, 2)}%" for category in by_category.lst_tiki_category if category.level == 2)
        row['Ngành hàng Tiktok'] = '\n'.join(f"{category.name} - {round(category.ratio_revenue * 100, 2)}%" for category in by_category.lst_tiktok_category if category.level == 1)
        row['Product name'] = '\n'.join(product.get('product_name') for product in report_response.get('top_10_product') + report_response.get('bottom_10_product'))

    df.to_excel(input_file_path, index=False)
    print(f"DONE {len(df)} rows processed in {datetime.now() - start_time} \n")

if __name__ == '__main__':
    asyncio.run(run())
