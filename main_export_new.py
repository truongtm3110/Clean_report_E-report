import json
import os.path
from hashlib import md5
from elasticsearch import Elasticsearch
import warnings
import pandas as pd


def get_product_url(product_base_id):
    splt = product_base_id.split('__')
    platform_id = int(splt[0])
    product_id = int(splt[1])
    seller_id = None
    if len(splt) > 2:
        seller_id = int(splt[2])

    if platform_id == 1:
        url = f"https://shopee.vn/product/{seller_id}/{product_id}"
    elif platform_id == 2:
        url = f"https://www.lazada.vn/products/sp-i{product_id}.html"
    elif platform_id == 3:
        url = f"https://tiki.vn/sp-p{product_id}.html?spid={seller_id}"
    elif platform_id == 4:
        url = f"https://www.sendo.vn/product-{product_id}.html"
    elif platform_id == 8:
        url = f"https://www.tiktok.com/view/product/{product_id}"
    else:
        raise RuntimeError("khong tao duoc link")
    return url


# pip install urllib3==1.26.6
folder_ = f"C:\\Users\\Admin\\PycharmProjects\\Clean_report_E-report\\data_raw_cate"
warnings.filterwarnings("ignore")


# for filename in os.listdir(folder):
#     if filename.endswith('.json'):

# def export_data_from_json(folder):
# for filename in os.listdir(folder):
#     if filename.endswith('.json'):
#         file_path = os.path.join(folder, filename)
#         input_query_file = file_path
#         name_platform = filename.split('__')[1]
#         output_file = f'{folder}\\{filename.replace(".json", ".xlsx")}'
#         index_name = f'market_current__{name_platform}_vn,market_current_die__{name_platform}_vn'
#
#         # index_name = 'market_current__shopee_vn,market_current_die__shopee_vn'
#         is_replace_raw_output = True
#
#         # http://<username>:<password>@sv6.beecost.net:9200
#         es = Elasticsearch(['https://thangtm:b8Cv5Ey5qpMjMJaVu7UqJ@sv6.beecost.net:9200'],
#                            request_timeout=10000,
#                            verify_certs=False
#                            )
#
#         with open(input_query_file, 'rt', encoding='utf-8') as f:
#             search_body = f.read()
#             search_body = search_body.replace('\n', ' ').replace('"""', '"')
#             search_body_hash = md5(search_body.encode('utf-8')).hexdigest().lower()
#             raw_output_file = f'{search_body_hash}.json'
#             print("search_body", search_body)
#
#             if os.path.exists(raw_output_file) and not is_replace_raw_output:
#                 with open(raw_output_file, 'rt', encoding='utf-8') as f:
#                     result = json.loads(f.read())
#
#             else:
#                 search_body_json = json.loads(search_body)
#                 result = es.search(index=index_name,
#                                    body=json.loads(search_body),
#                                    request_timeout=120
#                                    )
#
#                 print("result", result)
#                 print("", type(result))
#                 with open(raw_output_file, 'wt', encoding='utf-8') as f:
#                     f.write(json.dumps(result, ensure_ascii=False))
#
#             # print('exported', result)
#             response = result
#             output_product_lst = []
#             for hit in response['hits']['hits']:
#                 output_product_lst.append(
#                     {
#                         "product_base_id": hit['_source'].get('product_base_id'),
#                         "product_name": hit['_source'].get('product_name'),
#                         "platform_id": hit['_source'].get('platform_id'),
#                         "price": hit['_source'].get('price'),
#                         "price_min": hit['_source'].get('price_min'),
#                         "price_max": hit['_source'].get('price_max'),
#                         "official_type": hit['_source'].get('official_type'),
#                         "url_thumbnail": hit['_source'].get('url_thumbnail'),
#                         "shop_base_id": hit['_source'].get('shop_base_id'),
#                         "price_ranges": hit['_source'].get('price_ranges'),
#                         "brand": hit['_source'].get('brand'),
#                         "shop_platform_name": hit['_source'].get('shop_platform_name'),
#                         # "category": hit['_source'].get('category_base_id'),
#                         # "order_count": hit['_source'].get('order_count'),
#                         "order_revenue_custom": hit['fields'].get('order_revenue_custom')[0],
#                         "order_count_custom": hit['fields'].get('order_count_custom')[0],
#                         # "revenue_202201": hit['fields'].get('revenue_202201')[0],
#                         # "revenue_202202": hit['fields'].get('revenue_202202')[0],
#                         # "revenue_202203": hit['fields'].get('revenue_202203')[0],
#                         # "revenue_202204": hit['fields'].get('revenue_202204')[0],
#                         # "revenue_202205": hit['fields'].get('revenue_202205')[0],
#                         # "revenue_202206": hit['fields'].get('revenue_202206')[0],
#                         # "revenue_202207": hit['fields'].get('revenue_202207')[0],
#                         # "revenue_202208": hit['fields'].get('revenue_202208')[0],
#                         # "revenue_202209": hit['fields'].get('revenue_202209')[0],
#                         # "revenue_202210": hit['fields'].get('revenue_202210')[0],
#                         # "revenue_202211": hit['fields'].get('revenue_202211')[0],
#                         # "revenue_202212": hit['fields'].get('revenue_202212')[0],
#                         # "revenue_202301": hit['fields'].get('revenue_202301')[0],
#                         # "revenue_202302": hit['fields'].get('revenue_202302')[0],
#                         # "revenue_202303": hit['fields'].get('revenue_202303')[0],
#                         # "revenue_202304": hit['fields'].get('revenue_202304')[0],
#                         # "revenue_202305": hit['fields'].get('revenue_202305')[0],
#                         # "revenue_202306": hit['fields'].get('revenue_202306')[0],
#                         # "revenue_202307": hit['fields'].get('revenue_202307')[0],
#                         # "revenue_202308": hit['fields'].get('revenue_202308')[0],
#                         # "revenue_202309": hit['fields'].get('revenue_202309')[0],
#                         # "revenue_202310": hit['fields'].get('revenue_202310')[0],
#                         # "revenue_202311": hit['fields'].get('revenue_202311')[0],
#                         # "revenue_202312": hit['fields'].get('revenue_202312')[0],
#                         # "revenue_202401": hit['fields'].get('revenue_202401')[0],
#                         # "revenue_202402": hit['fields'].get('revenue_202402')[0],
#                         # "revenue_202403": hit['fields'].get('revenue_202403')[0],
#                         # "revenue_202404": hit['fields'].get('revenue_202404')[0],
#                         # "revenue_202405": hit['fields'].get('revenue_202405')[0],
#                         # "revenue_202406": hit['fields'].get('revenue_202406')[0],
#                         "revenue_202407": hit['fields'].get('revenue_202407')[0],
#                         "revenue_202408": hit['fields'].get('revenue_202408')[0],
#                         # "sale_202201": hit['fields'].get('sale_202201')[0],
#                         # "sale_202202": hit['fields'].get('sale_202202')[0],
#                         # "sale_202203": hit['fields'].get('sale_202203')[0],
#                         # "sale_202204": hit['fields'].get('sale_202204')[0],
#                         # "sale_202205": hit['fields'].get('sale_202205')[0],
#                         # "sale_202206": hit['fields'].get('sale_202206')[0],
#                         # "sale_202207": hit['fields'].get('sale_202207')[0],
#                         # "sale_202208": hit['fields'].get('sale_202208')[0],
#                         # "sale_202209": hit['fields'].get('sale_202209')[0],
#                         # "sale_202210": hit['fields'].get('sale_202210')[0],
#                         # "sale_202211": hit['fields'].get('sale_202211')[0],
#                         # "sale_202212": hit['fields'].get('sale_202212')[0],
#                         # "sale_202301": hit['fields'].get('sale_202301')[0],
#                         # "sale_202302": hit['fields'].get('sale_202302')[0],
#                         # "sale_202303": hit['fields'].get('sale_202303')[0],
#                         # "sale_202304": hit['fields'].get('sale_202304')[0],
#                         # "sale_202305": hit['fields'].get('sale_202305')[0],
#                         # "sale_202306": hit['fields'].get('sale_202306')[0],
#                         # "sale_202307": hit['fields'].get('sale_202307')[0],
#                         # "sale_202308": hit['fields'].get('sale_202308')[0],
#                         # "sale_202309": hit['fields'].get('sale_202309')[0],
#                         # "sale_202310": hit['fields'].get('sale_202310')[0],
#                         # "sale_202311": hit['fields'].get('sale_202311')[0],
#                         # "sale_202312": hit['fields'].get('sale_202312')[0],
#                         # "sale_202401": hit['fields'].get('sale_202401')[0],
#                         # "sale_202402": hit['fields'].get('sale_202402')[0],
#                         # "sale_202403": hit['fields'].get('sale_202403')[0],
#                         # "sale_202404": hit['fields'].get('sale_202404')[0],
#                         # "sale_202405": hit['fields'].get('sale_202405')[0],
#                         # "sale_202406": hit['fields'].get('sale_202406')[0],
#                         "sale_202407": hit['fields'].get('sale_202407')[0],
#                         "sale_202408": hit['fields'].get('sale_202408')[0],
#                         'url': str(get_product_url(hit['_source'].get('product_base_id')))
#                     }
#                 )
#
#             df = pd.DataFrame(output_product_lst)
#             # df.to_excel(output_file, index=False, engine='xlsxwriter')
#             with pd.ExcelWriter(output_file, engine='xlsxwriter',
#                                 engine_kwargs={'options': {'strings_to_urls': False}}) as writer:
#                 df.to_excel(writer, index=False)
#
#
# export_data_from_json(folder_)