import functools
import json
from typing import Union, List

from fastapi import Depends

from app.constant.constant_metric import get_map_category_obj
from app.schemas.report import result_analytic_report as rar
from app.service.extractor.clean_product_name import clean_product_name
# from app.service.shop.shop_detail_batch_service import get_shop_detail_batch
from app.service.universal.universal_service import get_platform_by_id, get_url_shop
from helper.array_helper import array_to_map
from helper.datetime_helper import convert_str_to_datetime, get_start_end_of_month
from helper.elasticsearch_tranform import get_es_value_agg
from helper.logger_helper import LoggerSimple
from helper.type_helper import cast_int

logger = LoggerSimple(name=__name__).logger


def sort_lst_price_range(lst_price_range: List[rar.PriceRangeStatistic]):
    def compare(item1: rar.PriceRangeStatistic, item2: rar.PriceRangeStatistic):
        a = item1.begin or 0
        b = item2.begin or 0
        if a < b:
            return -1
        elif a > b:
            return 1
        else:
            return 0

    lst_price_range = sorted(lst_price_range, key=functools.cmp_to_key(compare))
    return lst_price_range


async def _tranform_aggs(aggs) -> Union[
    rar.ResultAnalyticReport, None]:
    aggregations = aggs.get('aggregations')
    revenue = get_es_value_agg(agg=aggregations.get('revenue'))
    sale = get_es_value_agg(agg=aggregations.get('sale'))
    shop = get_es_value_agg(agg=aggregations.get('shop'))
    product = get_es_value_agg(agg=aggregations.get('product'))
    brand = get_es_value_agg(agg=aggregations.get('brand'))
    if revenue is None or revenue == 0 or sale is None or sale == 0:
        return None
    lst_revenue_sale_monthly: List[rar.RevenueSaleMonthly] = []
    revenue_monthly = None
    revenue_yearly = None
    gr_monthly = None
    gr_quarter = None

    set_monthly = set()
    for key in aggregations:
        if key.startswith('monthly__'):
            # _agg = aggregations.get(key)
            # _term = key.split('__')[-1].split('_')[0]  # sale or revenue
            _month_str = key.split('__')[-1].split('_')[1]  # YYYYDD
            set_monthly.add(_month_str)
            # logger.info(_month_str)
    _lst_revenue_monthly = []
    for monthly in sorted(list(set_monthly)):
        _revenue = get_es_value_agg(agg=aggregations.get(f'monthly__revenue_{monthly}'))
        if _revenue and _revenue > 0:
            _lst_revenue_monthly.append(_revenue)
    _min = min(_lst_revenue_monthly) if len(_lst_revenue_monthly) > 0 else None
    _avg = sum(_lst_revenue_monthly) / len(_lst_revenue_monthly) if len(_lst_revenue_monthly) > 0 else None
    _max = max(_lst_revenue_monthly) if len(_lst_revenue_monthly) > 0 else None
    _score_max = 100
    _score_min = int((_min / _avg) * 100) if _min and _avg else None
    for monthly in sorted(list(set_monthly)):
        _revenue = get_es_value_agg(agg=aggregations.get(f'monthly__revenue_{monthly}'))
        _sale = get_es_value_agg(agg=aggregations.get(f'monthly__sale_{monthly}'))
        month_start, month_end = get_start_end_of_month(
            month_datetime=convert_str_to_datetime(str=monthly, format='%Y%m'))
        _score = 0
        if _revenue and _min and _max:
            if _min == _max and _min > 0:
                _score = 100
            else:
                _score = int(((_revenue - _min) * (_score_max - _score_min) / (_max - _min)) + _score_min)
        lst_revenue_sale_monthly.append(rar.RevenueSaleMonthly(
            sale=_sale,
            revenue=_revenue,
            begin_ts=int(month_start.timestamp()),
            begin=month_start.strftime('%Y%m%d'),
            end_ts=int(month_end.timestamp()),
            end=month_end.strftime('%Y%m%d'),
            score=_score
        ))
        # logger.info(f'{monthly} - {int(_revenue / 1_000_000_000)} - {_score}')
    if len(lst_revenue_sale_monthly) >= 6:
        revenue_monthly = sum([x.revenue or 0 for x in lst_revenue_sale_monthly[-6:]]) / len(
            lst_revenue_sale_monthly[-6:])
        _current = lst_revenue_sale_monthly[-1].revenue
        _adjacent = lst_revenue_sale_monthly[-2].revenue
        if _current and _adjacent and _current > 0 and _adjacent > 0:
            gr_monthly = (_current - _adjacent) / _adjacent
        revenue_yearly = sum([x.revenue or 0 for x in lst_revenue_sale_monthly[-12:]])
        _quarter_current = sum([x.revenue or 0 for x in lst_revenue_sale_monthly[-3:]]) / 3
        _quarter_adjacent = sum([x.revenue or 0 for x in lst_revenue_sale_monthly[-6:-3]]) / 3
        if _quarter_current and _quarter_adjacent and _quarter_current > 0 and _quarter_adjacent > 0:
            gr_quarter = (_quarter_current - _quarter_adjacent) / _quarter_adjacent

    revenue_shopee = None
    revenue_lazada = None
    revenue_tiki = None
    revenue_tiktok = None
    lst_marketplace = []
    for bucket in aggregations.get('by_marketplace').get('buckets'):
        _revenue = get_es_value_agg(agg=bucket.get('revenue'))
        _sale = get_es_value_agg(agg=bucket.get('sale'))
        ratio_revenue = _revenue / revenue if _revenue and revenue and revenue > 0 else None
        ratio_sale = _sale / sale if _sale and sale and sale > 0 else None
        platform_id = int(bucket.get('key'))
        platform = get_platform_by_id(platform_id=platform_id)
        if platform_id == 1:
            revenue_shopee = _revenue
        elif platform_id == 2:
            revenue_lazada = _revenue
        elif platform_id == 3:
            revenue_tiki = _revenue
        elif platform_id == 8:
            revenue_tiktok = _revenue
        marketplace = rar.PlatformStatistic(
            platform_id=platform_id,
            name=platform.get('name') if platform else None,
            sale=_sale,
            revenue=_revenue,
            ratio_revenue=ratio_revenue,
            ratio_sale=ratio_sale,
        )
        lst_marketplace.append(marketplace)

    map_category_obj = get_map_category_obj()
    min_threshold_category = 0.1
    min_threshold_sub_category = 0.03
    lst_bee_category: List[rar.CategoryStatistic] = []
    for bucket in aggregations.get('bee_category').get('buckets'):
        _category_base_id = bucket.get('key')
        _category_obj = map_category_obj.get(_category_base_id)
        if _category_base_id is None or _category_obj is None:
            # logger.warning(f'NOT FOUND cat: {_category_base_id}')
            continue
        _revenue = get_es_value_agg(agg=bucket.get('revenue'))
        if _category_obj.get('level') == 2:
            _threshold = _revenue / revenue if _revenue and revenue and revenue > 0 else None
            if _threshold and _threshold >= min_threshold_sub_category:
                lst_bee_category.append(
                    rar.CategoryStatistic(
                        id=bucket.get('key'),
                        revenue=_revenue,
                        ratio_revenue=_threshold,
                        name=_category_obj.get('label'),
                        level=_category_obj.get('level'),
                        parent_name=_category_obj.get('parent_name'),
                        parent_id=_category_obj.get('parent'),
                    )
                )

    by_sub_category = rar.BySubCategory()
    by_cat2_category = rar.BySubCategory()
    lst_shopee_category: List[rar.CategoryStatistic] = []
    lst_lazada_category: List[rar.CategoryStatistic] = []
    lst_tiki_category: List[rar.CategoryStatistic] = []
    lst_tiktok_category: List[rar.CategoryStatistic] = []

    # print(json.dumps(aggregations.get('category').get('buckets'), ensure_ascii=False))

    for bucket in aggregations.get('category').get('buckets'):
        _category_base_id = bucket.get('key')
        _category_obj = map_category_obj.get(_category_base_id)
        if _category_base_id is None or _category_obj is None:
            # logger.warning(f'NOT FOUND cat: {_category_base_id}')
            continue
        _revenue = get_es_value_agg(agg=bucket.get('revenue'))
        _revenue_total_of_platform = None
        if _category_base_id.startswith('1__'):
            _revenue_total_of_platform = revenue_shopee
        elif _category_base_id.startswith('2__'):
            _revenue_total_of_platform = revenue_lazada
        elif _category_base_id.startswith('3__'):
            _revenue_total_of_platform = revenue_tiki
        elif _category_base_id.startswith('8__'):
            _revenue_total_of_platform = revenue_tiktok
        elif _category_base_id.startswith('c'):
            _revenue_total_of_platform = revenue
        _threshold = _revenue / _revenue_total_of_platform if _revenue and _revenue_total_of_platform and _revenue_total_of_platform > 0 else None
        cat_statistic = rar.CategoryStatistic(
            id=bucket.get('key'),
            revenue=_revenue,
            ratio_revenue=_threshold,
            name=_category_obj.get('label'),
            level=_category_obj.get('level'),
            parent_name=_category_obj.get('parent_name'),
            parent_id=_category_obj.get('parent'),
        )
        # if _category_base_id.startswith('8__'):
        #     logger.info(f'cat_statistic: {cat_statistic}')
        if _category_obj.get('is_leaf') and _threshold and _threshold >= min_threshold_sub_category:
            # if _category_obj.get('is_leaf'):
            if _category_base_id.startswith('1__'):
                by_sub_category.shopee.append(cat_statistic)
            elif _category_base_id.startswith('2__'):
                by_sub_category.lazada.append(cat_statistic)
            elif _category_base_id.startswith('3__'):
                by_sub_category.tiki.append(cat_statistic)
            elif _category_base_id.startswith('8__'):
                by_sub_category.tiktok.append(cat_statistic)
            elif _category_base_id.startswith('c'):
                by_sub_category.all.append(cat_statistic)

        if _category_obj.get('level') and _threshold and _threshold >= min_threshold_sub_category:
            if _category_base_id.startswith('1__'):
                lst_shopee_category.append(cat_statistic)
            elif _category_base_id.startswith('2__'):
                lst_lazada_category.append(cat_statistic)
            elif _category_base_id.startswith('3__'):
                lst_tiki_category.append(cat_statistic)
            elif _category_base_id.startswith('8__'):
                lst_tiktok_category.append(cat_statistic)

        if _category_obj.get('level') and str(
                _category_obj.get('level')) == '2' and _threshold and _threshold >= min_threshold_category:
            if _category_base_id.startswith('1__'):
                by_cat2_category.shopee.append(cat_statistic)
            elif _category_base_id.startswith('2__'):
                by_cat2_category.lazada.append(cat_statistic)
            elif _category_base_id.startswith('3__'):
                by_cat2_category.tiki.append(cat_statistic)
            elif _category_base_id.startswith('8__'):
                by_cat2_category.tiktok.append(cat_statistic)
            elif _category_base_id.startswith('c'):
                by_cat2_category.all.append(cat_statistic)

    lst_price_range = []
    for bucket in aggregations.get('by_price_range').get('buckets'):
        key = bucket.get('key')
        _sale = get_es_value_agg(agg=bucket.get('sale'))
        _revenue = get_es_value_agg(agg=bucket.get('revenue'))
        _ratio_revenue = _revenue / revenue if _revenue and revenue and revenue > 0 else None
        if _ratio_revenue is None or _ratio_revenue < 0.03:
            continue
        terms = key.split('-')
        begin = None
        end = None
        if len(terms) == 2:
            begin = cast_int(terms[0])
            end = cast_int(terms[1])
        lst_platform_statistic = []
        for platform in bucket.get('platform').get('buckets'):
            platform_id = platform.get('key')
            _sale_platform = get_es_value_agg(platform.get('sale'))
            _revenue_platform = get_es_value_agg(platform.get('revenue'))
            lst_platform_statistic.append(
                rar.PlatformStatistic(
                    platform_id=platform_id,
                    revenue=_revenue_platform,
                    sale=_sale_platform,
                    ratio_revenue=_revenue_platform / _revenue if _revenue and _revenue_platform and _revenue > 0 else None,
                )
            )
        lst_price_range.append(rar.PriceRangeStatistic(
            begin=begin,
            end=end,
            sale=_sale,
            revenue=_revenue,
            ratio_revenue=_ratio_revenue,
            lst_platform=lst_platform_statistic
        ))
    lst_price_range = sort_lst_price_range(lst_price_range=lst_price_range)
    # ratio_brand = rar.RatioBrand()
    no_brand = rar.BrandStatistic()
    lst_top_brand_revenue = []
    lst_top_brand_sale = []
    lst_brand = []

    lst_top_brand_revenue_top10 = []
    _count_bucket = 0
    if aggregations.get('lst_top_brand_revenue') and len(aggregations.get('lst_top_brand_revenue').get('buckets')) > 0:
        for bucket in aggregations.get('lst_top_brand_revenue').get('buckets'):
            _brand = bucket.get('key')
            if _brand != 'N/A':
                _count_bucket += 1
                lst_top_brand_revenue_top10.append(bucket)
            if _brand == 'N/A':
                no_brand.product = get_es_value_agg(agg=bucket.get('product'))
                no_brand.sale = get_es_value_agg(agg=bucket.get('sale'))
                no_brand.revenue = get_es_value_agg(agg=bucket.get('revenue'))
                no_brand.shop = get_es_value_agg(agg=bucket.get('shop'))
                no_brand.brand = 0
                no_brand.ratio_revenue = no_brand.revenue / revenue if no_brand.revenue else None
                no_brand.ratio_sale = no_brand.sale / sale if no_brand.sale else None
            if _count_bucket == 10:
                break

    revenue_top10_brand = sum([get_es_value_agg(agg=bucket.get('revenue')) for bucket in lst_top_brand_revenue_top10])
    for bucket in lst_top_brand_revenue_top10:
        _brand = bucket.get('key')
        _revenue = get_es_value_agg(agg=bucket.get('revenue'))
        lst_top_brand_revenue.append(rar.BrandStatistic(
            name=_brand,
            revenue=_revenue,
            ratio_revenue=_revenue / revenue_top10_brand if _revenue and revenue_top10_brand and revenue_top10_brand > 0 else None,
        ))
    brand_obj = rar.BrandStatistic(
        shop=shop - no_brand.shop if shop and no_brand.shop else None,
        product=product - no_brand.product if product and no_brand.product else None,
        revenue=revenue - no_brand.revenue if revenue and no_brand.revenue else None,
        sale=sale - no_brand.sale if sale and no_brand.sale else None,
        brand=brand,
        ratio_revenue=1 - no_brand.ratio_revenue if no_brand.ratio_revenue else None,
        ratio_sale=1 - no_brand.ratio_sale if no_brand.ratio_sale else None
    )
    ratio_brand = rar.RatioBrand(
        no_brand=no_brand,
        brand=brand_obj
    )
    sale_top10_brand = None
    if aggregations.get('lst_top_brand_sale') and aggregations.get('lst_top_brand_sale').get('buckets') and len(
            aggregations.get('lst_top_brand_sale').get('buckets')) > 0:
        sale_top10_brand = sum([get_es_value_agg(agg=bucket.get('sale')) for bucket in
                                aggregations.get('lst_top_brand_sale').get('buckets')[:10]])
        for bucket in aggregations.get('lst_top_brand_sale').get('buckets')[:10]:
            _brand = bucket.get('key')
            _sale = get_es_value_agg(agg=bucket.get('sale'))
            lst_top_brand_sale.append(rar.BrandStatistic(
                name=_brand,
                sale=_sale,
                ratio_sale=_sale / sale_top10_brand if sale_top10_brand and _sale and sale_top10_brand > 0 else None,
            ))
    if aggregations.get('lst_top_brand_revenue') and aggregations.get('lst_top_brand_revenue').get('buckets') and len(
            aggregations.get('lst_top_brand_revenue').get('buckets')) > 0:
        for bucket in aggregations.get('lst_top_brand_revenue').get('buckets'):
            _brand = bucket.get('key')
            _revenue = get_es_value_agg(agg=bucket.get('revenue'))
            _sale = get_es_value_agg(agg=bucket.get('sale'))
            lst_brand.append(rar.BrandStatistic(
                name=_brand,
                revenue=_revenue,
                sale=_sale,
                shop=get_es_value_agg(agg=bucket.get('shop')),
                product=get_es_value_agg(agg=bucket.get('product')),
            ))

    ratio_shop = rar.RatioShop()
    lst_top_shop = []
    lst_shop = []

    revenue_total_mall_n_non = sum(
        [get_es_value_agg(agg=bucket.get('revenue')) for bucket in aggregations.get('by_shop_ratio').get('buckets')])
    for bucket in aggregations.get('by_shop_ratio').get('buckets'):
        _key = bucket.get('key')
        _product = get_es_value_agg(agg=bucket.get('product'))
        _sale = get_es_value_agg(agg=bucket.get('sale'))
        _revenue = get_es_value_agg(agg=bucket.get('revenue'))
        _shop = get_es_value_agg(agg=bucket.get('shop'))
        _ratio_revenue = ''
        shop_statistic = rar.ShopStatistic(
            shop=_shop,
            product=_product,
            revenue=_revenue,
            sale=_sale,
            ratio_revenue=_revenue / revenue_total_mall_n_non if _revenue and revenue_total_mall_n_non and revenue_total_mall_n_non > 0 else None
        )
        if int(_key) == 0:
            ratio_shop.normal = shop_statistic
        if int(_key) == 1:
            ratio_shop.mall = shop_statistic

    revenue_top10_shop = sum(
        [get_es_value_agg(agg=bucket.get('revenue')) for bucket in
         aggregations.get('lst_top_shop').get('buckets')]) if aggregations.get('lst_top_shop').get('buckets') and len(
        aggregations.get('lst_top_shop').get('buckets')) > 0 else None
    # lst_shop_base_id = [bucket.get('key') for bucket in aggregations.get('lst_top_shop').get('buckets')]
    # lst_shop_base = await get_shop_detail_batch(lst_shop_base_id=lst_shop_base_id, session_metric=session_metric)
    # map_shop_base = array_to_map(array_dict=lst_shop_base, key='shop_base_id')
    # for bucket in aggregations.get('lst_top_shop').get('buckets')[:10]:
    #     _key = bucket.get('key')
    #     _platform_id = int(_key.split('__')[0])
    #     _product = get_es_value_agg(agg=bucket.get('product'))
    #     _sale = get_es_value_agg(agg=bucket.get('sale'))
    #     _revenue = get_es_value_agg(agg=bucket.get('revenue'))
    #     _shop = get_es_value_agg(agg=bucket.get('shop'))
    #     shop_base = map_shop_base.get(_key)
    #     name = None
    #     official_type = None
    #     if shop_base is not None:
    #         name = shop_base.get('name')
    #         official_type = 1 if shop_base.get('is_official_shop') is True else 0
    #
    #     lst_top_shop.append(rar.ShopStatistic(
    #         platform_id=_platform_id,
    #         shop_base_id=_key,
    #         name=name,
    #         # url_image=url_image,
    #         # url_shop=url_shop,
    #         official_type=official_type,
    #         revenue=_revenue,
    #         ratio_revenue=_revenue / revenue_top10_shop if _revenue and revenue_top10_shop and revenue_top10_shop > 0 else None
    #     ))

    for bucket in aggregations.get('lst_top_shop').get('buckets'):
        _key = bucket.get('key')
        _platform_id = int(_key.split('__')[0])
        _product = get_es_value_agg(agg=bucket.get('product'))
        _sale = get_es_value_agg(agg=bucket.get('sale'))
        _revenue = get_es_value_agg(agg=bucket.get('revenue'))
        # _shop = get_es_value_agg(agg=bucket.get('shop'))
        shop_base = None
        name = None
        url_image = None
        url_shop = None
        official_type = None
        if shop_base is not None:
            name = shop_base.get('name')
            url_image = shop_base.get('url_image')
            url_shop = get_url_shop(shop_base_id=_key, shop_username=shop_base.get('username'))
            official_type = 1 if shop_base.get('is_official_shop') is True else 0

        lst_shop.append(rar.ShopStatistic(
            shop_base_id=_key,
            platform_id=_platform_id,
            name=name,
            url_image=url_image,
            url_shop=url_shop,
            official_type=official_type,
            revenue=_revenue,
            sale=_sale,
            product=_product,

        ))
    by_category = rar.ByCategory(
        lst_bee_category=lst_bee_category,
        lst_shopee_category=lst_shopee_category,
        lst_lazada_category=lst_lazada_category,
        lst_tiki_category=lst_tiki_category,
        lst_tiktok_category=lst_tiktok_category
    )
    result_analytic_report = rar.ResultAnalyticReport(
        by_overview=rar.ByOverview(
            revenue=revenue,
            sale=sale,
            shop=shop,
            product=product,
            brand=brand,
            lst_revenue_sale_monthly=lst_revenue_sale_monthly,
            revenue_monthly=revenue_monthly,
            revenue_yearly=revenue_yearly,
            gr_monthly=gr_monthly,
            gr_quarter=gr_quarter,
        ),
        by_marketplace=rar.ByMarketplace(
            lst_marketplace=lst_marketplace
        ),
        by_price_range=rar.ByPriceRange(
            lst_price_range=lst_price_range
        ),
        by_brand=rar.ByBrand(
            ratio=ratio_brand,
            lst_top_brand_revenue=lst_top_brand_revenue,
            lst_top_brand_sale=lst_top_brand_sale,
            lst_brand=lst_brand
        ),
        by_shop=rar.ByShop(
            ratio=ratio_shop,
            lst_top_shop=lst_top_shop,
            lst_shop=lst_shop,
        ),
        by_category=by_category,
        by_sub_category=by_sub_category,
        by_cat2_category=by_cat2_category,
    )

    return result_analytic_report


async def _transform_product(lst_product_raw):
    if lst_product_raw is None or len(lst_product_raw) == 0:
        return None
    lst_product_statistic: List[rar.ProductStatistic] = []
    # lst_shop_base_id = list(set([p.get('shop_base_id') for p in lst_product_raw]))
    # lst_shop_base = await get_shop_detail_batch(lst_shop_base_id=lst_shop_base_id, session_metric=session_metric)
    # map_shop_base = array_to_map(array_dict=lst_shop_base, key='shop_base_id')

    for _product in lst_product_raw:
        shop_base_id = _product.get('shop_base_id')
        shop_base = None
        shop_url = None
        shop_platform_name = None
        if shop_base:
            shop_username = shop_base.get('username')
            shop_url = get_url_shop(shop_base_id=shop_base_id, shop_username=shop_username)
            shop_platform_name = shop_base.get('name')
        order_count_30d = _product.get('order_count_30d')
        order_revenue_30d = _product.get('order_revenue_30d')
        order_count_30d_adjacent = _product.get('order_count_30d_adjacent')
        order_revenue_30d_adjacent = _product.get('order_revenue_30d_adjacent')

        gr_order_count_30d_adjacent = (order_count_30d - order_count_30d_adjacent) / order_count_30d_adjacent \
            if order_count_30d_adjacent and order_count_30d and order_count_30d_adjacent > 0 and order_count_30d > 0 else None
        gr_order_revenue_30d_adjacent = (order_revenue_30d - order_revenue_30d_adjacent) / order_revenue_30d_adjacent \
            if order_revenue_30d_adjacent and order_revenue_30d and order_revenue_30d_adjacent > 0 and order_revenue_30d > 0 else None

        product_statistic = rar.ProductStatistic(
            product_base_id=_product.get('product_base_id'),
            # product_name=_product.get('product_name'),
            product_name=clean_product_name(_product.get('product_name')),
            url_thumbnail=_product.get('url_thumbnail'),
            official_type=_product.get('official_type'),
            brand=_product.get('bee_brand'),
            shop_platform_name=shop_platform_name,
            shop_url=shop_url,
            price=_product.get('price'),
            order_count=_product.get('order_count'),
            rating_avg=_product.get('rating_avg'),
            rating_count=_product.get('rating_count'),
            revenue=_product.get('revenue'),
            platform_created_at=_product.get('platform_created_at'),
            price_updated_at=_product.get('price_updated_at'),
            price_min=_product.get('price_min'),
            price_max=_product.get('price_max'),
            order_count_30d=order_count_30d,
            order_revenue_30d=order_revenue_30d,
            order_count_30d_adjacent=order_count_30d_adjacent,
            order_revenue_30d_adjacent=order_revenue_30d_adjacent,
            gr_order_count_30d_adjacent=gr_order_count_30d_adjacent,
            gr_order_revenue_30d_adjacent=gr_order_revenue_30d_adjacent,
        )
        # logger.info(product_statistic)
        lst_product_statistic.append(product_statistic)

    return lst_product_statistic
