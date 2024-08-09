import json
from typing import Union, List

from sqlmodel import SQLModel


class BrandStatistic(SQLModel):
    name: str | None = None
    shop: int | None = None
    product: int | None = None
    brand: int | None = None
    revenue: Union[float, int] | None = None
    sale: int | None = None
    ratio_revenue: float | None = None
    ratio_sale: float | None = None


class ProductStatistic(SQLModel):
    platform_id: Union[int, str] | None = None
    product_base_id: str | None = None
    product_name: str | None = None
    url_thumbnail: str | None = None
    official_type: int | None = None
    brand: str | None = None
    shop_platform_name: str | None = None
    shop_url: str | None = None
    price: Union[int, float] | None = None
    order_count: int | None = None
    rating_avg: float | None = None
    rating_count: int | None = None
    revenue: Union[int, float] | None = None
    platform_created_at: int | None = None
    price_updated_at: int | None = None
    price_min: Union[int, float] | None = None
    price_max: Union[int, float] | None = None
    order_count_30d: int | None = None
    order_revenue_30d: Union[int, float] | None = None
    order_count_custom: int | None = None
    order_revenue_custom: Union[int, float] | None = None
    review_count_custom: int | None = None
    gr_order_count_30d_adjacent: float | None = None
    gr_order_revenue_30d_adjacent: float | None = None
    gr_order_count_custom_adjacent: float | None = None
    gr_order_revenue_custom_adjacent: float | None = None
    order_count_30d_adjacent: int | None = None
    order_revenue_30d_adjacent: int | None = None
    order_count_custom_adjacent: int | None = None
    order_revenue_custom_adjacent: Union[int, float] | None = None

    rating_bad: int | None = None
    rating_good: int | None = None
    rating_normal: int | None = None


class RevenueSaleMonthly(SQLModel):
    begin: str
    end: str
    revenue: Union[float, int] | None = None
    sale: int | None = None
    begin_ts: int | None = None  # timestamp
    end_ts: int | None = None  # timestamp
    score: Union[float, int] | None = None


class ShopStatistic(SQLModel):
    name: str | None = None
    platform_id: Union[int, str] | None = None
    shop_base_id: str | None = None
    url_image: str | None = None
    url_shop: str | None = None
    official_type: int | None = None
    shop: int | None = None
    product: int | None = None
    revenue: Union[float, int] | None = None
    ratio_revenue: float | None = None
    sale: int | None = None
    review: int | None = None
    rating_bad: int | None = None
    rating_good: int | None = None
    rating_normal: int | None = None
    rating_count: float | None = None
    rating_avg: float | None = None

    lst_product: List[ProductStatistic] | None = None
    lst_revenue_sale_monthly: List[RevenueSaleMonthly] | None = None  # new


class RatioShop(SQLModel):
    mall: ShopStatistic | None = None
    normal: ShopStatistic | None = None


class CategoryStatistic(SQLModel):
    id: str | None = None
    name: str | None = None
    level: str | None = None
    sale: int | None = None
    revenue: Union[float, int] | None = None
    ratio_revenue: float | None = None
    parent_id: str | None = None
    parent_name: str | None = None
    is_leaf: bool | None = None


class PlatformStatistic(SQLModel):
    platform_id: Union[str, int] | None = None
    name: str | None = None
    revenue: Union[float, int] | None = None
    sale: int | None = None
    shop: int | None = None  # new - Số shop từng sàn trong 1 năm
    ratio_revenue: float | None = None
    ratio_sale: float | None = None

    ratio_top_brand_revenue: float | None = None
    lst_brand_revenue: List[BrandStatistic] | None = None  # new - Top brand theo DS
    ratio_top_brand_sale: float | None = None
    lst_brand_sale: List[BrandStatistic] | None = None  # new - Top brand theo SL
    ratio_top_shop_revenue: float | None = None
    lst_shop_revenue: List[ShopStatistic] | None = None  # new - Top shop revenue
    ratio_top_shop_sale: float | None = None
    lst_shop_sale: List[ShopStatistic] | None = None  # new - Top shop sale
    lst_product_revenue: List[ProductStatistic] | None = None  # new - Top product theo DS
    lst_revenue_sale_monthly: List[RevenueSaleMonthly] | None = None  # new
    lst_bee_category_revenue: List[CategoryStatistic] | None = None
    lst_bee_category_sale: List[CategoryStatistic] | None = None
    ratio_shop: RatioShop | None = None


class ByOverview(SQLModel):
    revenue: Union[float, int] | None = None
    sale: int | None = None
    url_product_thumbnail: str | None = None  # new
    shop: int | None = None
    product: int | None = None
    brand: int | None = None
    lst_revenue_sale_monthly: List[RevenueSaleMonthly] | None = None
    max_revenue_sale_monthly: RevenueSaleMonthly | None = None
    revenue_monthly: Union[float, int] | None = None  # doanh thu trung bình 6 tháng gần nhất
    revenue_yearly: Union[float, int] | None = None  # doanh thu trung bình 12 tháng gần nhất
    gr_monthly: float | None = None  # tăng trưởng doanh thu tháng gần nhất so với tháng trước
    gr_quarter: float | None = None  # tăng trưởng doanh thu quý gần nhất so với quý trước
    gr_adjacent: float | None = None  # tăng trưởng doanh thu cùng kỳ


class ReportStatistic(SQLModel):  # new
    name: str | None = None
    is_have_report: bool | None = None
    slug: str | None = None
    product: int | None = None
    gr_revenue_quarter: float | None = None
    search_volume: int | None = None


class BrandSummary(SQLModel):
    name: str | None = None
    slug: str | None = None
    url_thumbnail: str | None = None
    revenue_monthly: Union[float, int] | None = None
    revenue_yearly: Union[float, int] | None = None
    gr_quarter: float | None = None
    lst_gr_revenue_monthly: List[RevenueSaleMonthly] | None = None


class ByMarketplace(SQLModel):
    lst_marketplace: List[PlatformStatistic] | None = None


class PriceRangeStatistic(SQLModel):
    begin: int | None = None
    end: int | None = None
    sale: int | None = None
    revenue: int | None = None
    ratio_revenue: float | None = None
    ratio_sale: float | None = None
    lst_platform: List[PlatformStatistic] | None = None


class ByPriceRange(SQLModel):
    lst_price_range: List[PriceRangeStatistic] | None = None


class RatioBrand(SQLModel):
    brand: BrandStatistic | None = None
    no_brand: BrandStatistic | None = None


class ByBrand(SQLModel):
    ratio: RatioBrand | None = None
    lst_brand_revenue: List[BrandStatistic] | None = None
    ratio_top_brand_revenue: float | None = None
    lst_brand_sale: List[BrandStatistic] | None = None
    ratio_top_brand_sale: float | None = None
    lst_brand: List[BrandStatistic] | None = None


class ByShop(SQLModel):
    ratio: RatioShop | None = None
    # statistic_by_revenue_monthly
    # statistic_by_shop_location
    ratio_top_shop_revenue: float | None = None
    lst_shop_revenue: List[ShopStatistic] | None = None
    ratio_top_shop_sale: float | None = None
    lst_shop_sale: List[ShopStatistic] | None = None

    lst_shop: List[ShopStatistic] | None = None
    # lst_shop: List[ShopStatistic] | None = None


class ByProduct(SQLModel):
    lst_product_revenue: List[ProductStatistic] | None = None  # new 60 sản phẩm doanh thu cao nhất trong năm All sàn
    lst_product_revenue_custom: List[ProductStatistic] | None = None
    lst_product_revenue_30d: List[ProductStatistic] | None = None
    lst_product_sale_30d: List[ProductStatistic] | None = None
    lst_product_new_30d: List[ProductStatistic] | None = None


class ByCategory(SQLModel):
    lst_bee_category: List[CategoryStatistic] | None = None
    lst_shopee_category: List[CategoryStatistic] | None = None
    lst_lazada_category: List[CategoryStatistic] | None = None
    lst_tiki_category: List[CategoryStatistic] | None = None

    # lst_bee_category_l0: List[CategoryStatistic] | None = None
    # lst_bee_category_l1: List[CategoryStatistic] | None = None
    # lst_shopee_category_l1: List[CategoryStatistic] | None = None
    # lst_shopee_category_l2: List[CategoryStatistic] | None = None
    # lst_shopee_category_l3: List[CategoryStatistic] | None = None
    # lst_lazada_category_l1: List[CategoryStatistic] | None = None
    # lst_lazada_category_l2: List[CategoryStatistic] | None = None
    # lst_lazada_category_l3: List[CategoryStatistic] | None = None
    # lst_tiki_category_l1: List[CategoryStatistic] | None = None
    # lst_tiki_category_l2: List[CategoryStatistic] | None = None
    # lst_tiki_category_l3: List[CategoryStatistic] | None = None


class BySubCategory(SQLModel):
    shopee: List[CategoryStatistic] = []
    tiki: List[CategoryStatistic] = []
    lazada: List[CategoryStatistic] = []
    all: List[CategoryStatistic] = []


class ByBrandCompetitor(SQLModel):
    lst_brand: List[BrandSummary] | None = None


class ByReport(SQLModel):
    lst_report: List[ReportStatistic] | None = None  # new


class ShopLocation(SQLModel):
    name: str | None = None  # name sau khi normalize
    name_raw: str | None = None
    revenue: float | None = None
    ratio_revenue: float | None = None
    map_code: str | None = None #new


class ByShopLocation(SQLModel):
    lst_shop_location: List[ShopLocation] | None = None


class ShopReview(SQLModel):  # new
    name: str | None = None
    platform_id: Union[int, str] | None = None
    shop_base_id: str | None = None
    url_image: str | None = None
    url_shop: str | None = None
    official_type: int | None = None
    rating_count: int | None = None
    rating_good_count: int | None = None
    rating_bad_count: int | None = None
    rating_avg: int | None = None

    shop: int | None = None
    product: int | None = None
    revenue: Union[float, int] | None = None
    ratio_revenue: float | None = None
    sale: int | None = None


class ByReview(SQLModel):
    lst_shop_review: List[ShopStatistic] | None = None
    lst_product_review: List[ProductStatistic] | None = None


class ResultAnalyticReportPdf(SQLModel):
    by_overview: ByOverview | None = None
    by_category: ByCategory | None = None  # Dùng để mapping category cho report
    by_marketplace: ByMarketplace | None = None
    by_price_range: ByPriceRange | None = None
    by_brand: ByBrand | None = None
    by_shop: ByShop | None = None
    by_product: ByProduct | None = None
    by_shop_location: ByShopLocation | None = None  # new
    by_review: ByReview | None = None  # new

    by_sub_category: BySubCategory | None = None
    by_cat2_category: BySubCategory | None = None
    by_brand_competitor: ByBrandCompetitor | None = None
    by_report: ByReport | None = None  # new

    def toJSON(self):
        return json.loads(self.json())