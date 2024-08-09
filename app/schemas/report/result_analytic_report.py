from typing import Union, List

from sqlmodel import SQLModel


class RevenueSaleMonthly(SQLModel):
    begin: str
    end: str
    revenue: Union[float, int] | None = None
    sale: int | None = None
    begin_ts: int | None = None  # timestamp
    end_ts: int | None = None  # timestamp
    score: Union[float, int] | None = None


class ByOverview(SQLModel):
    revenue: Union[float, int] | None | None = None
    sale: int | None | None = None
    shop: int | None | None = None
    product: int | None | None = None
    brand: int | None | None = None
    lst_revenue_sale_monthly: List[RevenueSaleMonthly] | None | None = None
    max_revenue_sale_monthly: RevenueSaleMonthly | None | None = None
    revenue_monthly: Union[float, int] | None | None = None  # doanh thu trung bình 6 tháng gần nhất
    revenue_yearly: Union[float, int] | None | None = None  # doanh thu trung bình 12 tháng gần nhất
    gr_monthly: float | None | None = None  # tăng trưởng doanh thu tháng gần nhất so với tháng trước
    gr_quarter: float | None | None = None  # tăng trưởng doanh thu quý gần nhất so với quý trước


class CategoryStatistic(SQLModel):
    id: str | None | None = None
    name: str | None | None = None
    level: str | int | None | None = None
    sale: int | None | None = None
    revenue: Union[float, int] | None | None = None
    ratio_revenue: float | None | None = None
    parent_id: str | None | None = None
    parent_name: str | None | None = None
    is_leaf: bool | None | None = None



class KeywordStatistic(SQLModel):
    name:  str | None = None
    revenue: Union[float, int] | None = None
    sale: int | None = None
    shop: int | None = None
    slug: str | None = None
    url_thumbnail: str | None = None


class PlatformStatistic(SQLModel):
    platform_id: Union[str, int] | None = None
    name: str | None = None
    revenue: Union[float, int] | None = None
    sale: int | None = None
    ratio_revenue: float | None = None
    ratio_sale: float | None = None


class BrandStatistic(SQLModel):
    name: str | None = None
    shop: int | None = None
    product: int | None = None
    brand: int | None = None
    revenue: Union[float, int] | None = None
    sale: int | None = None
    ratio_revenue: float | None = None
    ratio_sale: float | None = None


class BrandSummary(SQLModel):
    name: str | None = None
    slug: str | None = None
    url_thumbnail: str | None = None
    revenue_monthly: Union[float, int] | None = None
    revenue_yearly: Union[float, int] | None = None
    gr_quarter: float | None = None
    lst_gr_revenue_monthly: List[RevenueSaleMonthly] | None = None


class ShopStatistic(SQLModel):
    name: str | None | None = None
    platform_id: Union[int, str] | None | None = None
    shop_base_id: str | None | None = None
    url_image: str | None | None = None
    url_shop: str | None | None = None
    official_type: int | None | None = None
    shop: int | None | None = None
    product: int | None | None = None
    revenue: Union[float, int] | None | None = None
    ratio_revenue: float | None | None = None
    sale: int | None | None = None


class ProductStatistic(SQLModel):
    product_base_id: str | None | None = None
    product_name: str | None | None = None
    url_thumbnail: str | None | None = None
    official_type: int | None | None = None
    brand: str | None | None = None
    shop_platform_name: str | None | None = None
    shop_url: str | None | None = None
    price: Union[int, float] | None | None = None
    order_count: int | None | None = None
    rating_avg: float | None | None = None
    rating_count: int | None | None = None
    revenue: Union[int, float] | None | None = None
    platform_created_at: int | None | None = None
    price_updated_at: int | None | None = None
    price_min: Union[int, float] | None | None = None
    price_max: Union[int, float] | None | None = None
    order_count_30d: int | None | None = None
    order_revenue_30d: int | None | None = None
    gr_order_count_30d_adjacent: float | None | None = None
    gr_order_revenue_30d_adjacent: float | None | None = None
    order_count_30d_adjacent: int | None | None = None
    order_revenue_30d_adjacent: int | None | None = None


class ByMarketplace(SQLModel):
    lst_marketplace: List[PlatformStatistic] | None = None


class PriceRangeStatistic(SQLModel):
    begin: int | None | None = None
    end: int | None | None = None
    sale: int | None | None = None
    revenue: int | None | None = None
    ratio_revenue: float | None | None = None
    lst_platform: List[PlatformStatistic] | None | None = None


class ByPriceRange(SQLModel):
    lst_price_range: List[PriceRangeStatistic] | None = None


class RatioBrand(SQLModel):
    brand: BrandStatistic | None = None
    no_brand: BrandStatistic | None = None


class ByBrand(SQLModel):
    ratio: RatioBrand | None = None
    lst_top_brand_revenue: List[BrandStatistic] | None = None
    lst_top_brand_sale: List[BrandStatistic] | None = None
    lst_brand: List[BrandStatistic] | None = None


class RatioShop(SQLModel):
    mall: ShopStatistic | None = None
    normal: ShopStatistic | None = None


class ByShop(SQLModel):
    ratio: RatioShop | None = None
    # statistic_by_revenue_monthly
    # statistic_by_shop_location
    lst_top_shop: List[ShopStatistic] | None = None
    lst_shop: List[ShopStatistic] | None = None


class ByProduct(SQLModel):
    lst_product_revenue_30d: List[ProductStatistic] | None = None
    lst_product_sale_30d: List[ProductStatistic] | None = None
    lst_product_top_revenue_custom: List[ProductStatistic] | None = None
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


class ByKeyword(SQLModel):
    lst_keyword: List[KeywordStatistic] | None = None


class ResultAnalyticReport(SQLModel):
    by_overview: ByOverview | None = None
    by_category: ByCategory | None = None  # Dùng để mapping category cho report
    by_marketplace: ByMarketplace | None = None
    by_price_range: ByPriceRange | None = None
    by_brand: ByBrand | None = None
    by_shop: ByShop | None = None
    by_product: ByProduct | None = None

    by_sub_category: BySubCategory | None = None
    by_cat2_category: BySubCategory | None = None
    by_brand_competitor: ByBrandCompetitor | None = None
    by_keyword: ByKeyword | None = None
    # by_review
