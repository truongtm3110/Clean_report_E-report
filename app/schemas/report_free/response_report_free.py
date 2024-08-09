from datetime import datetime
from typing import Union, List

from sqlmodel import SQLModel

from app.schemas.report.response_report_detail import CategoryBase


class RevenueSaleMonthlyHidden(SQLModel):
    begin: str
    end: str
    begin_ts: int | None = None  # timestamp
    end_ts: int | None = None  # timestamp
    score: Union[float, int] | None = None


class ByOverviewHidden(SQLModel):
    shop: int | None = None
    product: int | None = None
    brand: int | None = None
    lst_revenue_sale_monthly: List[RevenueSaleMonthlyHidden] | None = None
    gr_monthly: float | None = None
    gr_quarter: float | None = None


class CategoryStatisticHidden(SQLModel):
    id: str | None = None
    name: str | None = None
    level: str | int | None = None
    ratio_revenue: float | None = None
    parent_id: str | None = None
    parent_name: str | None = None
    is_leaf: bool | None = None


class KeywordStatistic(SQLModel):
    name:  str | None = None
    revenue: Union[float, int] | None = None
    sale: int | None = None
    shop: int | None = None
    slug: str | None = None
    url_thumbnail: str | None = None


class PlatformStatisticHidden(SQLModel):
    platform_id: Union[str, int] | None = None
    name: str | None = None
    ratio_revenue: float | None = None
    ratio_sale: float | None = None


class BrandStatisticHidden(SQLModel):
    name: str | None = None
    shop: int | None = None
    product: int | None = None
    brand: int | None = None
    ratio_revenue: float | None = None
    ratio_sale: float | None = None


class BrandSummaryHidden(SQLModel):
    name: str | None = None
    slug: str | None = None
    url_thumbnail: str | None = None
    gr_quarter: float | None = None
    lst_gr_revenue_monthly: List[RevenueSaleMonthlyHidden] | None = None


class ShopStatisticHidden(SQLModel):
    name: str | None = None
    platform_id: Union[int, str] | None = None
    shop_base_id: str | None = None
    url_image: str | None = None
    url_shop: str | None = None
    official_type: int | None = None
    shop: int | None = None
    product: int | None = None
    ratio_revenue: float | None = None


class ProductStatisticHidden(SQLModel):
    product_base_id: str | None = None
    product_name: str | None = None
    url_thumbnail: str | None = None
    official_type: int | None = None
    brand: str | None = None
    shop_platform_name: str | None = None
    shop_url: str | None = None
    price: Union[int, float] | None = None
    rating_avg: float | None = None
    rating_count: int | None = None
    platform_created_at: int | None = None
    price_updated_at: int | None = None
    price_min: Union[int, float] | None = None
    price_max: Union[int, float] | None = None


class ByMarketplaceHidden(SQLModel):
    lst_marketplace: List[PlatformStatisticHidden] | None = None


class PriceRangeStatisticHidden(SQLModel):
    begin: int | None = None
    end: int | None = None
    ratio_revenue: float | None = None
    lst_platform: List[PlatformStatisticHidden] | None = None


class ByPriceRangeHidden(SQLModel):
    lst_price_range: List[PriceRangeStatisticHidden] | None = None


class RatioBrand(SQLModel):
    brand: BrandStatisticHidden | None = None
    no_brand: BrandStatisticHidden | None = None


class ByBrandHidden(SQLModel):
    ratio: RatioBrand | None = None
    lst_top_brand_revenue: List[BrandStatisticHidden] | None = None
    lst_top_brand_sale: List[BrandStatisticHidden] | None = None
    lst_brand: List[BrandStatisticHidden] | None = None


class RatioShopHidden(SQLModel):
    mall: ShopStatisticHidden | None = None
    normal: ShopStatisticHidden | None = None


class ByShopHidden(SQLModel):
    ratio: RatioShopHidden | None = None
    lst_top_shop: List[ShopStatisticHidden] | None = None
    lst_shop: List[ShopStatisticHidden] | None = None


class ByProductHidden(SQLModel):
    lst_product_revenue_30d: List[ProductStatisticHidden] | None = None
    lst_product_sale_30d: List[ProductStatisticHidden] | None = None
    lst_product_top_revenue_custom: List[ProductStatisticHidden] | None = None
    lst_product_new_30d: List[ProductStatisticHidden] | None = None


class ByCategoryHidden(SQLModel):
    lst_bee_category: List[CategoryStatisticHidden] | None = None
    lst_shopee_category: List[CategoryStatisticHidden] | None = None
    lst_lazada_category: List[CategoryStatisticHidden] | None = None
    lst_tiki_category: List[CategoryStatisticHidden] | None = None


class BySubCategoryHidden(SQLModel):
    shopee: List[CategoryStatisticHidden] = []
    tiki: List[CategoryStatisticHidden] = []
    lazada: List[CategoryStatisticHidden] = []
    all: List[CategoryStatisticHidden] = []


class ByBrandCompetitorHidden(SQLModel):
    lst_brand: List[BrandSummaryHidden] | None = None


class ByKeyword(SQLModel):
    lst_keyword: List[KeywordStatistic] | None = None


class ResultAnalyticReportHidden(SQLModel):
    by_overview: ByOverviewHidden | None = None
    by_category: ByCategoryHidden | None = None  # Dùng để mapping category cho report

    by_marketplace: ByMarketplaceHidden | None = None
    by_price_range: ByPriceRangeHidden | None = None

    by_brand: ByBrandHidden | None = None
    by_shop: ByShopHidden | None = None

    by_product: ByProductHidden | None = None

    by_sub_category: BySubCategoryHidden | None = None
    by_cat2_category: BySubCategoryHidden | None = None
    by_brand_competitor: ByBrandCompetitorHidden | None = None
    by_keyword: ByKeyword | None = None


class ResponseReportDetailBase(SQLModel):
    id: int | None = None
    slug: str | None = None
    name: str | None = None
    status: str | None = None
    price: int | None = None
    tier_report: str | None = 'e_community'
    can_download: bool | None = None
    introduction: str | None = None
    report_type: str | None = None
    report_format: str | None = None
    report_version: str | None = None
    url_thumbnail: str | None = None
    url_cover: str | None = None
    url_report: str | None = None
    available_url_report: bool | None = None
    category_report_id: str | None = None
    lst_category: List[CategoryBase] | None = None
    lst_report_related: List[dict] | None = None
    updated_at: datetime | None = None
    data_filter_report: dict | None = None
    filter_custom: dict | None = None
    optimized_query: bool = False
    free_by_add_on: bool = False
    is_unsellable: bool = False


class ResponseReportDetailFree(ResponseReportDetailBase):
    data_analytic: ResultAnalyticReportHidden | None = None
