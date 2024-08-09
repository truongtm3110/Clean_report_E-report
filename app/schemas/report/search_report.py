from enum import Enum
from typing import List, Union

from sqlmodel import SQLModel, Field

from app.models.models_ereport import ReportCategory


class Sort(str, Enum):
    popularity = "popularity"
    revenue = "revenue"
    default = "default"
    created_at = "created_at"


class FieldReport(str, Enum):
    name = 'name'
    slug = 'slug'
    tb_category_id = 'tb_category_id'
    url_thumbnail = 'url_thumbnail'
    revenue = 'revenue'
    revenue_monthly = 'revenue_monthly'
    gr_monthly = 'gr_monthly'
    gr_quarter = 'gr_quarter'
    shop = 'shop'
    created_at = 'created_at'


class Report(SQLModel):
    id: int | None = None
    slug: str | None = None
    name: str | None = None
    introduction: str | None = None
    url_thumbnail: str | None = None
    created_at: str | None = None
    report_type: str | None = None
    revenue_monthly: Union[float, int] | None = None
    # gr_monthly: float = None
    product: int | None = None
    gr_quarter: float | None = None
    source: str | None = None
    shop: int | None = None
    category_report_id: str | None = None
    lst_category: List[ReportCategory] | None = None  # cây danh mục
    lst_brand: List[str] | None = None
    start_date: str | None = None
    end_date: str | None = None


class ReportTaobao(SQLModel):
    tb_category_id: int = None
    slug: str = None
    name: str = None
    url_thumbnail: str = None
    # revenue_monthly: Union[float, int] = None
    revenue: Union[float, int] = None
    # gr_monthly: float = None
    gr_quarter: float = None
    shop: int = None
    category_report_id: str = None
    lst_category: List[ReportCategory] = None  # cây danh mục
    lst_brand: List[str] = None


class Order(str, Enum):
    asc = 'asc'
    desc = 'desc'


class RequestSearchReport(SQLModel):
    lst_query: List[str] = None
    lst_category_report_id: List[Union[int, str]] = None
    sort: Sort = Sort.popularity
    order: Order = Order.desc
    lst_field: List[FieldReport] = [FieldReport.name, FieldReport.slug]
    is_smart_queries: bool = True
    is_get_total: bool = True
    limit: int = Field(default=12, lt=50)
    offset: int = Field(default=0, lt=200)
    source: List[str] = None
    lst_report_type: List[str] = None


class ResponseSearchReport(SQLModel):
    total: int | None = None
    breadcrumb: List[ReportCategory] | None = None
    lst_category: List[ReportCategory] | None = None  # cây danh mục
    lst_query: List[str] | None = None
    lst_report: List[Report] | None = None


class ResponseSearchTaobaoReport(SQLModel):
    total: int = None
    breadcrumb: List[ReportCategory] = None
    lst_category: List[ReportCategory] = None  # cây danh mục
    lst_query: List[str] = None
    lst_report: List[ReportTaobao] = None
