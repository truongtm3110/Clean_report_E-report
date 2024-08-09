from datetime import datetime
from typing import List, Union

from sqlmodel import SQLModel

from app.models.models_ereport.category import Category
from app.schemas.report.result_analytic_report import ResultAnalyticReport
from app.schemas.report.result_analytic_report_hidden import ResultAnalyticReportHidden
from app.schemas.report.result_analytic_report_pdf import ResultAnalyticReportPdf


class CategoryBase(SQLModel):
    id: Union[str, int]
    name: str | None = None
    level: int | None = None
    parent_id: Union[str, int] | None = None
    parent_name: str | None = None
    is_leaf: bool | None = None


class ResponseReportDetail(SQLModel):
    id: int | None = None
    slug: str | None = None
    name: str | None = None
    status: str | None = None
    price: int | None = None
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
    data_analytic: ResultAnalyticReport | None = None
    optimized_query: bool = False
    free_by_add_on: bool = False
    is_unsellable: bool = False


class ResponseReportDetailHidden(SQLModel):
    id: int | None = None
    slug: str | None = None
    name: str | None = None
    status: str | None = None
    price: int | None = None
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
    data_analytic: ResultAnalyticReportHidden | None = None
    optimized_query: bool = False
    free_by_add_on: bool = False
    is_unsellable: bool = False


class ResponseReportDetailPdf(SQLModel):
    id: int = None
    slug: str = None
    name: str = None
    can_download: bool = None
    report_type: str = None
    report_format: str = None
    report_version: str = None
    url_thumbnail: str = None
    url_cover: str = None
    url_report: str = None
    category_report_id: str = None
    lst_category: List[CategoryBase] = None
    lst_report_related: List[dict] = None
    updated_at: datetime = None
    data_filter_report: dict = None
    filter_custom: dict = None
    data_analytic: ResultAnalyticReportPdf = None
