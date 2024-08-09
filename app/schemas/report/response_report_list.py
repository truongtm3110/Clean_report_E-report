from datetime import datetime
from typing import List

from pydantic import BaseModel

from app.models.models_ereport import ReportCategory


class BasicInfoReport(BaseModel):
    id: int
    slug: str
    name: str
    claimed_at: datetime | None = None
    expired_at: datetime | None = None
    status: str | None = None
    source: str | None = None
    search_volume_shopee: int | None = None
    start_date: str | None = None
    end_date: str | None = None
    category_report_id: str | None = None
    category_report_name: str | None = None
    url_thumbnail: str | None = None
    report_type: str | None = None
    lst_brand: list[str] | None = None
    lst_category: List[ReportCategory] | None = None
