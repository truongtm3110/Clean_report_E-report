from datetime import datetime

from pydantic import BaseModel

from app.schemas.subscription.quota_remain_schema import CurrentPlan


class UserInfo(BaseModel):
    id: int
    email: str
    display_name: str = None
    first_name: str
    last_name: str
    avatar: str = None
    current_plan: CurrentPlan | None = None
