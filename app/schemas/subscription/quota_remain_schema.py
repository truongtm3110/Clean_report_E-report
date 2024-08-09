from datetime import datetime

from pydantic import BaseModel


class Claim(BaseModel):
    claim: int
    claim_basic: int
    claim_pro: int
    claim_expert: int


class CurrentPlan(BaseModel):
    plan_id: int | None = None
    plan_name: str | None = None
    plan_code: str | None = None
    remain_claim: int | None = None
    remain_claim_basic: int | None = None
    remain_claim_pro: int | None = None
    remain_claim_expert: int | None = None
    expired_at: datetime | None = None
