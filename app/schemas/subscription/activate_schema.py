from datetime import datetime

from pydantic import BaseModel


class HardActivateRequest(BaseModel):
    email: str
    plan_code: str
    value: float | None = None
    payment_method: str | None = None
    activator_email: str | None = None
    note: str | None = None
    is_trial: bool | None = None
    start_date: str | None = datetime.now().strftime("%Y%m%d")
    end_date: str | None = None
