from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from app.models.models_ereport import UserQuota
from helper.error_helper import log_error


async def get_user_quota(session: AsyncSession, user_id: int) -> UserQuota | None:
    try:
        statement = select(UserQuota).where(UserQuota.user_id == user_id)
        result = await session.execute(statement)
        user_quota = result.scalar_one_or_none()
        return user_quota
    except Exception as e:
        log_error(e)
        return None


async def is_quota_available(user_quota: UserQuota) -> bool:
    return user_quota.remain_claim > 0


async def update_user_quota(session: AsyncSession, user_quota: UserQuota, tier: str, quota_delta: int = 1) -> bool:
    try:
        if user_quota is None:
            return False
        user_quota.remain_claim -= quota_delta
        if tier == "e_basic":
            user_quota.remain_claim_basic -= quota_delta
        elif tier == "e_pro":
            user_quota.remain_claim_pro -= quota_delta
        elif tier == "e_expert":
            user_quota.remain_claim_expert -= quota_delta
        session.add(user_quota)
        await session.commit()
        return True
    except Exception as e:
        log_error(e)
        return False
