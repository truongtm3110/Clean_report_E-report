from sqlalchemy.ext.asyncio import AsyncSession

from app.models.models_ereport import UserQuota, User, Plan
from app.schemas.subscription.quota_remain_schema import Claim
from helper.error_helper import log_error
from sqlalchemy.future import select
from sqlalchemy.exc import IntegrityError
from datetime import datetime



def build_quota_or_remain(claim: int = 0, basic: int = 0, pro: int = 0, expert: int = 0) -> Claim:
    if basic is None:
        basic = 0
    if pro is None:
        pro = 0
    if expert is None:
        expert = 0
    return Claim(claim=claim, claim_basic=basic, claim_pro=pro, claim_expert=expert)


def build_quota_by_plan(plan: Plan) -> Claim:
    return build_quota_or_remain(
        plan.quota_claim,
        plan.quota_claim_basic,
        plan.quota_claim_pro,
        plan.quota_claim_expert
    )


async def create_user_quota(user: User,
                            quota_claim: int,
                            quota_claim_basic: int,
                            quota_claim_pro: int,
                            quota_claim_expert: int,
                            session: AsyncSession):
    try:
        # Check if a quota already exists for the user
        statement = select(UserQuota).where(UserQuota.user_id == user.id)
        existing_quota = await session.execute(statement)
        quota = existing_quota.scalar_one_or_none()

        if quota:
            # Update existing quota
            quota.quota_claim = quota_claim
            quota.quota_claim_basic = quota_claim_basic
            quota.quota_claim_pro = quota_claim_pro
            quota.quota_claim_expert = quota_claim_expert
            quota.remain_claim = quota_claim
            quota.remain_claim_basic = quota_claim_basic
            quota.remain_claim_pro = quota_claim_pro
            quota.remain_claim_expert = quota_claim_expert
            quota.updated_at = datetime.now()
        else:
            # Create new quota
            quota = UserQuota(
                user_id=user.id,
                quota_claim=quota_claim,
                quota_claim_basic=quota_claim_basic,
                quota_claim_pro=quota_claim_pro,
                quota_claim_expert=quota_claim_expert,
                remain_claim=quota_claim,
                remain_claim_basic=quota_claim_basic,
                remain_claim_pro=quota_claim_pro,
                remain_claim_expert=quota_claim_expert,
                created_at=datetime.now(),
                updated_at=datetime.now()
            )
            session.add(quota)

        await session.commit()
        await session.refresh(quota)
        return quota
    except IntegrityError as e:
        await session.rollback()
        log_error(e)
        raise ValueError("Failed to create or update user quota due to integrity error")
    except Exception as e:
        await session.rollback()
        log_error(e)
        raise e