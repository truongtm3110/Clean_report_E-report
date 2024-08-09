from datetime import datetime, timedelta

from sqlalchemy import or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select, desc

from app.models.models_ereport import UserClaimReport
from app.service.plan.plan_service import get_user_plan
from app.service.quota.quota_service import update_user_quota, get_user_quota, is_quota_available
from helper.error_helper import log_error


async def create_claim_report(user_id: int,
                              report_id: int,
                              tier_report: str,
                              claimed_at: datetime,
                              expired_at: datetime,
                              session: AsyncSession):
    try:
        user_claim_report = UserClaimReport(
            user_id=user_id,
            report_id=report_id,
            tier_report=tier_report,
            claimed_at=claimed_at,
            expired_at=expired_at,
        )
        session.add(user_claim_report)
        await session.commit()
        return True
    except Exception as e:
        log_error(e)
        return False


async def is_report_claimed(user_id, report_id, session):
    try:
        statement = (select(UserClaimReport).where(UserClaimReport.user_id == user_id,
                                                   UserClaimReport.report_id == report_id,
                                                   or_(
                                                       UserClaimReport.tier_report == 'e_pro',
                                                       UserClaimReport.tier_report == 'e_basic',
                                                       UserClaimReport.tier_report == 'e_trial',
                                                   ),
                                                   UserClaimReport.expired_at > datetime.now())
                     .order_by(UserClaimReport.created_at, desc('created_at')))
        result = await session.execute(statement)
        user_claim_report = result.scalar_one_or_none()
        return user_claim_report is not None
    except Exception as e:
        log_error(e)
        return False


# TODO: bug remain claim bị âm
async def claim_report(user, report, session):
    claimed_at = datetime.now()
    expired_at = claimed_at + timedelta(days=1)
    # expired_at = datetime(claimed_at.year, claimed_at.month, claimed_at.day, 23, 59, 59)
    user_plan, _ = await get_user_plan(user.id, session)
    if user_plan is None:
        return False, "permission denied"
    user_quota = await get_user_quota(session, user.id)
    is_available = await is_quota_available(user_quota)
    if not is_available:
        return False, "quota is not available"
    is_claimed = await is_report_claimed(user.id, report.id, session)
    if is_claimed:
        return False, "report was claimed before"
    tier_report = user_plan.plan_code
    done = await create_claim_report(user.id, report.id, tier_report, claimed_at, expired_at, session)
    if not done:
        return False, "failed to claim report"
    done = await update_user_quota(session, user_quota, tier_report)
    return done, "claimed"
