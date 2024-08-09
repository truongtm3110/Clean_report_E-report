import datetime
from sqlalchemy.exc import NoResultFound, SQLAlchemyError
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.models.models_ereport import Plan, Subscription, User, UserQuota
from helper.logger_helper import LoggerSimple
from helper.error_helper import log_error

logger = LoggerSimple(name=__name__).logger


async def get_plan_by_code(plan_code: str, session: AsyncSession) -> Plan | None:
    try:
        statement = select(Plan).where(Plan.plan_code == plan_code)
        result = await session.execute(statement)
        return result.scalar_one_or_none()
    except Exception as e:
        log_error(e)
        return None


async def get_plan_by_id(plan_id: int, session: AsyncSession) -> Plan | None:
    try:
        statement = select(Plan).where(Plan.id == plan_id)
        result = await session.execute(statement)
        return result.scalar_one_or_none()
    except Exception as e:
        log_error(e)
        return None


async def get_user_plan(user_id: int, session: AsyncSession):
    try:
        statement = select(Plan, Subscription).join(Subscription).where(Subscription.user_id == user_id)
        result = (await session.execute(statement)).all()
        (plan, subscription), = result if result else [(None, None)]
        return plan, subscription
    except Exception as e:
        log_error(e)
        return None, None


async def get_list_plan(session: AsyncSession) -> list[Plan]:
    try:
        statement = select(Plan)
        result = await session.execute(statement)
        return result.scalars().all()
    except Exception as e:
        log_error(f"Failed to get list of plans: {e}")
        return []


async def set_community_user_plan(user: User, session: AsyncSession):
    try:
        subscription_start_at = datetime.now()
        subscription_end_at = subscription_start_at + datetime.timedelta(days=90)  # Approximation of 3 months

        new_subscription = Subscription(
            user_id=user.id,
            plan_id=4,
            is_trial=0,
            value=0.0,
            activator_email=user.email,
            subscription_start_at=subscription_start_at,
            subscription_end_at=subscription_end_at,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )

        session.add(new_subscription)
        await session.commit()

    except Exception as e:
        log_error(e)
        await session.rollback()


async def set_package(email: str, package_id: str, end_time: str, session: AsyncSession):
    if not email or not package_id or not end_time:
        return {"success": False, "message": "Missing required parameters"}

    end_time_dt = datetime.datetime.strptime(end_time, "%Y%m%d")

    try:
        logger.info(f"Starting set_package for email: {email} and package_id: {package_id}")
        user_result = select(User).where(User.email == email)
        user_query_result = await session.execute(user_result)
        user = user_query_result.unique().scalar_one_or_none()
        if not user:
            message = f"User not found for email: {email}"
            logger.error(message)
            return {"success": False, "message": message}

        plan_result = select(Plan).where(Plan.plan_code == package_id)
        plan_query_result = await session.execute(plan_result)
        plan = plan_query_result.unique().scalar_one_or_none()
        if not plan:
            message = f"Plan not found for package_id: {package_id}"
            logger.error(message)
            return {"success": False, "message": message}

        existing_subscription = await session.execute(
            select(Subscription).where(Subscription.user_id == user.id)
        )
        existing_subscription = existing_subscription.scalar_one_or_none()

        if existing_subscription:
            existing_subscription.plan_id = plan.id
            existing_subscription.is_trial = 0
            existing_subscription.value = 0.0
            existing_subscription.activator_email = email
            existing_subscription.note = "Package subscription updated"
            existing_subscription.subscription_start_at = datetime.datetime.now()
            existing_subscription.subscription_end_at = end_time_dt
            existing_subscription.updated_at = datetime.datetime.now()
        else:
            new_subscription = Subscription(
                user_id=user.id,
                plan_id=plan.id,
                is_trial=0,
                value=0.0,
                activator_email=email,
                payment_method="CRM",
                note="Package subscription",
                subscription_start_at=datetime.datetime.now(),
                subscription_end_at=end_time_dt,
                created_at=datetime.datetime.now(),
                updated_at=datetime.datetime.now()
            )
            session.add(new_subscription)

        # Check for existing UserQuota
        existing_user_quota = await session.execute(
            select(UserQuota).where(UserQuota.user_id == user.id)
        )
        existing_user_quota = existing_user_quota.scalar_one_or_none()

        if(plan.quota_claim_expert):
            quota_claim_expert = plan.quota_claim_expert
        else:
            quota_claim_expert = 0

        # Update or create UserQuota
        if existing_user_quota:
            existing_user_quota.quota_claim = plan.quota_claim
            existing_user_quota.quota_claim_basic = plan.quota_claim_basic
            existing_user_quota.quota_claim_pro = plan.quota_claim_pro
            existing_user_quota.quota_claim_expert = quota_claim_expert
            existing_user_quota.remain_claim = plan.quota_claim
            existing_user_quota.remain_claim_basic = plan.quota_claim_basic
            existing_user_quota.remain_claim_pro = plan.quota_claim_pro
            existing_user_quota.remain_claim_expert = quota_claim_expert
            existing_user_quota.updated_at = datetime.datetime.now()
        else:
            new_user_quota = UserQuota(
                user_id=user.id,
                quota_claim=plan.quota_claim,
                quota_claim_basic=plan.quota_claim_basic,
                quota_claim_pro=plan.quota_claim_pro,
                quota_claim_expert=quota_claim_expert | 0,
                remain_claim=plan.quota_claim,
                remain_claim_basic=plan.quota_claim_basic,
                remain_claim_pro=plan.quota_claim_pro,
                remain_claim_expert=quota_claim_expert,
                created_at=datetime.datetime.now(),
                updated_at=datetime.datetime.now()
            )
            session.add(new_user_quota)

        await session.commit()
        logger.info(f"Package activated successfully for email: {email}")
        return {"success": True, "message": "Package activated successfully"}

    except NoResultFound as e:
        await session.rollback()
        message = "Database entity not found"
        logger.error(f"{message}, Error: {e}")
        return {"success": False, "message": message}
    except SQLAlchemyError as e:
        await session.rollback()
        message = "Database operation failed"
        logger.error(f"{message}, Error: {e}")
        return {"success": False, "message": message}
    except Exception as e:
        await session.rollback()
        message = "Unexpected error occurred"
        logger.error(f"{message}, Error: {e}")
        return {"success": False, "message": message}