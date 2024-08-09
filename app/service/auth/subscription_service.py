from sqlalchemy.future import select

from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
from sqlalchemy.exc import IntegrityError

from app.models.models_ereport import Plan, User, Subscription
from app.service.auth.quota_service import build_quota_by_plan, create_user_quota
from helper.error_helper import log_error


async def create_subscription(user: User,
                              plan: Plan,
                              is_trial: bool,
                              value: float,
                              payment_method: str,
                              activator_email: str,
                              note: str,
                              start_date: str,
                              end_date: str,
                              session: AsyncSession):
    start_date = datetime.strptime(start_date, "%Y%m%d")
    end_date = datetime.strptime(end_date, "%Y%m%d")
    try:
        # Check if a subscription already exists for the user
        statement = select(Subscription).where(Subscription.user_id == user.id)
        existing_subscription = await session.execute(statement)
        subscription = existing_subscription.scalar_one_or_none()

        if subscription:
            # Update existing subscription
            subscription.plan_id = plan.id
            subscription.is_trial = is_trial
            subscription.value = value
            subscription.payment_method = payment_method
            subscription.activator_email = activator_email
            subscription.note = note
            subscription.subscription_start_at = start_date
            subscription.subscription_end_at = end_date
            subscription.updated_at = datetime.now()
        else:
            # Create new subscription
            subscription = Subscription(
                user_id=user.id,
                plan_id=plan.id,
                is_trial=is_trial,
                value=value,
                payment_method=payment_method,
                activator_email=activator_email,
                note=note,
                subscription_start_at=start_date,
                subscription_end_at=end_date
            )
            session.add(subscription)

        await session.commit()
        await session.refresh(subscription)
        return subscription
    except IntegrityError as e:
        await session.rollback()
        log_error(e)
        raise ValueError("Failed to create or update subscription due to integrity error")
    except Exception as e:
        await session.rollback()
        log_error(e)
        raise e

async def subscribe_and_create_quota_for_user(user: User,
                                              plan: Plan,
                                              is_trial: bool,
                                              value: float,
                                              payment_method: str,
                                              activator_email: str,
                                              note: str,
                                              start_date: str,
                                              end_date: str,
                                              session: AsyncSession):
    quota = build_quota_by_plan(plan)
    print(f"quota: {quota}")
    try:
        await create_subscription(user,
                                  plan,
                                  is_trial,
                                  value,
                                  payment_method,
                                  activator_email,
                                  note,
                                  start_date,
                                  end_date,
                                  session)
        await create_user_quota(user,
                                quota.claim,
                                quota.claim_basic,
                                quota.claim_pro,
                                quota.claim_expert,
                                session)
        return True
    except Exception as e:
        log_error(e)
        return False