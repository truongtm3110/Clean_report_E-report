from datetime import datetime, time, timedelta

from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import select

from app.models.models_ereport import User, Subscription
from app.models.payment import PaymentTransaction
from app.service.auth.subscription_service import subscribe_and_create_quota_for_user
from app.service.plan.plan_service import get_plan_by_code, get_plan_by_id
from helper.error_helper import log_error


async def get_subscription_by_user(user: User, session: AsyncSession) -> Subscription | None:
    try:
        statement = select(Subscription).where(Subscription.user_id == user.id)
        result = await session.execute(statement)
        subscription = result.scalar()
        return subscription
    except Exception as e:
        log_error(e)
        return None


async def update_user_subscription(transaction: PaymentTransaction, session: AsyncSession) -> bool:
    try:
        print(f"update_user_subscription")
        statement = select(User).where(User.id == transaction.user_id)
        result = await session.execute(statement)
        user = result.unique().scalar_one_or_none()
        if user is None:
            raise ValueError("User not found")

        plan = await get_plan_by_id(transaction.cart_info.get('id', 0), session)
        if plan is None:
            raise ValueError("Plan not found")

        start_date = datetime.combine(datetime.today(), time())
        end_date = start_date + timedelta(seconds=transaction.cart_info.get('duration', 0))
        print(f"start_date: {start_date}, end_date: {end_date}")

        done = await subscribe_and_create_quota_for_user(user,
                                                         plan,
                                                         False,
                                                         transaction.transaction_value,
                                                         transaction.payment_method,
                                                         user.email,
                                                         '',
                                                         start_date.strftime("%Y%m%d"),
                                                         end_date.strftime("%Y%m%d"),
                                                         session)
        print(f"subscribe_and_create_quota_for_user result: {done}")
        if not done:
            raise Exception("Failed to update user subscription")
        return True
    except Exception as e:
        await session.rollback()
        log_error(e)
        raise ValueError("Failed to update user subscription")