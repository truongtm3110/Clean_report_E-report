import pytz

from datetime import datetime
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select

from app.models.models_ereport.discount_code import DiscountCode
from app.models.models_ereport.discount_code_usage import DiscountCodeUsage
from app.models.models_ereport.discount_tracking import DiscountTracking
from helper.logger_helper import LoggerSimple
from helper.error_helper import log_error

logger = LoggerSimple(name=__name__).logger


async def get_discount_by_code(code: str, session: AsyncSession):
    try:
        statement = select(DiscountCode).where(DiscountCode.code == code)
        result = await session.execute(statement)
        return result.scalars().first()
    except Exception as e:
        log_error(e)
        return None


async def apply_discount(discount: DiscountCode, session: AsyncSession, transaction_id: str, user_id: int):
    try:
        discount_usage = DiscountCodeUsage(
            discount_code_id=discount.id,
            transaction_id=transaction_id,
            user_id=user_id,
            used_at=datetime.now()
        )
        session.add(discount_usage)
        await session.commit()
    except Exception as e:
        log_error(e)
        await session.rollback()
        raise e


async def get_discount_id_by_code(code: str, session: AsyncSession):
    try:
        statement = select(DiscountCode).where(DiscountCode.code == code)
        result = await session.execute(statement)
        discount = result.scalars().first()
        print(discount)
        return discount.id
    except Exception as e:
        log_error(e)
        return None


async def insert_discount_usage(discount_code_id: int, transaction_id: int, user_id: int, session: AsyncSession):
    try:
        print("insert_discount_usage")
        vietnam_tz = pytz.timezone('Asia/Ho_Chi_Minh')

        discount_usage = DiscountCodeUsage(
            discount_code_id=discount_code_id,
            transaction_id=transaction_id,
            user_id=user_id,
            used_at=datetime.now(vietnam_tz)
        )
        session.add(discount_usage)
        await session.commit()
    except Exception as e:
        log_error(e)
        await session.rollback()
        raise e


async def get_discount_by_id(discount_code_id: int, session: AsyncSession):
    try:
        statement = select(DiscountCode).where(DiscountCode.id == discount_code_id)
        result = await session.execute(statement)
        return result.scalars().first()
    except Exception as e:
        log_error(e)
        return None


async def get_discount_tracking_by_code_id(discount_code_id: int, session: AsyncSession):
    try:
        statement = select(DiscountTracking).where(DiscountTracking.discount_code_id == discount_code_id)
        result = await session.execute(statement)
        return result.scalars().first()
    except Exception as e:
        log_error(e)
        return None


async def update_discount_tracking(discount_code_id: int, transaction_value: float, session: AsyncSession):
    try:
        print("update_discount_tracking")
        discount_tracking = await get_discount_tracking_by_code_id(discount_code_id, session)
        if discount_tracking is None:
            discount_tracking = DiscountTracking(
                discount_code_id=discount_code_id,
                successful_orders=0,
                total_sales_value=0.0
            )
            session.add(discount_tracking)

        discount_tracking.successful_orders += 1
        discount_tracking.total_sales_value += transaction_value
        await session.commit()
    except Exception as e:
        log_error(e)
        await session.rollback()
        raise e


async def update_usage_in_discount_code(discount_code_id: int, session: AsyncSession):
    try:
        print("update_usage_in_discount_code")
        discount = await get_discount_by_id(discount_code_id, session)
        if discount is None:
            raise Exception("Discount code not found")

        discount.usage_count += 1
        discount.updated_at = datetime.now()
        await session.commit()
    except Exception as e:
        log_error(e)
        await session.rollback()
        raise e