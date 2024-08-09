import datetime
import random
import string
import time

from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from app.constant.constant_global import TransactionStatus
from app.models.models_ereport import User
from app.models.payment.payment_transaction import PaymentTransaction
from app.schemas.report.cart_info import CartInfo
from app.service.discount.discount_service import insert_discount_usage, update_discount_tracking, \
    update_usage_in_discount_code
from helper.error_helper import log_error


async def create_transaction(
        user: User,
        cart_info: CartInfo,
        payment_method: str,
        discount_code_id: int,
        session: AsyncSession
) -> PaymentTransaction:
    transaction_id = generate_transaction_code(timestamp=int(time.time()))
    transaction_content = transaction_id
    transaction = PaymentTransaction(
        user_id=user.id,
        cart_info=cart_info.dict(),
        payment_method=payment_method,
        transaction_value=cart_info.price,
        currency='VND',
        transaction_content=transaction_content,
        transaction_type=cart_info.cart_info_type,
        status=TransactionStatus.CREATED,
        transaction_id=transaction_id,
        expired_at=datetime.datetime.now() + datetime.timedelta(minutes=15),
        discount_code_id=discount_code_id
    )
    try:
        session.add(transaction)
        await session.commit()
        await session.refresh(transaction)
    except Exception as e:
        await session.rollback()
        raise e
    return transaction


def generate_transaction_code(timestamp: int, number_of_character=8):
    def get_random_string(length, is_digits):
        result_str = []
        while len(result_str) < length:
            if is_digits:
                random_character = random.choice(string.digits)
            else:
                random_character = random.choice(string.ascii_letters)

            if random_character != 'L' and random_character != 'I' and random_character != 'O' \
                    and random_character != 'l' and random_character != 'o' and random_character != '0' and random_character != 'i':
                result_str.append(random_character)

        return result_str

    final = get_random_string(length=int(number_of_character / 2) + 5, is_digits=False)

    final.extend(get_random_string(length=int(number_of_character / 2) + 1, is_digits=True))
    random.shuffle(final)

    return 'EREPORT_' + str(timestamp) + '_' + ''.join(final[0:number_of_character]).lower().upper()


async def get_transaction_by_transaction_id(transaction_id: str, session: AsyncSession) -> PaymentTransaction | None:
    # try:
    statement = select(PaymentTransaction).where(PaymentTransaction.transaction_id == transaction_id)
    return (await session.execute(statement)).scalar_one_or_none()
    # except Exception as e:
    #     log_error(e)
    #     return None


async def check_transaction_is_completed(session: AsyncSession, transaction_id: str, user_id: int) -> bool:
    try:
        statement = select(PaymentTransaction).where(
            PaymentTransaction.transaction_id == transaction_id,
            PaymentTransaction.user_id == user_id,
            PaymentTransaction.status == TransactionStatus.COMPLETED
        )
        transaction = (await session.execute(statement)).scalar_one_or_none()
        if transaction is None:
            return False
        return True
    except Exception as e:
        log_error(e)
        return False
