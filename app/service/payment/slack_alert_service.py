from sqlalchemy import select
from sqlmodel.ext.asyncio.session import AsyncSession
from app.models.payment import PaymentTransaction

async def get_payment_transaction_by_transaction_id(transaction_id: str, session: AsyncSession) -> PaymentTransaction | None:
    statement = select(PaymentTransaction).where(PaymentTransaction.transaction_id == transaction_id)
    result = await session.execute(statement)
    transaction = result.scalar()
    return transaction