from abc import ABC, abstractmethod

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.payment.payment_transaction import PaymentTransaction


class PaymentGateway(ABC):
    transaction: PaymentTransaction = None

    @abstractmethod
    async def create_payment(self, redirect_url: str = None) -> dict:
        pass

    @abstractmethod
    async def process_payment_hook(self, received_data: dict, session: AsyncSession) -> bool:
        pass


def init_payment_gateway(transaction, user=None, session=None):
    from app.service.payment.gateway_factory.momo import MomoPaymentGateway
    from app.service.payment.gateway_factory.vnpay import VNPayPaymentGateway

    if transaction.payment_method == 'momo':
        return MomoPaymentGateway(transaction=transaction, user=user, session=session)
    elif transaction.payment_method == 'vnpay':
        return VNPayPaymentGateway(transaction=transaction)
    else:
        raise ValueError(f'Unsupported payment method: {transaction.payment_method}')
