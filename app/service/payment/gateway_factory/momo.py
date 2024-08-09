import base64
import hashlib
import hmac
import json
import time
import uuid
from datetime import datetime
from urllib.parse import urlparse

import requests
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import Depends

from app.api.report.endpoint_report import authenticate
from app.constant.constant_global import MomoSettings, MomoResponse, TransactionStatus
from app.db.database import get_async_session
from app.main import logger
from app.models.models_ereport import User
from app.service.discount.discount_service import insert_discount_usage, update_discount_tracking, \
    update_usage_in_discount_code
from app.service.payment.gateway_factory.gateway_factory import PaymentGateway
from app.service.payment.transaction_service import get_transaction_by_transaction_id, check_transaction_is_completed
from app.service.subscription.subscription_service import update_user_subscription
from helper.error_helper import log_error


class MomoPaymentGateway(PaymentGateway):
    def __init__(self, transaction, user=None, session=None):
        self.transaction = transaction
        self.user = user
        self.session = session

    async def create_payment(self, redirect_url: str = None) -> dict:
        if redirect_url is None or redirect_url == "":
            raise ValueError("invalid redirect url")
        response_data = await service_momo_create_payment_method(
            redirect_url=redirect_url,
            order_info=self.transaction.transaction_content,
            amount=int(self.transaction.transaction_value),
            order_id=self.transaction.transaction_id,
            extra_data_plain={
                'product_url': redirect_url
            },
            user=self.user,
            session=self.session
        )
        payment_url = response_data.get('payUrl', '')
        if payment_url == '':
            raise ValueError("failed to create payment method")

        return {
            'payment_url': payment_url,
            'transaction_id': self.transaction.transaction_id,
            'transaction_value': self.transaction.transaction_value
        }

    async def process_payment_hook(self, received_data: dict, session: AsyncSession) -> bool:
        # get transaction by transaction_id
        transaction = await get_transaction_by_transaction_id(received_data.get('orderInfo'), session)
        if transaction is None:
            return False

        if transaction.status in (TransactionStatus.COMPLETED, TransactionStatus.FAILED):
            logger.info(f"Transaction {transaction.transaction_id} has already been done")
            return True

        # update transaction status
        transaction.payment_raw_data = received_data
        transaction.completed_at = datetime.fromtimestamp(time.time())
        transaction.status = TransactionStatus.COMPLETED if received_data.get(
            'resultCode') == MomoResponse.STATUS_CODE_SUCCESS else TransactionStatus.FAILED
        try:
            print("update transaction")
            session.add(transaction)
            await session.commit()
            await session.refresh(transaction)
        except Exception as e:
            await session.rollback()
            log_error(e)
            return False

        # Update user subscription
        done = False
        if transaction.status == TransactionStatus.COMPLETED:
            print("update user subscription")
            done = await update_user_subscription(transaction, session)
            await insert_discount_usage(transaction.discount_code_id, transaction.id, transaction.user_id, session)
            await update_discount_tracking(transaction.discount_code_id, transaction.transaction_value, session)
            await update_usage_in_discount_code(transaction.discount_code_id, session)
        return done


async def service_momo_create_payment_method(redirect_url: str, amount: int, order_id, order_info,
                                             session: AsyncSession,
                                             user: User,
                                             extra_data_plain=None,
                                             request_type: str = "captureWallet") -> dict:
    logger.info(f"momo create payment method: {redirect_url}, {amount}, {order_id}, {order_info}, {extra_data_plain}")
    partner_code = MomoSettings.MOMO_PARTNER_CODE
    access_key = MomoSettings.MOMO_ACCESS_KEY
    secret_key = MomoSettings.MOMO_SECRET_KEY
    order_info = order_info

    redirect_url_obj = urlparse(redirect_url)
    transaction_completed = await check_transaction_is_completed(session, order_id, user.id)
    print(f"transaction_completed: {transaction_completed}")
    if redirect_url_obj.query:
        if transaction_completed:
            redirect_url = redirect_url + "&transaction_id=" + order_id
        else:
            redirect_url = redirect_url
    else:
        if transaction_completed:
            redirect_url = redirect_url + "?transaction_id=" + order_id
        else:
            redirect_url = redirect_url

    ipn_url = MomoSettings.MOMO_IPN_URL
    amount = amount
    order_id = order_id

    request_id = str(uuid.uuid4())
    if extra_data_plain is None:
        extra_data = ""
    elif type(extra_data_plain) == dict:
        extra_data = str(base64.b64encode(json.dumps(extra_data_plain).encode('utf-8')), 'utf-8')
    elif type(extra_data_plain) == str:
        extra_data = str(base64.b64encode(extra_data_plain.encode('utf-8')), 'utf-8')
    else:
        extra_data = ""

    raw_signature = "accessKey=" + access_key + \
                    "&amount=" + str(amount) + \
                    "&extraData=" + extra_data + \
                    "&ipnUrl=" + ipn_url + \
                    "&orderId=" + order_id + \
                    "&orderInfo=" + order_info + \
                    "&partnerCode=" + partner_code + \
                    "&redirectUrl=" + redirect_url + \
                    "&requestId=" + request_id + \
                    "&requestType=" + request_type

    # signature
    h = hmac.new(bytes(secret_key, 'ascii'), bytes(raw_signature, 'ascii'), hashlib.sha256)
    signature = h.hexdigest()

    data = {
        'partnerCode': partner_code,  # Required
        'partnerName': "Metric",
        'requestId': request_id,  # Required
        'amount': amount,  # Required
        'orderId': order_id,  # Required
        'orderInfo': order_info,  # Required
        'redirectUrl': redirect_url,  # Required
        'ipnUrl': ipn_url,  # Required
        'requestType': request_type,  # Required
        'lang': "vi",  # Required
        'extraData': extra_data,  # Required
        'signature': signature  # Required
    }
    data = json.dumps(data)
    clen = len(data)

    endpoint = MomoSettings.MOMO_PAYMENT_ENDPOINT
    response = requests.post(endpoint, data=data,
                             headers={'Content-Type': 'application/json', 'Content-Length': str(clen)})
    logger.info(f"momo response: {response.text} , {response.status_code}")

    return response.json()
