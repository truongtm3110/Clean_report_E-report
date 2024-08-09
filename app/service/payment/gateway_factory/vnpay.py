import json
import time
from datetime import timedelta, datetime
from hashlib import md5

import requests
from sqlalchemy.ext.asyncio import AsyncSession

from app.constant.constant_global import VNPaySettings, TransactionStatus, VNPayResponse
from app.service.discount.discount_service import insert_discount_usage, update_discount_tracking, \
    update_usage_in_discount_code
from app.service.payment.gateway_factory.gateway_factory import PaymentGateway
from app.service.payment.transaction_service import get_transaction_by_transaction_id
from app.service.subscription.subscription_service import update_user_subscription
from helper.datetime_helper import get_date_time_now_with_timezone
from helper.error_helper import log_error


class VNPayPaymentGateway(PaymentGateway):
    def __init__(self, transaction):
        self.transaction = transaction

    async def create_payment(self, redirect_url: str = None) -> dict:
        qrcode = None
        if self.transaction.transaction_value is None or self.transaction.transaction_value <= 0:
            return None
        try:
            # is_prod = get_value_setting(key='ENVIRONMENT') == 'production'
            is_prod = True
            if is_prod:
                qrcode = await service_vnpayqr_create_qr_prod(amount=self.transaction.transaction_value,
                                                              description=f'{self.transaction.transaction_id}_BEE',
                                                              exp_min=15,
                                                              bill_number=self.transaction.transaction_id)
            else:
                qrcode = await service_vnpayqr_create_qr(amount=self.transaction.transaction_value,
                                                         description=f'{self.transaction.transaction_id}_LINH',
                                                         exp_min=15,
                                                         bill_number=self.transaction.transaction_id)
        except Exception as e:
            log_error(e)

        return {
            'qrcode': qrcode,
            'transaction_id': self.transaction.transaction_id,
            'transaction_value': self.transaction.transaction_value
        }

    async def process_payment_hook(self, received_data: dict, session: AsyncSession) -> bool:
        # get transaction by transaction_id
        transaction = await get_transaction_by_transaction_id(received_data.get('txnId'), session)
        if transaction is None:
            return False

        is_success = received_data.get('code') == VNPayResponse.STATUS_CODE_SUCCESS

        done = False
        if is_success:
            done = await update_user_subscription(transaction, session)
            await insert_discount_usage(transaction.discount_code_id, transaction.id, transaction.user_id, session)
            await update_discount_tracking(transaction.discount_code_id, transaction.transaction_value, session)
            await update_usage_in_discount_code(transaction.discount_code_id, session)
            await session.commit()

        # update transaction status
        transaction.payment_raw_data = received_data
        transaction.completed_at = datetime.fromtimestamp(time.time())
        transaction.status = TransactionStatus.COMPLETED if is_success else TransactionStatus.FAILED
        # try:
        session.add(transaction)
        await session.commit()
        # except Exception as e:
        #     await session.rollback()
        #     log_error(e)
        #     return False

        return done


async def service_vnpayqr_create_qr_prod(amount: int, description: str, bill_number: str,
                                         exp_min: int = 20,
                                         ccy: str = "704",
                                         pay_type: str = "03"):
    time_now = get_date_time_now_with_timezone()
    exp_date = (time_now + timedelta(minutes=exp_min)).strftime("%y%m%d%H%M")
    service_code = "03"
    country_code = "VN"
    master_mer_code = "A000000775"
    txn_id = ""
    product_id = ""
    tip_and_fee = ""
    amount = str(amount)
    checksum_data = (VNPaySettings.VNPAYQR_APP_ID_PROD + "|" + VNPaySettings.VNPAYQR_MERCHANT_NAME_PROD + "|"
                     + service_code + "|" + country_code + "|" + master_mer_code + "|"
                     + VNPaySettings.VNPAYQR_MERCHANT_TYPE_PROD + "|" + VNPaySettings.VNPAYQR_MERCHANT_CODE_PROD + "|"
                     + VNPaySettings.VNPAYQR_TERMINAL_ID_PROD + "|" + pay_type + "|" + product_id + "|" + txn_id + "|"
                     + amount + "|" + tip_and_fee + "|" + ccy + "|" + exp_date + "|"
                     + VNPaySettings.VNPAYQR_CREATE_QR_SECRET_KEY_PROD)
    checksum = md5(checksum_data.encode()).hexdigest()
    data = {
        "appId": "MERCHANT",
        "merchantName": VNPaySettings.VNPAYQR_MERCHANT_NAME_PROD,
        "serviceCode": "03",
        "countryCode": "VN",
        "masterMerCode": master_mer_code,
        "merchantType": VNPaySettings.VNPAYQR_MERCHANT_TYPE_PROD,
        "merchantCode": VNPaySettings.VNPAYQR_MERCHANT_CODE_PROD,
        "payloadFormat": None,
        "terminalId": VNPaySettings.VNPAYQR_TERMINAL_ID_PROD,
        "payType": pay_type,
        "productId": product_id,
        "productName": None,
        "imageName": None,
        "txnId": txn_id,
        "amount": amount,
        "tipAndFee": tip_and_fee,
        "ccy": ccy,
        "expDate": exp_date,
        "desc": description,
        "checksum": checksum.upper(),
        "merchantCity": None,
        "merchantCC": None,
        "fixedFee": None,
        "percentageFee": None,
        "pinCode": None,
        "mobile": None,
        "billNumber": bill_number,
        "creator": None,
        "consumerID": "",
        "purpose": ""
    }
    headers = {
        "Content-type": "text/plain"
    }
    response = requests.post("https://createqr.vnpay.vn", data=json.dumps(data), headers=headers)
    response_data = response.json()
    res_code = response_data.get("code", "")
    if res_code is None:
        res_code = "null"
    res_message = response_data.get("message", "")
    if res_message is None:
        res_message = "null"
    res_qr_data = response_data.get("data", "")
    if res_qr_data is None:
        raise RuntimeError(f"QR data empty: {response_data}")
    res_url = response_data.get("url", "")
    if res_url is None:
        res_url = "null"
    checksum_data = f"{res_code}|{res_message}|{res_qr_data}|{res_url}|{VNPaySettings.VNPAYQR_CREATE_QR_SECRET_KEY_PROD}"
    res_checksum = md5(checksum_data.encode()).hexdigest()
    if res_checksum.upper() == response_data.get("checksum"):
        return res_qr_data
    else:
        raise RuntimeError(f"Checksum response invalid: {response_data}")


async def service_vnpayqr_create_qr(amount: int, description: str, bill_number: str,
                                    exp_min: int = 20,
                                    ccy: str = "704",
                                    pay_type: str = "03"):
    time_now = get_date_time_now_with_timezone()
    exp_date = (time_now + timedelta(minutes=exp_min)).strftime("%y%m%d%H%M")
    service_code = "03"
    country_code = "VN"
    master_mer_code = "A000000775"
    txn_id = ""
    product_id = ""
    tip_and_fee = ""
    amount = str(amount)
    checksum_data = VNPaySettings.VNPaySettings.VNPAYQR_APP_ID + "|" + VNPaySettings.VNPaySettings.VNPAYQR_MERCHANT_NAME + "|" + service_code + "|" + country_code + "|" + master_mer_code \
                    + "|" + VNPaySettings.VNPAYQR_MERCHANT_TYPE + "|" + VNPaySettings.VNPAYQR_MERCHANT_CODE + "|" + VNPaySettings.VNPAYQR_TERMINAL_ID + "|" + pay_type + "|" + \
                    product_id + "|" + txn_id + "|" + amount + "|" + tip_and_fee + "|" + ccy + "|" \
                    + exp_date + "|" + VNPaySettings.VNPAYQR_CREATE_QR_SECRET_KEY
    checksum = md5(checksum_data.encode()).hexdigest()
    data = {
        "appId": "MERCHANT",
        "merchantName": VNPaySettings.VNPAYQR_MERCHANT_NAME,
        "serviceCode": "03",
        "countryCode": "VN",
        "masterMerCode": master_mer_code,
        "merchantType": VNPaySettings.VNPAYQR_MERCHANT_TYPE,
        "merchantCode": VNPaySettings.VNPAYQR_MERCHANT_CODE,
        "payloadFormat": None,
        "terminalId": VNPaySettings.VNPAYQR_TERMINAL_ID,
        "payType": pay_type,
        "productId": product_id,
        "productName": None,
        "imageName": None,
        "txnId": txn_id,
        "amount": amount,
        "tipAndFee": tip_and_fee,
        "ccy": ccy,
        "expDate": exp_date,
        "desc": description,
        "checksum": checksum.upper(),
        "merchantCity": None,
        "merchantCC": None,
        "fixedFee": None,
        "percentageFee": None,
        "pinCode": None,
        "mobile": None,
        "billNumber": bill_number,
        "creator": None,
        "consumerID": "",
        "purpose": ""
    }
    headers = {
        "Content-type": "text/plain"
    }
    response = requests.post("http://14.160.87.123:18080/QRCreateAPIRestV2/rest/CreateQrcodeApi/createQrcode",
                             data=json.dumps(data), headers=headers)
    response_data = response.json()
    res_code = response_data.get("code", "")
    if res_code is None:
        res_code = "null"
    res_message = response_data.get("message", "")
    if res_message is None:
        res_message = "null"
    res_qr_data = response_data.get("data", "")
    if res_qr_data is None:
        raise RuntimeError(f"QR data empty: {response_data}")
    res_url = response_data.get("url", "")
    if res_url is None:
        res_url = "null"
    checksum_data = f"{res_code}|{res_message}|{res_qr_data}|{res_url}|{VNPaySettings.VNPAYQR_CREATE_QR_SECRET_KEY}"
    res_checksum = md5(checksum_data.encode()).hexdigest()
    if res_checksum.upper() == response_data.get("checksum"):
        return res_qr_data
    else:
        raise RuntimeError(f"Checksum response invalid: {response_data}")
