import re


def normalize_phone(phone: str):
    if phone is None or not (type(phone) is str or type(phone) is int):
        return phone
    phone_with_number_only = re.sub('[^0-9]', '', phone)
    if phone_with_number_only[0:3] == '840':
        return phone_with_number_only.replace('840', '0', 1)
    elif phone_with_number_only[0:2] == "84":
        return phone_with_number_only.replace('84', '0', 1)
    else:
        return phone_with_number_only


def normalize_phone_padding_zero(phone: str):
    phone_normalize = normalize_phone(phone)
    if not phone_normalize.startswith('0') and len(phone_normalize) > 5:
        phone_normalize = f'0{phone_normalize}'
    return phone_normalize


def padding_zero(phone: str):
    phone_normalize = phone
    if phone_normalize[0:3] == '840':
        phone_normalize = phone_normalize.replace('840', '0', 1)
    elif phone_normalize[0:2] == "84":
        phone_normalize = phone_normalize.replace('84', '0', 1)

    if not phone_normalize.startswith('0') and len(phone_normalize) > 5:
        phone_normalize = f'0{phone_normalize}'
    return phone_normalize


if __name__ == '__main__':
    # print(normalize_phone(phone='+849 { 77.200. 366 )'))
    # print(normalize_phone(phone='0977495897-091766923'))
    print(normalize_phone_padding_zero(phone='915997567'))
    # print(normalize_phone(phone='+84 98 2531677'))
