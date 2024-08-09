from helper.text_hash_helper import hash_text_to_int

signFirst = '%MjZ4p!zw2EMGX_xxKUL%5ppPFF^UX-C2TwBZtwbaRt$-cQWWH'
signLast = 'BKEAyA6^7R3Q*N-x!Nxht8PqVRTspYj8KV?U!H$sU7d-8mEK#Y'


# def calculate_sign(data_dict):
#     str = signFirst
#     for key in data_dict:
#         str = str + key + "=" + json.dumps(data_dict[key], ensure_ascii=False, separators=(',', ':'))
#     print(str)
#     sig = hash_text_to_int(str + signLast)
#     return sig

def calculate_sign(data_text):
    return hash_text_to_int(str(signFirst + data_text + signLast))


if __name__ == '__main__':
    print(calculate_sign({'name': 'Tuấn', '123mua': True}))
