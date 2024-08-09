import ctypes
import hashlib


def hash_text_to_int(text):
    hash_code = 0
    for char in text:
        hash_code = 31 * hash_code + ord(char)
    return abs(ctypes.c_int32(hash_code).value)


def text_to_hash_md5(text):
    hash_object = hashlib.md5(text.encode('utf-8'))
    return hash_object.hexdigest()


if __name__ == '__main__':
    # print(hash_text_to_int(
    #     '%MjZ4p!zw2EMGX_xxKUL%5ppPFF^UX-C2TwBZtwbaRt$-cQWWH{"posts":[{"id":123,"a":null,"is":false,"array":[1,2,3],"nested":{"field":null,"last":"\nChỉ 💋❤️🥰 có niềm vui, sự hạnh ời của bạn mới là cách “trả thù” tốt nhất.” 💋❤️🥰"}}]}BKEAyA6^7R3Q*N-x!Nxht8PqVRTspYj8KV?U!H$sU7d-8mEK#Y'))
    # print(hash_text_to_int('12'))
    print(hash('12'))
