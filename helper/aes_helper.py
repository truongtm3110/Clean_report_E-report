from Crypto import Random
from Crypto.Cipher import AES
import base64
from hashlib import md5

BLOCK_SIZE = 16


def pad(data):
    length = BLOCK_SIZE - (len(data) % BLOCK_SIZE)
    return data + (chr(length) * length).encode()


def unpad(data):
    return data[:-(data[-1] if type(data[-1]) == int else ord(data[-1]))]


def bytes_to_key(data, salt, output=48):
    # extended from https://gist.github.com/gsakkis/4546068
    assert len(salt) == 8, len(salt)
    data += salt
    key = md5(data).digest()
    final_key = key
    while len(final_key) < output:
        key = md5(key + data).digest()
        final_key += key
    return final_key[:output]


def encrypt(message, passphrase):
    salt = Random.new().read(8)
    key_iv = bytes_to_key(passphrase, salt, 32 + 16)
    key = key_iv[:32]
    iv = key_iv[32:]
    aes = AES.new(key, AES.MODE_CBC, iv)
    return base64.b64encode(b"Salted__" + salt + aes.encrypt(pad(message)))


def decrypt(encrypted, passphrase):
    encrypted = base64.b64decode(encrypted)
    assert encrypted[0:8] == b"Salted__"
    salt = encrypted[8:16]
    key_iv = bytes_to_key(passphrase, salt, 32 + 16)
    key = key_iv[:32]
    iv = key_iv[32:]
    aes = AES.new(key, AES.MODE_CBC, iv)
    return unpad(aes.decrypt(encrypted[16:]))


if __name__ == '__main__':
    password = "$2a$10$tWOQpnOielCX.gjc9A8LC.tLk.6UAhJhEl2o5n5itDfvNk7OA4dde".encode()
    ct_b64 = "U2FsdGVkX1/HCTiozEEPpe2SnVnDC1jaaodLDOJ3vJ8NpaCb8IMRutBmYQ0BSN84ZntRYodad6NNkQ2fMipsXFwX2JCjbWVtLHOkwQ7mJI79S8UlSS/sUU2DR0m5PaaQ"

    pt = decrypt(ct_b64, password)
    print("pt", pt)

    print("pt", decrypt(encrypt(pt, password), password))
