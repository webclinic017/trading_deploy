import json

from aiohttp import ClientSession
from cryptography.fernet import Fernet

from trading.settings import env


def encrypt_message(message):
    """
    Encrypts a message
    """
    key = env.bytes('ENCRYPT_KEY')
    encoded_message = message.encode()
    f = Fernet(key)
    return f.encrypt(encoded_message)


def decrypt_message(encrypted_message):
    """
    Decrypts an encrypted message
    """
    key = env.bytes('ENCRYPT_KEY')
    f = Fernet(key)
    decrypted_message = f.decrypt(eval(encrypted_message))

    return decrypted_message.decode()
