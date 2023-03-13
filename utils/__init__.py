import json

from aiohttp import ClientSession
from cryptography.fernet import Fernet

from trading.settings import env


def encrypt_message(message):
    """
    Encrypts a message
    """
    key = env.bytes("ENCRYPT_KEY")
    encoded_message = message.encode()
    f = Fernet(key)
    return f.encrypt(encoded_message)


def decrypt_message(encrypted_message):
    """
    Decrypts an encrypted message
    """
    key = env.bytes("ENCRYPT_KEY")
    f = Fernet(key)
    decrypted_message = f.decrypt(eval(encrypted_message))

    return decrypted_message.decode()


def divide_and_list(list_size, x):
    equal = x // list_size
    remaining = x - (equal * list_size)

    lst = [equal for _ in range(list_size)]

    for i in range(list_size):
        if remaining <= 0:
            break

        lst[i] = lst[i] + 1
        remaining = remaining - 1

    return lst


async def send_notifications(title, description, alert_class="alert-info"):
    # await channel_layer.group_send(
    #     "notifications",
    #     {
    #         "type": "send_notifications",
    #         "title": title,
    #         "description": description,
    #         "class": alert_class,
    #     },
    # )
    return
