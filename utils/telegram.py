from trading.settings import env
import requests


def send_message(message: str):
    """Send message to telegram channel"""
    TOKEN = env('TELEGRAM_TOKEN')
    chat_id = env("CHAT_ID")
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
    requests.get(url).json()