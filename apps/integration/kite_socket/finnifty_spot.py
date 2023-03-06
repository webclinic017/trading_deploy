import datetime as dt

from asgiref.sync import async_to_sync
from django.core.cache import cache

from apps.integration.models import ZerodhaApi
from trading.settings import env
from utils.broker.kiteext import KiteExt
from utils.telegram import send_message


def on_connect(ws, response):
    ws.subscribe(ws.instrument_tokens)
    ws.set_mode(ws.MODE_FULL, ws.instrument_tokens)


def on_ticks(ws, ticks):
    print(ticks)

    if ticks:
        print(ticks[-1]["last_price"])
        cache.set("FINNIFTY_LTP", ticks[-1]["last_price"])

    if dt.datetime.now().time() > dt.time(15, 30):
        ws.unsubscribe(ws.instrument_tokens)
        ws.close()
        # cache.delete("BANKNIFTY_LTP")


def on_close(ws, code, reason):
    if not code and not reason:
        ws.stop()


def fn_spot_connect_kws():
    send_message(f"{dt.datetime.now().replace(microsecond=0)} FINNFTY SPOT KWS")
    user = env("ZERODHA_WEBSOCKET_USER")
    zerodha = ZerodhaApi.objects.get(broker_api__user__username=user)

    kite = async_to_sync(KiteExt)(user_id=zerodha.userid, token=zerodha.session_token)
    print("CONNECTING")

    kws = kite.kws()
    kws.instrument_tokens = [257801]

    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close

    kws.connect()
