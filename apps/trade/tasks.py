import asyncio

import numpy as np
import pandas as pd
from asgiref.sync import async_to_sync
from django.core.cache import cache

from apps.integration.models import BrokerApi
from apps.trade.models import Order
from trading.celery import app
from utils.multi_broker import Broker as MultiBroker


async def get_live_positions(username, broker):
    order = await MultiBroker(username, broker)
    await order.initiate_session()
    df = await order.calculate_live_pnl()
    if not df.empty:
        df["username"] = username
        df['broker_name'] = broker

        return df[["username", "broker_name", "tradingsymbol", "sell_value", "buy_value", "net_qty"]].copy()

    return pd.DataFrame(columns=["username", "broker_name", "tradingsymbol", "sell_value", "buy_value", "net_qty"])


async def get_all_user_kotak_open_positions():
    users = [
        get_live_positions(broker_api.user.username, broker_api.broker)
        async for broker_api in BrokerApi.objects.filter(is_active=True, broker__in=["kotak", "kotak_neo", "dummy"])
    ]
    return pd.concat(await asyncio.gather(*users), ignore_index=True)


async def calculate_live_pnl():
    instruments = cache.get("OPTION_INSTRUMENTS")

    df = cache.get("OPEN_POSITION")
    df = pd.merge(df, instruments, on="tradingsymbol")
    df["pnl"] = df["sell_value"] - df["buy_value"] + (df["net_qty"] * df["last_price"])
    df["ce_buy_qty"] = np.where(df["tradingsymbol"].str.contains("CE") & (df["net_qty"] > 0), df["net_qty"], 0)
    df["pe_buy_qty"] = np.where(df["tradingsymbol"].str.contains("PE") & (df["net_qty"] > 0), df["net_qty"], 0)
    df["ce_sell_qty"] = np.where(df["tradingsymbol"].str.contains("CE") & (df["net_qty"] < 0), df["net_qty"] * -1, 0)
    df["pe_sell_qty"] = np.where(df["tradingsymbol"].str.contains("PE") & (df["net_qty"] < 0), df["net_qty"] * -1, 0)

    return df


@app.task(name="Get Open Position", bind=True)
def get_all_user_open_positions(self):
    data = async_to_sync(get_all_user_kotak_open_positions)()
    cache.set("OPEN_POSITION", data)
    return data.to_dict("records")


@app.task(name="Save Order", bind=True)
def save_order_details(self, user, broker, strategy, order_id, order_timestamp, tradingsymbol, transaction_type, expected_price):
    return Order.objects.create(
        user=user,
        broker=broker,
        order_id=order_id,
        order_timestamp=order_timestamp,
        tradingsymbol=tradingsymbol,
        transaction_type=transaction_type,
        expected_price=expected_price,
    )
