import asyncio

import numpy as np
import pandas as pd
from asgiref.sync import async_to_sync
from django.core.cache import cache

from apps.integration.models import BrokerApi
from apps.trade.models import Order
from trading.celery import app
from utils.multi_broker import Broker as MultiBroker
import datetime as dt
from django.utils import timezone


async def get_live_positions(username, broker):
    order = await MultiBroker(username, broker)
    await order.initiate_session()
    df = await order.calculate_live_pnl()
    if not df.empty:
        df["username"] = username
        df['broker_name'] = broker
        df['margin'] = await order.margin()
        return df[["username", "broker_name", "margin", "tradingsymbol", "sell_value", "buy_value", "net_qty"]].copy()
    else:
        df = pd.DataFrame([{'margin': await order.margin()}], columns=["username", "broker_name", "margin", "tradingsymbol", "sell_value", "buy_value", "net_qty"])
        df.fillna(0, inplace=True)

    return pd.DataFrame(columns=["username", "broker_name", "margin", "tradingsymbol", "sell_value", "buy_value", "net_qty"])


async def get_all_user_kotak_open_positions():
    users = [
        get_live_positions(broker_api.user.username, broker_api.broker)
        async for broker_api in BrokerApi.objects.filter(is_active=True, broker__in=["kotak", "kotak_neo", "dummy"])
    ]
    data = pd.concat(await asyncio.gather(*users), ignore_index=True)
    cache.set("OPEN_POSITION", data)
    return data


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


def update_stop_loss():
    df = cache.get("BNF_SNAPSHOT_5SEC")
    df = df[df['timestamp'].dt.time == dt.time(9, 15, 59)].copy()

    percent_stop_loss_map = {
        0: 0.45,
        1: 0.25,
        2: 0.35,
        3: 0.30,
        4: 0.30,
        5: 0.30,
        6: 0.30
    }

    point_stop_loss_map = {
        0: 175,
        1: 175,
        2: 140,
        3: 210,
        4: 210,
        5: 210,
        6: 210
    }

    current_day = (cache.get("EXPIRY") - timezone.localdate()).days

    if not df.empty:
        ce_df = df[df['instrument_type'] == 'CE']
        pe_df = df[df['instrument_type'] == 'PE']
        
        ce_premium = ce_df[ce_df['delta'] >= 0.45].sort_values('delta').iloc[0].last_price
        pe_premium = pe_df[pe_df['delta'] <= -0.45].sort_values('delta', ascending=False).iloc[0].last_price

        total_premium = ce_premium + pe_premium

        stop_loss = min(round(total_premium * percent_stop_loss_map[current_day]), point_stop_loss_map[current_day])

        cache.set("STRATEGY_STOP_LOSS", stop_loss)

        return stop_loss