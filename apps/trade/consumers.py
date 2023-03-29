import asyncio
import datetime as dt

import numpy as np
import pandas as pd
from channels.generic.websocket import AsyncJsonWebsocketConsumer
from django.core.cache import cache
from django.utils import timezone

from apps.trade.models import DeployedOptionStrategy, DeployedOptionStrategyUser
from django_pandas.io import read_frame
from apps.trade.tasks import get_all_user_kotak_open_positions
from utils.multi_broker import Broker as MultiBroker


async def adjust_positions(username=None, broker=None):
    await get_all_user_kotak_open_positions()
    df = await quantity_mistmatch()
    instruments = cache.get("OPTION_INSTRUMENTS")

    df["difference_qty"] = df["expected_qty"] - df["net_qty"]

    df = df[df["difference_qty"] != 0].reset_index(drop=True)

    if username:
        df = df[(df["username"] == username) & (df['broker_name'] == broker)]

    buy = df[df["difference_qty"] > 0].copy()
    sell = df[df["difference_qty"] < 0].copy()

    order_objs = []
    for _, row in buy.iterrows():
        order = await MultiBroker(row["username"], row["broker_name"])
        await order.initiate_session()
        data = instruments[instruments["tradingsymbol"] == row["tradingsymbol"]].iloc[0]
        order_objs.append(
            order.place_and_chase_order(
                instrument_name="BANKNIFTY",
                strike=float(data.strike),
                option_type=data.instrument_type,
                transaction_type="BUY",
                quantity=int(abs(row["difference_qty"])),
                expected_price=float(data.last_price),
                initial_slippage=10,
                slippage=10,
            )
        )

    await asyncio.gather(*order_objs)

    order_objs = []
    for _, row in sell.iterrows():
        order = await MultiBroker(row["username"], row["broker_name"])
        await order.initiate_session()
        data = instruments[instruments["tradingsymbol"] == row["tradingsymbol"]].iloc[0]
        order_objs.append(
            order.place_and_chase_order(
                instrument_name="BANKNIFTY",
                strike=float(data.strike),
                option_type=data.instrument_type,
                transaction_type="SELL",
                quantity=int(abs(row["difference_qty"])),
                expected_price=float(data.last_price),
                initial_slippage=10,
                slippage=10,
            )
        )

    await asyncio.gather(*order_objs)
    await get_all_user_kotak_open_positions()

    return df


async def quantity_mistmatch(df=None):
    if df is None:
        df = await calculate_live_pnl()
    user_in_cache = [
        x["user"].username for x in cache.get("deployed_strategies", {}).get("1", {}).get("user_params", [])
    ]
    user_in_cache_quantity = {
        x["user"].username: [x["quantity_multiple"], x["order_obj"]]
        for x in cache.get("deployed_strategies", {}).get("1", {}).get("user_params", [])
    }
    quantity_map = (
        df.groupby(["username", "tradingsymbol"]).agg({"net_qty": "sum", "broker_name": "first"}).reset_index()
    )

    tradingsymbols = cache.get("1_tradingsymbol")

    tradingsymbol_map = []
    broker_map = {}
    for key, row in tradingsymbols.items():
        for user in user_in_cache:
            data = []
            qty = user_in_cache_quantity[user][0][key]
            order_obj = user_in_cache_quantity[user][1]
            broker = order_obj.broker_name
            broker_map[order_obj.broker_name] = broker
            if row["pe_tradingsymbol"]:
                data.append(
                    {
                        "username": user,
                        "broker_name": order_obj.broker_name,
                        "expected_qty": -qty,
                        "tradingsymbol": row["pe_tradingsymbol"],
                    }
                )

            if row["ce_tradingsymbol"]:
                data.append(
                    {
                        "username": user,
                        "broker_name": order_obj.broker_name,
                        "expected_qty": -qty,
                        "tradingsymbol": row["ce_tradingsymbol"],
                    }
                )

            tradingsymbol_map.extend(data)

    expected_qty_df = pd.DataFrame(
        tradingsymbol_map, columns=["username", "broker_name", "expected_qty", "tradingsymbol"]
    )
    expected_qty_df = (
        expected_qty_df.groupby(["username", "broker_name", "tradingsymbol"])
        .agg({"expected_qty": "sum"})
        .reset_index()
    )
    quantity_map_df = pd.merge(
        quantity_map, expected_qty_df, on=["username", "broker_name", "tradingsymbol"], how="outer"
    ).fillna(0)

    return quantity_map_df


async def calculate_live_pnl():
    instruments = cache.get(
        "OPTION_INSTRUMENTS",
        pd.DataFrame(
            columns=[
                "tradingsymbol",
                "instrument_token",
                "last_price",
                "exchange_timestamp",
                "last_trade_time",
                "oi",
            ]
        ),
    )

    df = cache.get(
        "OPEN_POSITION",
        pd.DataFrame(
            columns=[
                "username",
                "broker_name",
                "margin",
                "tradingsymbol",
                "sell_value",
                "buy_value",
                "net_qty",
            ]
        ),
    )

    df = pd.merge(df, instruments, on="tradingsymbol")
    df["pnl"] = df["sell_value"] - df["buy_value"] + (df["net_qty"] * df["last_price"])
    df["ce_buy_qty"] = np.where(df["tradingsymbol"].str.contains("CE") & (df["net_qty"] > 0), df["net_qty"], 0)
    df["pe_buy_qty"] = np.where(df["tradingsymbol"].str.contains("PE") & (df["net_qty"] > 0), df["net_qty"], 0)
    df["ce_sell_qty"] = np.where(df["tradingsymbol"].str.contains("CE") & (df["net_qty"] < 0), df["net_qty"] * -1, 0)
    df["pe_sell_qty"] = np.where(df["tradingsymbol"].str.contains("PE") & (df["net_qty"] < 0), df["net_qty"] * -1, 0)

    return df


async def get_dummy_points():
    df = await calculate_live_pnl()
    df = df[
        ["username", "broker_name", "tradingsymbol", "sell_value", "buy_value", "net_qty", "pnl", "last_price"]
    ].copy()
    df_square_of_positions = df[df["net_qty"] == 0].sort_values(["username", "net_qty", "tradingsymbol"])
    df_open_positions = df[df["net_qty"] != 0].sort_values(["username", "net_qty", "tradingsymbol"])
    df = pd.concat([df_open_positions, df_square_of_positions], ignore_index=True)

    return round(df[df['username'] == 'dummy'].pnl.sum() / 75, 2)


class ChatConsumer(AsyncJsonWebsocketConsumer):
    async def connect(self):
        await self.accept()
        await self.initial_pcr_data()

    async def disconnect(self, close_code):
        pass

    async def initial_pcr_data(self):
        df = cache.get("LIVE_BNF_PCR")

        df["ce_oi_change"] = df["ce_total_oi"].pct_change(periods=72).fillna(0.0).replace(np.inf, -1.0)
        df["pe_oi_change"] = df["pe_total_oi"].pct_change(periods=72).fillna(0.0).replace(np.inf, -1.0)
        df["ce_oi_change_3min"] = df["ce_total_oi"].pct_change(periods=36).fillna(0.0).replace(np.inf, -1.0)
        df["pe_oi_change_3min"] = df["pe_total_oi"].pct_change(periods=36).fillna(0.0).replace(np.inf, -1.0)

        data = {
            "data": [
                {
                    "timestamp": row.timestamp.isoformat(),
                    "pe_total_oi": row.pe_total_oi,
                    "ce_total_oi": row.ce_total_oi,
                    "pcr": row.pe_total_oi / row.ce_total_oi,
                    "ce_oi_change": row.ce_oi_change,
                    "pe_oi_change": row.pe_oi_change,
                    "ce_oi_change_3min": row.ce_oi_change_3min,
                    "pe_oi_change_3min": row.pe_oi_change_3min,
                    "ce_pe_oi_change": row.ce_oi_change - row.pe_oi_change,
                    "pe_ce_oi_change": row.pe_oi_change - row.ce_oi_change,
                    "ce_pe_oi_change_3min": row.ce_oi_change_3min - row.pe_oi_change_3min,
                    "pe_ce_oi_change_3min": row.pe_oi_change_3min - row.ce_oi_change_3min,
                    "strike": row.strike,
                    "ce_iv": row.ce_iv,
                    "pe_iv": row.pe_iv,
                    "total_iv": row.ce_iv + row.pe_iv,
                    "ce_premium": row.ce_premium,
                    "pe_premium": row.pe_premium,
                    "total_premium": row.ce_premium + row.pe_premium,
                }
                for _, row in df.iterrows()
            ][::-1],
        }
        await self.send_json(data)

        ct = timezone.localtime()
        second = (ct.second // 5) * 5
        loop_time = ct.replace(second=second) + dt.timedelta(seconds=5)

        await asyncio.sleep((loop_time - ct).total_seconds())

        while True:
            if timezone.localtime().time() > dt.time(15, 30):
                break

            df = cache.get("LIVE_BNF_PCR")

            df["ce_oi_change"] = df["ce_total_oi"].pct_change(periods=72).fillna(0.0).replace(np.inf, -1.0)
            df["pe_oi_change"] = df["pe_total_oi"].pct_change(periods=72).fillna(0.0).replace(np.inf, -1.0)
            df["ce_oi_change_3min"] = df["ce_total_oi"].pct_change(periods=24).fillna(0.0).replace(np.inf, -1.0)
            df["pe_oi_change_3min"] = df["pe_total_oi"].pct_change(periods=24).fillna(0.0).replace(np.inf, -1.0)
            row = df.iloc[-1]

            data = {
                "data": [
                    {
                        "timestamp": row.timestamp.isoformat(),
                        "pe_total_oi": row.pe_total_oi,
                        "ce_total_oi": row.ce_total_oi,
                        "pcr": row.pe_total_oi / row.ce_total_oi,
                        "ce_oi_change": row.ce_oi_change,
                        "pe_oi_change": row.pe_oi_change,
                        "ce_oi_change_3min": row.ce_oi_change_3min,
                        "pe_oi_change_3min": row.pe_oi_change_3min,
                        "ce_pe_oi_change": row.ce_oi_change - row.pe_oi_change,
                        "pe_ce_oi_change": row.pe_oi_change - row.ce_oi_change,
                        "pe_ce_oi_change_3min": row.pe_oi_change_3min - row.ce_oi_change_3min,
                        "strike": row.strike,
                        "ce_iv": row.ce_iv,
                        "pe_iv": row.pe_iv,
                        "total_iv": row.ce_iv + row.pe_iv,
                        "ce_premium": row.ce_premium,
                        "pe_premium": row.pe_premium,
                        "total_premium": row.ce_premium + row.pe_premium,
                    }
                ]
            }

            await self.send_json(data)

            ct = timezone.localtime()
            second = (ct.second // 5) * 5
            loop_time = ct.replace(second=second) + dt.timedelta(seconds=5)

            await asyncio.sleep((loop_time - ct).total_seconds())


class AlgoStatusConsumer(AsyncJsonWebsocketConsumer):
    async def connect(self):
        await self.accept()
        if self.scope["user"].is_anonymous:
            await self.close(code=401)
            return
        self.pk = self.scope["url_route"]["kwargs"]["pk"]

        deployed_option_strategy = await self.get_deployed_option_strategy()

        if not deployed_option_strategy:
            await self.close(code=1011)
            return

        await self.channel_layer.group_add(
            self.pk,
            self.channel_name,
        )

        while True:
            await self.send_algo_status()
            ct = timezone.localtime()
            second = (ct.second // 1) * 1
            loop_time = ct.replace(second=second, microsecond=0) + dt.timedelta(seconds=1)
            await asyncio.sleep((loop_time - ct).total_seconds())

    async def get_deployed_option_strategy(self):
        return await DeployedOptionStrategy.objects.filter(pk=self.pk).afirst()

    async def send_algo_status(self):
        if str(self.pk) in cache.get("deployed_strategies", {}).keys():
            await self.send_json(True)
        else:
            await self.send_json(False)


class DeployedOptionStrategySymbolConsumer(AsyncJsonWebsocketConsumer):
    async def connect(self):
        await self.accept()
        if self.scope["user"].is_anonymous:
            await self.close(code=401)
            return
        self.pk = self.scope["url_route"]["kwargs"]["pk"]

        deployed_option_strategy = await self.get_deployed_option_strategy()

        if not deployed_option_strategy:
            await self.close(code=1011)
            return

        await self.channel_layer.group_add(
            self.pk,
            self.channel_name,
        )

        if deployed_option_strategy.strategy.strategy_type == "ce_pe_with_sl":
            parameters = sorted(
                [
                    {
                        "name": row.name,
                        "entry_time": row.parameters["entry_time"],
                        "exit_time": row.parameters["exit_time"],
                        "trail": row.parameters["trail"],
                        "sl_pct": row.parameters["sl_pct"],
                    }
                    for row in deployed_option_strategy.parameters.all()
                ],
                key=lambda x: int(x["name"]),
            )
            while True:
                await self.send_strategy_data(parameters)
                ct = timezone.localtime()
                second = (ct.second // 1) * 1
                loop_time = ct.replace(second=second, microsecond=0) + dt.timedelta(seconds=1)
                await asyncio.sleep((loop_time - ct).total_seconds())
        else:
            while True:
                await self.send_open_position_data()
                ct = timezone.localtime()
                second = (ct.second // 1) * 1
                loop_time = ct.replace(second=second, microsecond=0) + dt.timedelta(seconds=1)
                await asyncio.sleep((loop_time - ct).total_seconds())

    async def get_deployed_option_strategy(self):
        return await DeployedOptionStrategy.objects.filter(pk=self.pk).afirst()

    async def send_strategy_data(self, parameters, *args, **kwargs):
        tradingsymbol = cache.get(f"{self.pk}_tradingsymbol", {})
        insturments = cache.get("OPTION_GREEKS_INSTRUMENTS")
        for row in parameters:
            symbol = tradingsymbol.get(row["name"], {})
            if symbol:
                row["entered"] = symbol["entered"]
                row["exited"] = symbol["exited"]
                row["ce_exited"] = symbol["ce_exited"]
                row["pe_exited"] = symbol["pe_exited"]
                row["pe_entry_price"] = symbol["pe_entry_price"]
                row["ce_entry_price"] = symbol["ce_entry_price"]
                row["ce_sl"] = symbol["ce_sl"]
                row["pe_sl"] = symbol["pe_sl"]
                row["pts"] = 0

                row["modified_sl_to_cost"] = symbol.get("modified_sl_to_cost", False)

                if symbol["ce_tradingsymbol"]:
                    ce = insturments[insturments["tradingsymbol"] == symbol["ce_tradingsymbol"]].iloc[0]
                    if symbol.get("ce_exit_price"):
                        row["ce_exit_price"] = symbol["ce_exit_price"]
                        row["pts"] += float(row["ce_entry_price"]) - float(row["ce_exit_price"])
                    else:
                        row["pts"] += float(row["ce_entry_price"]) - float(ce["last_price"])
                    row["ce_strike"] = ce.strike

                if symbol["pe_tradingsymbol"]:
                    pe = insturments[insturments["tradingsymbol"] == symbol["pe_tradingsymbol"]].iloc[0]
                    if symbol.get("pe_exit_price"):
                        row["pe_exit_price"] = symbol["pe_exit_price"]
                        row["pts"] += float(row["pe_entry_price"]) - float(row["pe_exit_price"])
                    else:
                        row["pts"] += float(row["pe_entry_price"]) - float(pe["last_price"])
                    row["pe_strike"] = pe.strike

                row["pts"] = round(row["pts"], 2)

        await self.send_json(parameters)

    async def send_open_position_data(self, *args, **kwargs):
        position_data = []
        if cache.get("deployed_strategies", {}).get(str(self.pk)):
            trading_symbols = cache.get(f"{self.pk}_tradingsymbol", dict())
            insturments = cache.get("OPTION_GREEKS_INSTRUMENTS")
            for idx in sorted(trading_symbols.keys()):
                row = trading_symbols[idx]
                ce_strike = pe_strike = None
                ce_delta = pe_delta = 0
                ce_price = pe_price = 0
                if row["ce_tradingsymbol"]:
                    ce = insturments[insturments["tradingsymbol"] == row["ce_tradingsymbol"]].iloc[0]
                    ce_strike = ce.strike
                    ce_delta = ce["delta"]
                    ce_price = ce["last_price"]

                if row["pe_tradingsymbol"]:
                    pe = insturments[insturments["tradingsymbol"] == row["pe_tradingsymbol"]].iloc[0]
                    pe_strike = pe.strike
                    pe_delta = pe["delta"]
                    pe_price = pe["last_price"]

                position_data.append(
                    {
                        "idx": idx,
                        "one_side_exit_hold": cache.get(f"{self.pk}_{idx}_one_side_exit_hold", 0),
                        "ce_strike": ce_strike,
                        "ce_delta": ce_delta,
                        "ce_price": ce_price,
                        "pe_strike": pe_strike,
                        "pe_delta": pe_delta,
                        "pe_price": pe_price,
                        "exited_one_side": row["exited_one_side"],
                        "ce_exit_one_side": row["ce_exit_one_side"],
                        "pe_exit_one_side": row["pe_exit_one_side"],
                    }
                )

        await self.send_json(position_data)

    async def get_open_position(self):
        pass


class NotificationConsumner(AsyncJsonWebsocketConsumer):
    async def connect(self):
        await self.accept()

        if self.scope["user"].is_anonymous:
            await self.close(code=401)
            return

        await self.channel_layer.group_add(
            "notifications",
            self.channel_name,
        )

    async def send_notifications(self, event):
        data = event.copy()
        del data["type"]
        await self.send_json(data)


class LivekPositionConsumer(AsyncJsonWebsocketConsumer):
    async def connect(self):
        await self.accept()
        if self.scope["user"].is_anonymous:
            await self.close(code=401)
            return
        await self.return_live_pnl()

    async def return_live_pnl(self):
        while True:
            df = await calculate_live_pnl()
            df = df[
                ["username", "broker_name", "tradingsymbol", "sell_value", "buy_value", "net_qty", "pnl", "last_price"]
            ].copy()
            df_square_of_positions = df[df["net_qty"] == 0].sort_values(["username", "net_qty", "tradingsymbol"])
            df_open_positions = df[df["net_qty"] != 0].sort_values(["username", "net_qty", "tradingsymbol"])
            df = pd.concat([df_open_positions, df_square_of_positions], ignore_index=True)
            # df.sort_values(['username'], inplace=True)
            await self.send_json(df.to_dict("records"))
            ct = timezone.localtime()
            second = (ct.second // 1) * 1
            loop_time = ct.replace(second=second, microsecond=0) + dt.timedelta(seconds=1)
            await asyncio.sleep((loop_time - ct).total_seconds())


class LivePnlConsumer(AsyncJsonWebsocketConsumer):
    async def connect(self):
        await self.accept()
        if self.scope["user"].is_anonymous:
            await self.close(code=401)
            return
        self.quantity_df = pd.DataFrame(
            [
                {"username": row.user.username, "quantity": (row.parent.lot_size * row.lots)}
                async for row in DeployedOptionStrategyUser.objects.filter(parent__is_active=True, parent__id=1)
            ]
        )
        await self.get_jegan_qty()
        await self.return_live_pnl()

    async def get_jegan_qty(self):
        data = (await DeployedOptionStrategy.objects.filter(pk=2).afirst()).users.all()
        self.jegan_map = {}
        for row in data:
            self.jegan_map[row.user.username] = row.lots

    async def get_jegan_pts(self, parameters):
        tradingsymbol = cache.get(f"2_tradingsymbol", {})
        insturments = cache.get("OPTION_GREEKS_INSTRUMENTS")
        pts = 0
        for row in parameters:
            symbol = tradingsymbol.get(row["name"], {})
            if symbol:
                row["entered"] = symbol["entered"]
                row["exited"] = symbol["exited"]
                row["ce_exited"] = symbol["ce_exited"]
                row["pe_exited"] = symbol["pe_exited"]
                row["pe_entry_price"] = symbol["pe_entry_price"]
                row["ce_entry_price"] = symbol["ce_entry_price"]
                row["ce_sl"] = symbol["ce_sl"]
                row["pe_sl"] = symbol["pe_sl"]
                row["pts"] = 0

                row["modified_sl_to_cost"] = symbol.get("modified_sl_to_cost", False)

                if symbol["ce_tradingsymbol"]:
                    ce = insturments[insturments["tradingsymbol"] == symbol["ce_tradingsymbol"]].iloc[0]
                    if symbol.get("ce_exit_price"):
                        row["ce_exit_price"] = symbol["ce_exit_price"]
                        row["pts"] += float(row["ce_entry_price"]) - float(row["ce_exit_price"])
                    else:
                        row["pts"] += float(row["ce_entry_price"]) - float(ce["last_price"])
                    row["ce_strike"] = ce.strike

                if symbol["pe_tradingsymbol"]:
                    pe = insturments[insturments["tradingsymbol"] == symbol["pe_tradingsymbol"]].iloc[0]
                    if symbol.get("pe_exit_price"):
                        row["pe_exit_price"] = symbol["pe_exit_price"]
                        row["pts"] += float(row["pe_entry_price"]) - float(row["pe_exit_price"])
                    else:
                        row["pts"] += float(row["pe_entry_price"]) - float(pe["last_price"])
                    row["pe_strike"] = pe.strike

                row["pts"] = round(row["pts"], 2)
                pts += row["pts"]

        return pts

    async def disconnect(self, close_code):
        pass

    async def return_live_pnl(self):
        parameters = sorted(
            [
                {
                    "name": row.name,
                    "entry_time": row.parameters["entry_time"],
                    "exit_time": row.parameters["exit_time"],
                    "trail": row.parameters["trail"],
                    "sl_pct": row.parameters["sl_pct"],
                }
                for row in (await DeployedOptionStrategy.objects.filter(pk=2).afirst()).parameters.all()
            ],
            key=lambda x: int(x["name"]),
        )
        while True:
            pts = await self.get_jegan_pts(parameters)
            df = await calculate_live_pnl()
            broker_id_map = {"dummy": 0, "kotak_neo": 1, "kotak": 1}
            df["broker_id"] = df["broker_name"].map(lambda x: broker_id_map.get(x, 1))
            df = (
                df.groupby(["broker_id", "username"])
                .agg(
                    {
                        "broker_name": "first",
                        "pnl": "sum",
                        "ce_buy_qty": "sum",
                        "ce_sell_qty": "sum",
                        "pe_buy_qty": "sum",
                        "pe_sell_qty": "sum",
                    }
                )
                .reset_index()
            )
            df = pd.merge(df, self.quantity_df, on="username")
            df["jegan_pnl"] = df["username"].apply(lambda x: self.jegan_map.get(x, 0) * pts * 25)
            df["jegan_pts"] = df["jegan_pnl"] / df["quantity"]
            df["pnl_points"] = (df["pnl"] / df["quantity"]) - df["jegan_pts"]
            df = df.reset_index()
            df["index"] = df["index"] + 1
            await self.send_json(df.to_dict("records"))
            ct = timezone.localtime()
            second = (ct.second // 1) * 1
            loop_time = ct.replace(second=second, microsecond=0) + dt.timedelta(seconds=1)
            await asyncio.sleep((loop_time - ct).total_seconds())


class StopLossDifference(AsyncJsonWebsocketConsumer):
    async def connect(self):
        await self.accept()
        if self.scope["user"].is_anonymous:
            await self.close(code=401)
            return
        self.pk = self.scope["url_route"]["kwargs"]["pk"]

        if not self.pk != 1:
            await self.close(code=401)
            return
        
        while True:
            dummy_pts = await get_dummy_points()
            stop_loss = cache.get("STRATEGY_STOP_LOSS")
            await self.send_json({'stop_loss_difference': round(dummy_pts + stop_loss)})
            await asyncio.sleep(1)


class LivePnlConsumerStrategy(AsyncJsonWebsocketConsumer):
    async def connect(self):
        await self.accept()
        if self.scope["user"].is_anonymous:
            await self.close(code=401)
            return
        self.pk = self.scope["url_route"]["kwargs"]["pk"]
        self.quantity_df = pd.DataFrame(
            [
                {
                    "username": row.user.username,
                    "quantity": (row.parent.lot_size * row.lots),
                    "broker_name": row.broker,
                }
                async for row in DeployedOptionStrategyUser.objects.filter(parent__id=self.pk)
            ]
        )
        await self.get_jegan_qty()
        await self.return_live_pnl()

    async def get_jegan_qty(self):
        data = (await DeployedOptionStrategy.objects.filter(pk=2).afirst()).users.all()
        self.jegan_map = {}
        for row in data:
            self.jegan_map[row.user.username] = row.lots

    async def get_jegan_pts(self, parameters):
        tradingsymbol = cache.get(f"2_tradingsymbol", {})
        insturments = cache.get("OPTION_GREEKS_INSTRUMENTS")
        pts = 0
        for row in parameters:
            symbol = tradingsymbol.get(row["name"], {})
            if symbol:
                row["entered"] = symbol["entered"]
                row["exited"] = symbol["exited"]
                row["ce_exited"] = symbol["ce_exited"]
                row["pe_exited"] = symbol["pe_exited"]
                row["pe_entry_price"] = symbol["pe_entry_price"]
                row["ce_entry_price"] = symbol["ce_entry_price"]
                row["ce_sl"] = symbol["ce_sl"]
                row["pe_sl"] = symbol["pe_sl"]
                row["pts"] = 0

                row["modified_sl_to_cost"] = symbol.get("modified_sl_to_cost", False)

                if symbol["ce_tradingsymbol"]:
                    ce = insturments[insturments["tradingsymbol"] == symbol["ce_tradingsymbol"]].iloc[0]
                    if symbol.get("ce_exit_price"):
                        row["ce_exit_price"] = symbol["ce_exit_price"]
                        row["pts"] += float(row["ce_entry_price"]) - float(row["ce_exit_price"])
                    else:
                        row["pts"] += float(row["ce_entry_price"]) - float(ce["last_price"])
                    row["ce_strike"] = ce.strike

                if symbol["pe_tradingsymbol"]:
                    pe = insturments[insturments["tradingsymbol"] == symbol["pe_tradingsymbol"]].iloc[0]
                    if symbol.get("pe_exit_price"):
                        row["pe_exit_price"] = symbol["pe_exit_price"]
                        row["pts"] += float(row["pe_entry_price"]) - float(row["pe_exit_price"])
                    else:
                        row["pts"] += float(row["pe_entry_price"]) - float(pe["last_price"])
                    row["pe_strike"] = pe.strike

                row["pts"] = round(row["pts"], 2)
                pts += row["pts"]

        return pts

    async def disconnect(self, close_code):
        pass

    async def return_live_pnl(self):
        parameters = sorted(
            [
                {
                    "name": row.name,
                    "entry_time": row.parameters["entry_time"],
                    "exit_time": row.parameters["exit_time"],
                    "trail": row.parameters["trail"],
                    "sl_pct": row.parameters["sl_pct"],
                }
                for row in (await DeployedOptionStrategy.objects.filter(pk=2).afirst()).parameters.all()
            ],
            key=lambda x: int(x["name"]),
        )
        while True:
            pts = await self.get_jegan_pts(parameters)
            df = await calculate_live_pnl()
            quantity_map_df = await quantity_mistmatch(df)
            quantity_map_df["mismatch"] = np.where(quantity_map_df["expected_qty"] != quantity_map_df["net_qty"], 1, 0)
            quantity_mismatch_df = quantity_map_df.groupby("username").agg({"mismatch": "max"})
            df = (
                df.groupby(["username"])
                .agg(
                    {
                        "broker_name": "first",
                        "pnl": "sum",
                        "ce_buy_qty": "sum",
                        "ce_sell_qty": "sum",
                        "pe_buy_qty": "sum",
                        "pe_sell_qty": "sum",
                        "margin": "first",
                    }
                )
                .reset_index()
            )
            df = pd.merge(self.quantity_df, df, on=["username", "broker_name"], how="left")
            df.fillna(0, inplace=True)
            df["jegan_pnl"] = df["username"].apply(lambda x: self.jegan_map.get(x, 0) * pts * 25)
            df["jegan_pts"] = df["jegan_pnl"] / df["quantity"]
            df["pnl_points"] = (df["pnl"] / df["quantity"]) - df["jegan_pts"]
            df = df.reset_index()
            df.fillna(0, inplace=True)
            df["index"] = df["index"] + 1
            user_in_cache = [
                x["user"].username for x in cache.get("deployed_strategies", {}).get("1", {}).get("user_params", [])
            ]
            df["in_cache"] = df["username"].apply(lambda x: True if x in user_in_cache else False)
            df = pd.merge(df, quantity_mismatch_df, on="username", how="left").fillna(0)

            await self.send_json(df.to_dict("records"))
            ct = timezone.localtime()
            second = (ct.second // 1) * 1
            loop_time = ct.replace(second=second, microsecond=0) + dt.timedelta(seconds=1)
            await asyncio.sleep((loop_time - ct).total_seconds())
