import asyncio
import traceback

import pandas as pd
from django.core.cache import cache
from django.db.utils import OperationalError
from django.utils import timezone

from apps.integration.models import BrokerApi, KotakNeoApi, KotakSecuritiesApi
from utils.async_obj import AsyncObj
from utils.broker.dummy import DummyApi as DApi
from utils.broker.kotak_neo import KotakNeoApi as KNApi
from utils.broker.kotak_securities import KotakSecuritiesApi as KSApi
from utils.broker.kotak_securities import KotakSecuritiesApiError as KSError


class Broker(AsyncObj):
    DUMMY = "dummy"
    KOTAK_NEO = "kotak_neo"
    KOTAK = "kotak"
    ZERODAHA = "zerodha"
    DUCKTRADE = "ducktrade"

    KOTAK_NEO_TRANSACTION_TYPE_MAP = {"BUY": "B", "SELL": "S"}
    KOTAK_NEO_PRODUCT_CODE_MAP = {"NORMAL": "NRML"}

    KOTAK_TRANSACTION_TYPE_MAP = {"BUY": "BUY", "SELL": "SELL"}
    KOTAK_PRODUCT_CODE_MAP = {"NORMAL": "NRML"}

    async def __ainit__(self, username, broker_name):
        # sourcery skip: raise-specific-error
        self.username = username
        self.broker_name = broker_name

        self.broker = BrokerApi.objects.filter(user__username=username, is_active=True).first()

        if not self.broker:
            raise Exception("Broker not found")

        match self.broker_name:
            case self.KOTAK_NEO:
                self.broker: KotakNeoApi = self.broker.kotak_neo_api
            case self.KOTAK:
                self.broker: KotakSecuritiesApi = self.broker.kotak_api
            case self.DUMMY:
                pass
            case _:
                raise Exception("Broker not found")

    async def initiate_session(self):  # sourcery skip: raise-specific-error
        if self.broker_name != self.DUMMY:
            while True:
                try:
                    self.broker.refresh_from_db()
                    break
                except OperationalError as oe:
                    print(oe)
                    await asyncio.sleep(1)

        match self.broker_name:
            case self.KOTAK_NEO:
                self.api = await KNApi(
                    neo_fin_key=self.broker.decrypt_neo_fin_key(),
                    consumer_key=self.broker.decrypt_consumer_key(),
                    consumer_secret=self.broker.decrypt_consumer_secret(),
                    access_token=self.broker.access_token,
                    sid=self.broker.sid,
                    auth=self.broker.auth,
                    hs_server_id=self.broker.hs_server_id,
                )
            case self.KOTAK:
                self.api = await KSApi(
                    userid=self.broker.userid,
                    consumer_key=self.broker.decrypt_consumer_key(),
                    access_token=self.broker.access_token,
                    consumer_secret=self.broker.decrypt_consumer_secret(),
                    session_token=self.broker.session_token,
                )
            case self.DUMMY:
                self.api: DApi = await DApi(user=self.broker.user)
            case _:
                raise Exception("Broker not found")

    async def get_instrument_from_kite_token(self, kite_instrument_token):
        df = cache.get("OPTION_INSTRUMENTS")
        return df[(df["kite_instrument_token"] == kite_instrument_token)].iloc[0]

    async def get_instrument_from_strike_and_option_type(self, instrument, strike, option_type):
        df = cache.get("OPTION_INSTRUMENTS")
        return df[(df["strike"] == strike) & (df["instrument_type"] == option_type)].iloc[0]

    async def get_ltp(self, kite_instrument_token):
        df = cache.get("OPTION_INSTRUMENTS")
        return df[df["kite_instrument_token"] == kite_instrument_token].iloc[0].last_price

    async def get_order_report(self, order_id):
        return await self.api.single_order_report(order_id, True)

    async def place_order(
        self,
        kite_instrument_token,
        transaction_type,
        quantity,
        expected_price=0,
        order_type="NORMAL",
        slippage=0,
    ):
        row = await self.get_instrument_from_kite_token(kite_instrument_token)
        price = expected_price
        if expected_price and slippage:
            if transaction_type == "BUY":
                expected_price = expected_price + slippage
            else:
                expected_price = expected_price - slippage

        match self.broker_name:
            case self.KOTAK_NEO:
                return await self.api.place_order(
                    tradingsymbol=row.tradingsymbol,  # ts
                    quantity=str(quantity),  # qt
                    transaction_type=self.KOTAK_NEO_TRANSACTION_TYPE_MAP[transaction_type],  # tt
                    price=str(expected_price),  # pr
                    order_type="L" if expected_price else "M",  # pt
                    exchange_segment="nse_fo",  # es
                    product_code=self.KOTAK_NEO_PRODUCT_CODE_MAP[order_type],  # pc
                )
            case self.KOTAK:
                try:
                    return await self.api.place_order(
                        instrument_token=int(row.kotak_sec_instrument_token),
                        exchange="NSE",
                        transaction_type=self.KOTAK_TRANSACTION_TYPE_MAP[transaction_type],
                        quantity=int(quantity),
                        price=float(expected_price),
                    )
                except KSError as ke:
                    if ke.code == 40000:
                        return await self.place_order(
                            kite_instrument_token,
                            transaction_type,
                            quantity,
                            expected_price,
                            order_type,
                            slippage,
                        )
                    print(ke)
            case self.DUMMY:
                return await self.api.place_order(
                    row.tradingsymbol,
                    "NFO",
                    transaction_type,
                    quantity,
                    price=price,
                    trigger_price=0,
                )

    async def modify_order(
        self,
        kite_instrument_token,
        order_id,
        quantity,
        transaction_type,
        expected_price=0,
        order_type="NORMAL",
        slippage=0,
    ):
        row = await self.get_instrument_from_kite_token(kite_instrument_token)

        if expected_price and slippage:
            if order_type == "BUY":
                expected_price = expected_price + slippage
            else:
                expected_price = expected_price - slippage

        match self.broker_name:
            case self.KOTAK_NEO:
                return await self.api.modify_order(
                    order_id=order_id,  # no
                    token=row.kotak_neo_instrument_token,  # tk
                    tradingsymbol=row.tradingsymbol,  # ts
                    quantity=str(quantity),  # qt
                    transaction_type=self.KOTAK_NEO_TRANSACTION_TYPE_MAP[transaction_type],  # tt
                    price=str(expected_price),  # pr
                    order_type="L" if expected_price else "M",  # pt
                    exchange_segment="nse_fo",  # es
                    product_code=self.KOTAK_NEO_PRODUCT_CODE_MAP[order_type],  # pc
                )
            case self.KOTAK:
                try:
                    return await self.api.modify_order(
                        order_id=order_id,
                        quantity=int(quantity),
                        price=float(expected_price),
                    )
                except KSError as ke:
                    if ke.code == 40000:
                        await asyncio.sleep(1)
                        return await self.modify_order(
                            kite_instrument_token,
                            order_id,
                            quantity,
                            transaction_type,
                            expected_price,
                            order_type,
                            slippage,
                        )
                    print(ke)

    async def place_and_chase_order(
        self,
        instrument_name,
        strike: float,
        option_type: str,
        transaction_type: str,
        quantity: int,
        expected_price: float = 0.0,
        max_price: float = 0.0,
        initial_slippage: float = 10.0,
        slippage: float = 0.0,
        sleep_time: float = 2.5,
        order_in_limit: bool = True,
        tick_size: float = 0.05,
    ):
        ct = timezone.localtime()
        # print({
        #     "order_number": None,
        #     "order_status": expected_price,
        #     "order_entry_time": ct,
        #     "error_message": None,
        # })
        # return {
        #     "order_number": None,
        #     "order_status": expected_price,
        #     "order_entry_time": ct,
        #     "error_message": None,
        # }
        instrument = await self.get_instrument_from_strike_and_option_type(instrument_name, strike, option_type)
        kite_instrument_token = instrument.kite_instrument_token

        if order_in_limit and not expected_price:
            expected_price = await self.get_ltp(kite_instrument_token)

        while True:
            order = await self.place_order(
                kite_instrument_token, transaction_type, quantity, expected_price, slippage=initial_slippage
            )
            try:
                order_id = order["order_id"]
                error_message = order.get("error_message")
                break
            except Exception as e:
                print(traceback.print_exc())
                print("Place Order Exception", self.username, order)
                print(e)

        order_report = await self.api.single_order_report(order_id, True, error_message)

        if order_report["status"] == "REJECTED":
            print(self.username, "ORDER REJECTED")
            now_time = timezone.localtime().replace(microsecond=0)
            print(f"{int(strike)}{option_type} {transaction_type} {order_id} {now_time}")
            print()
        elif order_report["status"] not in ["COMPLETED", "CANCELLED"] and expected_price:
            while True:
                print(order_report["status"])
                await asyncio.sleep(sleep_time)
                now_time = timezone.localtime().replace(microsecond=0)
                order_report = await self.get_order_report(order_id)

                if order_report["status"] == "REJECTED":
                    print(self.username, "ORDER REJECTED")
                    now_time = timezone.localtime().replace(microsecond=0)
                    print(f"{int(strike)}{option_type} {transaction_type} {order_id} {now_time}")
                    print()
                elif order_report["status"] not in ["COMPLETED", "CANCELLED"]:
                    print(
                        f"{self.username} MODIFY ORDER: {int(strike)}{option_type} \
                            {transaction_type} {order_id} {now_time}"
                    )
                    modify_quantity = order_report["pending_qty"]
                    modify_price = await self.get_ltp(kite_instrument_token)
                    modify_price = (
                        max(modify_price - slippage, 0.5) if transaction_type == "SELL" else modify_price + slippage
                    )

                    if max_price:
                        modify_price = (
                            max(modify_price, max_price)
                            if transaction_type == "SELL"
                            else min(modify_price, max_price)
                        )

                    await self.modify_order(
                        kite_instrument_token=kite_instrument_token,
                        order_id=order_id,
                        quantity=modify_quantity,
                        transaction_type=transaction_type,
                        expected_price=modify_price,
                    )

                    await asyncio.sleep(0.25)

                    order_report = await self.api.single_order_report(order_id, True)
                    if order_report["status"] in ["COMPLETED", "CANCELLED"]:
                        break
                else:
                    break

        return {
            "order_number": order_id,
            "order_status": order_report["status"],
            "order_entry_time": ct,
            "error_message": error_message,
        }

    async def calculate_live_pnl(self):
        print(self.broker_name)
        match self.broker_name:
            case self.KOTAK_NEO:
                df = await self.api.positions()
            case self.KOTAK:
                df = await self.api.positions("TODAYS")
                instruments = cache.get("OPTION_INSTRUMENTS")
                instruments = instruments[["kotak_sec_instrument_token", "tradingsymbol"]].copy()
                df = pd.merge(
                    df,
                    instruments,
                    left_on="kotak_sec_instrument_token",
                    right_on="kotak_sec_instrument_token",
                )
                df.drop(columns=["kotak_sec_instrument_token"], inplace=True)
            case self.DUMMY:
                df = await self.api.positions()

        if not df.empty:
            df["net_qty"] = df["buy_qty"] - df["sell_qty"]

        return df

    async def get_open_position(self) -> list:
        match self.broker_name:
            case self.KOTAK_NEO:
                df = await self.api.positions()
            case self.KOTAK:
                df = await self.api.positions("TODAYS")
                instruments = cache.get("OPTION_INSTRUMENTS")
                instruments = instruments[["kotak_sec_instrument_token", "tradingsymbol"]].copy()
                df = pd.merge(
                    df,
                    instruments,
                    left_on="kotak_sec_instrument_token",
                    right_on="kotak_sec_instrument_token",
                )
                df.drop(columns=["kotak_sec_instrument_token"], inplace=True)
            case self.DUMMY:
                df = await self.api.positions()

        instruments = cache.get("OPTION_INSTRUMENTS")

        tradingsymbols = instruments.tradingsymbol

        df["net_qty"] = df["buy_qty"] - df["sell_qty"]

        df = df[(df["net_qty"] != 0) & (df["tradingsymbol"].isin(tradingsymbols))].reset_index(drop=True)

        return df.to_dict("records")

    async def square_off_all(self, market=False):
        positions = await self.get_open_position()
        instruments = cache.get("OPTION_INSTRUMENTS")
        data = []
        for row in positions:
            transaction_type = "SELL" if row["net_qty"] > 0 else "BUY"
            x = instruments[instruments["tradingsymbol"] == row["tradingsymbol"]].iloc[0]
            data.append(
                self.place_and_chase_order(
                    instrument_name="BANKNIFTY",
                    strike=x["strike"],
                    option_type=x["instrument_type"],
                    transaction_type=transaction_type,
                    quantity=abs(row["net_qty"]),
                    expected_price=0 if market else x["last_price"],
                    initial_slippage=10,
                    slippage=5,
                )
            )
        return await asyncio.gather(*data)
