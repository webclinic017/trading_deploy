import asyncio

import numpy as np
from django.core.cache import cache
from django.utils import timezone

from utils.multi_broker import Broker as MultiBroker


class Strategy:
    def __init__(
        self,
        user_params: list,
        opt_strategy,
        instrument_name="BANKNIFTY",
        buy_slippage: float = 10.0,
        sell_slippage: float = 10.0,
        entry_slippage: float = 10.0,
        sleep_time: int = 10,
        skip_price: float = 5.0,
    ):
        self.opt_strategy = opt_strategy
        self.user_params = user_params
        self.sell_slippage = sell_slippage
        self.buy_slippage = buy_slippage
        self.entry_slippage = entry_slippage
        self.skip_price = skip_price
        self.sleep_time = sleep_time
        self.instrument_name = instrument_name
        self.strategy = str(opt_strategy.pk)

    def get_greeks_instruments(self):
        return cache.get("OPTION_GREEKS_INSTRUMENTS")

    async def place_sl_orders(self, user, orders):
        order: MultiBroker = user["order_obj"]
        await order.initiate_session()

        return await asyncio.gather(
            *[
                order.place_stop_loss_order(
                    instrument_name=self.instrument_name,
                    strike=x["strike"],
                    option_type=x["option_type"],
                    transaction_type="BUY",
                    quantity=user["quantity"],
                    expected_price=x["expected_price"],
                    slippage=self.buy_slippage,
                )
                for x in orders
            ]
        )

    async def place_orders(self, user, orders):
        order: MultiBroker = user["order_obj"]
        await order.initiate_session()

        return await asyncio.gather(
            *[
                order.place_and_chase_order(
                    instrument_name=self.instrument_name,
                    strike=x["strike"],
                    option_type=x["option_type"],
                    transaction_type="SELL",
                    quantity=user["quantity"],
                    expected_price=x["expected_price"],
                    initial_slippage=10,
                    slippage=self.buy_slippage,
                )
                for x in orders
            ]
        )

    async def place_straddle(self, idx, sl_pct, ce_strike=0, pe_strike=0):
        await self.initiate()
        atm = round(cache.get("BANKNIFTY_LTP") / 100) * 100

        if not ce_strike:
            ce_strike = atm

        if not pe_strike:
            pe_strike = atm

        instruments = self.get_greeks_instruments()
        sell_pending, buy_pending = [], []

        ce = instruments[(instruments["strike"] == ce_strike) & (instruments["instrument_type"] == "CE")].iloc[0]
        pe = instruments[(instruments["strike"] == pe_strike) & (instruments["instrument_type"] == "PE")].iloc[-1]

        tradingsymbol = cache.get(f"{self.strategy}_tradingsymbol", {})

        sell_pending = [
            {
                "strike": pe.strike,
                "option_type": "PE",
                "expected_price": pe.last_price,
                "expected_time": timezone.localtime(),
                "idx": idx,
                "reason": "ENTERING PE",
            },
            {
                "strike": ce.strike,
                "option_type": "CE",
                "expected_price": ce.last_price,
                "expected_time": timezone.localtime(),
                "idx": idx,
                "reason": "ENTERING CE",
            },
        ]

        ce_tradingsymbol, pe_tradingsymbol = (ce["tradingsymbol"], pe["tradingsymbol"])

        user_order_data = await asyncio.gather(
            *[
                self.place_orders(
                    user=user,
                    orders=sell_pending,
                )
                for user in self.user_params
            ]
        )

        ce_sl_price = round(round((ce.last_price * (100 + sl_pct) / 100) / 0.05) * 0.05, 2)
        pe_sl_price = round(round((pe.last_price * (100 + sl_pct) / 100) / 0.05) * 0.05, 2)

        buy_pending = [
            {
                "strike": pe.strike,
                "option_type": "PE",
                "expected_price": pe_sl_price,
                "expected_time": timezone.localtime(),
                "idx": idx,
                "reason": "SL PE",
            },
            {
                "strike": ce.strike,
                "option_type": "CE",
                "expected_price": ce_sl_price,
                "expected_time": timezone.localtime(),
                "idx": idx,
                "reason": "SL CE",
            },
        ]

        user_order_data_sl = await asyncio.gather(
            *[
                self.place_sl_orders(
                    user=user,
                    orders=buy_pending,
                )
                for user in self.user_params
            ]
        )

        tradingsymbol[idx] = {
            "ce_tradingsymbol": ce_tradingsymbol,
            "pe_tradingsymbol": pe_tradingsymbol,
            "ce_entry_price": ce.last_price,
            "pe_entry_price": pe.last_price,
            "ce_sl": ce_sl_price,
            "pe_sl": pe_sl_price,
            "entered": True,
            "ce_exited": False,
            "pe_exited": False,
            "exited": False,
        }

        cache.set(f"{self.strategy}_tradingsymbol", tradingsymbol)

        user_wise_straddle_data = {}

        for user_param, entry_data, sl_data in zip(self.user_params, user_order_data, user_order_data_sl):
            user_wise_straddle_data[user_param["user"]] = {
                "ce_entry_order_id": entry_data[1]["order_number"],
                "ce_entry_price": ce.last_price,
                "pe_entry_order_id": entry_data[0]["order_number"],
                "pe_entry_price": pe.last_price,
                "ce_exit_order_id": sl_data[1]["order_number"],
                "ce_exit_price": ce_sl_price,
                "pe_exit_order_id": sl_data[0]["order_number"],
                "pe_exit_price": pe_sl_price,
                "ce_exit_status": sl_data[1]["order_status"],
                "pe_exit_status": sl_data[0]["order_status"],
            }

        user_wise_straddle_datas = cache.get(f"{self.strategy}_user_wise_straddle_datas", {})

        user_wise_straddle_datas[idx] = user_wise_straddle_data

        cache.set(f"{self.strategy}_user_wise_straddle_datas", user_wise_straddle_datas)

    async def initiate(self):
        self.today = timezone.localdate()
        self.expiry = cache.get("EXPIRY")
        self.days_left = (self.expiry - self.today).days

    async def run(self):
        await self.initiate()
        strategy_list = cache.get("deployed_strategies", {})
        strategy_list[self.strategy] = {
            "user_params": self.user_params,
        }
        cache.set("deployed_strategies", strategy_list)

    async def modify_order(self, user, order_id, strike, option_type, price):
        order: MultiBroker = user["order_obj"]
        await order.initiate_session()

        await order.modify_stop_loss_order(
            order_id=order_id,
            instrument_name=self.instrument_name,
            strike=strike,
            option_type=option_type,
            transaction_type="BUY",
            quantity=user["quantity"],
            expected_price=price,
            slippage=self.buy_slippage,
        )

    async def exit_stop_loss_order(self, user, order_id, strike, option_type):
        order: MultiBroker = user["order_obj"]
        await order.initiate_session()

        await order.modify_and_chase_order(
            order_number=order_id,
            instrument_name=self.instrument_name,
            strike=strike,
            option_type=option_type,
            expected_price=0,
            slippage=10,
            transaction_type="BUY",
        )

    async def modify_to_cost(self, idx):
        tradingsymbol = cache.get(f"{self.strategy}_tradingsymbol", {})
        user_position = cache.get(f"{self.strategy}_user_wise_straddle_datas", {})

        instruments = self.get_greeks_instruments()

        if idx in tradingsymbol.keys():
            user_pos = user_position[idx]

            for user in self.user_params:
                user_symbol = user_pos[user["user"]]

                if tradingsymbol[idx]["pe_exited"] and not tradingsymbol[idx]["exited"]:
                    symbol = instruments[instruments["tradingsymbol"] == tradingsymbol[idx]["ce_tradingsymbol"]].iloc[
                        0
                    ]
                    await self.modify_order(
                        user=user,
                        order_id=user_symbol["ce_exit_order_id"],
                        strike=symbol["strike"],
                        option_type=symbol["instrument_type"],
                        price=tradingsymbol[idx]["ce_entry_price"],
                    )
                    tradingsymbol[idx]["modified_sl_to_cost"] = True

                if tradingsymbol[idx]["ce_exited"] and not tradingsymbol[idx]["exited"]:
                    symbol = instruments[instruments["tradingsymbol"] == tradingsymbol[idx]["pe_tradingsymbol"]].iloc[
                        0
                    ]
                    await self.modify_order(
                        user=user,
                        order_id=user_symbol["pe_exit_order_id"],
                        strike=symbol["strike"],
                        option_type=symbol["instrument_type"],
                        price=tradingsymbol[idx]["pe_entry_price"],
                    )

                    tradingsymbol[idx]["modified_sl_to_cost"] = True

                # await self.modify_order(user, user_pos[user['user']])
        cache.set(f"{self.strategy}_tradingsymbol", tradingsymbol)

    async def exit_order(self, idx):
        tradingsymbol = cache.get(f"{self.strategy}_tradingsymbol", {})
        user_position = cache.get(f"{self.strategy}_user_wise_straddle_datas", {})

        instruments = self.get_greeks_instruments()

        if idx in tradingsymbol.keys():
            user_pos = user_position[idx]

            if not tradingsymbol[idx]["ce_exited"]:
                symbol = instruments[instruments["tradingsymbol"] == tradingsymbol[idx]["ce_tradingsymbol"]].iloc[0]

                await asyncio.gather(
                    *[
                        self.exit_stop_loss_order(
                            user=user,
                            order_id=user_pos[user["user"]]["ce_exit_order_id"],
                            strike=symbol["strike"],
                            option_type=symbol["instrument_type"],
                        )
                        for user in self.user_params
                    ]
                )

            if not tradingsymbol[idx]["pe_exited"]:
                symbol = instruments[instruments["tradingsymbol"] == tradingsymbol[idx]["pe_tradingsymbol"]].iloc[0]

                await asyncio.gather(
                    *[
                        self.exit_stop_loss_order(
                            user=user,
                            order_id=user_pos[user["user"]]["pe_exit_order_id"],
                            strike=symbol["strike"],
                            option_type=symbol["instrument_type"],
                        )
                        for user in self.user_params
                    ]
                )

            tradingsymbol[idx]["ce_exited"] = True
            tradingsymbol[idx]["pe_exited"] = True
            tradingsymbol[idx]["exited"] = True

        cache.set(f"{self.strategy}_tradingsymbol", tradingsymbol)

    async def update_position(self):
        user_position = cache.get(f"{self.strategy}_user_wise_straddle_datas", {})
        tradingsymbol = cache.get(f"{self.strategy}_tradingsymbol", {})
        for index, user in enumerate(self.user_params):
            for idx, data in user_position.items():
                # if tradingsymbol[idx].get("ce_exit_price"):
                #     del tradingsymbol[idx]["ce_exit_price"]
                # if tradingsymbol[idx].get("pe_exit_price"):
                #     del tradingsymbol[idx]["pe_exit_price"]
                order: MultiBroker = user["order_obj"]
                await order.initiate_session()

                user_data = data[user["user"]]

                # Updating Entry Price
                if not data[user["user"]].get("ce_entry_price_updated"):
                    ce_entry_order_id = await order.single_order_report(user_data["ce_entry_order_id"])
                    data[user["user"]]["ce_entry_price_updated"] = True
                    data[user["user"]]["ce_entry_price"] = round(float(ce_entry_order_id["avgPrc"]), 2)
                    if index == 0:
                        tradingsymbol[idx]["ce_entry_price"] = round(float(ce_entry_order_id["avgPrc"]), 2)

                if not data[user["user"]].get("pe_entry_price_updated"):
                    pe_entry_order_id = await order.single_order_report(user_data["pe_entry_order_id"])
                    data[user["user"]]["ce_entry_price_updated"] = True
                    data[user["user"]]["pe_entry_price"] = round(float(pe_entry_order_id["avgPrc"]), 2)
                    if index == 0:
                        tradingsymbol[idx]["pe_entry_price"] = round(float(pe_entry_order_id["avgPrc"]), 2)
                # Updatating Exit Price
                if (not data[user["user"]].get("pe_exit_price_updated")) or (not data[user["user"]].get("ce_exit_price_updated")):
                    if not data[user["user"]].get("ce_exit_price_updated"):
                        ce_sl_status = await order.single_order_report(user_data["ce_exit_order_id"])
                        if ce_sl_status["ordSt"] == "complete":
                            tradingsymbol[idx]["ce_exited"] = True
                            if index == 0:
                                tradingsymbol[idx]["ce_exit_price"] = round(float(ce_sl_status["avgPrc"]), 2)
                            data[user["user"]]["ce_exit_price_updated"] = True
                            data[user["user"]]["ce_exited_real_price"] = round(float(ce_sl_status["avgPrc"]), 2)
                        else:
                            tradingsymbol[idx]["ce_sl"] = round(float(ce_sl_status["trgPrc"]), 2)
                    else:
                        ce_sl_status = {'ordSt': "complete"}

                    if not data[user["user"]].get("pe_exit_price_updated"):
                        pe_sl_status = await order.single_order_report(user_data["pe_exit_order_id"])
                        if pe_sl_status["ordSt"] == "complete":
                            tradingsymbol[idx]["pe_exited"] = True
                            if index == 0:
                                tradingsymbol[idx]["pe_exit_price"] = round(float(pe_sl_status["avgPrc"]), 2)
                            data[user["user"]]["pe_exit_price_updated"] = True
                            data[user["user"]]["pe_exited_real_price"] = round(float(pe_sl_status["avgPrc"]), 2)
                        else:
                            tradingsymbol[idx]["pe_sl"] = pe_sl_status["trgPrc"]
                    else:
                        pe_sl_status = {'ordSt': "complete"}

                    if pe_sl_status["ordSt"] == "complete" and ce_sl_status["ordSt"] == "complete":
                        tradingsymbol[idx]["exited"] = True
                    
                    data[user["user"]]["ce_exit_status"] = ce_sl_status["ordSt"]
                    data[user["user"]]["pe_exit_status"] = pe_sl_status["ordSt"]

        cache.set(f"{self.strategy}_user_wise_straddle_datas", user_position)
        cache.set(f"{self.strategy}_tradingsymbol", tradingsymbol)
