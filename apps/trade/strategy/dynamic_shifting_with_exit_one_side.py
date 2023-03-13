import asyncio
import datetime as dt

import pandas as pd
from colorama import Fore
from django.core.cache import cache
from django.utils import timezone

from utils import send_notifications
from utils.multi_broker import Broker as MultiBroker


class Strategy:
    def __init__(
        self,
        user_params: list,
        parameters: list,
        opt_strategy,
        entry_time: dt.time = dt.time(9, 15, 10),
        exit_time: dt.time = dt.time(15, 25, 59),
        min_delta: list = [45.0, 45.0, 45.0],
        max_delta: float = 58.0,
        shift_min_delta: float = 30.0,
        shift_max_delta: float = 58.0,
        shift_min_delta_entry: float = 45.0,
        shift_max_delta_entry: float = 58.0,
        multiplier: float = 1.0,
        point_difference: float = -50.0,
        sigma_diff: float = 50.0,
        entry_sigma: float = 15.0,
        chasing_order_time: float = 2.5,
        chasing_time_sleep: float = 0.5,
        buy_slippage: float = 5.0,
        sell_slippage: float = 5.0,
        entry_slippage: float = 10.0,
        sleep_time: int = 10,
        skip_price: float = 5.0,
        oneside_check_time: dt.time = dt.time(15, 10, 0),
        expiry_check_time: dt.time = dt.time(14, 45, 0),
        expiry_check_sigma_time: dt.time = dt.time(14, 0, 0),
        strategy="1",
        instrument_name: str = "BANKNIFTY",
    ):
        self.user_params = user_params
        self.parameters = parameters
        self.no_of_strategy = len(self.parameters)
        self.entry_time = entry_time
        self.exit_time = exit_time
        self.min_delta = [x / 100 for x in min_delta]
        self.max_delta = max_delta / 100
        self.shift_min_delta = shift_min_delta / 100
        self.shift_max_delta = shift_max_delta / 100
        self.shift_min_delta_entry = shift_min_delta_entry / 100
        self.shift_max_delta_entry = shift_max_delta_entry / 100
        self.multiplier = multiplier
        self.point_difference = point_difference
        self.sigma_diff = sigma_diff / 100
        self.entry_sigma = entry_sigma / 100
        self.chasing_order_time = chasing_order_time
        self.chasing_time_sleep = chasing_time_sleep
        self.buy_slippage = buy_slippage
        self.sell_slippage = sell_slippage
        self.entry_slippage = entry_slippage
        self.skip_price = skip_price
        self.sleep_time = sleep_time
        self.oneside_check_time = oneside_check_time
        self.expiry_check_time = expiry_check_time
        self.expiry_check_sigma_time = expiry_check_sigma_time
        self.no_of_strategy = len(parameters)
        self.parameters = parameters
        self.instrument_name = instrument_name
        self.entry_times = [
            (dt.datetime.combine(timezone.localdate(), entry_time))
            + dt.timedelta(seconds=(idx * 2 * self.sleep_time) - 1)
            for idx, _ in enumerate(self.parameters)
        ]
        self.opt_strategy = opt_strategy
        self.strategy = str(self.opt_strategy.pk)

    def get_greeks_instruments(self):
        return cache.get("OPTION_GREEKS_INSTRUMENTS")

    async def find_strike(self, instruments, near, option_type, query_type, near_type):
        df = instruments
        bnf_ltp = cache.get("BANKNIFTY_LTP")
        if option_type == "CE":
            df = df[df["strike"] > bnf_ltp - 200].copy()
        else:
            df = df[df["strike"] < bnf_ltp + 200].copy()

        if near_type == "delta":
            if query_type == ">":
                df = (
                    df[(df["instrument_type"] == option_type) & (df[near_type] >= near)]
                    .sort_values("strike", ascending=False)
                    .copy()
                )
            else:
                df = (
                    df[(df["instrument_type"] == option_type) & (df[near_type] <= near)]
                    .sort_values("strike", ascending=True)
                    .copy()
                )
        else:
            if query_type == ">":
                df = df[(df["instrument_type"] == option_type) & (df[near_type] >= near)].copy()
                if option_type == "CE":
                    df.sort_values("strike", inplace=True, ignore_index=True, ascending=False)
                else:
                    df.sort_values("strike", inplace=True, ignore_index=True, ascending=True)
            else:
                df = df[(df["instrument_type"] == option_type) & (df[near_type] <= near)].copy()

                if option_type == "PE":
                    df.sort_values("strike", inplace=True, ignore_index=True, ascending=False)
                else:
                    df.sort_values("strike", inplace=True, ignore_index=True, ascending=True)

        if not df.empty:
            return df.iloc[0].strike, df.iloc[0].sigma

        print("STRIKE EMPTY NOT FOUND.")
        return 0, 0

    async def place_orders(self, user, buy_pending, sell_pending):
        buy_data, sell_data = [], []
        if buy_pending or sell_pending:
            order: MultiBroker = user["order_obj"]
            await order.initiate_session()

        if buy_pending:
            buy_data = await asyncio.gather(
                *[
                    order.place_and_chase_order(
                        instrument_name=self.instrument_name,
                        strike=x["strike"],
                        option_type=x["option_type"],
                        transaction_type="BUY",
                        quantity=user["quantity_multiple"][x["idx"]],
                        expected_price=x["expected_price"],
                        initial_slippage=10,
                        slippage=self.buy_slippage,
                    )
                    for x in buy_pending
                ]
            )

        if sell_pending:
            sell_data = await asyncio.gather(
                *[
                    order.place_and_chase_order(
                        instrument_name=self.instrument_name,
                        strike=x["strike"],
                        option_type=x["option_type"],
                        transaction_type="SELL",
                        quantity=user["quantity_multiple"][x["idx"]],
                        expected_price=x["expected_price"],
                        initial_slippage=10,
                        slippage=self.buy_slippage,
                    )
                    for x in sell_pending
                ]
            )
        return buy_data, sell_data

    async def save_order_in_db(self, user_order_datas, user_params):
        for user_order_data in user_order_datas:
            buy_data = user_order_data[0]
            sell_data = user_order_data[1]

    async def place_entry_order(self, conditions):
        instruments = self.get_greeks_instruments()
        sell_pending, buy_pending = [], []

        if timezone.localtime().time() <= self.entry_time:
            await asyncio.sleep(
                (
                    timezone.localtime().replace(
                        hour=self.entry_time.hour,
                        minute=self.entry_time.minute,
                        second=self.entry_time.second,
                        microsecond=0,
                    )
                    - timezone.localtime()
                ).total_seconds()
            )

        print(timezone.localtime().replace(microsecond=0))

        tradingsymbol = {}
        user_order_datas = []
        for idx, _ in enumerate(conditions):
            ce = (
                instruments[
                    (instruments["delta"] > self.min_delta[idx])
                    & (instruments["delta"] < self.max_delta)
                    & (instruments["instrument_type"] == "CE")
                ]
                .sort_values("delta")
                .iloc[0]
            )
            pe = (
                instruments[
                    (instruments["delta"] < -self.min_delta[idx])
                    & (instruments["delta"] > -self.max_delta)
                    & (instruments["instrument_type"] == "PE")
                ]
                .sort_values("delta")
                .iloc[-1]
            )
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
                        buy_pending=buy_pending,
                        sell_pending=sell_pending,
                    )
                    for user in self.user_params
                ]
            )

            tradingsymbol[idx] = {
                "ce_tradingsymbol": ce_tradingsymbol,
                "pe_tradingsymbol": pe_tradingsymbol,
                "exited_one_side": False,
                "ce_exit_one_side": False,
                "pe_exit_one_side": False,
            }

        user_order_datas.append(user_order_data)

        strategy_list = cache.get("deployed_strategies", {})
        strategy_list[self.strategy] = {
            "user_params": self.user_params,
            "no_of_strategy": self.no_of_strategy,
        }
        cache.set("deployed_strategies", strategy_list)
        cache.set(f"{self.strategy}_tradingsymbol", tradingsymbol)

        await send_notifications(
            self.opt_strategy.strategy_name.upper(),
            "ALGO ENTERED!",
            "alert-success",
        )

        for user_ordera_data in user_order_datas:
            for order_data, user in zip(user_ordera_data, self.user_params):
                # self.save_order_in_db(order_data, user)
                pass

        return True

    async def check_shifting_orders(
        self,
        instruments,
        idx,
        ce,
        pe,
        multiplier,
        now_time=timezone.localtime(),
    ):
        buy_pending, sell_pending = [], []
        if ((pe["delta"] + ce["delta"]) > (min(abs(pe["delta"]), ce["delta"]) * multiplier)) and ce[
            "last_price"
        ] > self.skip_price:
            if ce.strike - ce["bnf_ltp"] <= self.point_difference:
                if now_time <= self.expiry_check_timestamp:
                    strike, sigma = await self.find_strike(instruments, -pe["delta"], "CE", ">", "delta")
                else:
                    strike, sigma = await self.find_strike(instruments, pe["last_price"], "CE", ">", "last_price")

                if strike != int(ce["strike"]) and abs(sigma - ce["sigma"]) < self.sigma_diff:
                    buy_pending.append(
                        {
                            "strike": ce.strike,
                            "option_type": "CE",
                            "expected_price": ce.last_price,
                            "expected_time": timezone.localtime(),
                            "idx": idx,
                            "reason": "EXIT CE - SHIFTING CALL AWAY",
                        }
                    )
                    ce = instruments[
                        (instruments["strike"] == strike) & (instruments["instrument_type"] == "CE")
                    ].iloc[0]
                    sell_pending.append(
                        {
                            "strike": ce.strike,
                            "option_type": "CE",
                            "expected_price": ce.last_price,
                            "expected_time": timezone.localtime(),
                            "idx": idx,
                            "reason": "ENTER CE - SHIFTING CALL AWAY",
                        }
                    )
                else:
                    print("CALL AWAY SHIFT")
                    if not ce["last_price"] > self.skip_price:
                        print("SKIP PRICE")
                    elif strike == int(ce["strike"]):
                        print("STRIKE")
                    else:
                        print("SIGMA", abs(sigma - ce["sigma"]), round(self.sigma_diff))
            else:
                if now_time <= self.expiry_check_timestamp:
                    strike, sigma = await self.find_strike(instruments, -ce["delta"], "PE", ">", "delta")
                else:
                    strike, sigma = await self.find_strike(instruments, ce["last_price"], "PE", "<", "last_price")

                if strike != int(pe["strike"]) and abs(sigma - ce["sigma"]) < self.sigma_diff:
                    buy_pending.append(
                        {
                            "strike": pe.strike,
                            "option_type": "PE",
                            "expected_price": pe.last_price,
                            "expected_time": timezone.localtime(),
                            "idx": idx,
                            "reason": "EXIT PE - SHIFTING PUT IN",
                        }
                    )
                    pe = instruments[
                        (instruments["strike"] == strike) & (instruments["instrument_type"] == "PE")
                    ].iloc[0]
                    sell_pending.append(
                        {
                            "strike": pe.strike,
                            "option_type": "PE",
                            "expected_price": pe.last_price,
                            "expected_time": timezone.localtime(),
                            "idx": idx,
                            "reason": "ENTER PE - SHIFTING PUT IN",
                        }
                    )
                else:
                    print("SHIFTED PUT IN")
                    if not ce["last_price"] > self.skip_price:
                        print("SKIP PRICE")
                    elif strike == int(pe["strike"]):
                        print("STRIKE")
                    else:
                        print("SIGMA", abs(sigma - ce["sigma"]), round(self.sigma_diff))
        elif ((ce["delta"] + pe["delta"]) < -(min(abs(pe["delta"]), ce["delta"]) * multiplier)) and pe[
            "last_price"
        ] > self.skip_price:
            if pe.strike - pe["bnf_ltp"] >= -self.point_difference:
                if now_time <= self.expiry_check_timestamp:
                    strike, sigma = await self.find_strike(instruments, -ce["delta"], "PE", "<", "delta")
                else:
                    strike, sigma = await self.find_strike(instruments, ce["last_price"], "PE", ">", "last_price")

                if strike != pe["strike"] and abs(sigma - pe["sigma"]) < self.sigma_diff:
                    buy_pending.append(
                        {
                            "strike": pe.strike,
                            "option_type": "PE",
                            "expected_price": pe.last_price,
                            "expected_time": timezone.localtime(),
                            "idx": idx,
                            "reason": "EXIT PE - SHIFTING PUT AWAY",
                        }
                    )
                    pe = instruments[
                        (instruments["strike"] == strike) & (instruments["instrument_type"] == "PE")
                    ].iloc[0]
                    sell_pending.append(
                        {
                            "strike": pe.strike,
                            "option_type": "PE",
                            "expected_price": pe.last_price,
                            "expected_time": timezone.localtime(),
                            "idx": idx,
                            "reason": "ENTER PE - SHIFTING PUT AWAY",
                        }
                    )
                else:
                    print("PUT AWAY SHIFT")
                    if not ce["last_price"] > self.skip_price:
                        print("SKIP PRICE")
                    if strike == int(pe["strike"]):
                        print("STRIKE")
                    else:
                        print("SIGMA", abs(sigma - pe["sigma"]), round(self.sigma_diff))
            else:
                if now_time <= self.expiry_check_timestamp:
                    strike, sigma = await self.find_strike(instruments, -pe["delta"], "CE", "<", "delta")
                else:
                    strike, sigma = await self.find_strike(instruments, pe["last_price"], "CE", "<", "last_price")

                if strike != ce["strike"] and abs(sigma - ce["sigma"]) < self.sigma_diff:
                    buy_pending.append(
                        {
                            "strike": ce.strike,
                            "option_type": "CE",
                            "expected_price": ce.last_price,
                            "expected_time": timezone.localtime(),
                            "idx": idx,
                            "reason": "EXIT CE - SHIFTING CALL IN",
                        }
                    )
                    ce = instruments[
                        (instruments["strike"] == strike) & (instruments["instrument_type"] == "CE")
                    ].iloc[0]
                    sell_pending.append(
                        {
                            "strike": ce.strike,
                            "option_type": "CE",
                            "expected_price": ce.last_price,
                            "expected_time": timezone.localtime(),
                            "idx": idx,
                            "reason": "ENTER PE - SHIFTING PUT AWAY",
                        }
                    )
                else:
                    print("CALL IN SHIFT")
                    if not ce["last_price"] > self.skip_price:
                        print("SKIP PRICE")
                    if strike == int(ce["strike"]):
                        print("STRIKE")
                    else:
                        print("SIGMA", abs(sigma - ce["sigma"]), round(self.sigma_diff))

        return (
            buy_pending,
            sell_pending,
            ce["tradingsymbol"],
            pe["tradingsymbol"],
        )

    async def initiate(self):
        tz = timezone.localtime().tzinfo

        self.today = timezone.localdate()
        self.expiry = cache.get("EXPIRY")
        self.days_left = (self.expiry - self.today).days
        self.oneside_check_timestamp = dt.datetime.combine(self.today, self.oneside_check_time).replace(tzinfo=tz)
        self.expiry_check_timestamp = dt.datetime.combine(self.expiry, self.expiry_check_time).replace(tzinfo=tz)
        self.expiry_check_sigma_timestamp = dt.datetime.combine(self.expiry, self.expiry_check_sigma_time).replace(
            tzinfo=tz
        )

    async def run(self, entered=False, data: dict | None = None):
        if timezone.localtime().time() <= dt.time(9, 15, 12):
            await asyncio.sleep(
                (
                    timezone.localtime().replace(hour=9, minute=15, second=12, microsecond=0) - timezone.localtime()
                ).total_seconds()
            )

        await self.initiate()

        strategies = []
        for row in self.parameters:
            if self.days_left in row["one_side_without_check_exit"]:
                strategies.append(self.one_side_without_check_exit)
            elif self.days_left in row["one_side_check_exit"]:
                strategies.append(self.one_side_check_exit)

        conditions = [row["day_wise"][str(self.days_left)] for row in self.parameters]
        instruments = self.get_greeks_instruments()

        if entered and not data:
            tradingsymbol = cache.get(f"{self.strategy}_tradingsymbol", {})
            if not tradingsymbol:
                raise Exception("Trading Symbol not found")
        elif entered and data:
            strategy_list = cache.get("deployed_strategies", {})
            strategy_list[self.strategy] = {
                "user_params": self.user_params,
                "no_of_strategy": self.no_of_strategy,
            }
            cache.set("deployed_strategies", strategy_list)
            tradingsymbol = {}
            for idx in sorted(data.keys()):
                if not data[idx].get("ce_strike") and not data[idx].get("pe_strike"):
                    raise Exception("Provide CE Strike and PE Strike")

                ce_tradingsymbol = pe_tradingsymbol = None

                if data[idx].get("ce_strike"):
                    ce_tradingsymbol = (
                        instruments[
                            (instruments["strike"] == data[idx]["ce_strike"])
                            & (instruments["instrument_type"] == "CE")
                        ]
                        .iloc[0]
                        .tradingsymbol
                    )

                if data[idx].get("pe_strike"):
                    pe_tradingsymbol = (
                        instruments[
                            (instruments["strike"] == data[idx]["pe_strike"])
                            & (instruments["instrument_type"] == "PE")
                        ]
                        .iloc[0]
                        .tradingsymbol
                    )

                exited_one_side = ce_exit_one_side = pe_exit_one_side = False

                if not ce_tradingsymbol:
                    exited_one_side, ce_exit_one_side, pe_exit_one_side = True, True, False

                if not pe_tradingsymbol:
                    exited_one_side, ce_exit_one_side, pe_exit_one_side = True, False, True

                tradingsymbol[idx] = {
                    "ce_tradingsymbol": ce_tradingsymbol,
                    "pe_tradingsymbol": pe_tradingsymbol,
                    "exited_one_side": exited_one_side,
                    "ce_exit_one_side": ce_exit_one_side,
                    "pe_exit_one_side": pe_exit_one_side,
                }
            print(tradingsymbol)
            cache.set(f"{self.strategy}_tradingsymbol", tradingsymbol)

        if entered:
            strategy_list = cache.get("strategies", {})
            strategy_list[self.strategy] = {
                "user_params": self.user_params,
                "no_of_strategy": self.no_of_strategy,
            }
            cache.set("strategies", strategy_list)
        else:
            entered = await self.place_entry_order(conditions)

        ct = timezone.localtime()
        st = self.sleep_time * self.no_of_strategy
        if ct.second % st == 0:
            diff = (ct.replace(microsecond=0) + dt.timedelta(seconds=st) - timezone.localtime()).total_seconds()
        else:
            diff = (
                ct.replace(second=((ct.second // st) * st), microsecond=0)
                + dt.timedelta(seconds=st)
                - timezone.localtime()
            ).total_seconds()
        del st, ct
        await asyncio.sleep(diff)

        exit_trigger = False
        while (
            timezone.localtime().time() < self.exit_time
            and self.strategy in (cache.get("deployed_strategies", {})).keys()
        ):
            for idx, (func, cond) in enumerate(zip(strategies, conditions)):
                self.user_params = (cache.get("deployed_strategies", {}))[self.strategy]["user_params"]
                cache.set(f"{self.strategy}_hold", True)
                tradingsymbol = cache.get(f"{self.strategy}_tradingsymbol", {})
                now_time = timezone.localtime()

                instruments = self.get_greeks_instruments()
                live_pcr_df = cache.get(
                    "LIVE_BNF_PCR",
                    pd.DataFrame(columns=["timestamp", "pe_total_oi", "ce_total_oi", "pcr"]),
                )
                live_pcr_df["ce_oi_change"] = live_pcr_df["ce_total_oi"].pct_change(periods=72)
                live_pcr_df["pe_oi_change"] = live_pcr_df["pe_total_oi"].pct_change(periods=72)

                ce_tradingsymbol = tradingsymbol[idx]["ce_tradingsymbol"]
                pe_tradingsymbol = tradingsymbol[idx]["pe_tradingsymbol"]

                make_ce_exit = make_pe_exit = ce_reentry = pe_reentry = False
                buy_pending, sell_pending = [], []
                tradingsymbol = cache.get(f"{self.strategy}_tradingsymbol", {})
                tradingsymbol_temp = tradingsymbol.copy()

                if ce_tradingsymbol:
                    ce = instruments[(instruments["tradingsymbol"] == ce_tradingsymbol)].iloc[0]

                if pe_tradingsymbol:
                    pe = instruments[(instruments["tradingsymbol"] == pe_tradingsymbol)].iloc[0]

                exited_one_side = tradingsymbol_temp[idx]["exited_one_side"]
                ce_exit_one_side = tradingsymbol_temp[idx]["ce_exit_one_side"]
                pe_exit_one_side = tradingsymbol_temp[idx]["pe_exit_one_side"]

                if not live_pcr_df.empty:
                    row = live_pcr_df.iloc[-1]
                    make_ce_exit, make_pe_exit, ce_reentry, pe_reentry = func(
                        idx, row, exited_one_side, ce_exit_one_side, pe_exit_one_side, cond
                    )
                    # print(round(row.ce_oi_change * 100, 2))
                    # print(round(row.pe_oi_change * 100, 2))
                    # print(round((row.pe_oi_change - row.ce_oi_change) * 100, 2))

                if exited_one_side:
                    if ce_reentry and ce_exit_one_side:
                        (
                            buy_pending_temp,
                            sell_pending_temp,
                            ce_tradingsymbol,
                            pe_tradingsymbol,
                            exited_one_side,
                            ce_exit_one_side,
                        ) = await self.get_ce_reentry(idx, instruments, pe, now_time)

                        buy_pending.extend(buy_pending_temp)
                        sell_pending.extend(sell_pending_temp)

                        del buy_pending_temp, sell_pending_temp

                    elif pe_reentry and pe_exit_one_side:
                        (
                            buy_pending_temp,
                            sell_pending_temp,
                            ce_tradingsymbol,
                            pe_tradingsymbol,
                            exited_one_side,
                            pe_exit_one_side,
                        ) = await self.get_pe_reentry(idx, instruments, ce, now_time)

                        buy_pending.extend(buy_pending_temp)
                        sell_pending.extend(sell_pending_temp)

                        del buy_pending_temp, sell_pending_temp
                else:
                    if make_ce_exit:
                        buy_pending_temp, ce_tradingsymbol, exited_one_side, ce_exit_one_side = await self.get_ce_exit(
                            idx, ce
                        )
                        buy_pending.extend(buy_pending_temp)
                        del buy_pending_temp
                    elif make_pe_exit:
                        buy_pending_temp, pe_tradingsymbol, exited_one_side, pe_exit_one_side = await self.get_pe_exit(
                            idx, pe
                        )
                        buy_pending.extend(buy_pending_temp)
                        del buy_pending_temp
                    else:
                        (
                            buy_pending_temp,
                            sell_pending_temp,
                            ce_tradingsymbol,
                            pe_tradingsymbol,
                        ) = await self.check_shifting_orders(
                            instruments=instruments,
                            idx=idx,
                            ce=ce,
                            pe=pe,
                            multiplier=self.multiplier,
                            now_time=now_time,
                        )
                        buy_pending.extend(buy_pending_temp)
                        sell_pending.extend(sell_pending_temp)

                        del buy_pending_temp, sell_pending_temp

                user_order_data = await asyncio.gather(
                    *[
                        self.place_orders(
                            user=user,
                            buy_pending=buy_pending,
                            sell_pending=sell_pending,
                        )
                        for user in self.user_params
                    ]
                )

                tradingsymbol[idx] = {
                    "ce_tradingsymbol": ce_tradingsymbol,
                    "pe_tradingsymbol": pe_tradingsymbol,
                    "exited_one_side": exited_one_side,
                    "ce_exit_one_side": ce_exit_one_side,
                    "pe_exit_one_side": pe_exit_one_side,
                }
                cache.set(f"{self.strategy}_hold", False)

                call_sigma = put_sigma = call_delta = put_delta = 0
                ce_print = pe_print = "NONE"
                total_delta = 0
                if ce_tradingsymbol:
                    call_delta = ce.delta * 100
                    call_sigma = ce.sigma * 100
                    ce_print = ce_tradingsymbol
                if pe_tradingsymbol:
                    put_delta = pe.delta * 100
                    put_sigma = pe.sigma * 100
                    pe_print = pe_tradingsymbol

                total_delta = call_delta + put_delta
                total_sigma = call_sigma + put_sigma

                print(timezone.localtime().replace(microsecond=0))
                if cache.get(f"{self.strategy}_{idx}_one_side_exit_hold", 0):
                    print("Hold Marked")
                print("INDEX:", idx)
                print(f"{Fore.GREEN}{ce_print} {Fore.RED}{pe_print}{Fore.WHITE}")
                print(
                    f"{Fore.GREEN}CALL DELTA: {round(call_delta, 2)} {Fore.RED}PUT DELTA: {round(put_delta, 2)} {Fore.WHITE}= Total: {round(total_delta, 2)}"  # noqa: E501
                )
                print(
                    f"{Fore.GREEN}CALL IV: {round(call_sigma, 2)} {Fore.RED}PUT IV: {round(put_sigma, 2)} {Fore.WHITE}= Total: {round(total_sigma, 2)}"  # noqa: E501
                )

                if idx == self.no_of_strategy - 1:
                    total_call_delta = 0
                    total_put_delta = 0

                    for _, v in tradingsymbol.items():
                        if v["ce_tradingsymbol"]:
                            total_call_delta += (
                                instruments[instruments["tradingsymbol"] == v["ce_tradingsymbol"]].iloc[0].delta
                            )  # noqa #501
                        if v["pe_tradingsymbol"]:
                            total_put_delta += (
                                instruments[instruments["tradingsymbol"] == v["pe_tradingsymbol"]].iloc[0].delta
                            )  # noqa #501

                    total_call_delta = total_call_delta / self.no_of_strategy
                    total_put_delta = total_put_delta / self.no_of_strategy
                    print()
                    print(
                        Fore.GREEN + f"TOTAL CALL DELTA: {round(total_call_delta * 100, 2)}",
                        Fore.RED + f"TOTAL PUT DELTA: {round(total_put_delta * 100 ,2)}" + Fore.WHITE,
                        f"CE + PE TOTAL DELTA: {round((total_call_delta + total_put_delta) * 100, 2)}",
                    )
                    print()
                    del total_call_delta, total_put_delta
                del ce_tradingsymbol, pe_tradingsymbol

                if (
                    timezone.localtime().time() > self.exit_time
                    or self.strategy not in cache.get("deployed_strategies", {}).keys()
                ):
                    exit_trigger = True
                    break
                cache.set(f"{self.strategy}_tradingsymbol", tradingsymbol)
                await self.save_order_in_db(user_order_data, self.user_params)

                # sleep for next sleep_timeth second
                ct = timezone.localtime()
                if ct.second % self.sleep_time == 0:
                    diff = (
                        ct.replace(microsecond=0) + dt.timedelta(seconds=self.sleep_time) - timezone.localtime()
                    ).total_seconds()
                else:
                    diff = (
                        ct.replace(second=((ct.second // self.sleep_time) * self.sleep_time), microsecond=0)
                        + dt.timedelta(seconds=self.sleep_time)
                        - timezone.localtime()
                    ).total_seconds()
                await asyncio.sleep(diff)
            print()
            if exit_trigger:
                break

        await self.exit_algo()

    async def user_entry(self, user_param_user_obj):
        instruments = self.get_greeks_instruments()
        user_exists = False
        for _, row in enumerate(self.user_params):
            if row["user"] == user_param_user_obj["user"]:
                user_exists = True
                break
        if self.strategy in (cache.get("deployed_strategies", {})).keys() and not user_exists:
            buy_pending, sell_pending = [], []

            tradingsymbol: dict = cache.get(f"{self.strategy}_tradingsymbol", {})
            for idx, row in tradingsymbol.items():
                if not row["ce_exit_one_side"]:
                    ce = instruments[(instruments["tradingsymbol"] == row["ce_tradingsymbol"])].iloc[0]
                    sell_pending.append(
                        {
                            "strike": ce.strike,
                            "option_type": "CE",
                            "expected_price": ce.last_price,
                            "expected_time": timezone.localtime(),
                            "idx": idx,
                            "reason": "EXIT CE - EXIT ALGO",
                        }
                    )
                    del ce

                if not row["pe_exit_one_side"]:
                    pe = instruments[(instruments["tradingsymbol"] == row["pe_tradingsymbol"])].iloc[0]
                    sell_pending.append(
                        {
                            "strike": pe.strike,
                            "option_type": "PE",
                            "expected_price": pe.last_price,
                            "expected_time": timezone.localtime(),
                            "idx": idx,
                            "reason": "EXIT PE - EXIT ALGO",
                        }
                    )

            user_order_data = await asyncio.gather(
                *[
                    self.place_orders(
                        user=user_param_user_obj,
                        buy_pending=buy_pending,
                        sell_pending=sell_pending,
                    )
                ]
            )

        self.save_order_in_db(user_order_data, self.user_params)
        self.user_params.append(user_param_user_obj)

        strategy_list = cache.get("deployed_strategies", {})
        strategy_list[self.strategy] = {
            "user_params": self.user_params,
            "no_of_strategy": self.no_of_strategy,
        }
        cache.set("deployed_strategies", strategy_list)
        cache.set(f"{self.strategy}_tradingsymbol", tradingsymbol)

        await send_notifications(
            self.opt_strategy.strategy_name.upper(),
            f"{user_param_user_obj['user'].username} ALGO ENTRED!".upper(),
            "alert-success",
        )

    async def exit_user_algo(self, user):
        instruments = self.get_greeks_instruments()
        user_exists = False
        user_index = None
        user_param_user_obj = None
        for user_idx, row in enumerate(self.user_params):
            if row["user"] == user:
                user_exists = True
                user_index = user_idx
                user_param_user_obj = row
                break
        if self.strategy in (cache.get("deployed_strategies", {})).keys() and user_exists:
            buy_pending, sell_pending = [], []

            tradingsymbol: dict = cache.get(f"{self.strategy}_tradingsymbol", {})
            for idx, row in tradingsymbol.items():
                if not row["ce_exit_one_side"]:
                    ce = instruments[(instruments["tradingsymbol"] == row["ce_tradingsymbol"])].iloc[0]
                    buy_pending.append(
                        {
                            "strike": ce.strike,
                            "option_type": "CE",
                            "expected_price": ce.last_price,
                            "expected_time": timezone.localtime(),
                            "idx": idx,
                            "reason": "EXIT CE - EXIT ALGO",
                        }
                    )
                    del ce

                if not row["pe_exit_one_side"]:
                    pe = instruments[(instruments["tradingsymbol"] == row["pe_tradingsymbol"])].iloc[0]
                    buy_pending.append(
                        {
                            "strike": pe.strike,
                            "option_type": "PE",
                            "expected_price": pe.last_price,
                            "expected_time": timezone.localtime(),
                            "idx": idx,
                            "reason": "EXIT PE - EXIT ALGO",
                        }
                    )

            user_order_data = await asyncio.gather(
                *[
                    self.place_orders(
                        user=user_param_user_obj,
                        buy_pending=buy_pending,
                        sell_pending=sell_pending,
                    )
                ]
            )

            self.save_order_in_db(user_order_data, self.user_params)
            self.user_params.pop(user_index)

            if self.user_params:
                strategy_list = cache.get("deployed_strategies", {})
                strategy_list[self.strategy] = {
                    "user_params": self.user_params,
                    "no_of_strategy": self.no_of_strategy,
                }
                cache.set("deployed_strategies", strategy_list)
                cache.set(f"{self.strategy}_tradingsymbol", tradingsymbol)
            else:
                cache.set(f"{self.strategy}_tradingsymbol", {})
                strategy_list = cache.get("deployed_strategies", {})
                del strategy_list[self.strategy]
                cache.set("deployed_strategies", strategy_list)
                cache.set(f"{self.strategy}_hold", False)

    async def exit_algo(self):
        instruments = self.get_greeks_instruments()
        if self.strategy in (cache.get("deployed_strategies", {})).keys():
            buy_pending, sell_pending = [], []

            tradingsymbol: dict = cache.get(f"{self.strategy}_tradingsymbol", {})
            for idx, row in tradingsymbol.items():
                if not row["ce_exit_one_side"]:
                    ce = instruments[(instruments["tradingsymbol"] == row["ce_tradingsymbol"])].iloc[0]
                    buy_pending.append(
                        {
                            "strike": ce.strike,
                            "option_type": "CE",
                            "expected_price": ce.last_price,
                            "expected_time": timezone.localtime(),
                            "idx": idx,
                            "reason": "EXIT CE - EXIT ALGO",
                        }
                    )
                    del ce

                if not row["pe_exit_one_side"]:
                    pe = instruments[(instruments["tradingsymbol"] == row["pe_tradingsymbol"])].iloc[0]
                    buy_pending.append(
                        {
                            "strike": pe.strike,
                            "option_type": "PE",
                            "expected_price": pe.last_price,
                            "expected_time": timezone.localtime(),
                            "idx": idx,
                            "reason": "EXIT PE - EXIT ALGO",
                        }
                    )

            user_order_data = await asyncio.gather(
                *[
                    self.place_orders(
                        user=user,
                        buy_pending=buy_pending,
                        sell_pending=sell_pending,
                    )
                    for user in self.user_params
                ]
            )

            self.save_order_in_db(user_order_data, self.user_params)

            cache.set(f"{self.strategy}_tradingsymbol", {})
            strategy_list = cache.get("deployed_strategies", {})
            del strategy_list[self.strategy]
            cache.set("deployed_strategies", strategy_list)
            cache.set(f"{self.strategy}_hold", False)
            await send_notifications(
                self.opt_strategy.strategy_name.upper(),
                "ALGO EXITED!",
                "alert-danger",
            )

    async def get_ce_reentry(self, idx, instruments, pe, now_time=timezone.localtime()):
        instruments = self.get_greeks_instruments()

        buy_pending = []
        sell_pending = []

        pe_reentry_strike, _ = await self.find_strike(instruments, -self.shift_min_delta_entry, "PE", "<", "delta")
        if (
            (pe["delta"] > -self.shift_min_delta and now_time <= self.expiry_check_timestamp)
            or pe["delta"] < -self.shift_max_delta
        ) and pe_reentry_strike != pe["strike"]:
            ce_reentry_strike, _ = await self.find_strike(instruments, self.shift_min_delta_entry, "CE", "<", "delta")
            if ce_reentry_strike and pe_reentry_strike:
                buy_pending.append(
                    {
                        "strike": pe.strike,
                        "option_type": "PE",
                        "expected_price": pe.last_price,
                        "expected_time": timezone.localtime(),
                        "idx": idx,
                        "reason": "EXIT PE - RESTRUCTURING",
                    }
                )
                ce = instruments[
                    (instruments["strike"] == ce_reentry_strike) & (instruments["instrument_type"] == "CE")
                ].iloc[0]
                pe = instruments[
                    (instruments["strike"] == pe_reentry_strike) & (instruments["instrument_type"] == "PE")
                ].iloc[0]
                sell_pending.extend(
                    [
                        {
                            "strike": pe.strike,
                            "option_type": "PE",
                            "expected_price": pe.last_price,
                            "expected_time": timezone.localtime(),
                            "idx": idx,
                            "reason": "ENTERING PE - RESTRUCTURING",
                        },
                        {
                            "strike": ce.strike,
                            "option_type": "CE",
                            "expected_price": ce.last_price,
                            "expected_time": timezone.localtime(),
                            "idx": idx,
                            "reason": "ENTERING CE - RESTRUCTURING",
                        },
                    ]
                )

                return buy_pending, sell_pending, ce["tradingsymbol"], pe["tradingsymbol"], False, False
        else:
            ce_reentry_strike, _ = await self.find_strike(instruments, -pe["delta"], "CE", "<", "delta")
            if ce_reentry_strike:
                ce = instruments[
                    (instruments["strike"] == ce_reentry_strike) & (instruments["instrument_type"] == "CE")
                ].iloc[0]
                sell_pending.append(
                    {
                        "strike": ce.strike,
                        "option_type": "CE",
                        "expected_price": ce.last_price,
                        "expected_time": timezone.localtime(),
                        "idx": idx,
                        "reason": "ENTERING CE",
                    }
                )

                return buy_pending, sell_pending, ce["tradingsymbol"], pe["tradingsymbol"], False, False

        return buy_pending, sell_pending, None, pe["tradingsymbol"], True, True

    async def get_pe_reentry(self, idx, instruments, ce, now_time=timezone.localtime()):
        instruments = self.get_greeks_instruments()

        buy_pending = []
        sell_pending = []

        ce_reentry_strike, _ = await self.find_strike(instruments, self.shift_min_delta_entry, "CE", ">", "delta")
        if (
            (ce["delta"] < self.shift_min_delta and now_time <= self.expiry_check_timestamp)
            or ce["delta"] > self.shift_max_delta
        ) and ce_reentry_strike != ce["strike"]:
            pe_reentry_strike, _ = await self.find_strike(instruments, -self.shift_min_delta_entry, "PE", ">", "delta")
            if ce_reentry_strike and pe_reentry_strike:
                buy_pending.append(
                    {
                        "strike": ce.strike,
                        "option_type": "CE",
                        "expected_price": ce.last_price,
                        "expected_time": timezone.localtime(),
                        "idx": idx,
                        "reason": "EXIT CE - RESTRUCTURING",
                    }
                )
                ce = instruments[
                    (instruments["strike"] == ce_reentry_strike) & (instruments["instrument_type"] == "CE")
                ].iloc[0]
                pe = instruments[
                    (instruments["strike"] == pe_reentry_strike) & (instruments["instrument_type"] == "PE")
                ].iloc[0]
                sell_pending.extend(
                    [
                        {
                            "strike": pe.strike,
                            "option_type": "PE",
                            "expected_price": pe.last_price,
                            "expected_time": timezone.localtime(),
                            "idx": idx,
                            "reason": "ENTERING PE - RESTRUCTURING",
                        },
                        {
                            "strike": ce.strike,
                            "option_type": "CE",
                            "expected_price": ce.last_price,
                            "expected_time": timezone.localtime(),
                            "idx": idx,
                            "reason": "ENTERING CE - RESTRUCTURING",
                        },
                    ]
                )

                return buy_pending, sell_pending, ce["tradingsymbol"], pe["tradingsymbol"], False, False

        else:
            pe_reentry_strike, _ = await self.find_strike(instruments, -ce["delta"], "PE", ">", "delta")
            if pe_reentry_strike:
                pe = instruments[
                    (instruments["strike"] == pe_reentry_strike) & (instruments["instrument_type"] == "PE")
                ].iloc[0]
                sell_pending.append(
                    {
                        "strike": pe.strike,
                        "option_type": "PE",
                        "expected_price": pe.last_price,
                        "expected_time": timezone.localtime(),
                        "idx": idx,
                        "reason": "ENTERING PE",
                    }
                )

                return buy_pending, sell_pending, ce["tradingsymbol"], pe["tradingsymbol"], False, False

        return buy_pending, sell_pending, ce["tradingsymbol"], None, True, True

    async def get_ce_exit(self, idx, ce):
        return (
            [
                {
                    "strike": ce.strike,
                    "option_type": "CE",
                    "expected_price": ce.last_price,
                    "expected_time": timezone.localtime(),
                    "idx": idx,
                    "reason": "EXIT CE",
                }
            ],
            None,
            True,
            True,
        )

    async def get_pe_exit(self, idx, pe):
        return (
            [
                {
                    "strike": pe.strike,
                    "option_type": "PE",
                    "expected_price": pe.last_price,
                    "expected_time": timezone.localtime(),
                    "idx": idx,
                    "reason": "EXIT PE",
                }
            ],
            None,
            True,
            True,
        )

    def one_side_without_check_exit(self, idx, row, exited_one_side, ce_exit_one_side, pe_exit_one_side, parameters):
        change = parameters["change"] / 100
        reentry_oi = parameters["reentry_oi"] / 100
        make_ce_exit = make_pe_exit = False
        ce_reentry = pe_reentry = False
        if (
            not exited_one_side
            and row["timestamp"] < self.oneside_check_timestamp
            and row["timestamp"] < self.expiry_check_timestamp
            and not cache.get(f"{self.strategy}_{idx}_one_side_exit_hold", 0)
        ):
            if row["ce_oi_change"] - row["pe_oi_change"] < change:
                make_ce_exit = True

            elif row["pe_oi_change"] - row["ce_oi_change"] < change:
                make_pe_exit = True
        else:
            if ce_exit_one_side and row["ce_oi_change"] - row["pe_oi_change"] > reentry_oi:
                ce_reentry = True
            elif pe_exit_one_side and row["pe_oi_change"] - row["ce_oi_change"] > reentry_oi:
                pe_reentry = True
        return make_ce_exit, make_pe_exit, ce_reentry, pe_reentry

    def one_side_check_exit(self, idx, row, exited_one_side, ce_exit_one_side, pe_exit_one_side, parameters):
        change = parameters["change"] / 100
        reentry_oi = parameters["reentry_oi"] / 100
        less_than = parameters["less_than"] / 100
        make_ce_exit = make_pe_exit = False
        ce_reentry = pe_reentry = False
        if (
            not exited_one_side
            and row["timestamp"] < self.oneside_check_timestamp
            and row["timestamp"] < self.expiry_check_timestamp
            and not cache.get(f"{self.strategy}_{idx}_one_side_exit_hold", 0)
        ):
            if row["ce_oi_change"] - row["pe_oi_change"] < change and row["ce_oi_change"] < less_than:
                make_ce_exit = True

            elif row["pe_oi_change"] - row["ce_oi_change"] < change and row["pe_oi_change"] < less_than:
                make_pe_exit = True
        else:
            if ce_exit_one_side and row["ce_oi_change"] - row["pe_oi_change"] > reentry_oi:
                ce_reentry = True
            elif pe_exit_one_side and row["pe_oi_change"] - row["ce_oi_change"] > reentry_oi:
                pe_reentry = True

        return make_ce_exit, make_pe_exit, ce_reentry, pe_reentry

    async def manual_exit(self, idx, option_type):
        await self.initiate()

        tradingsymbol = cache.get(f"{self.strategy}_tradingsymbol", {})
        tradingsymbol_temp = tradingsymbol.copy()
        instruments = self.get_greeks_instruments()
        row = tradingsymbol_temp[idx]

        buy_pending = []
        sell_pending = []

        if option_type == "CE" and not row["exited_one_side"]:
            ce = instruments[(instruments["tradingsymbol"] == row["ce_tradingsymbol"])].iloc[0]

            buy_pending, ce_tradingsymbol, exited_one_side, ce_exit_one_side = await self.get_ce_exit(idx, ce)

            tradingsymbol_temp[idx]["ce_tradingsymbol"] = ce_tradingsymbol
            tradingsymbol_temp[idx]["exited_one_side"] = exited_one_side
            tradingsymbol_temp[idx]["ce_exit_one_side"] = ce_exit_one_side

        elif option_type == "PE" and not row["exited_one_side"]:
            pe = instruments[(instruments["tradingsymbol"] == row["pe_tradingsymbol"])].iloc[0]

            buy_pending, pe_tradingsymbol, exited_one_side, pe_exit_one_side = await self.get_pe_exit(idx, pe)

            tradingsymbol_temp[idx]["pe_tradingsymbol"] = pe_tradingsymbol
            tradingsymbol_temp[idx]["exited_one_side"] = exited_one_side
            tradingsymbol_temp[idx]["pe_exit_one_side"] = pe_exit_one_side

        user_order_data = await asyncio.gather(
            *[
                self.place_orders(
                    user=user,
                    buy_pending=buy_pending,
                    sell_pending=sell_pending,
                )
                for user in self.user_params
            ]
        )

        tradingsymbol = tradingsymbol_temp.copy()
        cache.set(f"{self.strategy}_tradingsymbol", tradingsymbol)
        await self.save_order_in_db(user_order_data, self.user_params)

    async def manual_reentry(self, idx):
        await self.initiate()

        tradingsymbol = cache.get(f"{self.strategy}_tradingsymbol", {})
        instruments = self.get_greeks_instruments()
        row = tradingsymbol[idx]
        now_time = timezone.localtime()

        if row["exited_one_side"]:
            if row["ce_exit_one_side"]:
                pe = instruments[(instruments["tradingsymbol"] == row["pe_tradingsymbol"])].iloc[0]

                (
                    buy_pending,
                    sell_pending,
                    ce_tradingsymbol,
                    pe_tradingsymbol,
                    exited_one_side,
                    ce_exit_one_side,
                ) = await self.get_ce_reentry(idx, instruments, pe, now_time)

                user_order_data = await asyncio.gather(
                    *[
                        self.place_orders(
                            user=user,
                            buy_pending=buy_pending,
                            sell_pending=sell_pending,
                        )
                        for user in self.user_params
                    ]
                )
                tradingsymbol[idx]["ce_tradingsymbol"] = ce_tradingsymbol
                tradingsymbol[idx]["pe_tradingsymbol"] = pe_tradingsymbol
                tradingsymbol[idx]["exited_one_side"] = exited_one_side
                tradingsymbol[idx]["ce_exit_one_side"] = ce_exit_one_side
                cache.set(f"{self.strategy}_tradingsymbol", tradingsymbol)
                cache.set(f"{self.strategy}_{idx}_one_side_exit_hold", 1)

                await self.save_order_in_db(user_order_data, self.user_params)

            elif row["pe_exit_one_side"]:
                ce = instruments[(instruments["tradingsymbol"] == row["ce_tradingsymbol"])].iloc[0]

                (
                    buy_pending,
                    sell_pending,
                    ce_tradingsymbol,
                    pe_tradingsymbol,
                    exited_one_side,
                    pe_exit_one_side,
                ) = await self.get_pe_reentry(idx, instruments, ce, now_time)

                user_order_data = await asyncio.gather(
                    *[
                        self.place_orders(
                            user=user,
                            buy_pending=buy_pending,
                            sell_pending=sell_pending,
                        )
                        for user in self.user_params
                    ]
                )

                tradingsymbol[idx]["ce_tradingsymbol"] = ce_tradingsymbol
                tradingsymbol[idx]["pe_tradingsymbol"] = pe_tradingsymbol
                tradingsymbol[idx]["exited_one_side"] = exited_one_side
                tradingsymbol[idx]["pe_exit_one_side"] = pe_exit_one_side
                cache.set(f"{self.strategy}_tradingsymbol", tradingsymbol)
                cache.set(f"{self.strategy}_{idx}_one_side_exit_hold", 1)

                await self.save_order_in_db(user_order_data, self.user_params)

    async def manual_shifting(self, idx):
        await self.initiate()

        tradingsymbol = cache.get(f"{self.strategy}_tradingsymbol", {})
        tradingsymbol_temp = tradingsymbol.copy()
        instruments = self.get_greeks_instruments()
        row = tradingsymbol_temp[idx]
        now_time = timezone.localtime()

        if not row["exited_one_side"]:
            pe = instruments[(instruments["tradingsymbol"] == row["pe_tradingsymbol"])].iloc[0]
            ce = instruments[(instruments["tradingsymbol"] == row["ce_tradingsymbol"])].iloc[0]
            (buy_pending, sell_pending, ce_tradingsymbol, pe_tradingsymbol) = await self.check_shifting_orders(
                instruments, idx, ce, pe, 0.1, now_time
            )

            user_order_data = await asyncio.gather(
                *[
                    self.place_orders(
                        user=user,
                        buy_pending=buy_pending,
                        sell_pending=sell_pending,
                    )
                    for user in self.user_params
                ]
            )

            await self.save_order_in_db(user_order_data, self.user_params)
            tradingsymbol[idx]["ce_tradingsymbol"] = ce_tradingsymbol
            tradingsymbol[idx]["pe_tradingsymbol"] = pe_tradingsymbol
            cache.set(f"{self.strategy}_tradingsymbol", tradingsymbol)

    async def manual_shift_single_strike(self, idx, option_type, points):
        await self.initiate()

        tradingsymbol = cache.get(f"{self.strategy}_tradingsymbol", {})
        tradingsymbol_temp = tradingsymbol.copy()
        instruments = self.get_greeks_instruments()
        row = tradingsymbol_temp[idx]

        buy_pending, sell_pending = [], []

        if option_type == "CE" and not row["ce_exit_one_side"]:
            ce = instruments[(instruments["tradingsymbol"] == row["ce_tradingsymbol"])].iloc[0]
            buy_pending.append(
                {
                    "strike": ce.strike,
                    "option_type": "CE",
                    "expected_price": ce.last_price,
                    "expected_time": timezone.localtime(),
                    "idx": idx,
                    "reason": "MANUAL SINGLE SIDE SHIFT - EXIT",
                }
            )
            ce = instruments[
                (instruments["strike"] == (ce["strike"] - points)) & (instruments["instrument_type"] == "CE")
            ].iloc[
                0
            ]  # noqa E501
            sell_pending.append(
                {
                    "strike": ce.strike,
                    "option_type": "CE",
                    "expected_price": ce.last_price,
                    "expected_time": timezone.localtime(),
                    "idx": idx,
                    "reason": "MANUAL SINGLE SIDE SHIFT - ENTRY",
                }
            )

            tradingsymbol_temp[idx]["ce_tradingsymbol"] = ce["tradingsymbol"]

        elif option_type == "PE" and not row["pe_exit_one_side"]:
            pe = instruments[(instruments["tradingsymbol"] == row["pe_tradingsymbol"])].iloc[0]
            buy_pending.append(
                {
                    "strike": pe.strike,
                    "option_type": "PE",
                    "expected_price": pe.last_price,
                    "expected_time": timezone.localtime(),
                    "idx": idx,
                    "reason": "MANUAL SINGLE SIDE SHIFT - EXIT",
                }
            )
            pe = instruments[
                (instruments["strike"] == (pe["strike"] + points)) & (instruments["instrument_type"] == "PE")
            ].iloc[
                0
            ]  # noqa E501
            sell_pending.append(
                {
                    "strike": pe.strike,
                    "option_type": "PE",
                    "expected_price": pe.last_price,
                    "expected_time": timezone.localtime(),
                    "idx": idx,
                    "reason": "MANUAL SINGLE SIDE SHIFT - ENTRY",
                }
            )

            tradingsymbol_temp[idx]["pe_tradingsymbol"] = pe["tradingsymbol"]

        user_order_data = await asyncio.gather(
            *[
                self.place_orders(
                    user=user,
                    buy_pending=buy_pending,
                    sell_pending=sell_pending,
                )
                for user in self.user_params
            ]
        )
        await self.save_order_in_db(user_order_data, self.user_params)
        tradingsymbol = tradingsymbol_temp.copy()
        cache.set(f"{self.strategy}_tradingsymbol", tradingsymbol)

    async def release_one_side_exit_hold(self, idx):
        cache.set(f"{self.strategy}_{idx}_one_side_exit_hold", 0)

    async def one_side_exit_hold(self, idx):
        cache.set(f"{self.strategy}_{idx}_one_side_exit_hold", 1)
