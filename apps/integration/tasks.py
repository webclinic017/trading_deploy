import contextlib
import datetime as dt
import time

import numpy as np
import pandas as pd
from django.core.cache import cache
from django.utils import timezone

from apps.integration.kite_socket.bnf_option_kws import option_connect_kws
from apps.integration.kite_socket.bnf_spot_kws import spot_connect_kws
from apps.integration.kite_socket.finnifty_option import fn_option_connect_kws
from apps.integration.kite_socket.finnifty_spot import fn_spot_connect_kws
from apps.integration.kite_socket.nifty_option import nifty_option_connect_kws
from apps.integration.kite_socket.nifty_spot import nifty_spot_connect_kws
from apps.integration.models import KotakNeoApi, KotakSecuritiesApi, ZerodhaApi
from trading.celery import app
from utils.bs_greeks import find_greeks
from utils.telegram import send_message


@app.task(name="Save Zerodha", bind=True)
def login_zerodha(self):
    for zerodha in ZerodhaApi.objects.filter(broker_api__is_active=True):
        zerodha.save()


@app.task(name="Save Kotak Securities", bind=True)
def login_kotak(self):
    for kotak in KotakSecuritiesApi.objects.filter(broker_api__is_active=True):
        kotak.save()


@app.task(name="Save Kotak Neo", bind=True)
def login_kotak_neo(self):
    for neo in KotakNeoApi.objects.filter(broker_api__active=True):
        try:
            neo.save()
        except Exception as e:
            ct = timezone.localtime().replace(microsecond=0)
            neo.login_error = True
            neo.update_error = True
            neo.save()
            send_message(f"{ct} - {neo.broker_api.user.username} Login Kotak Neo Error {e}")


@app.task(name="Update Kotak Neo", bind=True)
def update_neo_token(self):
    for neo in KotakNeoApi.objects.filter(broker_api__active=True):
        try:
            neo.update_auth_token = True
            neo.save()
        except Exception as e:
            ct = timezone.localtime().replace(microsecond=0)
            neo.update_token_error = True
            neo.update_error = True
            neo.save()
            send_message(f"{ct} - {neo.broker_api.user.username} Update Kotak Neo Error {e}")


@app.task(name="Bank Nifty Spot Data", bind=True)
def banknifty_spot_data(self):
    spot_connect_kws()


@app.task(name="Bank Nifty Option Data", bind=True)
def banknifty_option_data(self):
    option_connect_kws()


@app.task(name="Fin Nifty Spot Data", bind=True)
def finnifty_spot_data(self):
    fn_spot_connect_kws()


@app.task(name="Fin Nifty Option Data", bind=True)
def finnifty_option_data(self):
    fn_option_connect_kws()


@app.task(name="Nifty Spot Data", bind=True)
def nifty_spot_data(self):
    nifty_spot_connect_kws()


@app.task(name="Nifty Option Data", bind=True)
def nifty_option_data(self):
    nifty_option_connect_kws()


@app.task(name="Bank Nifty Live Greeks", bind=True)
def banknifty_live_greeks(self):
    if timezone.localtime().time() < dt.time(9, 15, 2):
        ct = timezone.localtime()
        time.sleep((ct.replace(hour=9, minute=15, second=2, microsecond=0) - ct).total_seconds())

    while True:
        ct = timezone.localtime().replace(microsecond=0)
        if ct.time() >= dt.time(15, 30):
            break
        instruments = cache.get("OPTION_INSTRUMENTS")
        instruments["bnf_ltp"] = cache.get("BANKNIFTY_LTP")
        instruments["time_left"] = ((instruments["expiry"] - ct).dt.total_seconds() / 86400) / 365
        instruments["timestamp"] = ct
        (
            instruments["sigma"],
            instruments["delta"],
            instruments["theta"],
            instruments["gamma"],
            instruments["vega"],
        ) = np.vectorize(find_greeks)(
            instruments["last_price"],
            instruments["bnf_ltp"],
            instruments["strike"],
            instruments["time_left"],
            0.10,
            instruments["instrument_type"],
        )
        cache.set("OPTION_GREEKS_INSTRUMENTS", instruments)

        with contextlib.suppress(Exception):
            diff = (ct.replace(microsecond=0) + dt.timedelta(seconds=1) - timezone.localtime()).total_seconds()
            time.sleep(diff)


@app.task(name="Bank Nifty Save Snapshot Every 5 Second", bind=True)
def bank_nifty_save_snapshot_every_five_second(self):
    columns = [
        "timestamp",
        "kotak_neo_instrument_token",
        "kotak_sec_instrument_token",
        "kite_instrument_token",
        "tradingsymbol",
        "expiry",
        "strike",
        "instrument_type",
        "last_price",
        "exchange_timestamp",
        "last_trade_time",
        "oi",
        "bnf_ltp",
        "atm",
        "sigma",
        "delta",
    ]
    cache.set("BNF_SNAPSHOT_5SEC", pd.DataFrame(columns=columns))
    cache.set(
        "LIVE_BNF_PCR",
        pd.DataFrame(
            columns=[
                "timestamp",
                "pe_total_oi",
                "ce_total_oi",
                "pcr",
                "strike",
                "ce_iv",
                "pe_iv",
                "total_iv",
                "ce_premium",
                "pe_premium",
                "total_premium",
            ]
        ),
    )

    if timezone.localtime().time() < dt.time(9, 15, 4):
        ct = timezone.localtime()
        time.sleep((ct.replace(hour=9, minute=15, second=4, microsecond=0) - ct).total_seconds())
    else:
        ct = timezone.localtime()
        if (ct + dt.timedelta(seconds=1)).second % 5 == 0:
            diff = (ct.replace(microsecond=0) + dt.timedelta(seconds=9) - timezone.localtime()).total_seconds()
        else:
            diff = (
                ct.replace(second=((ct.second // 5) * 5), microsecond=0)
                + dt.timedelta(seconds=4)
                - timezone.localtime()
            ).total_seconds()

        with contextlib.suppress(Exception):
            time.sleep(diff)
    while True:
        ct = timezone.localtime().replace(microsecond=0)
        if ct.time() >= dt.time(15, 30):
            break
        instruments = cache.get("OPTION_INSTRUMENTS")
        ltp = cache.get("BANKNIFTY_LTP")
        instruments["bnf_ltp"] = ltp
        instruments["timestamp"] = ct.replace(microsecond=0)
        instruments["time_left"] = (
            (instruments["expiry"] - instruments["timestamp"]).dt.total_seconds() / 86400
        ) / 365
        (
            instruments["sigma"],
            instruments["delta"],
            instruments["theta"],
            instruments["gamma"],
            instruments["vega"],
        ) = np.vectorize(find_greeks)(
            instruments["last_price"],
            instruments["bnf_ltp"],
            instruments["strike"],
            instruments["time_left"],
            0.10,
            instruments["instrument_type"],
        )
        instruments["atm"] = (instruments["bnf_ltp"] / 100).round(0) * 100
        bnf_snapshot_5sec = cache.get("BNF_SNAPSHOT_5SEC", pd.DataFrame(columns=columns))
        print(instruments)
        bnf_snapshot_5sec = pd.concat([bnf_snapshot_5sec, instruments[columns]], ignore_index=True)

        pe_total_oi = int(instruments[instruments["instrument_type"] == "PE"].oi.sum())
        ce_total_oi = int(instruments[instruments["instrument_type"] == "CE"].oi.sum())

        live_pcr = cache.get(
            "LIVE_BNF_PCR",
            pd.DataFrame(columns=["timestamp", "pe_total_oi", "ce_total_oi", "pcr"]),
        )

        atm = float(round(ltp / 100) * 100)
        ce = instruments[(instruments["instrument_type"] == "CE") & (instruments["strike"] == atm)].iloc[0]
        pe = instruments[(instruments["instrument_type"] == "PE") & (instruments["strike"] == atm)].iloc[0]
        ce_iv = ce.sigma
        pe_iv = pe.sigma
        ce_premium = ce.last_price
        pe_premium = pe.last_price

        df = pd.DataFrame(
            [
                {
                    "timestamp": ct,
                    "pe_total_oi": pe_total_oi,
                    "ce_total_oi": ce_total_oi,
                    "pcr": pe_total_oi / ce_total_oi if ce_total_oi > 0 else np.inf,
                    "strike": atm,
                    "ce_iv": ce_iv,
                    "pe_iv": pe_iv,
                    "total_iv": round(ce_iv + pe_iv, 2),
                    "ce_premium": ce_premium,
                    "pe_premium": pe_premium,
                    "total_premium": round(ce_premium + pe_premium, 2),
                }
            ]
        )

        cache.set("BNF_SNAPSHOT_5SEC", bnf_snapshot_5sec)
        cache.set("LIVE_BNF_PCR", pd.concat([live_pcr, df], ignore_index=True))

        if (ct + dt.timedelta(seconds=1)).second % 5 == 0:
            diff = (
                ct.replace(second=((ct.second // 5) * 5), microsecond=0)
                + dt.timedelta(seconds=9)
                - timezone.localtime()
            ).total_seconds()
        else:
            diff = (
                ct.replace(second=((ct.second // 5) * 5), microsecond=0)
                + dt.timedelta(seconds=4)
                - timezone.localtime()
            ).total_seconds()

        with contextlib.suppress(Exception):
            time.sleep(diff)
