import datetime as dt
from datetime import date

import numpy as np
import pandas as pd
from asgiref.sync import async_to_sync
from dateutil.parser import parse
from django.core.cache import cache
from django.utils import timezone

from apps.integration.models import ZerodhaApi
from trading.settings import env
from utils.broker.kiteext import KiteExt


def on_connect(ws, response):
    ws.subscribe(ws.instrument_tokens)
    ws.set_mode(ws.MODE_FULL, ws.instrument_tokens)


def on_ticks(ws, ticks):
    instruments = cache.get("FN_OPTION_INSTRUMENTS")
    df = pd.DataFrame(ticks)
    if not df.empty:
        set_instrument_cache(df, instruments)
    if timezone.localtime().time() > dt.time(15, 30):
        ws.unsubscribe(ws.instrument_tokens)
        ws.close()


def set_instrument_cache(df, instruments):
    df = df[
        [
            "instrument_token",
            "last_price",
            "exchange_timestamp",
            "last_trade_time",
            "oi",
        ]
    ].copy()
    df.rename(columns={"instrument_token": "kite_instrument_token"}, inplace=True)
    instruments = instruments.merge(df, how="left", on="kite_instrument_token")
    instruments["last_price"] = instruments["last_price_y"].fillna(instruments["last_price_x"])
    instruments["exchange_timestamp"] = instruments["exchange_timestamp_y"].fillna(instruments["exchange_timestamp_x"])
    instruments["last_trade_time"] = instruments["last_trade_time_y"].fillna(instruments["last_trade_time_x"])
    instruments["oi"] = instruments["oi_y"].fillna(instruments["oi_x"])
    instruments.drop(
        columns=[
            "last_price_x",
            "last_price_y",
            "exchange_timestamp_x",
            "exchange_timestamp_y",
            "last_trade_time_x",
            "last_trade_time_y",
            "oi_x",
            "oi_y",
        ],
        inplace=True,
    )
    cache.set("OPTION_INSTRUMENTS", instruments)


def on_close(ws, code, reason):
    if not code and not reason:
        ws.stop()


def get_instruments(kite: KiteExt, exchange=None, name=None, expiry=None):
    try:
        instrument = pd.DataFrame(async_to_sync(kite.instruments)(exchange=exchange))
        if expiry:
            instrument = instrument[(instrument["name"] == name) & (instrument["expiry"] == expiry)].reset_index(
                drop=True
            )
        else:
            instrument = instrument[(instrument["name"] == name)].reset_index(drop=True)
    except Exception:
        return get_instruments(kite, exchange, name, expiry)
    else:
        return instrument[instrument["instrument_type"].isin(["CE", "PE"])]


def get_kotak_neo_instruments():
    kotak_neo_columns = [
        "pSymbol",
        "pGroup",
        "pExchSeg",
        "pInstType",
        "pSymbolName",
        "pTrdSymbol",
        "pOptionType",
        "pScripRefKey",
        "pISIN",
        "pAssetCode",
        "pSubGroup",
        "pCombinedSymbol",
        "pDesc",
        "pAmcCode",
        "pContractId",
        "dTickSize",
        "lLotSize",
        "lExpiryDate",
        "lMultiplier",
        "lPrecision",
        "dStrikePrice",
        "pExchange",
        "pInstName",
        "pExpiryDate",
        "pIssueDate",
        "pMaturityDate",
        "pListingDate",
        "pNoDelStartDate",
        "pNoDelEndDate",
        "pBookClsStartDate",
        "pBookClsEndDate",
        "pRecordDate",
        "pCreditRating",
        "pReAdminDate",
        "pExpulsionDate",
        "pLocalUpdateTime",
        "pDeliveryUnits",
        "pPriceUnits",
        "pLastTradingDate",
        "pTenderPeridEndDate",
        "pTenderPeridStartDate",
        "pSellVarMargin",
        "pBuyVarMargin",
        "pInstrumentInfo",
        "pRemarksText",
        "pSegment",
        "pNav",
        "pNavDate",
        "pMfAmt",
        "pSipSecurity",
        "pFaceValue",
        "pTrdUnits",
        "pExerciseStartDate",
        "pExerciseEndDate",
        "pElmMargin",
        "pVarMargin",
        "pTotProposedLimitValue",
        "pScripBasePrice",
        "pSettlementType",
        "pCurrectionTime",
        "iPermittedToTrade",
        "iBoardLotQty",
        "iMaxOrderSize",
        "iLotSize",
        "dOpenInterest",
        "dHighPriceRange",
        "dLowPriceRange",
        "dPriceNum",
        "dGenDen",
        "dGenNum",
        "dPriceQuatation",
        "dIssuerate",
        "dPriceDen",
        "dWarningQty",
        "dIssueCapital",
        "dExposureMargin",
        "dMinRedemptionQty",
        "lFreezeQty",
    ]

    kotak_neo_instruments = pd.read_csv(
        f"https://lapi.kotaksecurities.com/wso2-scripmaster/v1/prod/{str(date.today())}/nse_fo.csv",
        header=0,
        names=kotak_neo_columns,
    )
    kotak_neo_instruments = kotak_neo_instruments[
        kotak_neo_instruments["pSymbolName"].isin(["NIFTY", "FINNIFTY"])
    ].reset_index(drop=True)
    kotak_neo_instruments = kotak_neo_instruments[["pSymbol", "pTrdSymbol", "lFreezeQty"]].copy()
    kotak_neo_instruments.columns = ["kotak_neo_instrument_token", "tradingsymbol", "freeze_qty"]
    kotak_neo_instruments["freeze_qty"] = kotak_neo_instruments["freeze_qty"] - 1
    kotak_neo_instruments = kotak_neo_instruments[
        ~(kotak_neo_instruments["kotak_neo_instrument_token"].astype(str).str.contains(" "))
    ].reset_index(drop=True)

    return kotak_neo_instruments


def get_kite_instruments():
    user = env("ZERODHA_WEBSOCKET_USER")
    zerodha = ZerodhaApi.objects.get(broker_api__user__username=user)

    kite = async_to_sync(KiteExt)(user_id=zerodha.userid, token=zerodha.session_token)
    kite_instruments = get_instruments(kite, exchange="NFO", name="FINNIFTY")
    kite_instruments.rename(columns={"instrument_token": "kite_instrument_token"}, inplace=True)

    return kite, kite_instruments


def get_kotak_sec_instruments():
    kotak_sec_instruments = pd.read_csv(
        "https://preferred.kotaksecurities.com/security/production/TradeApiInstruments_FNO_"
        + str(date.today().strftime("%d_%m_%Y"))
        + ".txt",
        delimiter="|",
    )
    kotak_sec_instruments = kotak_sec_instruments[
        (kotak_sec_instruments["segment"] == "FO")
        & (kotak_sec_instruments["instrumentName"] == "FINNIFTY")
        & (kotak_sec_instruments["optionType"].isin(["CE", "PE"]))
    ].reset_index(drop=True)
    kotak_sec_instruments = kotak_sec_instruments.rename(
        columns={
            "instrumentToken": "kotak_sec_instrument_token",
            "optionType": "instrument_type",
        }
    )
    expiry_map = {row: parse(row).date() for row in kotak_sec_instruments["expiry"].unique()}
    kotak_sec_instruments = kotak_sec_instruments[
        ["kotak_sec_instrument_token", "instrument_type", "expiry", "strike"]
    ]
    kotak_sec_instruments["expiry"] = kotak_sec_instruments["expiry"].apply(lambda x: expiry_map[x])

    return kotak_sec_instruments


def fn_option_connect_kws():
    tz = timezone.get_current_timezone()
    # Zerodha Instruments
    kite, kite_instruments = get_kite_instruments()

    # Kotak Neo Instruments
    kotak_neo_instruments = get_kotak_neo_instruments()

    # Kotak Securities Instruments
    kotak_sec_instruments = get_kotak_sec_instruments()
   

    # Merge Instruments
    instruments = pd.merge(kotak_neo_instruments, kite_instruments, on=["tradingsymbol"])
    instruments = pd.merge(instruments, kotak_sec_instruments, on=["expiry", "strike", "instrument_type"])
    

    expiry = sorted(instruments["expiry"].unique())[0]
    instruments = instruments[instruments["expiry"] == expiry].reset_index(drop=True)
    cache.set("EXPIRY", expiry)
    instruments = instruments[
        [
            "kotak_neo_instrument_token",
            "kotak_sec_instrument_token",
            "kite_instrument_token",
            "name",
            "tradingsymbol",
            "expiry",
            "strike",
            "instrument_type",
            "freeze_qty",
        ]
    ].copy()
    instruments["last_price"] = np.nan
    instruments["exchange_timestamp"] = np.nan
    instruments["last_trade_time"] = np.nan
    instruments["oi"] = np.nan
    instruments["expiry"] = instruments["expiry"].apply(lambda x: parse(f"{x} 15:30:00").replace(tzinfo=tz))
    instruments["str_expiry"] = instruments["expiry"].apply(lambda y: y.strftime("%d-%b-%Y").upper())

    kite_instrument_tokens = instruments["kite_instrument_token"].to_list()
    cache.set("FN_OPTION_INSTRUMENTS", instruments)

    kws = kite.kws()
    kws.instrument_tokens = kite_instrument_tokens

    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close
    kws.connect()
