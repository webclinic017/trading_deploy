import asyncio
import base64
import datetime as dt
import json
import os
from urllib.parse import quote
from urllib.request import urlretrieve

import pandas as pd
from aiohttp import ClientSession
from dateutil.relativedelta import relativedelta
from django.utils.timezone import localdate

from utils.async_obj import AsyncObj
from utils.http_request import http_request


class KotakNeoApiError(Exception):
    def __init__(self, code, http_code, message, description):
        self.code = code
        self.http_code = http_code
        self.message = message
        self.description = description
        super().__init__(self.message)


class KotakNeoApi(AsyncObj):
    ORDER_STATUS = {
        "rejected": "REJECTED",
        "pending": "PENDING",
        "completed": "COMPLETED",
        "cancelled": "CANCELLED",
    }

    async def __ainit__(
        self,
        neo_fin_key: str,
        consumer_key: str,
        consumer_secret: str,
        access_token: str = None,
        sid: str | None = None,
        auth: str | None = None,
        hs_server_id: str | None = None,
        rid: str | None = None,
        host: str = "https://gw-napi.kotaksecurities.com",
    ):
        self.neo_fin_key = neo_fin_key
        self.consumer_key = consumer_key
        self.consumer_secret = consumer_secret
        self.access_token = access_token
        self.sid = sid
        self.rid = rid
        self.auth = auth
        self.hs_server_id = hs_server_id
        self.host = host
        self.login_url = f"{self.host}/login/1.0"
        self.opi_url = f"{self.host}/Orders/2.0"
        self.master_script_url = f"{self.host}/Files/1.0"

        self.order_status_map = {
            "put order req received": "NEW",
            "validation pending": "PENDING",
            "rejected": "REJECTED",
            "open": "OPEN",
            "open pending": "OPEN_PENDING",
            "modified": "MODIFIED",
            "modify pending": "MODIFY_PENDING",
            "modify validation pending": "MODIFY_VALIDATION_PENDING",
            "pending": "PENDING",
            "complete": "COMPLETED",
            "cancelled": "CANCELLED",
        }

        if not self.access_token:
            await self.access_token_generate()

        return self

    async def access_token_generate(self):
        """Generate access token for the session."""
        url = "https://napi.kotaksecurities.com/oauth2/token"

        headers = {
            "Authorization": "Basic "
            + base64.b64encode(f"{self.consumer_key}:{self.consumer_secret}".encode()).decode(),
            "Content-Type": "application/x-www-form-urlencoded",
        }

        payload = "grant_type=client_credentials"

        _, resp, _ = await http_request("POST", url=url, headers=headers, payload=payload)

        self.access_token = resp["access_token"]

    async def login(
        self,
        mobile_number: str,
        pan_number: str,
        password: str,
        mpin: str,
    ):
        url = f"{self.login_url}/login/v2/validate"
        headers = {
            "accept": "*/*",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }

        payload = (
            json.dumps(
                {
                    "pan": pan_number,
                    "password": password,
                }
            )
            if pan_number
            else json.dumps(
                {
                    "mobileNumber": mobile_number,
                    "password": password,
                }
            )
        )
        _, resp, _ = await http_request("POST", url, headers, payload)

        if resp.get("code") == "900901" or resp.get("message") == "Invalid Credentials":
            await self.access_token_generate()
            headers["Authorization"] = f"Bearer {self.access_token}"
            _, resp, _ = await http_request("POST", url, headers, payload)

        sid = resp["data"]["sid"]
        auth = resp["data"]["token"]

        headers["Auth"] = auth
        headers["sid"] = sid
        payload = (
            json.dumps(
                {
                    "pan": pan_number,
                    "mpin": mpin,
                }
            )
            if pan_number
            else json.dumps(
                {
                    "mobileNumber": mobile_number,
                    "mpin": mpin,
                }
            )
        )
        async with ClientSession() as client:
            async with client.post(url, headers=headers, data=payload) as resp:
                resp = await resp.json()
                self.sid = resp["data"]["sid"]
                self.auth = resp["data"]["token"]
                self.hs_server_id = resp["data"]["hsServerId"]
                self.rid = resp["data"]["rid"]

    async def update_auth_token(self):
        url = f"{self.login_url}/login/refresh"
        headers = {
            "accept": "*/*",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
            "sid": self.sid,
            "Auth": self.auth,
        }

        payload = json.dumps({"rid": self.rid})

        _, resp, _ = await http_request("POST", url, headers, payload)

        self.sid = resp["data"]["sid"]
        self.auth = resp["data"]["token"]
        self.hs_server_id = resp["data"]["hsServerId"]
        self.rid = resp["data"]["rid"]

    async def place_order(
        self,
        tradingsymbol,  # ts
        quantity,  # qt
        transaction_type,  # tt
        price,  # pr
        order_type,  # pt
        exchange_segment,  # es
        product_code,  # pc
        disclosed_quantity="0",  # dq
        trigger_price="0",  # tp
        order_duration="DAY",  # rt
        market_protection="0",  # mp
        pos_sqrf_flag="N",  # pf
        after_market="NO",  # am
        tag="",  # ig
    ):
        url = f"{self.opi_url}/quick/order/rule/ms/place?sId={self.hs_server_id}"

        jdata = quote(
            json.dumps(
                {
                    "am": after_market,
                    "dq": disclosed_quantity,
                    "es": exchange_segment,
                    "mp": market_protection,
                    "pc": product_code,
                    "pf": pos_sqrf_flag,
                    "pr": price,
                    "pt": order_type,
                    "qt": quantity,
                    "rt": order_duration,
                    "tp": trigger_price,
                    "ts": tradingsymbol,
                    "tt": transaction_type,
                    "ig": tag,
                }
            )
        )

        payload = f"jData={jdata}"

        headers = {
            "accept": "application/json",
            "Sid": self.sid,
            "Auth": self.auth,
            "neo-fin-key": self.neo_fin_key,
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Bearer {self.access_token}",
        }

        status, resp, cookie = await http_request("POST", url, headers, payload)

        if status == 429 or resp.get("code") == "900807":
            raise KotakNeoApiError(resp["code"], status, resp["message"], resp["description"])
        elif status == 200 and resp.get("stat") == "Not_Ok":
            raise KotakNeoApiError(resp.get("stCode"), status, resp.get("errMsg"), resp.get("description", ""))

        return {
            "order_id": resp["nOrdNo"],
            "message": f"Your Order has been Placed and Forwarded to the Exchange: {resp['nOrdNo']}",
        }

    async def modify_order(
        self,
        order_id,  # no
        token,  # tk
        tradingsymbol,  # ts
        quantity,  # qt
        transaction_type,  # tt
        price,  # pr
        order_type,  # pt
        exchange_segment,  # es
        product_code,  # pc
        disclosed_quantity="0",  # dq
        trigger_price="0",  # tp
        validity="DAY",  # vd
        market_protection="0",  # mp
        after_market="NO",  # am
        date_days="NA",  # dd
    ):
        url = f"{self.opi_url}/quick/order/vr/modify?sId={self.hs_server_id}"

        jdata = quote(
            json.dumps(
                {
                    "tk": token,
                    "mp": market_protection,
                    "pc": product_code,
                    "dd": date_days,
                    "dq": disclosed_quantity,
                    "vd": validity,
                    "ts": tradingsymbol,
                    "tt": transaction_type,
                    "pr": price,
                    "tp": trigger_price,
                    "qt": quantity,
                    "no": order_id,
                    "es": exchange_segment,
                    "pt": order_type,
                    "am": after_market,
                }
            )
        )

        payload = f"jData={jdata}"

        headers = {
            "accept": "application/json",
            "Sid": self.sid,
            "Auth": self.auth,
            "neo-fin-key": self.neo_fin_key,
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Bearer {self.access_token}",
        }

        http_status, resp, _ = await http_request("POST", url, headers, payload)

        if http_status == 429 or resp.get("code") == "900807":
            raise KotakNeoApiError(resp["code"], http_status, resp["message"], resp["description"])
        elif http_status == 200 and resp.get("stat") == "Not_Ok":
            print(resp)
            raise KotakNeoApiError(resp["code"], http_status, resp["errMsg"], resp.get("description", ""))

        return {"message": f"Your Order has been Modified Successfully for Order No: {order_id}"}

    async def cancel_order(
        self,
        order_id,  # on
        after_market="NO",  # am
        tradingsymbol=None,  # ts
    ):
        url = f"{self.opi_url}/quick/order/cancel?sId={self.hs_server_id}"

        jdata = {
            "on": order_id,
            "am": after_market,
        }

        if tradingsymbol:
            jdata["ts"] = tradingsymbol

        jdata = quote(json.dumps(jdata))

        payload = f"jData={jdata}"

        headers = {
            "accept": "application/json",
            "Sid": self.sid,
            "Auth": self.auth,
            "neo-fin-key": self.neo_fin_key,
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Bearer {self.access_token}",
        }

        http_status, resp, _ = await http_request("POST", url, headers, payload)

        if http_status == 429 or resp.get("code") == "900807":
            raise KotakNeoApiError(resp["code"], http_status, resp["message"], resp["description"])

        return {"message": f"Your Order has been Cancelled Successfully for Order No: {order_id}"}

    async def order_book(self):
        url = f"{self.opi_url}/quick/user/orders?sId={self.hs_server_id}"

        headers = {
            "accept": "application/json",
            "Sid": self.sid,
            "Auth": self.auth,
            "neo-fin-key": self.neo_fin_key,
            "Authorization": f"Bearer {self.access_token}",
        }

        http_status, resp, _ = await http_request("GET", url, headers)

        if http_status == 429 or resp.get("code") == "900807":
            raise KotakNeoApiError(resp["code"], http_status, resp["message"], resp["description"])

        return resp["data"]

    async def order_history(self, order_no):
        url = f"{self.opi_url}/quick/order/history?sId={self.hs_server_id}"

        headers = {
            "accept": "application/json",
            "Sid": self.sid,
            "Auth": self.auth,
            "neo-fin-key": self.neo_fin_key,
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Bearer {self.access_token}",
        }

        jdata = quote(json.dumps({"nOrdNo": order_no}))

        payload = f"jData={jdata}"

        http_status, resp, _ = await http_request("POST", url, headers, payload)

        if http_status == 429 or resp.get("code") == "900807":
            raise KotakNeoApiError(resp["code"], http_status, resp["message"], resp["description"])

        return resp["data"]

    async def single_order_report(self, order_id, is_fno, error_message=None):
        try:
            data = await self.order_history(order_id)
            columns = [
                "activity_timestamp",
                "exchange_order_id",
                "filled_qty",
                "message",
                "qty",
                "status",
                "pending_qty",
            ]

            rename_columns = {
                "flDtTm": "activity_timestamp",
                "ordSt": "status",
                "exchOrdId": "exchange_order_id",
                "rejRsn": "message",
                "unFldSz": "pending_qty",
            }
            df = pd.DataFrame(data)
            df.rename(columns=rename_columns, inplace=True)
            df["qty"] = df["qty"].astype(int)
            df["pending_qty"] = df["pending_qty"].astype(int)
            df["filled_qty"] = df["qty"] - df["pending_qty"]
            df["status"] = df["status"].apply(lambda x: self.order_status_map[x])

            df = df[columns].copy()
            return df.iloc[0]
        except Exception as e:
            import traceback

            traceback.print_exc()
            print(e)
            return {}

    async def trade_book(self):
        url = f"{self.opi_url}/quick/user/trades?sId={self.hs_server_id}"

        headers = {
            "accept": "application/json",
            "Sid": self.sid,
            "Auth": self.auth,
            "neo-fin-key": self.neo_fin_key,
            "Authorization": f"Bearer {self.access_token}",
        }

        http_status, resp, _ = await http_request("GET", url, headers)

        if http_status == 429 or resp.get("code") == "900807":
            raise KotakNeoApiError(resp["code"], http_status, resp["message"], resp["description"])

        return resp["data"]

    async def positions(self):
        url = f"{self.opi_url}/quick/user/positions?sId={self.hs_server_id}"

        headers = {
            "accept": "application/json",
            "Sid": self.sid,
            "Auth": self.auth,
            "neo-fin-key": self.neo_fin_key,
            "Authorization": f"Bearer {self.access_token}",
        }

        http_status, resp, _ = await http_request("GET", url, headers)

        if http_status == 429 or resp.get("code") == "900807":
            raise KotakNeoApiError(resp["code"], http_status, resp["message"], resp["description"])

        columns = [
            "tradingsymbol",
            "buy_qty",
            "sell_qty",
            "buy_value",
            "sell_value",
        ]

        df = pd.DataFrame(resp["data"])
        df["flBuyQty"] = df["flBuyQty"].astype(int) + df["cfBuyQty"].astype(int)
        df["flSellQty"] = df["flSellQty"].astype(int) + df["cfSellQty"].astype(int)
        df["sellAmt"] = df["sellAmt"].astype(float) + df["cfSellAmt"].astype(float)
        df["buyAmt"] = df["buyAmt"].astype(float) + df["cfBuyAmt"].astype(float)

        df.rename(
            columns={
                "trdSym": "tradingsymbol",
                "buyAmt": "buy_value",
                "sellAmt": "sell_value",
                "flBuyQty": "buy_qty",
                "flSellQty": "sell_qty",
            },
            inplace=True,
        )

        return df[columns].copy()

    async def limits(
        self,
        segment: str = "ALL",
        exchange: str = "ALL",
        product: str = "ALL",
    ):
        url = f"{self.opi_url}/quick/user/limits?sId={self.hs_server_id}"

        jdata = quote(
            json.dumps(
                {
                    "seg": segment,
                    "exch": exchange,
                    "prod": product,
                }
            )
        )

        payload = f"jData={jdata}"

        headers = {
            "accept": "application/json",
            "Sid": self.sid,
            "Auth": self.auth,
            "neo-fin-key": self.neo_fin_key,
            "Content-Type": "application/x-www-form-urlencoded",
            "Authorization": f"Bearer {self.access_token}",
        }

        http_status, resp, _ = await http_request("POST", url, headers, payload)

        if http_status == 429 or resp.get("code") == "900807":
            raise KotakNeoApiError(resp["code"], http_status, resp["message"], resp["description"])
        elif http_status == 200 and resp.get("stat") == "Not_Ok":
            print(resp)
            raise KotakNeoApiError(resp["code"], http_status, resp["errMsg"], resp.get("description", ""))

        return resp

    async def download_files(self):
        url = f"{self.master_script_url}/masterscrip/file-paths"

        headers = {
            "accept": "*/*",
            "Authorization": f"Bearer {self.access_token}",
        }

        http_status, resp, _ = await http_request("GET", url, headers)

        if http_status == 429 or resp.get("code") == "900807":
            raise KotakNeoApiError(resp["code"], http_status, resp["message"], resp["description"])

        for file_path in resp["data"]["filesPaths"]:
            f = file_path.split("/")
            directory = f"neo_symbol/{localdate()}"
            if not os.path.exists(directory):
                os.mkdir(directory)

            urlretrieve(file_path, f"{directory}/{f[-1]}")

        return True

    async def instruments(self, instrument_type):
        if instrument_type not in ["bse_cm", "cde_fo", "mcx_fo", "nse_cm", "nse_fo"]:
            raise KotakNeoApiError("404", 404, "Instrument Type Does Not exists", "")

        if not os.path.exists(f"neo_symbol/{localdate()}/{instrument_type}.csv"):
            await self.download_files()

        if instrument_type == "nse_fo":
            columns = [
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

            df = pd.read_csv(
                f"neo_symbol/{localdate()}/{instrument_type}.csv",
                header=0,
                names=columns,
            )

            df = df[~df.pExchange.isna()].copy()

            df = df[
                [
                    "pSymbol",
                    "pInstType",
                    "pSymbolName",
                    "pTrdSymbol",
                    "pOptionType",
                    "dTickSize",
                    "iLotSize",
                    "lExpiryDate",
                    "dStrikePrice",
                    "lFreezeQty",
                ]
            ].copy()

            df.rename(
                columns={
                    "pSymbol": "kotak_neo_instrument_token",
                    "pInstType": "kotak_neo_instrument_type",
                    "pSymbolName": "name",
                    "pTrdSymbol": "kotak_neo_tradingsymbol",
                    "pOptionType": "instrument_type",
                    "dTickSize": "tick_size",
                    "iLotSize": "lot_size",
                    "lExpiryDate": "expiry",
                    "dStrikePrice": "strike_price",
                    "lFreezeQty": "freeze_qty",
                },
                inplace=True,
            )

            df["tick_size"] = df["tick_size"] / 100
            df["strike_price"] = df["strike_price"] / 100
            df["freeze_qty"] = df["freeze_qty"] - 1

            df["expiry"] = df["expiry"].apply(
                lambda x: (dt.datetime.fromtimestamp(x) + relativedelta(years=10)).date()
            )
            df.reset_index(drop=True, inplace=True)

            return df
