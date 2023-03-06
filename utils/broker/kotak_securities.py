import random
import string

import pandas as pd
from dateutil.parser import parse

from utils.async_obj import AsyncObj
from utils.http_request import http_request


class KotakSecuritiesApiError(Exception):
    def __init__(self, code, message, *args: object) -> None:
        self.code = code
        self.message = message

        super().__init__(self.message)


class KotakSecuritiesApi(AsyncObj):
    async def __ainit__(
        self,
        userid,
        consumer_key,
        access_token=None,
        consumer_secret=None,
        ip="127.0.0.1",
        app_id="test",
        host="https://tradeapi.kotaksecurities.com/apim",
    ):
        self.userid = userid
        self.host = host
        self.consumer_key = consumer_key
        self.access_token = access_token
        self.ip = ip
        self.app_id = app_id

        self.session_path = "session/1.0"
        self.order_api = "orders/1.0"
        self.report_api = "reports/1.0"
        self.positions_api = "positions/1.0"

        self.status_map = {
            "NEWF": "NEW_PENDING",
            "OPN": "OPEN",
            "CNRF": "CONFIRMATION_PENDING",
            "TRAD": "COMPLETE",
            "CAN": "CANCELLED"
        }

        await self.session_init()

    async def generate_token(self, n=7):
        return "".join(random.choices(string.ascii_uppercase + string.digits, k=n))

    async def session_init(self):
        url = f"{self.host}/{self.session_path}/session/init"

        headers = {
            "accept": "application/json",
            "userid": self.userid,
            "consumerKey": self.consumer_key,
            "ip": self.ip,
            "appId": self.app_id,
            "Authorization": f"Bearer {self.access_token}",
        }

        _, resp, _ = await http_request(
            method="GET",
            url=url,
            headers=headers,
        )

    async def login(
        self,
        password,
    ):
        url = f"{self.host}/{self.session_path}/session/login/userid"

        headers = {
            "accept": "application/json",
            "consumerKey": self.consumer_key,
            "ip": self.ip,
            "appId": self.app_id,
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }

        payload = {"userid": self.userid, "password": password}

        _, resp, _ = await http_request(
            method="POST",
            url=url,
            headers=headers,
            payload=payload,
            payload_decode=True,
        )

        data = resp.get("success", resp.get("Success", {}))

        self.one_time_token = data["oneTimeToken"]

    async def session_2fa(self, access_code=None):
        if not hasattr(self, "one_time_token") and not self.one_time_token:
            raise KotakSecuritiesApiError(
                "No one time token found. Please invoke 'session_login_api' function first"
            )

        url = f"{self.host}/{self.session_path}/session/2FA/oneTimeToken"

        headers = {
            "accept": "application/json",
            "consumerKey": self.consumer_key,
            "ip": self.ip,
            "appId": self.app_id,
            "oneTimeToken": self.one_time_token,
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }

        payload = (
            {
                "userid": self.userid,
                "accessCode": access_code,
            }
            if access_code
            else {"userid": self.userid}
        )
        _, resp, _ = await http_request(
            method="POST",
            url=url,
            headers=headers,
            payload=payload,
            payload_decode=True,
        )

        data = resp.get("success", resp.get("Success", {}))
        self.session_token = data["sessionToken"]

    async def place_order(
        self,
        instrument_token,
        exchange,
        transaction_type,
        quantity,
        disclosed_quantity=0,
        price=0,
        trigger_price=0,
        order_type="NORMAL",
        validity="GFD",
        variety="REGULAR",
        tag="string",
    ):
        if not hasattr(self, "session_token") and not self.session_token:
            raise KotakSecuritiesApiError(
                "No session token found. Please invoke 'login' function first"
            )

        match order_type:
            case "NORMAL":
                url = f"{self.host}/{self.order_api}/order/normal"

                payload = {
                    "instrumentToken": instrument_token,
                    "transactionType": transaction_type,
                    "quantity": quantity,
                    "price": price,
                    "validity": validity,
                    "variety": variety,
                    "disclosedQuantity": disclosed_quantity,
                    "triggerPrice": trigger_price,
                    "tag": tag,
                }

            case "MIS":
                url = f"{self.host}/{self.order_api}/order/mis"

            case _:
                raise KotakSecuritiesApiError("Order Type must be NORMAL or MIS")

        headers = {
            "accept": "application/json",
            "consumerKey": self.consumer_key,
            "sessionToken": self.session_token,
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }

        status, resp, cookie = await http_request(
            method="POST",
            url=url,
            headers=headers,
            payload=payload,
            payload_decode=True,
        )

        if status == 200:
            success = resp.get("success", resp.get("Success", {}))
            error = resp.get("fault", {})

            if success:
                data = success[exchange]
                return {
                    "order_id": str(data["orderId"]),
                    "message": f"Your Order has been Placed and Forwarded to the Exchange: {data['orderId']}",
                }

            elif error:
                code = error.get("code")
                message = error.get("message", "")

                if code == 999108 and "Insufficient Margin." in message:
                    order_id = f"REJECTED_{await self.generate_token()}"
                    return {
                        "order_id": order_id,
                        "message": f"Your Order has been Placed and Forwarded to the Exchange: {order_id}",
                    }

                elif code == 999007 and "Max Order Frequency Limit reached." in message:
                    raise KotakSecuritiesApiError(999000, "Max Order Frequency Limit reached.")

                elif (
                    code == 1000
                    and "Price is not within the price band limit of" in message
                ):
                    x = message.split()
                    upper_band = x[-1]
                    lower_band = x[-3]
                    custom_message = f"Price is not within the price band limit of {lower_band} and {upper_band}"
                    raise KotakSecuritiesApiError(999001, custom_message)

            print(resp)

    async def modify_order(
        self,
        order_id,
        quantity,
        disclosed_quantity=0,
        price=0,
        trigger_price=0,
        order_type="NORMAL",
        validity="GFD",
    ):
        if not hasattr(self, "session_token") and not self.session_token:
            raise KotakSecuritiesApiError(
                "No session token found. Please invoke 'login' function first"
            )

        match order_type:
            case "NORMAL":
                url = f"{self.host}/{self.order_api}/order/normal"

                payload = {
                    "orderId": order_id,
                    "quantity": quantity,
                    "price": price,
                    "disclosedQuantity": disclosed_quantity,
                    "triggerPrice": trigger_price,
                    "validity": validity,
                }

            case "MIS":
                url = f"{self.host}/{self.order_api}/order/mis"

            case _:
                raise KotakSecuritiesApiError("Order Type must be NORMAL or MIS")

        headers = {
            "accept": "application/json",
            "consumerKey": self.consumer_key,
            "sessionToken": self.session_token,
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }

        status, resp, cookie = await http_request(
            method="PUT",
            url=url,
            headers=headers,
            payload=payload,
            payload_decode=True,
        )

        if status == 200:
            success = resp.get("success", resp.get("Success", {}))
            error = resp.get("fault", {})

            if success:
                return {
                    "message": f"Your Order has been Modified Successfully for Order No: {order_id}"
                }
            elif error:
                code = error.get("code")
                message = error.get("message", "")

                if code == 999007 and "Max Order Frequency Limit reached." in message:
                    raise KotakSecuritiesApiError(999000, "Max Order Frequency Limit reached.")

                elif code == 999113 and "Please change the order details." in message:
                    raise KotakSecuritiesApiError(
                        999007,
                        "Order Modification not allowed, Please change the order details.",
                    )

                elif code == 999113 and "Insufficient Margin." in message:
                    raise KotakSecuritiesApiError(
                        999004,
                        "Your Order modification has been Rejected due to Insufficient Margin.",
                    )

                elif (
                    code == 999113
                    and "Order Modification not allowed as current Order status is FIL"
                    in message
                ):
                    raise KotakSecuritiesApiError(
                        999003,
                        "Order Modification not allowed as current Order status is COMPLETE.",
                    )

                elif (
                    code == 999113
                    and "Order Modification not allowed as current Order status is CHRF"
                    in message
                ):
                    raise KotakSecuritiesApiError(
                        999005,
                        "Order Modification not allowed as current Order status is MODIFYING.",
                    )

                elif code == 999113 and "Order is Cancelled / Rejected" in message:
                    raise KotakSecuritiesApiError(
                        999005,
                        "Order Modification not allowed as current Order is Cancelled / Rejected.",
                    )

                elif (
                    code == 999113
                    and "Price is not within the price band limit of" in message
                ):
                    x = message.split()
                    upper_band = x[-1]
                    lower_band = x[-3]
                    custom_message = f"Price is not within the price band limit of {lower_band} and {upper_band}"
                    raise KotakSecuritiesApiError(999001, custom_message)
            print(resp)

    async def cancel_order(
        self,
        order_id,
        order_type="NORMAL",
    ):
        if not hasattr(self, "session_token") and not self.session_token:
            raise KotakSecuritiesApiError(
                "No session token found. Please invoke 'login' function first"
            )

        match order_type:
            case "NORMAL":
                url = f"{self.host}/{self.order_api}/order/normal/{order_id}"

            case "MIS":
                url = f"{self.host}/{self.order_api}/order/mis/{order_id}"

            case _:
                raise KotakSecuritiesApiError("Order Type must be NORMAL or MIS")

        headers = {
            "accept": "application/json",
            "consumerKey": self.consumer_key,
            "sessionToken": self.session_token,
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }

        status, resp, cookie = await http_request(
            method="DELETE",
            url=url,
            headers=headers,
        )

        if status == 200:
            success = resp.get("success", resp.get("Success", {}))
            error = resp.get("fault", {})

            if success:
                return {
                    "message": f"Your Order has been Cancelled Successfully for Order No: {order_id}"
                }

            elif error:
                code = error.get("code")
                message = error.get("message", "")

                if code == 5055 and "Order is Cancelled / Rejected" in message:
                    raise KotakSecuritiesApiError(
                        999005,
                        "Order Cancellation not allowed as current Order is Cancelled / Rejected.",
                    )

            print(resp)

    async def positions(self, position_type: str):
        if not hasattr(self, "session_token") and not self.session_token:
            raise KotakSecuritiesApiError(
                "No session token found. Please invoke 'login' function first"
            )

        match position_type:
            case "TODAYS":
                url = f"{self.host}/{self.positions_api}/positions/todays"

            case "OPEN":
                url = f"{self.host}/{self.positions_api}/positions/open"

            case "STOCKS":
                url = f"{self.host}/{self.positions_api}/positions/stocks"

            case _:
                raise KotakSecuritiesApiError(
                    999100, "position_type must be in ['TODAYS', 'OPEN', 'STOCKS']"
                )

        headers = {
            "accept": "application/json",
            "consumerKey": self.consumer_key,
            "sessionToken": self.session_token,
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }

        status, resp, cookie = await http_request(
            method="GET",
            url=url,
            headers=headers,
        )

        if status == 200:
            columns = [
                "trading_symbol",
                "instrument_name",
                "instrument_token",
                "lot_size",
                "buy_qty",
                "sell_qty",
                "buy_value",
                "sell_value",
                "buy_avg",
                "sell_avg",
            ]

            success = resp.get("success", resp.get("Success", {}))
            error = resp.get("fault", {})

            if not success and not error:
                return pd.DataFrame(columns=columns)

            elif success:
                df = pd.DataFrame(success)
                df.rename(
                    columns={
                        "symbol": "trading_symbol",
                        "instrumentName": "instrument_name",
                        "instrumentToken": "instrument_token",
                        "marketLot": "lot_size",
                        "buyTradedQtyLot": "buy_qty",
                        "sellTradedQtyLot": "sell_qty",
                        "buyTradedVal": "buy_value",
                        "sellTradedVal": "sell_value",
                        "buyTrdAvg": "buy_avg",
                        "sellTrdAvg": "sell_avg",
                    },
                    inplace=True,
                )

                return df[columns].copy()

            print(resp)

    async def order_report(self, order_id=None, is_fno=False):
        if not hasattr(self, "session_token") and not self.session_token:
            raise KotakSecuritiesApiError(
                "No session token found. Please invoke 'login' function first"
            )

        if not order_id:
            url = f"{self.host}/{self.report_api}/orders"

            columns = [
                "order_id",
                "exchange_order_id",
                "exchange",
                "transaction_type",
                "qty",
                "instrument_name",
                "instrument_token",
                "instrument_type",
                "strike_price",
                "option_type",
                "is_fno",
                "lot_size",
                "expiry_date",
                "disclosed_qty",
                "order_timestamp",
                "filled_qty",
                "pending_qty",
                "cancelled_qty",
                "price",
                "trigger_price",
                "product",
                "status",
                "validity",
                "variety",
                "tag",
            ]

            rename_columns = {
                "orderId": "order_id",
                "exchangeOrderId": "exchange_order_id",
                "exchange": "exchange",
                "transactionType": "transaction_type",
                "orderQuantity": "qty",
                "instrumentName": "instrument_name",
                "instrumentToken": "instrument_token",
                "instrumentType": "instrument_type",
                "strikePrice": "strike_price",
                "optionType": "option_type",
                "isFNO": "is_fno",
                "marketLot": "lot_size",
                "expiryDate": "expiry_date",
                "disclosedQuantity": "disclosed_qty",
                "orderTimestamp": "order_timestamp",
                "filledQuantity": "filled_qty",
                "pendingQuantity": "pending_qty",
                "cancelledQuantity": "cancelled_qty",
                "price": "price",
                "triggerPrice": "trigger_price",
                "product": "product",
                "status": "status",
                "validity": "validity",
                "variety": "variety",
                "tag": "tag",
            }
        else:
            columns = [
                "activity_timestamp",
                "disclosed_qty",
                "exchange_order_id",
                "exchange_trade_id",
                "exchange_status",
                "filled_qty",
                "message",
                "qty",
                "price",
                "status",
                "trigger_price",
                "validity",
                "version",
            ]

            rename_columns = {
                "activityTimestamp": "activity_timestamp",
                "disclosedQuantity": "disclosed_qty",
                "exchOrderId": "exchange_order_id",
                "exchTradeId": "exchange_trade_id",
                "exchangeStatus": "exchange_status",
                "filledQuantity": "filled_qty",
                "message": "message",
                "orderQuantity": "qty",
                "price": "price",
                "status": "status",
                "triggerPrice": "trigger_price",
                "validity": "validity",
                "version": "version",
            }

            if is_fno:
                url = f"{self.host}/{self.report_api}/orders/{order_id}/Y"

            else:
                url = f"{self.host}/{self.report_api}/orders/{order_id}"

        headers = {
            "accept": "application/json",
            "consumerKey": self.consumer_key,
            "sessionToken": self.session_token,
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }

        status, resp, cookie = await http_request(
            method="GET",
            url=url,
            headers=headers,
        )

        if status == 200:
            success = resp.get("success", resp.get("Success", {}))
            error = resp.get("fault", {})

            if not success and not error:
                return pd.DataFrame(columns=columns)

            elif success:
                df = pd.DataFrame(success)
                df.rename(columns=rename_columns, inplace=True)
                df["status"] = df["status"].apply(lambda x: self.status_map[x])
                df["price"] = df["price"].apply(round, 2)

                if "order_timestamp" in df.columns:
                    print("Test")
                    df["order_timestamp"] = df["order_timestamp"].apply(parse)

                if "activity_timestamp" in df.columns:
                    df['activity_timestamp'] = df["activity_timestamp"].apply(parse)
                return df[columns].copy()

        print(resp)

    async def trade_report(self, order_id=None, is_fno=False):
        if not hasattr(self, "session_token") and not self.session_token:
            raise KotakSecuritiesApiError(
                "No session token found. Please invoke 'login' function first"
            )

        if not order_id:
            url = f"{self.host}/{self.report_api}/trades"
        elif is_fno:
            url = f"{self.host}/{self.report_api}/trades/{order_id}/Y"
        else:
            url = f"{self.host}/{self.report_api}/trades/{order_id}"

        headers = {
            "accept": "application/json",
            "consumerKey": self.consumer_key,
            "sessionToken": self.session_token,
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }

        status, resp, cookie = await http_request(
            method="GET",
            url=url,
            headers=headers,
        )

        return status, resp, cookie
