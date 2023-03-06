import csv
import datetime
import urllib
import warnings

import dateutil.parser
from kiteconnect import KiteTicker
from six import PY2, StringIO

from utils.async_obj import AsyncObj
from utils.http_request import http_request


class LoginException(Exception):
    pass


class KiteExtTicker(KiteTicker):
    def __init__(
        self,
        user_id,
        enctoken,
        root="wss://ws.zerodha.com/",
        api_key="kitefront",
        user_agent="kite3-web",
        version="2.9.12",
    ):
        super().__init__(api_key=api_key, access_token=enctoken)

        enctoken = urllib.parse.quote(enctoken)
        self.socket_url = (
            f"{root}?api_key={api_key}&user_id={user_id}&enctoken={enctoken}&user-agent={user_agent}&version={version}"
        )


class KiteExt(AsyncObj):
    """
    The Kite Connect API wrapper class.

    In production, you may initialise a single instance of this class per `api_key`.
    """

    # Default root API endpoint. It's possible to
    # override this by passing the `root` parameter during initialisation.
    _default_root_uri = "https://api.kite.trade"
    _default_login_uri = "https://kite.zerodha.com/connect/login"
    _default_timeout = 7  # In seconds

    # Kite connect header version
    kite_header_version = "3"

    # Constants
    # Products
    PRODUCT_MIS = "MIS"
    PRODUCT_CNC = "CNC"
    PRODUCT_NRML = "NRML"
    PRODUCT_CO = "CO"

    # Order types
    ORDER_TYPE_MARKET = "MARKET"
    ORDER_TYPE_LIMIT = "LIMIT"
    ORDER_TYPE_SLM = "SL-M"
    ORDER_TYPE_SL = "SL"

    # Varities
    VARIETY_REGULAR = "regular"
    VARIETY_CO = "co"
    VARIETY_AMO = "amo"
    VARIETY_ICEBERG = "iceberg"
    VARIETY_AUCTION = "auction"

    # Transaction type
    TRANSACTION_TYPE_BUY = "BUY"
    TRANSACTION_TYPE_SELL = "SELL"

    # Validity
    VALIDITY_DAY = "DAY"
    VALIDITY_IOC = "IOC"
    VALIDITY_TTL = "TTL"

    # Position Type
    POSITION_TYPE_DAY = "day"
    POSITION_TYPE_OVERNIGHT = "overnight"

    # Exchanges
    EXCHANGE_NSE = "NSE"
    EXCHANGE_BSE = "BSE"
    EXCHANGE_NFO = "NFO"
    EXCHANGE_CDS = "CDS"
    EXCHANGE_BFO = "BFO"
    EXCHANGE_MCX = "MCX"
    EXCHANGE_BCD = "BCD"

    # Margins segments
    MARGIN_EQUITY = "equity"
    MARGIN_COMMODITY = "commodity"

    # Status constants
    STATUS_COMPLETE = "COMPLETE"
    STATUS_REJECTED = "REJECTED"
    STATUS_CANCELLED = "CANCELLED"

    # GTT order type
    GTT_TYPE_OCO = "two-leg"
    GTT_TYPE_SINGLE = "single"

    # GTT order status
    GTT_STATUS_ACTIVE = "active"
    GTT_STATUS_TRIGGERED = "triggered"
    GTT_STATUS_DISABLED = "disabled"
    GTT_STATUS_EXPIRED = "expired"
    GTT_STATUS_CANCELLED = "cancelled"
    GTT_STATUS_REJECTED = "rejected"
    GTT_STATUS_DELETED = "deleted"

    # URIs to various calls
    _routes = {
        "api.token": "/session/token",
        "api.token.invalidate": "/session/token",
        "api.token.renew": "/session/refresh_token",
        "user.profile": "/user/profile",
        "user.margins": "/user/margins",
        "user.margins.segment": "/user/margins/{segment}",
        "orders": "/orders",
        "trades": "/trades",
        "order.info": "/orders/{order_id}",
        "order.place": "/orders/{variety}",
        "order.modify": "/orders/{variety}/{order_id}",
        "order.cancel": "/orders/{variety}/{order_id}",
        "order.trades": "/orders/{order_id}/trades",
        "portfolio.positions": "/portfolio/positions",
        "portfolio.holdings": "/portfolio/holdings",
        "portfolio.holdings.auction": "/portfolio/holdings/auctions",
        "portfolio.positions.convert": "/portfolio/positions",
        # MF api endpoints
        "mf.orders": "/mf/orders",
        "mf.order.info": "/mf/orders/{order_id}",
        "mf.order.place": "/mf/orders",
        "mf.order.cancel": "/mf/orders/{order_id}",
        "mf.sips": "/mf/sips",
        "mf.sip.info": "/mf/sips/{sip_id}",
        "mf.sip.place": "/mf/sips",
        "mf.sip.modify": "/mf/sips/{sip_id}",
        "mf.sip.cancel": "/mf/sips/{sip_id}",
        "mf.holdings": "/mf/holdings",
        "mf.instruments": "/mf/instruments",
        "market.instruments.all": "/instruments",
        "market.instruments": "/instruments/{exchange}",
        "market.margins": "/margins/{segment}",
        "market.historical": "/instruments/historical/{instrument_token}/{interval}",
        "market.trigger_range": "/instruments/trigger_range/{transaction_type}",
        "market.quote": "/quote",
        "market.quote.ohlc": "/quote/ohlc",
        "market.quote.ltp": "/quote/ltp",
        # GTT endpoints
        "gtt": "/gtt/triggers",
        "gtt.place": "/gtt/triggers",
        "gtt.info": "/gtt/triggers/{trigger_id}",
        "gtt.modify": "/gtt/triggers/{trigger_id}",
        "gtt.delete": "/gtt/triggers/{trigger_id}",
        # Margin computation endpoints
        "order.margins": "/margins/orders",
        "order.margins.basket": "/margins/basket",
    }

    async def __ainit__(self, user_id, password=None, twofa=None, token=None, *args, **kw):
        self.debug = False
        self.api_key = "kitefront"
        self.user_id = user_id
        self.session_expiry_hook = None
        self.disable_ssl = False
        self.access_token = None
        self.proxies = {}

        self.root = self._default_root_uri
        self.timeout = self._default_timeout

        self._routes.update(
            {
                "api.login": "/api/login",
                "api.twofa": "/api/twofa",
                "api.misdata": "/margins/equity",
            }
        )

        if not user_id:
            raise LoginException("Please Enter User ID.")

        if not ((password and twofa) or token):
            raise LoginException("Please Enter (password and twofa) or token")

        if password and twofa:
            await self.login_with_credentials(user_id, password, twofa)
        else:
            await self.login_with_token(user_id, token)

    async def login_with_credentials(self, user_id, password, twofa):
        self.headers = {"x-kite-version": "3"}

        status, resp, cookies = await http_request(
            "POST", f"{self.root}/api/login", payload={"user_id": user_id, "password": password}
        )

        if resp["status"] == "error":
            raise LoginException(resp["message"])

        status, resp, cookies = await http_request(
            "POST",
            self.root + self._routes["api.twofa"],
            payload={
                "request_id": resp["data"]["request_id"],
                "twofa_value": twofa,
                "user_id": resp["data"]["user_id"],
            },
        )

        if resp["status"] == "error":
            raise LoginException(resp["message"])

        self.user_id = cookies.get("user_id").value
        self.public_token = cookies.get("enctoken").value
        self.headers["Authorization"] = f"enctoken {self.public_token}"

    async def login_with_token(self, userid, token):
        self.user_id = userid
        self.public_token = token
        self.headers = {"x-kite-version": "3", "Authorization": f"enctoken {self.public_token}"}
        flag = None
        try:
            await self.profile()
        except Exception as e:
            flag = str(e)

        if flag:
            raise LoginException(flag)

    def kws(self):
        return KiteExtTicker(user_id=self.user_id, enctoken=self.public_token)

    async def profile(self):
        _, resp, _ = await http_request("GET", self.root + self._routes["user.profile"], headers=self.headers)
        return resp

    async def place_order(self, variety, exchange, tradingsymbol, transaction_type, quantity, product, order_type, price=None, validity=None, validity_ttl=None, disclosed_quantity=None, trigger_price=None, iceberg_legs=None, iceberg_quantity=None, auction_number=None, tag=None):
        params = locals()
        del (params["self"])
        print(params)

        for k in list(params.keys()):
            if params[k] is None:
                del (params[k])
        _, resp, _ = await http_request(
            "POST",
            self.root + self._routes["order.place"].format(**{'variety':variety}),
            headers=self.headers, payload=params
        )
        return resp

    async def modify_order(self, variety, order_id, parent_order_id=None, quantity=None, price=None, order_type=None, trigger_price=None, validity=None, disclosed_quantity=None):
        """Modify an open order."""
        params = locals()
        del (params["self"])

        for k in list(params.keys()):
            if params[k] is None:
                del (params[k])

        _, resp, _ = await http_request("PUT", self.root+self._routes["order.modify"].format(**{"variety": variety, "order_id": order_id}), headers=self.headers, payload=params)
        print(resp)
        return resp

    async def cancel_order(self, variety, order_id, parent_order_id=None):
        """Cancel an order."""
        _, resp, _ = await http_request("DELETE", self.root+self._routes["order.cancel"].format(**{"variety": variety, "order_id": order_id}), headers=self.headers)
        return resp

    async def exit_order(self, variety, order_id, parent_order_id=None):
        """Exit a CO order."""
        return await self.cancel_order(
            variety, order_id, parent_order_id=parent_order_id
        )

    async def convert_position(self, exchange, tradingsymbol, transaction_type, position_type, quantity, old_product, new_product):
        params = {
            "exchange": exchange,
            "tradingsymbol": tradingsymbol,
            "transaction_type": transaction_type,
            "position_type": position_type,
            "quantity": quantity,
            "old_product": old_product,
            "new_product": new_product
        }
        """Modify an open position's product type."""
        _, res, _ = await http_request("PUT", self.root + self._routes["portfolio.positions.convert"], headers=self.headers, payload=params)
        return res

    def _format_response(self, data):
        """Parse and format responses."""
        if type(data) == list:
            _list = data
        elif type(data) == dict:
            _list = [data]
        else:  # ADDED FOR ASYNC DEF QUOTE( * INSTRUMENTS )
            return data
        for item in _list:
            # Convert date time string to datetime object
            for field in ["order_timestamp", "exchange_timestamp", "created", "last_instalment", "fill_timestamp", "timestamp", "last_trade_time"]:
                if item.get(field) and len(item[field]) == 19:
                    item[field] = dateutil.parser.parse(item[field])

        return _list[0] if type(data) == dict else _list

    async def orders(self):
        """Get list of orders."""
        _, resp, _ = await http_request("GET", self.root + self._routes["orders"], headers=self.headers)
        # return self._format_response(self._get("orders"))
        return self._format_response(resp)

    async def order_history(self, order_id):
        """
        Get history of individual order.

        - `order_id` is the ID of the order to retrieve order history.
        """
        _, res, _ = await http_request('GET', self.root+self._routes["order.info"].format(**{"order_id": order_id}), headers=self.headers)
        return self._format_response(res)

    async def trades(self):
        """
        Retrieve the list of trades executed (all or ones under a particular order).

        An order can be executed in tranches based on market conditions.
        These trades are individually recorded under an order.

        """
        _, res, _ = await http_request('GET', self.root+self._routes["trades"], headers=self.headers)
        return self._format_response(res)

    async def order_trades(self, order_id):
        """
        Retrieve the list of trades executed for a particular order.

        - `order_id` is the ID of the order to retrieve trade history.
        """
        _, res, _ = await http_request('GET', self.root+self._routes["order.trades"].format(**{"order_id": order_id}), headers=self.headers)
        return self._format_response(res)

    async def positions(self):
        """Retrieve the list of positions."""
        
        # return self._get("portfolio.positions")
        _, res, _ = await http_request('GET', self.root+self._routes["portfolio.positions"], headers=self.headers)
        return res

    async def holdings(self):
        """Retrieve the list of equity holdings."""
        # return self._get("portfolio.holdings")
        _, res, _ = await http_request('GET', self.root+self._routes["portfolio.holdings"], headers=self.headers)
        return res

    async def get_auction_instruments(self):
        """ Retrieves list of available instruments for a auction session """
        # return self._get("portfolio.holdings.auction")
        _, res, _ = await http_request('GET', self.root+self._routes["portfolio.holdings.auction"], headers=self.headers)
        return res

    # UNTESTED
    async def mf_orders(self, order_id=None):
        """Get all mutual fund orders or individual order info."""
        if order_id:
            _, res, _ = await http_request('GET', self.root+self._routes["mf.order.info"].format(**{"order_id": order_id}), headers=self.headers)
        else:
             _, res, _ = await http_request('GET', self.root+self._routes["mf.orders"].format(**{"order_id": order_id}), headers=self.headers)
        return self._format_response(res)

    # UNTESTED
    async def place_mf_order(self, tradingsymbol, transaction_type, quantity=None, amount=None, tag=None):
        params={
            "tradingsymbol": tradingsymbol,
            "transaction_type": transaction_type,
            "quantity": quantity,
            "amount": amount,
            "tag": tag
        }
        """Place a mutual fund order."""
        _, res, _ = await http_request("PUT",self.root + self._routes['mf.order.place'], headers=self.headers, payload=params)
        return res

    def _parse_instruments(self, data):
        # decode to string for Python 3
        d = data
        # Decode unicode data
        if not PY2 and type(d) == bytes:
            d = data.decode("utf-8").strip()

        records = []
        reader = csv.DictReader(StringIO(d))

        for row in reader:
            row["instrument_token"] = int(row["instrument_token"])
            row["last_price"] = float(row["last_price"])
            row["strike"] = float(row["strike"])
            row["tick_size"] = float(row["tick_size"])
            row["lot_size"] = int(row["lot_size"])

            # Parse date
            if len(row["expiry"]) == 10:
                row["expiry"] = dateutil.parser.parse(row["expiry"]).date()

            records.append(row)

        return records

    async def instruments(self, exchange=None):
        """
        Retrieve the list of market instruments available to trade.

        Note that the results could be large, several hundred KBs in size,
        with tens of thousands of entries in the list.

        - `exchange` is specific exchange to fetch (Optional)
        """
        if exchange:
            _, res, _ = await http_request('GET', self.root+self._routes["market.instruments"].format(**{"exchange": exchange}), headers=self.headers)
        else:
            _, res, _ = await http_request('GET', self.root+self._routes["market.instruments.all"], headers=self.headers)
        
        return self._parse_instruments(res)

    async def quote(self, *instruments):
        """
        Retrieve quote for list of instruments.

        - `instruments` is a list of instruments, Instrument are in the format of `exchange:tradingsymbol`. For example NSE:INFY
        """
        ins = list(instruments)

        # If first element is a list then accept it as instruments list for legacy reason
        if instruments and type(instruments[0]) == list:
            ins = instruments[0]
        params = {"i": ins}
        print(params)

        _, data, _ = await http_request("GET", self.root + self._routes["market.quote"], headers=self.headers, payload=params)
        print(data)
        return {key: self._format_response(data[key]) for key in data}

    async def ohlc(self, *instruments):
        """
        Retrieve OHLC and market depth for list of instruments.

        - `instruments` is a list of instruments, Instrument are in the format of `exchange:tradingsymbol`. For example NSE:INFY
        """
        ins = list(instruments)

        # If first element is a list then accept it as instruments list for legacy reason
        if instruments and type(instruments[0]) == list:
            ins = instruments[0]
        _, data, _ = await http_request("GET", self.root + self._routes["market.quote.ohlc"], payload={"i": ins}, headers=self.headers)
        return data

    async def ltp(self, *instruments):
        """
        Retrieve last price for list of instruments.

        - `instruments` is a list of instruments, Instrument are in the format of `exchange:tradingsymbol`. For example NSE:INFY
        """
        ins = list(instruments)

        # If first element is a list then accept it as instruments list for legacy reason
        if instruments and type(instruments[0]) == list:
            ins = instruments[0]
        _, data, _ = await http_request("GET", self.root + self._routes["market.quote.ltp"], payload={"i": ins}, headers=self.headers)
        return data

    def _format_historical(self, data):
        records = []
        for d in data["candles"]:
            record = {
                "date": dateutil.parser.parse(d[0]),
                "open": d[1],
                "high": d[2],
                "low": d[3],
                "close": d[4],
                "volume": d[5],
            }
            if len(d) == 7:
                record["oi"] = d[6]
            records.append(record)
        return records

    async def historical_data(self, instrument_token, from_date, to_date, interval, continuous=False, oi=False):
        """
        Retrieve historical data (candles) for an instrument.

        Although the actual response JSON from the API does not have field
        names such has 'open', 'high' etc., this function call structures
        the data into an array of objects with field names. For example:

        - `instrument_token` is the instrument identifier (retrieved from the instruments()) call.
        - `from_date` is the From date (datetime object or string in format of yyyy-mm-dd HH:MM:SS.
        - `to_date` is the To date (datetime object or string in format of yyyy-mm-dd HH:MM:SS).
        - `interval` is the candle interval (minute, day, 5 minute etc.).
        - `continuous` is a boolean flag to get continuous data for futures and options instruments.
        - `oi` is a boolean flag to get open interest.
        """
        date_string_format = "%Y-%m-%d %H:%M:%S"
        from_date_string = from_date.strftime(date_string_format) if type(from_date) == datetime.datetime else from_date
        to_date_string = to_date.strftime(date_string_format) if type(to_date) == datetime.datetime else to_date
        params = {
                "from": from_date_string,
                "to": to_date_string,
                "interval": interval,
                "continuous": 1 if continuous else 0,
                "oi": 1 if oi else 0
            }
        url_args = {"instrument_token": instrument_token, "interval": interval}
        _, data, _ = await http_request("GET", self.root + self._routes["market.historical"].format(**url_args), payload=params, headers=self.headers)
        return data

    # UNTESTED
    async def get_gtts(self):
        """Fetch list of gtt existing in an account"""
        # return self._get("gtt")
        _, data, _ = await http_request("GET", self.root + self._routes["gtt"], headers=self.headers)
        return data

    # UNTESTED
    async def get_gtt(self, trigger_id):
        """Fetch details of a GTT"""
        url_args = {"trigger_id": trigger_id}
        _, data, _ = await http_request("GET", self.root + self._routes["gtt.info"].format(**url_args), headers=self.headers)
        return data

    # UNTESTED
    async def order_margins(self, params):
        """
        Calculate margins for requested order list considering the existing positions and open orders

        - `params` is list of orders to retrive margins detail
        """
        # return self._post("order.margins", params=params, is_json=True)
        print(params)
        _, data, _ = await http_request("POST", self.root + self._routes["order.margins"], headers=self.headers,payload=params)
        return data

    # UNTESTED
    async def basket_order_margins(self, params, consider_positions=True, mode=None):
        """
        Calculate total margins required for basket of orders including margin benefits

        - `params` is list of orders to fetch basket margin
        - `consider_positions` is a boolean to consider users positions
        - `mode` is margin response mode type. compact - Compact mode will only give the total margins
        """
        # return self._post("order.margins.basket",
        #                   params=params,
        #                   is_json=True,
        #                   query_params={'consider_positions': consider_positions, 'mode': mode})
        _, data, _ = await http_request("POST", self.root + self._routes["order.margins.basket"], payload=params, headers=self.headers, is_json=True, params={'consider_positions': consider_positions, 'mode': mode})
        return data

    def _warn(self, message):
        """ Add deprecation warning message """
        warnings.simplefilter('always', DeprecationWarning)
        warnings.warn(message, DeprecationWarning)
