import random
import string

from django.utils import timezone
from django_pandas.io import read_frame

from apps.trade.models import DummyOrder
from utils.async_obj import AsyncObj
import numpy as np


class DummyApi(AsyncObj):
    async def __ainit__(self, user):
        self.user = user

    async def generate_token(self, n=7):
        return "".join(random.choices(string.ascii_uppercase + string.digits, k=n))

    def apply_slippage(self, rate, slippage=4):
        return round(rate * abs((rate ** (1 / ((max(rate, 0.05)) * slippage))) - 1), 2)

    async def place_order(
        self,
        tradingsymbol,
        exchange,
        transaction_type,
        quantity,
        price=0,
        trigger_price=0,
        tag="string",
    ):
        while True:
            order_id = await self.generate_token(12)
            if not DummyOrder.objects.filter(order_id=order_id).exists():
                break

        DummyOrder.objects.create(
            user=self.user,
            tradingsymbol=tradingsymbol,
            order_id=order_id,
            order_timestamp=timezone.localtime(),
            exchange=exchange,
            quantity=quantity,
            transaction_type=transaction_type,
            price=price,
            trigger_price=trigger_price,
            tag=tag,
        )

        return {
            "order_id": order_id,
            "message": f"Your Order has been Placed and Forwarded to the Exchange: {order_id}",
        }

    async def single_order_report(self, order_id, is_fno, error_message=None):
        order = DummyOrder.objects.get(order_id=order_id)
        return {
            "order_id": order_id,
            "tradingsymbol": order.tradingsymbol,
            "exchange": order.exchange,
            "transaction_type": order.transaction_type,
            "quantity": order.quantity,
            "price": order.price,
            "trigger_price": order.trigger_price,
            "status": order.status,
            "tag": order.tag,
        }

    async def positions(self):
        qs = DummyOrder.objects.filter(
            user=self.user,
            order_timestamp__gte=timezone.localtime().replace(hour=0, minute=0, second=0, microsecond=0),
        )
        df = read_frame(qs)
        df["price"] = df["price"].astype(float)
        df['slippage'] = df['price'].apply(self.apply_slippage)
        df["price"] = np.where(df["transaction_type"] == "BUY", df['price'] + df['slippage'], df['price'] - df['slippage'])
        df['total_value'] = df["price"] * df['quantity']
        df = (
            df.groupby(["tradingsymbol", "transaction_type"])
            .agg(
                {
                    "quantity": "sum",
                    "price": "mean",
                    "total_value": "sum",
                }
            )
            .reset_index()
        )

        buy_df = df[df["transaction_type"] == "BUY"]
        sell_df = df[df["transaction_type"] == "SELL"]

        buy_df = buy_df.rename(
            columns={
                "quantity": "buy_qty",
                "price": "buy_avg",
                "total_value": "buy_value",
            }
        )
        buy_df.drop(columns=["transaction_type"], inplace=True)
        sell_df = sell_df.rename(
            columns={
                "quantity": "sell_qty",
                "price": "sell_avg",
                "total_value": "sell_value",
            }
        )
        sell_df.drop(columns=["transaction_type"], inplace=True)

        df = buy_df.merge(sell_df, on="tradingsymbol", how="outer").fillna(0)

        return df

    async def margin(self):
        return 0