from django.contrib.auth import get_user_model
from django.db import models
from django.urls import reverse

User = get_user_model()


class OptionStrategy(models.Model):
    STRATEGY_TYPE_CHOICES = (
        ("delta_managing", "Delta Managing"),
        ("ce_pe_with_sl", "CE PE With SL"),
    )
    name = models.CharField(max_length=100, unique=True)
    file_name = models.CharField(max_length=100, unique=True)
    strategy_type = models.CharField(max_length=100, choices=STRATEGY_TYPE_CHOICES, default="delta_managing")

    def __str__(self) -> str:
        return self.name


class DeployedOptionStrategy(models.Model):
    strategy_name = models.CharField(max_length=100, unique=True)
    strategy = models.ForeignKey(OptionStrategy, on_delete=models.CASCADE)
    underlying = models.CharField(max_length=50)
    lot_size = models.IntegerField()
    is_active = models.BooleanField(default=True)

    def get_absolute_url(self):
        return reverse("trades:deployed_strategy", kwargs={"pk": self.pk})

    def __str__(self) -> str:
        return self.strategy_name


class DeployedOptionStrategyUser(models.Model):
    DUMMY = "dummy"
    KOTAK_NEO = "kotak_neo"
    KOTAK = "kotak"
    ZERODAHA = "zerodha"
    DUCKTRADE = "ducktrade"

    broker_choices = [
        (DUMMY, "DUMMY"),
        (KOTAK_NEO, "KOTAK NEO"),
        (KOTAK, "KOTAK"),
        (ZERODAHA, "ZERODAHA"),
        (DUCKTRADE, "DUCK JAINAM TRADE"),
    ]

    parent = models.ForeignKey(DeployedOptionStrategy, on_delete=models.CASCADE, related_name="users")
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    broker = models.CharField(
        max_length=15,
        choices=broker_choices,
    )
    lots = models.IntegerField()
    is_active = models.BooleanField(default=True)

    def __str__(self) -> str:
        return f"{self.user} - {self.lots}"

    class Meta:
        unique_together = (
            (
                "parent",
                "user",
            ),
        )


class DeployedOptionStrategyParameters(models.Model):
    parent = models.ForeignKey(DeployedOptionStrategy, on_delete=models.CASCADE, related_name="parameters")
    name = models.CharField(max_length=100)
    parameters = models.JSONField(default=dict)
    is_active = models.BooleanField(default=True)

    def __str__(self) -> str:
        return f"{self.parent} - {self.name}"

    class Meta:
        unique_together = (
            (
                "parent",
                "name",
            ),
        )

class DummyOrder(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name="dummy_orders")
    tradingsymbol = models.CharField(max_length=100)
    order_id = models.CharField(max_length=100)
    order_timestamp = models.DateTimeField(null=True, blank=True)
    exchange = models.CharField(max_length=10, default="NFO")
    transaction_type = models.CharField(max_length=10)
    quantity = models.IntegerField()
    price = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    trigger_price = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    status = models.CharField(max_length=10, default="COMPLETED")
    tag = models.CharField(max_length=100, blank=True, null=True)

    def __str__(self) -> str:
        return self.order_id

class Order(models.Model):
    DUMMY = "dummy"
    KOTAK_NEO = "kotak_neo"
    KOTAK = "kotak"
    ZERODAHA = "zerodha"
    DUCKTRADE = "ducktrade"

    broker_choices = [
        (DUMMY, "DUMMY"),
        (KOTAK_NEO, "KOTAK NEO"),
        (KOTAK, "KOTAK"),
        (ZERODAHA, "ZERODAHA"),
        (DUCKTRADE, "DUCK JAINAM TRADE"),
    ]

    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name="orders")
    broker = models.CharField(
        max_length=15,
        choices=broker_choices,
    )
    strategy = models.ForeignKey(
        DeployedOptionStrategy,
        on_delete=models.PROTECT,
        related_name="deployed_option_strategies",
        null=True,
        blank=True,
    )
    order_id = models.CharField(max_length=100, null=True, blank=True)
    order_timestamp = models.DateTimeField(null=True, blank=True)
    exchange_order_id = models.CharField(max_length=100, null=True, blank=True)
    exchange_order_timestamp = models.DateTimeField(null=True, blank=True)
    tradingsymbol = models.CharField(max_length=50)
    exchange = models.CharField(max_length=10, default="NFO")
    transaction_type = models.CharField(max_length=10)
    price = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    expected_price = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    average_price = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    trigger_price = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    quantity = models.IntegerField(default=0)
    traded_value = models.DecimalField(max_digits=10, decimal_places=2, default=0)
    tag = models.CharField(max_length=100, null=True, blank=True)
    trade_history = models.JSONField(default=list, null=True, blank=True)

    def __str__(self) -> str:
        return f"{self.tradingsymbol} - {self.order_id}"
