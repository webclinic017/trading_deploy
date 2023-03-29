from django.contrib import admin

from apps.trade.models import (
    DeployedOptionStrategy,
    DeployedOptionStrategyParameters,
    DeployedOptionStrategyUser,
    DummyOrder,
    OptionStrategy,
    Order,
)


class DeployedOptionStrategyUserAdmin(admin.TabularInline):
    model = DeployedOptionStrategyUser
    extra = 0


class DeployedOptionStrategyParametersAdmin(admin.TabularInline):
    model = DeployedOptionStrategyParameters
    extra = 0


@admin.register(OptionStrategy)
class OptionStrategyAdmin(admin.ModelAdmin):
    list_display = (
        "name",
        "file_name",
    )


@admin.register(DeployedOptionStrategy)
class DeployedOptionStrategyAdmin(admin.ModelAdmin):
    list_display = (
        "strategy_name",
        "strategy",
        "is_active",
    )

    inlines = (
        DeployedOptionStrategyParametersAdmin,
        DeployedOptionStrategyUserAdmin,
    )


@admin.register(DummyOrder)
class DummyOrderAdmin(admin.ModelAdmin):
    list_display = (
        '__str__',
        'tradingsymbol',
        'order_timestamp',
        'transaction_type',
        'price',
    )


@admin.register(Order)
class OrderAdmin(admin.ModelAdmin):
    list_display = (
        '__str__',
    )
