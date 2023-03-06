from django.contrib import admin

from apps.integration.models import (
    BrokerApi,
    KotakNeoApi,
    KotakSecuritiesApi,
    ZerodhaApi,
)


@admin.register(BrokerApi)
class BrokerApiAdmin(admin.ModelAdmin):
    list_display = (
        "user",
        "broker",
        "is_active",
    )


@admin.register(KotakNeoApi)
class KotakNeoApiAdmin(admin.ModelAdmin):
    fields = (
        "broker_api",
        "mobile_number",
        "pan_number",
        "password",
        "mpin",
        "neo_fin_key",
        "consumer_key",
        "consumer_secret",
        "access_token",
        "sid",
        "rid",
        "auth",
        "hs_server_id",
        "update_auth_token",
    )
    readonly_fields = (
        "access_token",
        "sid",
        "rid",
        "auth",
        "hs_server_id",
    )


@admin.register(KotakSecuritiesApi)
class KotakSecuritiesApi(admin.ModelAdmin):
    fields = (
        "broker_api",
        "userid",
        "password",
        "pin",
        "consumer_key",
        "consumer_secret",
        "access_token",
        "one_time_token",
        "session_token",
    )


@admin.register(ZerodhaApi)
class ZerodhaApi(admin.ModelAdmin):
    fields = (
        "broker_api",
        "userid",
        "password",
        "two_fa",
        "session_token",
    )
