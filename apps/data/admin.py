from django.contrib import admin

# Register your models here.
from apps.data.models import DailyData, Instrument


@admin.register(Instrument)
class Instrument(admin.ModelAdmin):
    list_display = (
        "ticker",
    )
    list_filter = (
        "ticker",
    )

@admin.register(DailyData)
class DailyData(admin.ModelAdmin):
    list_display = (
        "instrument",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
    )
    list_filter = (
        "instrument__ticker",
    )
