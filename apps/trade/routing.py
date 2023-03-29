from django.urls import path

from apps.trade.consumers import (  # AlgoStatusConsumer,;
    ChatConsumer,
    DeployedOptionStrategySymbolConsumer,
    LivekPositionConsumer,
    LivePnlConsumer,
    NotificationConsumner,
    LivePnlConsumerStrategy,
    StopLossDifference
)

websocket_urlpatterns = [
    path("ws/bnf_pcr/", ChatConsumer.as_asgi()),
    path("ws/live_pnl/", LivePnlConsumer.as_asgi()),
    path("ws/live_pnl/<pk>", LivePnlConsumerStrategy.as_asgi()),
    path("ws/stop_loss_difference/<pk>", StopLossDifference.as_asgi()),
    path("ws/live_positions/", LivekPositionConsumer.as_asgi()),
    path("ws/deployed_option_strategy_symbol/<pk>", DeployedOptionStrategySymbolConsumer.as_asgi()),
    # path("ws/algo_status/<pk>", AlgoStatusConsumer.as_asgi()),
    path("ws/read_notifications", NotificationConsumner.as_asgi()),
]
