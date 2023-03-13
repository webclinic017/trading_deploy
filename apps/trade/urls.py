from django.urls import include, path
from rest_framework import routers

from .api_view import (
    EnterUserAlgo,
    ExitAlgo,
    ExitOneSide,
    ExitStraddleOrder,
    ExitUserAlgo,
    ModifySlToCost,
    OneSideExitHold,
    PlaceStraddle,
    RebalanceView,
    ReentryOneSide,
    ReleaseOneSideExitHold,
    ShiftSingleStrike,
    SquareOffAll,
    SquareOffMarket,
    StartAlgo,
    UpdatePosition,
    UpdateStraddleStrategyPosition,
)
from .views import (  # DeployedStrategyCreate,
    DeployeOptionStrategyDetailView,
    LivePnlView,
    LivePositionView,
    MultiBacktestingView,
    PCRView,
    StrategyListView,
)

# router = routers.DefaultRouter()
# router.register(r'update_position', UpdatePosition)

urlpatterns = [
    path("strategies/", StrategyListView.as_view(), name="strategy_list"),
    path("multi_backtesting/", MultiBacktestingView.as_view(), name="multi_backtesting"),
    path("pcr", PCRView.as_view(), name="pcr"),
    path('live_pnl', LivePnlView.as_view(), name='live_pnl'),
    path('live_position', LivePositionView.as_view(), name='live_position'),
    path('update_position', UpdatePosition.as_view(), name="update_position"),
    path('update_straddle_stragety_position', UpdateStraddleStrategyPosition.as_view(), name="update_straddle_stragety_position"),
    path('exit_straddle_order', ExitStraddleOrder.as_view(), name="exit_straddle_order"),
    path('rebalance_position', RebalanceView.as_view(), name="rebalance_position"),
    path('shift_single_strike', ShiftSingleStrike.as_view(), name="shift_single_strike"),
    path('start_algo', StartAlgo.as_view(), name='start_algo'),
    path('exit_algo', ExitAlgo.as_view(), name='exit_algo'),
    path('place_straddle', PlaceStraddle.as_view(), name='place_straddle'),
    path('modify_sl_to_cost', ModifySlToCost.as_view(), name='modify_sl_to_cost'),
    path('exit_user_algo', ExitUserAlgo.as_view(), name='exit_user_algo'),
    path('entry_user_algo', EnterUserAlgo.as_view(), name='entry_user_algo'),
    path('square_off_all', SquareOffAll.as_view(), name='square_off_all'),
    path('square_off_market', SquareOffMarket.as_view(), name='square_off_market'),
    path('reentry_one_side', ReentryOneSide.as_view(), name="reentry_one_side"),
    path('one_side_exit_hold', OneSideExitHold.as_view(), name="one_side_exit_hold"),
    path('release_one_side_exit_hold', ReleaseOneSideExitHold.as_view(), name="release_one_side_exit_hold"),
    path('exit_one_side', ExitOneSide.as_view(), name="exit_one_side"),
    path('deployed_strategy/<pk>', DeployeOptionStrategyDetailView.as_view(), name='deployed_strategy')
]