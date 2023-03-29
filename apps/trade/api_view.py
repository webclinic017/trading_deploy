from asgiref.sync import async_to_sync
from django.contrib.auth import get_user_model
from django.core.cache import cache
from django.utils.module_loading import import_string
from rest_framework import authentication, permissions
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView

from apps.trade.models import DeployedOptionStrategy
from apps.trade.square_off_all import square_off_all_user
from apps.trade.square_off_all import square_off_all
from apps.trade.strategy.dynamic_shifting_with_exit_one_side import (
    Strategy as OneSideExitStrategy,
)
from apps.trade.strategy.straddles_with_sl import Strategy as StraddleWithSL
from apps.trade.tasks import get_all_user_open_positions
from utils import divide_and_list, send_notifications
from utils.multi_broker import Broker as MultiBroker
from apps.trade.consumers import adjust_positions


User = get_user_model()


class UpdatePosition(APIView):
    # authentication_classes = [authentication.SessionAuthentication]
    permission_classes = (IsAuthenticated,)

    def get(self, request, format=None):
        get_all_user_open_positions()
        return Response({"message": "success"})


class RebalanceView(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request, format=None):
        row = request.data

        strategy = row["strategy"]
        idx = int(row["index"])

        opt_strategy = DeployedOptionStrategy.objects.filter(pk=strategy).first()

        user_params = cache.get("deployed_strategies", {}).get(str(strategy), {}).get("user_params", [])

        if opt_strategy and user_params:
            parameters = [p.parameters for p in opt_strategy.parameters.filter(is_active=True).order_by("name")]
            Strategy = import_string(f"apps.trade.strategy.{opt_strategy.strategy.file_name}.Strategy")

            strategy: OneSideExitStrategy = Strategy(
                user_params=user_params,
                parameters=parameters,
                opt_strategy=opt_strategy,
            )

            async_to_sync(strategy.manual_shifting)(idx)

            async_to_sync(send_notifications)(
                opt_strategy.strategy_name.upper(), f"{idx} - REBALANCE!", "alert-secondary"
            )

        return Response({"message": "success"})


class ShiftSingleStrike(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request, format=None):
        row = request.data

        strategy = row["strategy"]
        idx = int(row["index"])
        option_type = row["option_type"]
        points = row["points"]

        opt_strategy = DeployedOptionStrategy.objects.filter(pk=strategy).first()

        user_params = cache.get("deployed_strategies", {}).get(str(strategy), {}).get("user_params", [])

        if opt_strategy and user_params:
            parameters = [p.parameters for p in opt_strategy.parameters.filter(is_active=True).order_by("name")]
            Strategy = import_string(f"apps.trade.strategy.{opt_strategy.strategy.file_name}.Strategy")

            strategy: OneSideExitStrategy = Strategy(
                user_params=user_params,
                parameters=parameters,
                opt_strategy=opt_strategy,
            )

            async_to_sync(strategy.manual_shift_single_strike)(idx, option_type, points)

            async_to_sync(send_notifications)(
                opt_strategy.strategy_name.upper(),
                f"{idx} - {option_type} - MANUAL SHIFT SINGLE STRIKE {'OUT' if  points < 0 else 'IN'}!",
            )

        return Response({"message": "success"})


class ReentryOneSide(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request, format=None):
        row = request.data

        strategy = row["strategy"]
        idx = int(row["index"])

        opt_strategy = DeployedOptionStrategy.objects.filter(pk=strategy).first()

        user_params = cache.get("deployed_strategies", {}).get(str(strategy), {}).get("user_params", [])

        if opt_strategy and user_params:
            parameters = [p.parameters for p in opt_strategy.parameters.filter(is_active=True).order_by("name")]
            Strategy = import_string(f"apps.trade.strategy.{opt_strategy.strategy.file_name}.Strategy")

            strategy: OneSideExitStrategy = Strategy(
                user_params=user_params,
                parameters=parameters,
                opt_strategy=opt_strategy,
            )

            async_to_sync(strategy.manual_reentry)(idx)

            async_to_sync(send_notifications)(
                opt_strategy.strategy_name.upper(), f"{idx} - MANUAL REENTRY!", "alert-success"
            )

        return Response({"message": "success"})


class ExitOneSide(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request, format=None):
        row = request.data

        strategy = row["strategy"]
        idx = int(row["index"])
        option_type = row["option_type"]

        opt_strategy = DeployedOptionStrategy.objects.filter(pk=strategy).first()

        user_params = cache.get("deployed_strategies", {}).get(str(strategy), {}).get("user_params", [])

        if opt_strategy and user_params:
            parameters = [p.parameters for p in opt_strategy.parameters.filter(is_active=True).order_by("name")]
            Strategy = import_string(f"apps.trade.strategy.{opt_strategy.strategy.file_name}.Strategy")

            strategy: OneSideExitStrategy = Strategy(
                user_params=user_params,
                parameters=parameters,
                opt_strategy=opt_strategy,
            )

            async_to_sync(strategy.manual_exit)(idx, option_type)

            async_to_sync(send_notifications)(
                opt_strategy.strategy_name.upper(), f"{idx} - {option_type} - MANUAL ONE SIDE EXIT!", "alert-danger"
            )

        return Response({"message": "success"})


class OneSideExitHold(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request, format=None):
        row = request.data

        strategy = row["strategy"]
        idx = int(row["index"])

        opt_strategy = DeployedOptionStrategy.objects.filter(pk=strategy).first()

        user_params = cache.get("deployed_strategies", {}).get(str(strategy), {}).get("user_params", [])

        if opt_strategy and user_params:
            parameters = [p.parameters for p in opt_strategy.parameters.filter(is_active=True).order_by("name")]
            Strategy = import_string(f"apps.trade.strategy.{opt_strategy.strategy.file_name}.Strategy")

            strategy: OneSideExitStrategy = Strategy(
                user_params=user_params,
                parameters=parameters,
                opt_strategy=opt_strategy,
            )

            async_to_sync(strategy.one_side_exit_hold)(idx)

            async_to_sync(send_notifications)(
                opt_strategy.strategy_name.upper(), f"{idx} - ONE SIDE EXIT HOLD MARKED!", "alert-warning"
            )

        return Response({"message": "success"})


class ReleaseOneSideExitHold(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request, format=None):
        row = request.data

        strategy = row["strategy"]
        idx = int(row["index"])

        opt_strategy = DeployedOptionStrategy.objects.filter(pk=strategy).first()

        user_params = cache.get("deployed_strategies", {}).get(str(strategy), {}).get("user_params", [])

        if opt_strategy and user_params:
            parameters = [p.parameters for p in opt_strategy.parameters.filter(is_active=True).order_by("name")]
            Strategy = import_string(f"apps.trade.strategy.{opt_strategy.strategy.file_name}.Strategy")

            strategy: OneSideExitStrategy = Strategy(
                user_params=user_params,
                parameters=parameters,
                opt_strategy=opt_strategy,
            )

            async_to_sync(strategy.release_one_side_exit_hold)(idx)

            async_to_sync(send_notifications)(
                opt_strategy.strategy_name.upper(), f"{idx} - ONE SIDE EXIT HOLD RELEASED!"
            )

        return Response({"message": "success"})


class StartAlgo(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request, format=None):
        row = request.data

        strategy = row["strategy"]

        opt_strategy: DeployedOptionStrategy | None = DeployedOptionStrategy.objects.filter(pk=strategy).first()

        user_params = []
        for user in opt_strategy.users.filter(is_active=True):
            order_obj = async_to_sync(MultiBroker)(user.user.username, user.broker)
            user_params.append(
                {
                    "user": user.user,
                    "quantity": user.lots * 25,
                    "order_obj": order_obj,
                }
            )

        if opt_strategy and user_params:
            Strategy = import_string(f"apps.trade.strategy.{opt_strategy.strategy.file_name}.Strategy")

            strategy: StraddleWithSL = Strategy(
                user_params=user_params,
                opt_strategy=opt_strategy,
            )

            async_to_sync(strategy.run)()

        return Response({"message": "success"})


class PlaceStraddle(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request, format=None):
        row = request.data

        strategy = row["strategy"]

        idx = row["index"]

        opt_strategy: DeployedOptionStrategy | None = DeployedOptionStrategy.objects.filter(pk=strategy).first()

        user_params = cache.get("deployed_strategies", {}).get(str(strategy), {}).get("user_params", [])

        if opt_strategy and user_params:
            parameters = opt_strategy.parameters.get(name=idx, is_active=True).parameters
            Strategy = import_string(f"apps.trade.strategy.{opt_strategy.strategy.file_name}.Strategy")

            strategy: StraddleWithSL = Strategy(
                user_params=user_params,
                opt_strategy=opt_strategy,
            )

            async_to_sync(strategy.place_straddle)(idx, parameters["sl_pct"])

        return Response({"message": "success"})


class ModifySlToCost(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request, format=None):
        row = request.data

        strategy = row["strategy"]

        idx = row["index"]

        opt_strategy: DeployedOptionStrategy | None = DeployedOptionStrategy.objects.filter(pk=strategy).first()

        user_params = cache.get("deployed_strategies", {}).get(str(strategy), {}).get("user_params", [])

        if opt_strategy and user_params:
            Strategy = import_string(f"apps.trade.strategy.{opt_strategy.strategy.file_name}.Strategy")

            strategy: StraddleWithSL = Strategy(
                user_params=user_params,
                opt_strategy=opt_strategy,
            )

            async_to_sync(strategy.modify_to_cost)(idx=idx)

        return Response({"message": "success"})


class ExitStraddleOrder(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request, format=None):
        row = request.data

        strategy = row["strategy"]

        idx = row["index"]

        opt_strategy: DeployedOptionStrategy | None = DeployedOptionStrategy.objects.filter(pk=strategy).first()

        user_params = cache.get("deployed_strategies", {}).get(str(strategy), {}).get("user_params", [])

        if opt_strategy and user_params:
            Strategy = import_string(f"apps.trade.strategy.{opt_strategy.strategy.file_name}.Strategy")

            strategy: StraddleWithSL = Strategy(
                user_params=user_params,
                opt_strategy=opt_strategy,
            )

            async_to_sync(strategy.exit_order)(idx=idx)

        return Response({"message": "success"})


class UpdateStraddleStrategyPosition(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request, format=None):
        row = request.data

        strategy = row["strategy"]

        opt_strategy: DeployedOptionStrategy | None = DeployedOptionStrategy.objects.filter(pk=strategy).first()

        user_params = cache.get("deployed_strategies", {}).get(str(strategy), {}).get("user_params", [])

        if opt_strategy and user_params:
            Strategy = import_string(f"apps.trade.strategy.{opt_strategy.strategy.file_name}.Strategy")

            strategy: StraddleWithSL = Strategy(
                user_params=user_params,
                opt_strategy=opt_strategy,
            )

            async_to_sync(strategy.update_position)()

        return Response({"message": "success"})


class ExitAlgo(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request, format=None):
        row = request.data

        strategy = row["strategy"]

        opt_strategy: DeployedOptionStrategy | None = DeployedOptionStrategy.objects.filter(pk=strategy).first()

        user_params = cache.get("deployed_strategies", {}).get(str(strategy), {}).get("user_params", [])

        if opt_strategy and user_params:
            parameters = [p.parameters for p in opt_strategy.parameters.filter(is_active=True).order_by("name")]
            Strategy = import_string(f"apps.trade.strategy.{opt_strategy.strategy.file_name}.Strategy")

            strategy: OneSideExitStrategy = Strategy(
                user_params=user_params,
                parameters=parameters,
                opt_strategy=opt_strategy,
            )

            async_to_sync(strategy.exit_algo)()

            async_to_sync(send_notifications)(opt_strategy.strategy_name.upper(), "EXITED ALGO!", "alert-danger")

        return Response({"message": "success"})


class ExitUserAlgo(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request, format=None):
        row = request.data

        strategy = row["strategy"]

        opt_strategy: DeployedOptionStrategy | None = DeployedOptionStrategy.objects.filter(pk=strategy).first()

        user_params = cache.get("deployed_strategies", {}).get(str(strategy), {}).get("user_params", [])

        username = row["username"]

        user = User.objects.filter(username=username).first()

        if opt_strategy and user_params and user:
            parameters = [p.parameters for p in opt_strategy.parameters.filter(is_active=True).order_by("name")]
            Strategy = import_string(f"apps.trade.strategy.{opt_strategy.strategy.file_name}.Strategy")

            strategy: OneSideExitStrategy = Strategy(
                user_params=user_params,
                parameters=parameters,
                opt_strategy=opt_strategy,
            )

            async_to_sync(strategy.exit_user_algo)(user)

            async_to_sync(send_notifications)(opt_strategy.strategy_name.upper(), "EXITED ALGO!", "alert-danger")

        return Response({"message": "success"})


class SquareOffUser(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request, format=None):
        row = request.data
        deployed_strategies = cache.get("deployed_strategies", {}).copy()

        for _, strategy in deployed_strategies.items():
            user_params = strategy["user_params"]
            updated_user_params = [
                user_param for user_param in user_params if user_param["user"].username != row["username"]
            ]
            strategy["user_params"] = updated_user_params

        cache.set("deployed_strategies", deployed_strategies)

        async_to_sync(square_off_all)(row["username"], row["broker"])

        return Response({"message": "success"})


class EnterUserAlgo(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request, format=None):
        row = request.data

        strategy = row["strategy"]

        opt_strategy: DeployedOptionStrategy | None = DeployedOptionStrategy.objects.filter(pk=strategy).first()

        user_params = cache.get("deployed_strategies", {}).get(str(strategy), {}).get("user_params", [])

        username = row["username"]

        user = User.objects.filter(username=username).first()

        user_strategy = opt_strategy.users.filter(user=user).first()

        if opt_strategy and user_params and user:
            user_strategy_obj = opt_strategy.users.filter(user=user).first()

            if user_strategy_obj:
                parameters = [p.parameters for p in opt_strategy.parameters.filter(is_active=True).order_by("name")]
                order_obj = async_to_sync(MultiBroker)(user.username, user_strategy.broker)

                user_param_user_obj = {
                    "user": user_strategy_obj.user,
                    "quantity_multiple": [
                        item * 25 for item in divide_and_list(len(parameters), user_strategy_obj.lots)
                    ],
                    "order_obj": order_obj,
                }
                parameters = [p.parameters for p in opt_strategy.parameters.filter(is_active=True).order_by("name")]
                Strategy = import_string(f"apps.trade.strategy.{opt_strategy.strategy.file_name}.Strategy")

                strategy: OneSideExitStrategy = Strategy(
                    user_params=user_params,
                    parameters=parameters,
                    opt_strategy=opt_strategy,
                )

                async_to_sync(strategy.user_entry)(user_param_user_obj)

                async_to_sync(send_notifications)(opt_strategy.strategy_name.upper(), "EXITED ALGO!", "alert-danger")

        return Response(str(user_param_user_obj))


class SquareOffAll(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, request, format=None):
        async_to_sync(square_off_all_user)()
        return Response({"message": "success"})


class SquareOffMarket(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, request, format=None):
        async_to_sync(square_off_all_user)(True)
        return Response({"message": "success"})

class AdjustAllPosition(APIView):
    permission_classes = (IsAuthenticated,)

    def get(self, request, format=None):
        async_to_sync(adjust_positions)()
        return Response({"message": "success"})


class AdjustUserPosition(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request, format=None):
        row = request.data
        username = row['username']
        broker = row['broker']

        async_to_sync(adjust_positions)(username, broker)
        return Response({"message": "success"})


class UserEntry(APIView):
    permission_classes = (IsAuthenticated,)

    def post(self, request, format=None):
        row = request.data

        strategy = row["strategy"]

        opt_strategy: DeployedOptionStrategy | None = DeployedOptionStrategy.objects.filter(pk=strategy).first()

        user_params = cache.get("deployed_strategies", {}).get(str(strategy), {}).get("user_params", [])

        username = row["username"]

        user = User.objects.filter(username=username).first()

        if opt_strategy and user_params and user:
            user_strategy_obj = opt_strategy.users.filter(user=user).first()

            if user_strategy_obj:
                parameters = [p.parameters for p in opt_strategy.parameters.filter(is_active=True).order_by("name")]
                order_obj = NeoOrder(user.user.username)
                async_to_sync(order_obj.init())

                user_param_user_obj = {
                    "user": user_strategy_obj.user,
                    "quantity_multiple": [item * 25 for item in divide_and_list(len(parameters), user.lots)],
                    "order_obj": order_obj,
                }
                parameters = [p.parameters for p in opt_strategy.parameters.filter(is_active=True).order_by("name")]
                Strategy = import_string(f"apps.trade.strategy.{opt_strategy.strategy.file_name}.Strategy")

                strategy: OneSideExitStrategy = Strategy(
                    user_params=user_params,
                    parameters=parameters,
                    opt_strategy=opt_strategy,
                )

                # async_to_sync(strategy.user_entry)(user_param_user_obj)

                # async_to_sync(send_notifications)(opt_strategy.strategy_name.upper(), "EXITED ALGO!", "alert-danger")

        return Response(str(user_param_user_obj))

