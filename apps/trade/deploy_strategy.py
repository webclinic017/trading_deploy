from django.utils.module_loading import import_string

from apps.trade.models import DeployedOptionStrategy
from utils import divide_and_list
from utils.multi_broker import Broker as MultiBroker

option_strategy = DeployedOptionStrategy.objects.get(strategy_name="Banknifty One Side Exit")


async def run_strategy(min_delta=None, opt_strategy=option_strategy):
    if min_delta is None:
        min_delta = [45]
    Strategy = import_string(f"apps.trade.strategy.{opt_strategy.strategy.file_name}.Strategy")

    if opt_strategy.is_active:
        parameters = [p.parameters for p in opt_strategy.parameters.filter(is_active=True).order_by("name")]
        user_params = []
        for user in opt_strategy.users.filter(is_active=True):
            order_obj = await MultiBroker(user.user.username, user.broker)
            user_params.append(
                {
                    "user": user.user,
                    "quantity_multiple": [item * 25 for item in divide_and_list(len(parameters), user.lots)],
                    "order_obj": order_obj,
                }
            )

        strategy = Strategy(
            user_params=user_params,
            parameters=parameters,
            opt_strategy=opt_strategy,
            min_delta=min_delta,
        )
        await strategy.run()


async def run_true_strategy(data=None, opt_strategy=option_strategy):
    Strategy = import_string(f"apps.trade.strategy.{opt_strategy.strategy.file_name}.Strategy")

    if opt_strategy.is_active:
        parameters = [p.parameters for p in opt_strategy.parameters.filter(is_active=True).order_by("name")]
        user_params = []
        for user in opt_strategy.users.filter(is_active=True):
            order_obj = await MultiBroker(user.user.username, user.broker)
            user_params.append(
                {
                    "user": user.user,
                    "quantity_multiple": [item * 25 for item in divide_and_list(len(parameters), user.lots)],
                    "order_obj": order_obj,
                }
            )

        strategy = Strategy(
            user_params=user_params,
            parameters=parameters,
            opt_strategy=opt_strategy,
        )
        await strategy.run(entered=True, data=data)


async def manual_reentry(idx, opt_strategy=option_strategy):
    Strategy = import_string(f"apps.trade.strategy.{opt_strategy.strategy.file_name}.Strategy")

    if opt_strategy.is_active:
        parameters = [p.parameters for p in opt_strategy.parameters.filter(is_active=True).order_by("name")]
        user_params = []
        for user in opt_strategy.users.filter(is_active=True):
            order_obj = await MultiBroker(user.user.username, user.broker)
            user_params.append(
                {
                    "user": user.user,
                    "quantity_multiple": [item * 25 for item in divide_and_list(len(parameters), user.lots)],
                    "order_obj": order_obj,
                }
            )

        strategy = Strategy(
            user_params=user_params,
            parameters=parameters,
            opt_strategy=opt_strategy,
        )
        await strategy.manual_reentry(idx)


async def manual_exit(idx, option_type, opt_strategy=option_strategy):
    Strategy = import_string(f"apps.trade.strategy.{opt_strategy.strategy.file_name}.Strategy")

    if opt_strategy.is_active:
        parameters = [p.parameters for p in opt_strategy.parameters.filter(is_active=True).order_by("name")]
        user_params = []
        for user in opt_strategy.users.filter(is_active=True):
            order_obj = await MultiBroker(user.user.username, user.broker)
            user_params.append(
                {
                    "user": user.user,
                    "quantity_multiple": [item * 25 for item in divide_and_list(len(parameters), user.lots)],
                    "order_obj": order_obj,
                }
            )

        strategy = Strategy(
            user_params=user_params,
            parameters=parameters,
            opt_strategy=opt_strategy,
        )
        await strategy.manual_exit(idx, option_type)


async def manual_shifting(idx, opt_strategy=option_strategy):
    Strategy = import_string(f"apps.trade.strategy.{opt_strategy.strategy.file_name}.Strategy")

    if opt_strategy.is_active:
        parameters = [p.parameters for p in opt_strategy.parameters.filter(is_active=True).order_by("name")]
        user_params = []
        for user in opt_strategy.users.filter(is_active=True):
            order_obj = await MultiBroker(user.user.username, user.broker)
            user_params.append(
                {
                    "user": user.user,
                    "quantity_multiple": [item * 25 for item in divide_and_list(len(parameters), user.lots)],
                    "order_obj": order_obj,
                }
            )

        strategy = Strategy(
            user_params=user_params,
            parameters=parameters,
            opt_strategy=opt_strategy,
        )
        await strategy.manual_shifting(idx)


async def exit_algo(opt_strategy=option_strategy):
    Strategy = import_string(f"apps.trade.strategy.{opt_strategy.strategy.file_name}.Strategy")

    if opt_strategy.is_active:
        parameters = [p.parameters for p in opt_strategy.parameters.filter(is_active=True).order_by("name")]
        user_params = []
        for user in opt_strategy.users.filter(is_active=True):
            order_obj = await MultiBroker(user.user.username, user.broker)
            user_params.append(
                {
                    "user": user.user,
                    "quantity_multiple": [item * 25 for item in divide_and_list(len(parameters), user.lots)],
                    "order_obj": order_obj,
                }
            )

        strategy = Strategy(
            user_params=user_params,
            parameters=parameters,
            opt_strategy=opt_strategy,
        )
        await strategy.exit_algo()


async def manual_shift_single_strike(idx, option_type, points, opt_strategy=option_strategy):
    Strategy = import_string(f"apps.trade.strategy.{opt_strategy.strategy.file_name}.Strategy")

    if opt_strategy.is_active:
        parameters = [p.parameters for p in opt_strategy.parameters.filter(is_active=True).order_by("name")]
        user_params = []
        for user in opt_strategy.users.filter(is_active=True):
            order_obj = await MultiBroker(user.user.username, user.broker)
            user_params.append(
                {
                    "user": user.user,
                    "quantity_multiple": [item * 25 for item in divide_and_list(len(parameters), user.lots)],
                    "order_obj": order_obj,
                }
            )

        strategy = Strategy(
            user_params=user_params,
            parameters=parameters,
            opt_strategy=opt_strategy,
        )
        await strategy.manual_shift_single_strike(idx, option_type, points)


async def release_one_side_exit_hold(idx, opt_strategy=option_strategy):
    Strategy = import_string(f"apps.trade.strategy.{opt_strategy.strategy.file_name}.Strategy")

    if opt_strategy.is_active:
        parameters = [p.parameters for p in opt_strategy.parameters.filter(is_active=True).order_by("name")]
        user_params = []
        for user in opt_strategy.users.filter(is_active=True):
            order_obj = await MultiBroker(user.user.username, user.broker)
            user_params.append(
                {
                    "user": user.user,
                    "quantity_multiple": [item * 25 for item in divide_and_list(len(parameters), user.lots)],
                    "order_obj": order_obj,
                }
            )

        strategy = Strategy(
            user_params=user_params,
            parameters=parameters,
            opt_strategy=opt_strategy,
        )
        await strategy.release_one_side_exit_hold(idx)


async def one_side_exit_hold(idx, opt_strategy=option_strategy):
    Strategy = import_string(f"apps.trade.strategy.{opt_strategy.strategy.file_name}.Strategy")

    if opt_strategy.is_active:
        parameters = [p.parameters for p in opt_strategy.parameters.filter(is_active=True).order_by("name")]
        user_params = []
        for user in opt_strategy.users.filter(is_active=True):
            order_obj = await MultiBroker(user.user.username, user.broker)
            user_params.append(
                {
                    "user": user.user,
                    "quantity_multiple": [item * 25 for item in divide_and_list(len(parameters), user.lots)],
                    "order_obj": order_obj,
                }
            )

        strategy = Strategy(
            user_params=user_params,
            parameters=parameters,
            opt_strategy=opt_strategy,
        )
        await strategy.one_side_exit_hold(idx)
