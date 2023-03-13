import asyncio

# import pandas as pd
from django.core.cache import cache

from apps.integration.models import BrokerApi

# from apps.integration.models import KotakNeoApi
from utils.multi_broker import Broker as MultiBroker


async def square_off_all(username, broker, market=False):
    order = await MultiBroker(username, broker)
    await order.initiate_session()

    return await order.square_off_all(market)


async def square_off_all_user(market=False):
    square_off_data = [square_off_all(broker_api.user.username, broker_api.broker, market) async for broker_api in BrokerApi.objects.filter(is_active=True, broker__in=['kotak', 'kotak_neo'])] # noqa E501
    deployed_strategies = cache.get("deployed_strategies", {}).copy()

    for _, strategy in deployed_strategies.items():
        user_params = strategy['user_params']
        updated_user_params = [
            user_param
            for user_param in user_params
            if user_param['order_obj'].broker_name == 'dummy'
        ]
        strategy['user_params'] = updated_user_params
    
    cache.set("deployed_strategies", deployed_strategies)

    return await asyncio.gather(*square_off_data)