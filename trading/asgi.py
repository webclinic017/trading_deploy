"""
ASGI config for trading project.

It exposes the ASGI callable as a module-level variable named ``application``.

For more information on this file, see
https://docs.djangoproject.com/en/4.1/howto/deployment/asgi/
"""

import os

from channels.auth import AuthMiddlewareStack
from channels.routing import ProtocolTypeRouter, URLRouter
from django.core.asgi import get_asgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "trading.settings")
os.environ["DJANGO_ALLOW_ASYNC_UNSAFE"] = "true"
http = get_asgi_application()

from apps.trade import routing  # noqa E401

application = ProtocolTypeRouter(
    {
        "http": http,
        "websocket": AuthMiddlewareStack(URLRouter(routing.websocket_urlpatterns)),
    }
)
