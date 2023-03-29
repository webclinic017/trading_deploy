from django.contrib.auth.decorators import login_required
from django.core.cache import cache
from django.shortcuts import redirect, render
from django.utils.decorators import method_decorator
from django.views.generic import TemplateView
from django.views.generic.base import ContextMixin
from django.views.generic.detail import DetailView
from django.views.generic.list import ListView

from apps.trade.models import DeployedOptionStrategy, OptionStrategy


# Create your views here.
class NavView(ContextMixin):
    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        context["dynamic_backtest_link"] = DeployedOptionStrategy.objects.filter(is_active=True)
        return context


@method_decorator(login_required, name="dispatch")
class PCRView(NavView, TemplateView):
    template_name: str = "websockets.html"

    def get_context_data(self, *args, **kwargs):
        context = super(PCRView, self).get_context_data(*args, **kwargs)
        context["title"] = "PCR"
        return context


@method_decorator(login_required, name="dispatch")
class DeployeOptionStrategyDetailView(NavView, DetailView):
    template_name: str = "deployed_strategy_detail_view.html"
    model = DeployedOptionStrategy

    def get_context_data(self, *args, **kwargs):
        context = super(DeployeOptionStrategyDetailView, self).get_context_data(*args, **kwargs)
        context["title"] = context["object"].strategy_name
        context['stop_loss'] = cache.get("STRATEGY_STOP_LOSS", 0)
        if cache.get('ENTRY_TIME'):
            context['entry_time'] = cache.get('ENTRY_TIME').strftime("%H:%M")
        else:
            context['entry_time'] = ''
        deployed_option_strategy: DeployedOptionStrategy = context["object"]
        if deployed_option_strategy.strategy.strategy_type == "ce_pe_with_sl":
            self.template_name: str = "ce_pe_with_sl_detail_view.html"
        return context


@method_decorator(login_required(), name="dispatch")
class StrategyListView(ListView):
    model = OptionStrategy
    paginate_by: int = 25
    template_name = "strategy_list.html"

    def get_queryset(self):
        return OptionStrategy.objects.all()


@method_decorator(login_required, name="dispatch")
class LivePnlView(NavView, TemplateView):
    template_name: str = "pnl_websocket.html"

    def get_context_data(self, *args, **kwargs):
        context = super(LivePnlView, self).get_context_data(*args, **kwargs)
        context["title"] = "Live PNL"
        return context


@method_decorator(login_required, name="dispatch")
class LivePositionView(NavView, TemplateView):
    template_name: str = "positions_websocket.html"

    def get_context_data(self, *args, **kwargs):
        context = super(LivePositionView, self).get_context_data(*args, **kwargs)
        context["title"] = "Live PNL"
        return context


@method_decorator(login_required(), name="dispatch")
class MultiBacktestingView(ListView):
    model = OptionStrategy
    paginate_by: int = 25
    template_name = "multi_backtesting.html"

    def get_queryset(self):
        return OptionStrategy.objects.all()