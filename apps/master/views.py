from django.contrib import messages
from django.contrib.auth import authenticate, get_user_model, login, logout
from django.contrib.auth import views as auth_views
from django.contrib.auth.forms import AuthenticationForm
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import redirect, render
from django.urls import reverse_lazy
from django.views.generic import TemplateView, View

User = get_user_model()


class LoginView(View):
    def post(self, request):
        username = request.POST['username']
        password = request.POST['password']
        user = authenticate(username=username, password=password)

        if user is not None:
            login(request, user)
            return redirect(reverse_lazy('about'), kwargs={'user': user})
        else:
            form = AuthenticationForm()
            return render(request, "login.html", {'form': form, 'error': 'No User Found'})

    def get(self, request):
        form = AuthenticationForm()
        return render(request, "login.html", {'form': form})


class LogoutView(View):
    def get(self, request):
        logout(request)
        return redirect(reverse_lazy('about'))


class AboutView(TemplateView):
    template_name = "/home/finbyz/project/trading_deploy/templates/index.html"


# def index(request):
#     context = {}
#     return render(request, 'templates/index.html', context)