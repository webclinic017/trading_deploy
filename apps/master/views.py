from django.contrib import messages
from django.contrib.auth import authenticate, get_user_model, login, logout
from django.contrib.auth.decorators import login_required
from django.shortcuts import redirect, render
from django.utils.decorators import method_decorator
from django.views.generic.list import ListView

User = get_user_model()


@login_required()
def home_page(request):
    return render(request, "home.html", {})

def login_page(request):
    if request.user.is_authenticated:
        return redirect("master:home")

    if request.method == "POST":
        username = request.POST.get("username")
        password = request.POST.get("password")
        user = authenticate(request, username=username, password=password)
        print(username, password)
        if user is not None:
            login(request, user)
            return redirect("master:home")
        else:
            print("Invalid")
            messages.info(request, "Try again! username or password is incorrect")

    context = {}

    return render(request, "login.html", context)


@login_required()
def logout_page(request):
    logout(request)
    return redirect("master:login")

@method_decorator(login_required(), name="dispatch")
class UserList(ListView):
    model = User
    paginate_by: int = 25
    template_name = "user_list.html"

    def get_queryset(self):
        return User.objects.filter(is_superuser=False)


@method_decorator(login_required(), name="dispatch")
class MyAccount(ListView):
    model = User
    paginate_by: int = 25
    template_name = "my_account.html"

    def get_queryset(self):
        return User.objects.filter(is_superuser=False)