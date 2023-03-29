from django.urls import path

from .views import HomeView, MyAccount, UserList, login_page, logout_page

urlpatterns = [
    path("", HomeView.as_view(), name="home"),
    path("login/", login_page, name="login"),
    path("logout/", logout_page, name="logout"),
    path("users/", UserList.as_view(), name="user_list"),
    path("my_account/", MyAccount.as_view(), name="my_account"),
]
