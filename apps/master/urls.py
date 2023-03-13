from django.urls import path

from .views import MyAccount, UserList, home_page, login_page, logout_page

urlpatterns = [
    path("", home_page, name="home"),
    path("login/", login_page, name="login"),
    path("logout/", logout_page, name="logout"),
    path("users/", UserList.as_view(), name="user_list"),
    path("my_account/", MyAccount.as_view(), name="my_account"),
]
