from django.urls import path

from apps.master.views import AboutView, LoginView, LogoutView

urlpatterns = [
    path('login/', LoginView.as_view(), name='login'),
    path('logout/', LogoutView.as_view(), name='logout'),

    path('', AboutView.as_view(), name='about'),
]
