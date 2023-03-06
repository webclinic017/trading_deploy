from django.contrib import admin
from django.contrib.auth.admin import GroupAdmin as DjangoGroupAdmin
from django.contrib.auth.admin import UserAdmin as DjangoUserAdmin
from django.contrib.auth.models import Group as DjangoGroup

from apps.master.models import Group, User


@admin.register(User)
class UserAdmin(DjangoUserAdmin):
    readonly_fields = (
        "last_login",
        "date_joined",
    )


@admin.register(Group)
class GroupAdmin(DjangoGroupAdmin):
    pass


admin.site.unregister(DjangoGroup)
