from django.contrib.auth.models import AbstractUser
from django.contrib.auth.models import Group as DjangoGroup


# Create your models here.
class User(AbstractUser):
    """
    Users within the Django authentication system are represented by this
    model.

    Username and password are required. Other fields are optional.
    """
    
    class Meta(AbstractUser.Meta):
        swappable = "AUTH_USER_MODEL"


class Group(DjangoGroup):
    class Meta:
        proxy = True

