import pyotp
from asgiref.sync import async_to_sync
from django.contrib.auth import get_user_model
from django.db import models

from utils import decrypt_message, encrypt_message
from utils.broker.kiteext import KiteExt as ZApi
from utils.broker.kotak_neo import KotakNeoApi as KNApi
from utils.broker.kotak_securities import KotakSecuritiesApi as KSApi

User = get_user_model()


class BrokerApi(models.Model):
    DUMMY = "dummy"
    KOTAK_NEO = "kotak_neo"
    KOTAK = "kotak"
    ZERODAHA = "zerodha"
    DUCKTRADE = "ducktrade"

    broker_choices = [
        (DUMMY, "DUMMY"),
        (KOTAK_NEO, "KOTAK NEO"),
        (KOTAK, "KOTAK"),
        (ZERODAHA, "ZERODAHA"),
        (DUCKTRADE, "DUCK JAINAM TRADE"),
    ]

    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name="broker_api")
    broker = models.CharField(
        max_length=15,
        choices=broker_choices,
    )
    is_active = models.BooleanField()

    def __str__(self) -> str:
        return self.user.username

    class Meta:
        unique_together = ('user', 'broker')


class KotakNeoApi(models.Model):
    broker_api = models.OneToOneField(BrokerApi, related_name="kotak_neo_api", on_delete=models.CASCADE)
    mobile_number = models.CharField(max_length=13)
    pan_number = models.CharField(max_length=13, null=True, blank=True)
    password = models.CharField(max_length=255)
    mpin = models.CharField(max_length=255)
    neo_fin_key = models.CharField(max_length=255)
    consumer_key = models.CharField(max_length=255)
    consumer_secret = models.CharField(max_length=255)
    access_token = models.TextField(blank=True, null=True)
    sid = models.CharField(max_length=100, blank=True, null=True)
    rid = models.CharField(max_length=100, blank=True, null=True)
    auth = models.TextField(blank=True, null=True)
    hs_server_id = models.CharField(max_length=100, blank=True, null=True)
    update_auth_token = models.BooleanField(default=False)
    login_error = models.BooleanField(default=False)
    update_token_error = models.BooleanField(default=False)
    update_error = models.BooleanField(default=False)

    def __str__(self) -> str:
        return f"{self.broker_api.user}"

    # Password Encryption And Decryption
    def encrypt_password(self):
        try:
            decrypt_message(self.password)
        except Exception:
            self.password = encrypt_message(self.password)

    def decrypt_password(self):
        return decrypt_message(str(self.password))

    # Mpin Encryption And Decryption
    def encrypt_mpin(self):
        try:
            decrypt_message(str(self.mpin))
        except Exception:
            self.mpin = encrypt_message(self.mpin)

    def decrypt_mpin(self):
        return decrypt_message(str(self.mpin))

    # Neo Fin Key Encryption And Decryption
    def encrypt_neo_fin_key(self):
        try:
            decrypt_message(str(self.neo_fin_key))
        except Exception:
            self.neo_fin_key = encrypt_message(self.neo_fin_key)

    def decrypt_neo_fin_key(self):
        return decrypt_message(str(self.neo_fin_key))

    # Consumer Key Encryption And Decryption
    def encrypt_consumer_key(self):
        try:
            decrypt_message(str(self.consumer_key))
        except Exception:
            self.consumer_key = encrypt_message(self.consumer_key)

    def decrypt_consumer_key(self):
        return decrypt_message(str(self.consumer_key))

    # Consumer Secret Encryption And Decryption
    def encrypt_consumer_secret(self):
        try:
            decrypt_message(str(self.consumer_secret))
        except Exception:
            self.consumer_secret = encrypt_message(self.consumer_secret)

    def decrypt_consumer_secret(self):
        return decrypt_message(str(self.consumer_secret))

    def generate_session(self):
        neo: KNApi = async_to_sync(KNApi)(
            self.decrypt_neo_fin_key(),
            self.decrypt_consumer_key(),
            self.decrypt_consumer_secret(),
            self.access_token,
            self.sid,
            self.auth,
            self.hs_server_id,
            self.rid,
        )

        if self.update_auth_token:
            async_to_sync(neo.update_auth_token)()
        else:
            async_to_sync(neo.login)(
                self.mobile_number,
                self.pan_number,
                self.decrypt_password(),
                self.decrypt_mpin(),
            )
        self.sid = neo.sid
        self.rid = neo.rid
        self.auth = neo.auth
        if neo.hs_server_id:
            self.hs_server_id = neo.hs_server_id
        self.access_token = neo.access_token
        self.update_auth_token = False

    def save(self, *args, **kwargs) -> None:
        self.encrypt_password()
        self.encrypt_mpin()
        self.encrypt_consumer_key()
        self.encrypt_consumer_secret()
        self.encrypt_neo_fin_key()
        if not self.update_error:
            self.generate_session()
            self.login_error = False
        self.update_error = False
        super(KotakNeoApi, self).save(*args, **kwargs)


class KotakSecuritiesApi(models.Model):
    broker_api = models.OneToOneField(BrokerApi, related_name="kotak_api", on_delete=models.CASCADE)
    userid = models.CharField(max_length=10)
    password = models.CharField(max_length=255)
    pin = models.CharField(max_length=4, null=True, blank=True)
    consumer_key = models.CharField(max_length=255)
    consumer_secret = models.CharField(max_length=255)
    access_token = models.CharField(max_length=255, blank=True, null=True)
    one_time_token = models.CharField(max_length=100, blank=True, null=True)
    session_token = models.CharField(max_length=100, blank=True, null=True)

    def __str__(self) -> str:
        return f"{self.broker_api.user}"

        # Password Encryption And Decryption

    def encrypt_password(self):
        try:
            decrypt_message(self.password)
        except Exception:
            self.password = encrypt_message(self.password)

    def decrypt_password(self):
        return decrypt_message(str(self.password))

    # Consumer Key Encryption And Decryption
    def encrypt_consumer_key(self):
        try:
            decrypt_message(str(self.consumer_key))
        except Exception:
            self.consumer_key = encrypt_message(self.consumer_key)

    def decrypt_consumer_key(self):
        return decrypt_message(str(self.consumer_key))

        # Consumer Secret Encryption And Decryption

    def encrypt_consumer_secret(self):
        try:
            decrypt_message(str(self.consumer_secret))
        except Exception:
            self.consumer_secret = encrypt_message(self.consumer_secret)

    def decrypt_consumer_secret(self):
        return decrypt_message(str(self.consumer_secret))

    def generate_session(self):
        sec: KSApi = async_to_sync(KSApi)(
            self.userid,
            self.decrypt_consumer_key(),
            self.access_token,
            self.decrypt_consumer_secret(),
        )

        async_to_sync(sec.session_init)()
        async_to_sync(sec.login)(self.decrypt_password())
        async_to_sync(sec.session_2fa)()
        self.one_time_token = sec.one_time_token
        self.session_token = sec.session_token

    def save(self, *args, **kwargs) -> None:
        self.encrypt_password()
        self.encrypt_consumer_key()
        self.encrypt_consumer_secret()
        self.generate_session()
        super(KotakSecuritiesApi, self).save(*args, **kwargs)


class ZerodhaApi(models.Model):
    broker_api = models.OneToOneField(BrokerApi, related_name="zerodha_api", on_delete=models.CASCADE)
    userid = models.CharField(max_length=20)
    password = models.CharField(max_length=255, blank=True, null=True)
    two_fa = models.CharField(max_length=255, blank=True, null=True)
    session_token = models.CharField(max_length=255, blank=True, null=True)

    def __str__(self) -> str:
        return f"{self.broker_api.user}"

    def encrypt_password(self):
        try:
            decrypt_message(self.password)
        except Exception:
            self.password = encrypt_message(self.password)

    def decrypt_password(self):
        return decrypt_message(str(self.password))

    def encrypt_two_fa(self):
        try:
            decrypt_message(self.two_fa)
        except Exception:
            self.two_fa = encrypt_message(self.two_fa)

    def decrypt_two_fa(self):
        return decrypt_message(str(self.two_fa))

    def generate_session(self):
        totp = pyotp.TOTP(self.decrypt_two_fa())
        zer: ZApi = async_to_sync(ZApi)(
            self.userid,
            self.decrypt_password(),
            totp.now(),
        )
        self.session_token = zer.public_token

    def save(self, *args, **kwargs) -> None:
        self.encrypt_password()
        self.encrypt_two_fa()
        self.generate_session()
        super(ZerodhaApi, self).save(*args, **kwargs)

# class DuckTradeApi(models.Model):
#     pass
