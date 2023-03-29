from django.db import models


# Create your models here.
class Instrument(models.Model):
    ticker = models.CharField(max_length=50, primary_key=True)

    def __str__(self) -> str:
        return self.ticker


class DailyData(models.Model):
    instrument = models.ForeignKey(Instrument, on_delete=models.RESTRICT)
    date = models.DateField()
    open = models.DecimalField(max_digits=12, decimal_places=2)
    high = models.DecimalField(max_digits=12, decimal_places=2)
    low = models.DecimalField(max_digits=12, decimal_places=2)
    close = models.DecimalField(max_digits=12, decimal_places=2)
    volume = models.IntegerField(default=0)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=["instrument", "date"], name="instruement_date"),
        ]
