import datetime
import signal
from contextlib import contextmanager

import pandas as pd
from dateutil.parser import parse
from django.core.cache import cache
from django.db import IntegrityError
from django_pandas.io import read_frame

from apps.data.models import DailyData, Instrument
from trading.celery import app


class TimeoutException(Exception):
    pass


@contextmanager
def time_limit(seconds):
    def signal_handler(signum, frame):
        raise TimeoutException("Timed out!")
    signal.signal(signal.SIGALRM, signal_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)


@app.task(name="Upload daily data", bind=True)
def upload_daily_data(self):
    try:
        with time_limit(10):
            today = datetime.datetime.now()
            df = pd.read_csv(r'https://archives.nseindia.com/content/indices/ind_close_all_'+today.strftime("%d%m%Y")+'.csv')
            df = df[['Index Name', 'Open Index Value', 'High Index Value', 'Low Index Value', 'Closing Index Value', 'Volume']]
            df.columns = ['instrument', 'open', 'high', 'low', 'close', 'volume']
            df['date'] = pd.to_datetime(today.strftime("%d-%m-%Y"), dayfirst=True)
            df = df[df.instrument.isin(['Nifty 50', 'Nifty Bank', 'Nifty Financial Services'])]
            df.reset_index(inplace=True, drop=True)
            for i, row in df.iterrows():
                if row.instrument == 'Nifty 50':
                    instrument = Instrument.objects.get(ticker='NIFTY 50')
                elif row.instrument == 'Nifty Bank':
                    instrument = Instrument.objects.get(ticker='BANKNIFTY')
                elif row.instrument == 'Nifty Financial Services':
                    instrument = Instrument.objects.get(ticker='FINNIFTY')
                df.loc[i, 'instrument'] = instrument
            DailyData.objects.bulk_create([DailyData(**row) for row in df.to_dict("records")])
            print('success! data added')
    except IntegrityError:
        print('data already added')
    except TimeoutException:
        print("data not found")


@app.task(name="TR based entry", bind=True)
def tr_based_entry(self):
    # cache.set('expiry', '30-3-2023')
    today = datetime.datetime.now()
    print('today', today)
    qs = DailyData.objects.filter(
        date__lte=today.strftime("%Y-%m-%d"),
        date__gte=(today-datetime.timedelta(days=15)).strftime("%Y-%m-%d"),
        instrument__ticker='BANKNIFTY',
    ).order_by('-date')[:6]
    df = read_frame(qs, fieldnames=['instrument', 'date', 'open', 'high', 'low', 'close', 'volume'])
    df.sort_values(by=['date'])
    df['h-l'] = df['high']-df['low']
    df['h-c'] = df['high']-df['close']
    df['c-l'] = df['close']-df['low']
    df['TR'] = df[['h-l', 'h-c', 'c-l']].max(1)
    df['ATR'] = df['TR'].rolling(5).sum()/5
    df['ATR'] = df['ATR'].shift(-5)

    if df.iloc[0, 10] <= 1.8 * df.iloc[0, 11]:
        entry_time = parse("09:15").time()
        print("TR<=1.8 ATR")
    else:
        arr = ["09:30", "09:50", "09:50", "09:40", 0, 0, "09:20"]
        print("TR>1.8 ATR")
        days_to_exp = (cache.get('EXPIRY') - today.date()).days
        print('days_to_exp', days_to_exp)
        entry_time = parse(arr[days_to_exp]).time()
    cache.set('ENTRY_TIME', entry_time)
