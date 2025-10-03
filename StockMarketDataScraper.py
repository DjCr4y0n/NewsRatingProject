import pandas as pd
import yfinance as yf
from datetime import timedelta


def get_stock_data(ticker: str, newsTime: str):
    intervals = {
        0: "initial price",
        5: "+5 min",
        10: "+10 min",
        15: "+15 min",
        30: "+30 min",
        60: "+60 min"
    }


    if ticker.strip().lower() == "nan":
        return {label: None for label in intervals.values()}

    base_time = pd.to_datetime(newsTime).tz_localize("Europe/Warsaw")

    ticker_obj = yf.Ticker(ticker)
    data = ticker_obj.history(period="5d", interval="1m", auto_adjust=True)
    data.index = data.index.tz_convert("Europe/Warsaw")

    close_prices = {}

    if base_time.day_name() == "Saturday":
        base_time += timedelta(days=2)
        base_time = base_time.replace(hour=9, minute=0, second=0, microsecond=0)
    elif base_time.day_name() == "Sunday":
        base_time += timedelta(days=1)
        base_time = base_time.replace(hour=9, minute=0, second=0, microsecond=0)
    elif base_time.hour >= 17:
        if base_time.day_name() == "Friday":
            base_time += timedelta(days=3)
        else:
            base_time += timedelta(days=1)
        base_time = base_time.replace(hour=9, minute=0, second=0, microsecond=0)
    elif base_time.hour < 9:
        base_time = base_time.replace(hour=9, minute=0, second=0, microsecond=0)

    for offset, label in intervals.items():
        ts = base_time + timedelta(minutes=offset)

        found = False
        for i in range(6):
            ts_try = ts + timedelta(minutes=i)
            if ts_try in data.index:
                close_prices[label] = data.loc[ts_try, "Close"]
                found = True
                break
        if not found:
            close_prices[label] = None

    return close_prices






