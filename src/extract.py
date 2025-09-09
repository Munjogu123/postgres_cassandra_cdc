import json
import os
import pandas as pd
import requests
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine


def main():
    symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"]

    fetch_functions = {
        "price_table": lambda s: get_latest_prices(s),
        "bids_table": lambda s: get_order_book(s)[0],
        "asks_table": lambda s: get_order_book(s)[1],
        "trades_table": lambda s: get_recent_trades(s),
        "klines_table": lambda s: get_klines(s),
        "ticker_table": lambda s: get_ticker(s),
    }

    for symbol in symbols:
        for table, fetch_function in fetch_functions.items():
            df = fetch_function(symbol)
            load_postgres(df, table)


def fetch_json(endpoint, params=None):
    url = f"https://api.binance.com/{endpoint}"
    r = requests.get(url, params=params or {})
    r.raise_for_status()

    return r.json()


def add_timestamp(df, symbol=None):
    df["fetched_at"] = datetime.now()
    if symbol is not None:
        df["symbol"] = symbol

    return df


def load_postgres(df, tablename):
    load_dotenv()

    user = os.getenv("user")
    passwd = os.getenv("passwd")
    host = os.getenv("host")
    db = os.getenv("db")

    url = f"postgresql://{user}:{passwd}@{host}/{db}"
    engine = create_engine(url)

    try:
        df.to_sql(name=tablename, con=engine, if_exists="append", index=False)
        print(f"Loaded {len(df)} rows to the Database for table: {tablename}")
    except Exception as e:
        print(f"Could not load data in postgres. Received Exception: {str(e)}")


def get_latest_prices(symbol):
    data = fetch_json("/api/v3/ticker/price", {"symbol": symbol})
    prices_df = add_timestamp(pd.DataFrame([data]), symbol)

    return prices_df


def get_order_book(symbol, limit=20):
    data = fetch_json("/api/v3/depth", {"symbol": symbol, "limit": limit})
    bids_df = add_timestamp(
        pd.DataFrame(data["bids"], columns=["price", "quantity"]), symbol
    )
    asks_df = add_timestamp(
        pd.DataFrame(data["asks"], columns=["price", "quantity"]), symbol
    )

    return bids_df, asks_df


def get_recent_trades(symbol, limit=20):
    data = fetch_json("/api/v3/trades", params={"symbol": symbol, "limit": limit})
    trades_df = add_timestamp(pd.DataFrame(data), symbol)

    return trades_df


def get_klines(symbol, interval="15m"):
    data = fetch_json("/api/v3/klines", params={"symbol": symbol, "interval": interval})
    cols = [
        "open_time",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "volume",
        "close_time",
        "quote_asset_volume",
        "no_trades",
        "base_asset_volume",
        "buy_quote_asset_volume",
        "unused",
    ]
    klines_df = add_timestamp(pd.DataFrame(data, columns=cols), symbol)

    return klines_df


def get_ticker(symbol):
    data = fetch_json("/api/v3/ticker/24hr", params={"symbol": symbol})
    ticker_df = add_timestamp(pd.DataFrame([data]), symbol)

    return ticker_df


if __name__ == "__main__":
    main()
