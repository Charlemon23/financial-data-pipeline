# market_data_pipeline.py
"""
Market Data Pipeline
====================
Fetches, cleans, and stores OHLCV market data from Yahoo Finance (equities, FX) and CoinGecko (crypto).
Saves symbol name in dataset. Supports CSV and Parquet formats.
"""

import argparse
import logging
import os
import pandas as pd
import yfinance as yf
import requests
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def fetch_yahoo(symbol: str, start: str, end: str, interval: str) -> pd.DataFrame:
    logging.info(f"Fetching Yahoo Finance data for {symbol} from {start} to {end} [{interval}]")
    df = yf.download(symbol, start=start, end=end, interval=interval, progress=False)
    if df.empty:
        logging.warning(f"No data returned for {symbol} from Yahoo Finance.")
    return df

def fetch_coingecko(symbol: str, vs_currency: str, days: str) -> pd.DataFrame:
    logging.info(f"Fetching CoinGecko data for {symbol} vs {vs_currency} for last {days} days")
    url = f"https://api.coingecko.com/api/v3/coins/{symbol}/market_chart?vs_currency={vs_currency}&days={days}"
    r = requests.get(url)
    if r.status_code != 200:
        logging.error(f"CoinGecko API error: {r.text}")
        return pd.DataFrame()
    data = r.json()
    prices = pd.DataFrame(data["prices"], columns=["timestamp", "price"])
    prices["timestamp"] = pd.to_datetime(prices["timestamp"], unit="ms")
    prices.rename(columns={"timestamp": "Date", "price": "Close"}, inplace=True)
    prices["Open"] = prices["Close"]
    prices["High"] = prices["Close"]
    prices["Low"] = prices["Close"]
    prices["Adj Close"] = prices["Close"]
    prices["Volume"] = None
    return prices[["Date", "Open", "High", "Low", "Close", "Adj Close", "Volume"]]

def clean_data(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    df = df.dropna()
    df = df[~df.index.duplicated(keep="first")]
    df.reset_index(inplace=True)
    if "Date" not in df.columns:
        df.rename(columns={"index": "Date"}, inplace=True)
    df.insert(0, "Symbol", symbol)
    return df

def save_data(df: pd.DataFrame, symbol: str, asset_type: str, output_dir: str, file_format: str):
    folder = os.path.join(output_dir, asset_type)
    os.makedirs(folder, exist_ok=True)
    file_path = os.path.join(folder, f"{symbol}.{file_format}")
    if file_format == "csv":
        df.to_csv(file_path, index=False)
    elif file_format == "parquet":
        df.to_parquet(file_path, index=False)
    logging.info(f"Data saved to {file_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Market Data Pipeline")
    parser.add_argument("provider", choices=["yahoo", "coingecko"], help="Data provider")
    parser.add_argument("symbol", help="Asset symbol (e.g., AAPL, BTC)")
    parser.add_argument("--asset_type", default="equities", choices=["equities", "crypto", "fx"], help="Type of asset")
    parser.add_argument("--start", default="2020-01-01", help="Start date (YYYY-MM-DD) for Yahoo Finance")
    parser.add_argument("--end", default=datetime.today().strftime("%Y-%m-%d"), help="End date (YYYY-MM-DD) for Yahoo Finance")
    parser.add_argument("--interval", default="1d", help="Interval for Yahoo Finance (e.g., 1d, 1h, 5m)")
    parser.add_argument("--vs_currency", default="usd", help="Quote currency for CoinGecko")
    parser.add_argument("--days", default="30", help="Days of data for CoinGecko (e.g., 30, 90, max)")
    parser.add_argument("--output_dir", default="data", help="Directory to save data")
    parser.add_argument("--format", default="csv", choices=["csv", "parquet"], help="Output file format")
    args = parser.parse_args()

    if args.provider == "yahoo":
        df = fetch_yahoo(args.symbol, args.start, args.end, args.interval)
    elif args.provider == "coingecko":
        df = fetch_coingecko(args.symbol.lower(), args.vs_currency, args.days)

    if not df.empty:
        df = clean_data(df, args.symbol)
        save_data(df, args.symbol, args.asset_type, args.output_dir, args.format)
