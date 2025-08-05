import pandas as pd
import yfinance as yf
import sqlite3

def download_data(ticker, period='1y', interval='1d'):
    data = yf.download(ticker, period=period, interval=interval, group_by='column')
    data.reset_index(inplace=True)
    return data

def clean_data(df):
    # Flatten multi-level columns if they exist
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join(filter(None, map(str, col))).strip() for col in df.columns]
    # Normalize column names
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    df.dropna(inplace=True)
    return df

def store_to_db(df, db_name='financial_data.db', table_name='market_data'):
    conn = sqlite3.connect(db_name)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

if __name__ == "__main__":
    ticker = 'AAPL'
    df = download_data(ticker)
    df = clean_data(df)
    store_to_db(df)
    print(f"Data for {ticker} downloaded, cleaned, and stored successfully.")
