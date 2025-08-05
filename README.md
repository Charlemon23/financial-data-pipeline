# Market Data Pipeline

A production-ready Python tool for fetching, cleaning, and storing OHLCV market data from **Yahoo Finance** (equities, ETFs, FX) and **CoinGecko** (cryptocurrencies).

The pipeline stores the **asset symbol** alongside each data point, supports **daily, hourly, and minute intervals**, and saves data in **CSV or Parquet** format for downstream analytics.

---

## Features
- **Multiple Data Providers**:
  - **Yahoo Finance** for equities, ETFs, FX
  - **CoinGecko** for crypto assets
- **Automatic Symbol Storage** in dataset
- **Data Cleaning**:
  - Removes NaNs
  - Deduplicates timestamps
- **Multiple Formats**:
  - CSV
  - Parquet
- **Organized Output**:
  - Equities saved to `/data/equities/`
  - Crypto saved to `/data/crypto/`

---

## Installation
```bash
git clone https://github.com/<your-username>/market-data-pipeline.git
cd market-data-pipeline
pip install -r requirements.txt
