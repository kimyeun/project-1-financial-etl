import os
import time
import logging
import functools
from datetime import datetime, timedelta

import os

import yfinance as yf
import pandas as pd
import requests
from dotenv import load_dotenv

_tz_cache = "/tmp/yf_tz_cache"
os.makedirs(_tz_cache, exist_ok=True)
yf.set_tz_cache_location(_tz_cache)

load_dotenv()

logger = logging.getLogger(__name__)

FRED_API_KEY = os.getenv("FRED_API_KEY")
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

STOCK_TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]
FRED_SERIES = {
    "GDP": "GDP",
    "CPI": "CPIAUCSL",
    "UNEMPLOYMENT": "UNRATE",
    "FED_FUNDS_RATE": "FEDFUNDS",
    "10Y_TREASURY": "DGS10",
}


def retry_with_backoff(max_retries: int = 3, base_delay: float = 1.0):
    """Exponential backoff retry decorator."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        logger.error(f"[{func.__name__}] Failed after {max_retries} retries: {e}")
                        raise
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"[{func.__name__}] Attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
                    time.sleep(delay)
        return wrapper
    return decorator


@retry_with_backoff(max_retries=3, base_delay=1.0)
def fetch_stock_data(
    tickers: list[str] = STOCK_TICKERS,
    start_date: str = None,
    end_date: str = None,
) -> pd.DataFrame:
    """Fetch OHLCV stock data from yfinance."""
    if end_date is None:
        end_date = datetime.today().strftime("%Y-%m-%d")
    if start_date is None:
        start_date = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")

    logger.info(f"Fetching stock data for {tickers} from {start_date} to {end_date}")

    raw = yf.download(tickers, start=start_date, end=end_date, auto_adjust=True, progress=False)

    # Flatten multi-index columns
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = ["_".join(col).strip() for col in raw.columns]

    records = []
    for ticker in tickers:
        cols = {
            "date": raw.index,
            "ticker": ticker,
            "open": raw.get(f"Open_{ticker}"),
            "high": raw.get(f"High_{ticker}"),
            "low": raw.get(f"Low_{ticker}"),
            "close": raw.get(f"Close_{ticker}"),
            "volume": raw.get(f"Volume_{ticker}"),
        }
        df = pd.DataFrame(cols).dropna(subset=["close"])
        records.append(df)

    result = pd.concat(records, ignore_index=True)
    result["date"] = pd.to_datetime(result["date"]).dt.date
    logger.info(f"Fetched {len(result)} stock rows")
    return result


@retry_with_backoff(max_retries=3, base_delay=1.0)
def _fetch_fred_series(series_id: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch a single FRED series."""
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": start_date,
        "observation_end": end_date,
    }
    response = requests.get(FRED_BASE_URL, params=params, timeout=10)
    response.raise_for_status()

    data = response.json()
    observations = data.get("observations", [])
    df = pd.DataFrame(observations)[["date", "value"]]
    df["series_id"] = series_id
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return df


def fetch_macro_data(
    start_date: str = None,
    end_date: str = None,
) -> pd.DataFrame:
    """Fetch macro indicators from FRED API."""
    if end_date is None:
        end_date = datetime.today().strftime("%Y-%m-%d")
    if start_date is None:
        start_date = (datetime.today() - timedelta(days=365 * 3)).strftime("%Y-%m-%d")

    logger.info(f"Fetching FRED macro data from {start_date} to {end_date}")

    frames = []
    for name, series_id in FRED_SERIES.items():
        try:
            df = _fetch_fred_series(series_id, start_date, end_date)
            df["indicator_name"] = name
            frames.append(df)
            logger.info(f"Fetched {len(df)} rows for {name} ({series_id})")
        except Exception as e:
            logger.error(f"Failed to fetch FRED series {series_id}: {e}")

    if not frames:
        raise RuntimeError("All FRED series fetches failed")

    result = pd.concat(frames, ignore_index=True)
    result["date"] = pd.to_datetime(result["date"]).dt.date
    result.rename(columns={"value": "indicator_value"}, inplace=True)
    logger.info(f"Fetched {len(result)} total macro rows")
    return result
