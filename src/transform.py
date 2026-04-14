import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


def transform_stock_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and enrich stock price data."""
    logger.info(f"Transforming stock data: {len(df)} rows")

    if df.empty:
        logger.warning("Empty stock DataFrame, skipping transform")
        return df

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df.sort_values(["ticker", "date"], inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Fill minor gaps (forward fill up to 2 days) within each ticker
    df = df.groupby("ticker", group_keys=False).apply(_fill_price_gaps, include_groups=False)

    # Daily return
    df["daily_return"] = df.groupby("ticker")["close"].pct_change()

    # Moving averages
    df["ma_7"] = df.groupby("ticker")["close"].transform(lambda x: x.rolling(7, min_periods=1).mean())
    df["ma_30"] = df.groupby("ticker")["close"].transform(lambda x: x.rolling(30, min_periods=1).mean())

    # Volatility: rolling 30-day std of daily returns
    df["volatility_30d"] = df.groupby("ticker")["daily_return"].transform(
        lambda x: x.rolling(30, min_periods=5).std()
    )

    # RSI (14-day)
    df["rsi_14"] = df.groupby("ticker")["close"].transform(_compute_rsi)

    # VWAP (daily volume-weighted average price)
    df["vwap"] = (df["close"] * df["volume"]) / df.groupby(
        ["ticker", df["date"].dt.to_period("M")]
    )["volume"].transform("sum").replace(0, np.nan)

    # Drop rows where close is still null after fill
    before = len(df)
    df.dropna(subset=["close"], inplace=True)
    if len(df) < before:
        logger.warning(f"Dropped {before - len(df)} rows with null close after transform")

    # Round numeric columns
    float_cols = ["open", "high", "low", "close", "daily_return", "ma_7", "ma_30",
                  "volatility_30d", "rsi_14", "vwap"]
    for col in float_cols:
        if col in df.columns:
            df[col] = df[col].round(6)

    df["date"] = df["date"].dt.date
    logger.info(f"Stock transform complete: {len(df)} rows")
    return df


def _fill_price_gaps(group: pd.DataFrame) -> pd.DataFrame:
    ticker_val = group.name if hasattr(group, "name") else group["ticker"].iloc[0]
    group = group.set_index("date").asfreq("B")  # business day freq
    group[["open", "high", "low", "close"]] = group[["open", "high", "low", "close"]].ffill(limit=2)
    group["volume"] = group["volume"].fillna(0)
    group["ticker"] = ticker_val
    return group.reset_index()


def _compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return (100 - (100 / (1 + rs))).round(2)


def transform_macro_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean and normalize macro indicator data."""
    logger.info(f"Transforming macro data: {len(df)} rows")

    df = df.copy()
    df["date"] = pd.to_datetime(df["date"])
    df.sort_values(["indicator_name", "date"], inplace=True)
    df.reset_index(drop=True, inplace=True)

    # Forward-fill missing values within each series (max 3 periods)
    df["indicator_value"] = df.groupby("indicator_name")["indicator_value"].transform(
        lambda x: x.ffill(limit=3)
    )

    # YoY change per series
    df["yoy_change"] = df.groupby("indicator_name")["indicator_value"].transform(
        lambda x: x.pct_change(periods=12)  # assumes monthly; FRED varies
    )

    # Drop rows still missing after fill
    before = len(df)
    df.dropna(subset=["indicator_value"], inplace=True)
    if len(df) < before:
        logger.warning(f"Dropped {before - len(df)} rows with null indicator_value")

    df[["indicator_value", "yoy_change"]] = df[["indicator_value", "yoy_change"]].round(6)
    df["date"] = df["date"].dt.date
    logger.info(f"Macro transform complete: {len(df)} rows")
    return df
