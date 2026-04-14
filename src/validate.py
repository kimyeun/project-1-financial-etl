import json
import logging
from pathlib import Path

import pandas as pd
import great_expectations as ge
from great_expectations.dataset import PandasDataset

logger = logging.getLogger(__name__)

EXPECTATIONS_DIR = Path(__file__).parent.parent / "expectations"


def _load_suite(suite_name: str) -> dict:
    path = EXPECTATIONS_DIR / f"{suite_name}.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return {}


def validate_stock_data(df: pd.DataFrame) -> tuple[bool, list[str]]:
    """Validate stock price data. Returns (passed, list_of_failures)."""
    gdf = ge.from_pandas(df)
    failures = []

    # Schema checks
    for col in ["date", "ticker", "open", "high", "low", "close", "volume"]:
        result = gdf.expect_column_to_exist(col)
        if not result["success"]:
            failures.append(f"Missing column: {col}")

    # Null checks
    for col in ["date", "ticker", "close"]:
        result = gdf.expect_column_values_to_not_be_null(col)
        if not result["success"]:
            pct = result["result"].get("unexpected_percent", "?")
            failures.append(f"Null values in '{col}': {pct:.1f}%")

    # Price sanity: close > 0
    result = gdf.expect_column_values_to_be_between("close", min_value=0.01)
    if not result["success"]:
        cnt = result["result"].get("unexpected_count", "?")
        failures.append(f"Invalid close price (<= 0): {cnt} rows")

    # Volume >= 0
    result = gdf.expect_column_values_to_be_between("volume", min_value=0)
    if not result["success"]:
        cnt = result["result"].get("unexpected_count", "?")
        failures.append(f"Negative volume: {cnt} rows")

    # High >= Low
    result = gdf.expect_column_pair_values_A_to_be_greater_than_B(
        "high", "low", or_equal=True
    )
    if not result["success"]:
        cnt = result["result"].get("unexpected_count", "?")
        failures.append(f"high < low: {cnt} rows")

    # Ticker must be in expected set
    result = gdf.expect_column_values_to_be_in_set(
        "ticker", ["AAPL", "MSFT", "GOOGL", "AMZN", "META"]
    )
    if not result["success"]:
        unexpected = result["result"].get("partial_unexpected_list", [])
        failures.append(f"Unexpected tickers: {unexpected}")

    # Outlier check: close price z-score per ticker
    for ticker, group in df.groupby("ticker"):
        if len(group) < 10:
            continue
        mean = group["close"].mean()
        std = group["close"].std()
        if std == 0:
            continue
        outliers = ((group["close"] - mean).abs() / std > 5).sum()
        if outliers > 0:
            failures.append(f"[{ticker}] {outliers} close price outliers (z-score > 5)")

    passed = len(failures) == 0
    if passed:
        logger.info("Stock data validation PASSED")
    else:
        logger.warning(f"Stock data validation FAILED: {failures}")

    return passed, failures


def validate_macro_data(df: pd.DataFrame) -> tuple[bool, list[str]]:
    """Validate macro indicator data. Returns (passed, list_of_failures)."""
    gdf = ge.from_pandas(df)
    failures = []

    for col in ["date", "series_id", "indicator_name", "indicator_value"]:
        result = gdf.expect_column_to_exist(col)
        if not result["success"]:
            failures.append(f"Missing column: {col}")

    result = gdf.expect_column_values_to_not_be_null("date")
    if not result["success"]:
        failures.append("Null dates in macro data")

    result = gdf.expect_column_values_to_not_be_null("series_id")
    if not result["success"]:
        failures.append("Null series_id in macro data")

    # Allow some NaN in indicator_value (FRED sometimes returns missing periods)
    null_pct = df["indicator_value"].isna().mean() * 100
    if null_pct > 30:
        failures.append(f"Too many null indicator values: {null_pct:.1f}%")

    result = gdf.expect_column_values_to_be_in_set(
        "indicator_name", ["GDP", "CPI", "UNEMPLOYMENT", "FED_FUNDS_RATE", "10Y_TREASURY"]
    )
    if not result["success"]:
        unexpected = result["result"].get("partial_unexpected_list", [])
        failures.append(f"Unexpected indicator names: {unexpected}")

    passed = len(failures) == 0
    if passed:
        logger.info("Macro data validation PASSED")
    else:
        logger.warning(f"Macro data validation FAILED: {failures}")

    return passed, failures
