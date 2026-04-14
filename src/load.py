import os
import logging
from datetime import datetime, timezone
from contextlib import contextmanager

import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

DB_URL = (
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}"
    f"/{os.getenv('DB_NAME')}"
)


@contextmanager
def get_engine():
    engine = create_engine(DB_URL, pool_pre_ping=True)
    try:
        yield engine
    finally:
        engine.dispose()


def upsert_stock_prices(df: pd.DataFrame) -> int:
    """Upsert stock prices into stock_prices table. Returns number of rows affected."""
    if df.empty:
        logger.warning("Empty stock DataFrame, skipping load")
        return 0

    logger.info(f"Upserting {len(df)} stock price rows")

    upsert_sql = text("""
        INSERT INTO stock_prices (
            date, ticker, open, high, low, close, volume,
            daily_return, ma_7, ma_30, volatility_30d, rsi_14, vwap
        ) VALUES (
            :date, :ticker, :open, :high, :low, :close, :volume,
            :daily_return, :ma_7, :ma_30, :volatility_30d, :rsi_14, :vwap
        )
        ON CONFLICT (date, ticker) DO UPDATE SET
            open          = EXCLUDED.open,
            high          = EXCLUDED.high,
            low           = EXCLUDED.low,
            close         = EXCLUDED.close,
            volume        = EXCLUDED.volume,
            daily_return  = EXCLUDED.daily_return,
            ma_7          = EXCLUDED.ma_7,
            ma_30         = EXCLUDED.ma_30,
            volatility_30d = EXCLUDED.volatility_30d,
            rsi_14        = EXCLUDED.rsi_14,
            vwap          = EXCLUDED.vwap,
            updated_at    = NOW()
    """)

    records = df.where(pd.notnull(df), None).to_dict(orient="records")
    with get_engine() as engine:
        with engine.begin() as conn:
            conn.execute(upsert_sql, records)

    logger.info(f"Upserted {len(records)} stock rows")
    return len(records)


def upsert_macro_indicators(df: pd.DataFrame) -> int:
    """Upsert macro indicators into macro_indicators table. Returns rows affected."""
    if df.empty:
        logger.warning("Empty macro DataFrame, skipping load")
        return 0

    logger.info(f"Upserting {len(df)} macro indicator rows")

    upsert_sql = text("""
        INSERT INTO macro_indicators (
            date, series_id, indicator_name, indicator_value, yoy_change
        ) VALUES (
            :date, :series_id, :indicator_name, :indicator_value, :yoy_change
        )
        ON CONFLICT (date, series_id) DO UPDATE SET
            indicator_value = EXCLUDED.indicator_value,
            yoy_change      = EXCLUDED.yoy_change,
            updated_at      = NOW()
    """)

    records = df.where(pd.notnull(df), None).to_dict(orient="records")
    with get_engine() as engine:
        with engine.begin() as conn:
            conn.execute(upsert_sql, records)

    logger.info(f"Upserted {len(records)} macro rows")
    return len(records)


def insert_pipeline_log(
    run_id: str,
    stage: str,
    status: str,
    rows_extracted: int = 0,
    rows_loaded: int = 0,
    validation_passed: bool = True,
    error_message: str = None,
    started_at: datetime = None,
    finished_at: datetime = None,
) -> None:
    """Insert a pipeline execution log record."""
    log_sql = text("""
        INSERT INTO pipeline_logs (
            run_id, stage, status, rows_extracted, rows_loaded,
            validation_passed, error_message, started_at, finished_at
        ) VALUES (
            :run_id, :stage, :status, :rows_extracted, :rows_loaded,
            :validation_passed, :error_message, :started_at, :finished_at
        )
    """)

    with get_engine() as engine:
        with engine.begin() as conn:
            conn.execute(log_sql, {
                "run_id": run_id,
                "stage": stage,
                "status": status,
                "rows_extracted": rows_extracted,
                "rows_loaded": rows_loaded,
                "validation_passed": validation_passed,
                "error_message": error_message,
                "started_at": started_at or datetime.now(timezone.utc),
                "finished_at": finished_at or datetime.now(timezone.utc),
            })
    logger.info(f"[{run_id}] Log written: stage={stage}, status={status}")
