import logging
import time
import uuid
from datetime import datetime, timezone

from extract import fetch_stock_data, fetch_macro_data
from validate import validate_stock_data, validate_macro_data
from transform import transform_stock_data, transform_macro_data
from load import upsert_stock_prices, upsert_macro_indicators, insert_pipeline_log
from notify import notify_pipeline_success, notify_pipeline_failure, notify_validation_failure

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def run_pipeline(start_date: str = None, end_date: str = None) -> dict:
    run_id = str(uuid.uuid4())[:8]
    pipeline_start = time.time()
    started_at = datetime.now(timezone.utc)

    logger.info(f"=== Pipeline started [run_id={run_id}] ===")

    stock_rows_loaded = 0
    macro_rows_loaded = 0

    # ── STOCK PIPELINE ──────────────────────────────────────────────────────
    stage = "extract_stocks"
    stage_start = datetime.now(timezone.utc)
    try:
        stock_raw = fetch_stock_data(start_date=start_date, end_date=end_date)
        stock_extract_count = len(stock_raw)
    except Exception as e:
        _log_and_notify_failure(run_id, stage, str(e), stage_start)
        return {"run_id": run_id, "status": "failed", "stage": stage, "error": str(e)}

    stage = "validate_stocks"
    stage_start = datetime.now(timezone.utc)
    passed, failures = validate_stock_data(stock_raw)
    insert_pipeline_log(
        run_id=run_id, stage=stage,
        status="passed" if passed else "failed",
        rows_extracted=stock_extract_count,
        validation_passed=passed,
        error_message="; ".join(failures) if failures else None,
        started_at=stage_start, finished_at=datetime.now(timezone.utc),
    )
    if not passed:
        notify_validation_failure(run_id, "stock_prices", failures)
        logger.error(f"Stock validation failed, aborting pipeline: {failures}")
        return {"run_id": run_id, "status": "failed", "stage": stage, "failures": failures}

    stage = "transform_stocks"
    stage_start = datetime.now(timezone.utc)
    try:
        stock_transformed = transform_stock_data(stock_raw)
    except Exception as e:
        _log_and_notify_failure(run_id, stage, str(e), stage_start)
        return {"run_id": run_id, "status": "failed", "stage": stage, "error": str(e)}

    stage = "load_stocks"
    stage_start = datetime.now(timezone.utc)
    try:
        stock_rows_loaded = upsert_stock_prices(stock_transformed)
        insert_pipeline_log(
            run_id=run_id, stage=stage, status="success",
            rows_extracted=stock_extract_count, rows_loaded=stock_rows_loaded,
            started_at=stage_start, finished_at=datetime.now(timezone.utc),
        )
    except Exception as e:
        _log_and_notify_failure(run_id, stage, str(e), stage_start)
        return {"run_id": run_id, "status": "failed", "stage": stage, "error": str(e)}

    # ── MACRO PIPELINE ───────────────────────────────────────────────────────
    stage = "extract_macro"
    stage_start = datetime.now(timezone.utc)
    try:
        macro_raw = fetch_macro_data(start_date=start_date, end_date=end_date)
        macro_extract_count = len(macro_raw)
    except Exception as e:
        _log_and_notify_failure(run_id, stage, str(e), stage_start)
        return {"run_id": run_id, "status": "failed", "stage": stage, "error": str(e)}

    stage = "validate_macro"
    stage_start = datetime.now(timezone.utc)
    passed, failures = validate_macro_data(macro_raw)
    insert_pipeline_log(
        run_id=run_id, stage=stage,
        status="passed" if passed else "failed",
        rows_extracted=macro_extract_count,
        validation_passed=passed,
        error_message="; ".join(failures) if failures else None,
        started_at=stage_start, finished_at=datetime.now(timezone.utc),
    )
    if not passed:
        notify_validation_failure(run_id, "macro_indicators", failures)
        logger.error(f"Macro validation failed, aborting pipeline: {failures}")
        return {"run_id": run_id, "status": "failed", "stage": stage, "failures": failures}

    stage = "transform_macro"
    stage_start = datetime.now(timezone.utc)
    try:
        macro_transformed = transform_macro_data(macro_raw)
    except Exception as e:
        _log_and_notify_failure(run_id, stage, str(e), stage_start)
        return {"run_id": run_id, "status": "failed", "stage": stage, "error": str(e)}

    stage = "load_macro"
    stage_start = datetime.now(timezone.utc)
    try:
        macro_rows_loaded = upsert_macro_indicators(macro_transformed)
        insert_pipeline_log(
            run_id=run_id, stage=stage, status="success",
            rows_extracted=macro_extract_count, rows_loaded=macro_rows_loaded,
            started_at=stage_start, finished_at=datetime.now(timezone.utc),
        )
    except Exception as e:
        _log_and_notify_failure(run_id, stage, str(e), stage_start)
        return {"run_id": run_id, "status": "failed", "stage": stage, "error": str(e)}

    # ── DONE ─────────────────────────────────────────────────────────────────
    duration = time.time() - pipeline_start
    insert_pipeline_log(
        run_id=run_id, stage="pipeline_complete", status="success",
        rows_extracted=stock_extract_count + macro_extract_count,
        rows_loaded=stock_rows_loaded + macro_rows_loaded,
        started_at=started_at, finished_at=datetime.now(timezone.utc),
    )
    notify_pipeline_success(run_id, stock_rows_loaded, macro_rows_loaded, duration)
    logger.info(f"=== Pipeline complete [run_id={run_id}] in {duration:.1f}s ===")

    return {
        "run_id": run_id,
        "status": "success",
        "stock_rows_loaded": stock_rows_loaded,
        "macro_rows_loaded": macro_rows_loaded,
        "duration_sec": round(duration, 1),
    }


def _log_and_notify_failure(run_id: str, stage: str, error: str, started_at: datetime) -> None:
    logger.error(f"[{run_id}] Stage '{stage}' failed: {error}")
    try:
        insert_pipeline_log(
            run_id=run_id, stage=stage, status="error",
            error_message=error[:1000],
            started_at=started_at, finished_at=datetime.now(timezone.utc),
        )
    except Exception as log_err:
        logger.error(f"Failed to write pipeline log: {log_err}")
    notify_pipeline_failure(run_id, stage, error)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Financial ETL Pipeline")
    parser.add_argument("--start", help="Start date YYYY-MM-DD (default: 1 year ago)")
    parser.add_argument("--end", help="End date YYYY-MM-DD (default: today)")
    args = parser.parse_args()

    result = run_pipeline(start_date=args.start, end_date=args.end)
    exit(0 if result["status"] == "success" else 1)
