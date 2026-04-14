import os
import logging
from datetime import datetime

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


def _send_slack(payload: dict) -> bool:
    if not SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL not set, skipping notification")
        return False
    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=5)
        resp.raise_for_status()
        return True
    except Exception as e:
        logger.error(f"Slack notification failed: {e}")
        return False


def notify_pipeline_success(
    run_id: str,
    stock_rows: int,
    macro_rows: int,
    duration_sec: float,
) -> bool:
    """Send a Slack success notification."""
    payload = {
        "attachments": [
            {
                "color": "#36a64f",
                "title": "Financial ETL Pipeline — SUCCESS",
                "fields": [
                    {"title": "Run ID", "value": run_id, "short": True},
                    {"title": "Timestamp", "value": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"), "short": True},
                    {"title": "Stock Rows Loaded", "value": str(stock_rows), "short": True},
                    {"title": "Macro Rows Loaded", "value": str(macro_rows), "short": True},
                    {"title": "Duration", "value": f"{duration_sec:.1f}s", "short": True},
                ],
                "footer": "financial-etl",
            }
        ]
    }
    return _send_slack(payload)


def notify_pipeline_failure(
    run_id: str,
    stage: str,
    error: str,
) -> bool:
    """Send a Slack failure notification."""
    payload = {
        "attachments": [
            {
                "color": "#ff0000",
                "title": "Financial ETL Pipeline — FAILED",
                "fields": [
                    {"title": "Run ID", "value": run_id, "short": True},
                    {"title": "Timestamp", "value": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"), "short": True},
                    {"title": "Failed Stage", "value": stage, "short": True},
                    {"title": "Error", "value": error[:500], "short": False},
                ],
                "footer": "financial-etl",
            }
        ]
    }
    return _send_slack(payload)


def notify_validation_failure(
    run_id: str,
    dataset: str,
    failures: list[str],
) -> bool:
    """Send a Slack notification when data validation fails."""
    failure_text = "\n• ".join(failures)
    payload = {
        "attachments": [
            {
                "color": "#ff9900",
                "title": f"Financial ETL — Validation FAILED: {dataset}",
                "fields": [
                    {"title": "Run ID", "value": run_id, "short": True},
                    {"title": "Timestamp", "value": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"), "short": True},
                    {"title": "Validation Failures", "value": f"• {failure_text}", "short": False},
                ],
                "footer": "financial-etl",
            }
        ]
    }
    return _send_slack(payload)
