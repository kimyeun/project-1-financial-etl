"""
API 연동 빠른 테스트 스크립트
실행: python examples/api_test.py
"""

import os
import sys
import requests
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
# 1. yfinance 테스트
# ─────────────────────────────────────────────
def test_yfinance():
    print("\n[ yfinance ]")
    ticker = yf.Ticker("AAPL")
    df = ticker.history(period="5d")

    if df.empty:
        print("  FAIL: 데이터가 없습니다")
        return False

    print(f"  OK  : AAPL 최근 5일 데이터 {len(df)}행")
    print(df[["Open", "High", "Low", "Close", "Volume"]].tail(3).to_string())
    return True


# ─────────────────────────────────────────────
# 2. FRED API 테스트
# ─────────────────────────────────────────────
def test_fred():
    print("\n[ FRED API ]")
    api_key = os.getenv("FRED_API_KEY")

    if not api_key:
        print("  SKIP: FRED_API_KEY가 .env에 없습니다")
        return False

    url = "https://api.stlouisfed.org/fred/series/observations"
    params = {
        "series_id": "FEDFUNDS",
        "api_key": api_key,
        "file_type": "json",
        "sort_order": "desc",
        "limit": 5,
    }

    resp = requests.get(url, params=params, timeout=10)

    if resp.status_code != 200:
        print(f"  FAIL: HTTP {resp.status_code}")
        return False

    observations = resp.json().get("observations", [])
    print(f"  OK  : Fed Funds Rate 최근 {len(observations)}개")
    for obs in observations:
        print(f"        {obs['date']}  {obs['value']}%")
    return True


# ─────────────────────────────────────────────
# 3. Slack Webhook 테스트
# ─────────────────────────────────────────────
def test_slack():
    print("\n[ Slack Webhook ]")
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    if not webhook_url:
        print("  SKIP: SLACK_WEBHOOK_URL이 .env에 없습니다")
        return False

    payload = {"text": ":white_check_mark: Financial ETL — Slack 연동 테스트 성공!"}
    resp = requests.post(webhook_url, json=payload, timeout=5)

    if resp.status_code == 200:
        print("  OK  : Slack 메시지 전송 완료")
        return True
    else:
        print(f"  FAIL: HTTP {resp.status_code} / {resp.text}")
        return False


# ─────────────────────────────────────────────
# 실행
# ─────────────────────────────────────────────
if __name__ == "__main__":
    results = {
        "yfinance": test_yfinance(),
        "FRED API": test_fred(),
        "Slack   ": test_slack(),
    }

    print("\n─────────────────")
    print("결과 요약")
    print("─────────────────")
    all_pass = True
    for name, ok in results.items():
        status = "OK  " if ok else "FAIL/SKIP"
        print(f"  {name}: {status}")
        if ok is False:
            all_pass = False

    sys.exit(0 if all_pass else 1)
