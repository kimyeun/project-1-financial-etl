# Financial ETL Pipeline

금융 데이터를 수집·검증·변환·적재하는 엔드투엔드 ETL 파이프라인

![Python](https://img.shields.io/badge/Python-3.11-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)
![Great Expectations](https://img.shields.io/badge/Great_Expectations-0.18-orange)

---

## 파이프라인 구조

```
[yfinance / FRED API]
        │
        ▼
   Extract (extract.py)
   - 주식 OHLCV 5종목 1년치
   - 거시경제 지표 5종 3년치
   - Exponential backoff retry (3회)
        │
        ▼
   Validate (validate.py)
   - 스키마 · 결측값 · 이상치 검증
   - Great Expectations
        │
        ▼
   Transform (transform.py)
   - 비즈니스일 gap fill
   - RSI · 이동평균(7/30일) · VWAP · 변동성
        │
        ▼
   Load (load.py)
   - PostgreSQL Upsert
   - pipeline_logs 실행이력 기록
        │
        ▼
   Notify (notify.py)
   - Slack Webhook 성공/실패 알림
```

---

## 수집 데이터

| 구분 | 항목 | 소스 |
|------|------|------|
| 주식 | AAPL, MSFT, GOOGL, AMZN, META (OHLCV) | yfinance |
| 거시경제 | GDP, CPI, 실업률, 기준금리, 10년 국채 | FRED API |

---

## 실행 결과

```
=== Pipeline started [run_id=3a1a4add] ===
Fetched     1,250 stock rows  (5 tickers x 250 trading days)
Validation  PASSED
Transformed 1,305 rows  (business day gap fill 포함)
Upserted    1,305 stock rows → PostgreSQL
Fetched       899 macro rows  (GDP 11 / CPI 36 / UNRATE 36 / FEDFUNDS 36 / DGS10 780)
Validation  PASSED
Upserted      899 macro rows → PostgreSQL
=== Pipeline complete in 11.2s ===
```

---

## 기술 스택

| 역할 | 기술 |
|------|------|
| 언어 | Python 3.11 |
| 데이터 수집 | yfinance, FRED REST API, requests |
| 데이터 검증 | Great Expectations 0.18 |
| 데이터 처리 | pandas, numpy |
| 데이터베이스 | PostgreSQL 15, SQLAlchemy |
| 컨테이너 | Docker, Docker Compose |
| 알림 | Slack Webhook |

---

## 빠른 시작

```bash
# 1. 환경 변수 설정
cp .env.example .env
# .env 파일에 FRED_API_KEY 입력 (https://fred.stlouisfed.org/docs/api/api_key.html)

# 2. 실행 (한 줄)
docker-compose up --build
```

---

## 프로젝트 구조

```
project-1-financial-etl/
├── src/
│   ├── extract.py      # yfinance + FRED API, exponential backoff retry
│   ├── validate.py     # Great Expectations 품질 검증
│   ├── transform.py    # 파생 지표 생성 (RSI, MA, VWAP, 변동성)
│   ├── load.py         # PostgreSQL upsert + pipeline_logs 적재
│   ├── notify.py       # Slack Webhook 알림
│   └── main.py         # 파이프라인 오케스트레이션
├── sql/
│   └── create_tables.sql   # DDL (stock_prices, macro_indicators, pipeline_logs)
├── expectations/           # Great Expectations 검증 룰셋 (JSON)
├── examples/
│   └── api_test.py         # API 연동 확인 스크립트
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .env.example
```

---

## 주요 설계 포인트

- **Retry**: API 호출 실패 시 exponential backoff 3회 재시도 (1s → 2s → 4s)
- **Upsert**: `INSERT ... ON CONFLICT DO UPDATE`로 재실행 시 중복 적재 방지
- **Validation**: 검증 실패 시 파이프라인 즉시 중단 + Slack 알림
- **Observability**: 모든 stage 실행 결과를 `pipeline_logs` 테이블에 기록
- **Secret 관리**: `.env`로 API 키 · DB 접속 정보 분리, `.gitignore`로 커밋 제외
