# Financial ETL Pipeline

금융 데이터 수집 · 검증 · 적재 파이프라인 포트폴리오 프로젝트.

## 구성

| 단계 | 도구 | 내용 |
|------|------|------|
| Extract | yfinance / FRED API | 주가(OHLCV) + 거시경제 지표 수집 |
| Validate | Great Expectations | 결측값 · 이상치 · 스키마 검증 |
| Transform | pandas | 수익률 · 이동평균 · RSI · 변동성 산출 |
| Load | PostgreSQL | Upsert (중복 방지) + 실행 이력 기록 |
| Notify | Slack Webhook | 성공 / 실패 / 검증 오류 알림 |

## 수집 데이터

**주식**: AAPL · MSFT · GOOGL · AMZN · META  
**거시경제**: GDP · CPI · 실업률 · 기준금리 · 10년 국채

## 빠른 시작

```bash
# 1. 환경 변수 설정
cp .env.example .env
# .env 파일에 FRED_API_KEY, SLACK_WEBHOOK_URL 입력

# 2. DB + 파이프라인 실행 (한 줄)
docker-compose up --build

# 3. DB만 띄우고 로컬에서 실행
docker-compose up postgres -d
pip install -r requirements.txt
python src/main.py --start 2023-01-01
```

## API 연동 테스트

```bash
python examples/api_test.py
```

## 프로젝트 구조

```
project-1-financial-etl/
├── src/
│   ├── extract.py      # yfinance + FRED API (exponential backoff retry)
│   ├── validate.py     # Great Expectations 품질 검증
│   ├── transform.py    # pandas 전처리 및 파생 지표 생성
│   ├── load.py         # PostgreSQL upsert + pipeline_logs 적재
│   ├── notify.py       # Slack Webhook 알림
│   └── main.py         # 전체 파이프라인 오케스트레이션
├── expectations/       # Great Expectations 검증 룰셋 (JSON)
├── examples/
│   └── api_test.py     # API 연동 확인 스크립트
├── sql/
│   └── create_tables.sql   # DDL (stock_prices, macro_indicators, pipeline_logs)
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .env.example
```

## 주요 설계 포인트

- **Retry**: API 호출 실패 시 exponential backoff 3회 재시도 (1s → 2s → 4s)
- **Upsert**: `INSERT ... ON CONFLICT DO UPDATE` 로 중복 적재 방지
- **Validation**: 검증 실패 시 파이프라인 중단 + Slack 오렌지 알림
- **Logging**: 모든 단계별 실행 결과를 `pipeline_logs` 테이블에 기록
- **Secret 관리**: `.env` 파일로 API 키 · DB 접속 정보 분리
