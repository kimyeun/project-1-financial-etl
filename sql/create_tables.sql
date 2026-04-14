-- ============================================================
-- Financial ETL Pipeline — DDL
-- ============================================================

-- stock_prices: OHLCV + 파생 지표
CREATE TABLE IF NOT EXISTS stock_prices (
    id              BIGSERIAL       PRIMARY KEY,
    date            DATE            NOT NULL,
    ticker          VARCHAR(10)     NOT NULL,
    open            NUMERIC(12, 4),
    high            NUMERIC(12, 4),
    low             NUMERIC(12, 4),
    close           NUMERIC(12, 4)  NOT NULL,
    volume          BIGINT,
    daily_return    NUMERIC(10, 6),
    ma_7            NUMERIC(12, 4),
    ma_30           NUMERIC(12, 4),
    volatility_30d  NUMERIC(10, 6),
    rsi_14          NUMERIC(6, 2),
    vwap            NUMERIC(12, 4),
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_stock_prices_date_ticker UNIQUE (date, ticker)
);

CREATE INDEX IF NOT EXISTS idx_stock_prices_ticker      ON stock_prices (ticker);
CREATE INDEX IF NOT EXISTS idx_stock_prices_date        ON stock_prices (date DESC);
CREATE INDEX IF NOT EXISTS idx_stock_prices_ticker_date ON stock_prices (ticker, date DESC);


-- macro_indicators: FRED 거시경제 지표
CREATE TABLE IF NOT EXISTS macro_indicators (
    id               BIGSERIAL       PRIMARY KEY,
    date             DATE            NOT NULL,
    series_id        VARCHAR(30)     NOT NULL,
    indicator_name   VARCHAR(50)     NOT NULL,
    indicator_value  NUMERIC(18, 6),
    yoy_change       NUMERIC(10, 6),
    created_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_macro_date_series UNIQUE (date, series_id)
);

CREATE INDEX IF NOT EXISTS idx_macro_series_id   ON macro_indicators (series_id);
CREATE INDEX IF NOT EXISTS idx_macro_date        ON macro_indicators (date DESC);
CREATE INDEX IF NOT EXISTS idx_macro_name_date   ON macro_indicators (indicator_name, date DESC);


-- pipeline_logs: 파이프라인 실행 이력
CREATE TABLE IF NOT EXISTS pipeline_logs (
    id                 BIGSERIAL       PRIMARY KEY,
    run_id             VARCHAR(20)     NOT NULL,
    stage              VARCHAR(50)     NOT NULL,
    status             VARCHAR(20)     NOT NULL,   -- success | failed | error | passed
    rows_extracted     INTEGER         NOT NULL DEFAULT 0,
    rows_loaded        INTEGER         NOT NULL DEFAULT 0,
    validation_passed  BOOLEAN         NOT NULL DEFAULT TRUE,
    error_message      TEXT,
    started_at         TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    finished_at        TIMESTAMPTZ     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_logs_run_id     ON pipeline_logs (run_id);
CREATE INDEX IF NOT EXISTS idx_pipeline_logs_started_at ON pipeline_logs (started_at DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_logs_status     ON pipeline_logs (status);
