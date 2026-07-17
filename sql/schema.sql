-- TeamAlpha silver 스키마 (PostgreSQL/RDS) — schema_tables.md 와 1:1
-- 테이블 4: asset · asset_identifier · price_daily · fundamental (뷰·index_membership·shares_outstanding·dart_fetch_status 없음)
-- asset_id 를 중심으로 가격·재무를 연결. source 컬럼·asset_identifier 로 소스 추가에 열려 있음.

-- 1. asset — 종목 마스터 (소스 독립 정체성)
CREATE TABLE IF NOT EXISTS asset (
    asset_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name       TEXT NOT NULL,
    asset_type TEXT NOT NULL CHECK (asset_type IN ('stock', 'index')),
    exchange   TEXT NOT NULL,          -- 예: 'KRX'
    currency   TEXT NOT NULL           -- 예: 'KRW'
);

-- 2. asset_identifier — 소스별 종목코드 매핑 (소스 추가 확장점)
CREATE TABLE IF NOT EXISTS asset_identifier (
    asset_id   BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
    source     TEXT NOT NULL,          -- 'KRX' | 'DART' | (향후 'YAHOO'·'SEC'…)
    identifier TEXT NOT NULL,          -- KRX='005930', DART='00126380'
    PRIMARY KEY (asset_id, source, identifier)
);
CREATE INDEX IF NOT EXISTS ix_asset_identifier_lookup ON asset_identifier (source, identifier);

-- 3. price_daily — 일봉 (주식 + 지수 공용). shares/market_cap 흡수.
CREATE TABLE IF NOT EXISTS price_daily (
    asset_id      BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
    source        TEXT NOT NULL,       -- 가격 출처 (예: 'KRX')
    trade_date    DATE NOT NULL,
    open          NUMERIC(18,4),
    high          NUMERIC(18,4),
    low           NUMERIC(18,4),
    close         NUMERIC(18,4),
    adj_close     NUMERIC(18,4),       -- 가격 수정종가(분할·증자). 배당 반영은 소스 추가 후. 지수는 = close
    volume        BIGINT,
    trading_value NUMERIC(20,2),
    shares        BIGINT,              -- 상장주식수 (index는 NULL)
    market_cap    NUMERIC(24,2),       -- 시가총액. index는 구성종목 시총 합계가 들어간다(NULL 아님)
    PRIMARY KEY (asset_id, source, trade_date)
);

-- 4. fundamental — 재무 (long, DART). 한 행 = 종목×회계기간×공시×지표.
CREATE TABLE IF NOT EXISTS fundamental (
    asset_id       BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
    source         TEXT NOT NULL,      -- 'DART' …
    period_end     DATE NOT NULL,      -- 회계기간 종료일
    fiscal_period  TEXT NOT NULL CHECK (fiscal_period IN ('FY', 'Q1', 'Q2', 'Q3', 'Q4')),
    fs_type        TEXT NOT NULL CHECK (fs_type IN ('CFS', 'OFS')),  -- 연결 | 별도
    filing_id      TEXT,               -- 접수번호(rcept_no)
    filed          DATE,               -- 접수일
    available_date DATE,               -- PIT 사용가능일 (filed+1 or 법정기한+1)
    metric         TEXT NOT NULL,      -- 표준지표: revenue, net_income, total_equity…
    value          NUMERIC(20,2),
    PRIMARY KEY (asset_id, source, period_end, fiscal_period, fs_type, metric)
);
-- PIT 조회용 (available_date <= 기준일 필터)
CREATE INDEX IF NOT EXISTS ix_fundamental_pit ON fundamental (asset_id, metric, available_date);
