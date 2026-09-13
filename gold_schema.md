# TeamAlpha Gold v1 — 확정안

Gold는 별도 RDS를 만들지 않고 기존 Silver RDS의 같은 PostgreSQL database 안에
`gold` schema로 둔다. 총 **3개 테이블, 20개 컬럼**이다.

## 1. `gold.factor` — 9개

| 컬럼 | 설명 |
|---|---|
| `factor_id` | 팩터 버전 내부 ID |
| `factor_key` | 팩터 계열 키 |
| `version` | 계산식·설정 버전 |
| `description` | 팩터 의미 |
| `implementation_uri` | 실제 SQL/Python 구현 위치 |
| `implementation_hash` | 실행 코드 식별 hash |
| `config` | 입력, 파라미터, universe, PIT 조건 JSONB |
| `evaluation` | IC, 평가 기간, 통과 기준과 결과 JSONB |
| `status` | CANDIDATE / APPROVED / REJECTED / RETIRED |

`(factor_key, version)`은 유일하며 같은 `factor_key`에서 APPROVED 버전은 하나만
허용한다. APPROVED는 `evaluation.passed=true`, REJECTED는 `false`여야 한다.

## 2. `gold.factor_value` — 5개

| 컬럼 | 설명 |
|---|---|
| `factor_id` | 승인된 팩터 버전 |
| `asset_id` | Silver `public.asset` 종목 ID |
| `as_of_date` | 값이 투자 판단에 사용 가능한 PIT 날짜 |
| `value` | 방향을 적용하지 않은 팩터 원값(raw value) |
| `rank` | `score = value × predicted_sign`을 내림차순으로 매긴 거래일 단면 순위 |

기본키는 `(factor_id, asset_id, as_of_date)`다. APPROVED 상태인 팩터만 값을
적재할 수 있다. `predicted_sign=-1`이어도 `value`는 부호를 뒤집지 않고, raw value가
가장 낮은 종목을 rank 1로 저장한다.

## 3. `gold.factor_correlation` — 6개

| 컬럼 | 설명 |
|---|---|
| `left_factor_id` | 첫 번째 승인 팩터 |
| `right_factor_id` | 두 번째 승인 팩터 |
| `period_start` | 계산 시작일 |
| `period_end` | 계산 종료일 |
| `correlation` | 일별 rank Spearman 상관계수 |
| `observation_count` | 계산 관측치 수 |

기본키는 `(left_factor_id, right_factor_id, period_start, period_end)`다.
`left_factor_id < right_factor_id`를 강제해 역순 중복을 막고 두 팩터가 모두
APPROVED일 때만 적재한다.

## RDS 배치

```text
기존 PostgreSQL RDS
├── public
│   ├── asset
│   ├── asset_identifier
│   ├── price_daily
│   ├── fundamental
│   └── corporate_action
└── gold
    ├── factor
    ├── factor_value
    └── factor_correlation
```

같은 database에 두는 이유:

- `factor_value.asset_id`를 `public.asset`에 FK로 연결
- 별도 RDS 비용·Secret·네트워크·백업 불필요
- Silver를 읽어 Gold를 만드는 작업이 단순함

Gold 계산이 Silver 운영을 방해할 정도로 커질 때만 별도 RDS 또는 read replica를
검토한다. 물리적으로 분리하면 PostgreSQL FK를 사용할 수 없어 `asset` dimension
복제와 별도 정합성 검사가 필요하다.

## 레거시 예시: 12-1 모멘텀

처음 만든 레거시 Gold 구현은 최신 KRX 거래일 기준 12-1 모멘텀이다.

```text
signal_end   = as_of_date에서 21 거래일 전
signal_start = as_of_date에서 252 거래일 전
value        = adj_close[signal_end] / adj_close[signal_start] - 1
rank         = 같은 날짜 KOSPI·KOSDAQ 유니버스 내 value 내림차순 순위
```

- 구현: [`pipeline/gold/factors/momentum_12_1.sql`](pipeline/gold/factors/momentum_12_1.sql)
- 입력: `public.factor_price_feature_daily`의 KRX `adj_close`
- 유니버스: 최신 `as_of_date`에 KOSPI 또는 KOSDAQ인 `stock`
- 현재 단계: 메타데이터와 계산 SQL만 보존; 임시 2026-07 값은 제거됨
- 미확정: IC 계산 방식, 평가 기간, 최소 관측치, 승인 임계값, 자동 갱신 주기

메타데이터의 기존 상태와 별개로 현재 값은 0건이다. 다시 적재하려면 정식 연구 근거와 구현
계약을 먼저 갱신해야 하며, 레거시 SQL을 자동 실행하지 않는다.

## 적용과 검증

```bash
psql "$SILVER_DB_URL" -v ON_ERROR_STOP=1 -f sql/gold_schema.sql
psql "$SILVER_DB_URL" -v ON_ERROR_STOP=1 -c \
  "SELECT table_name FROM information_schema.tables WHERE table_schema='gold' ORDER BY table_name;"
```

DDL은 idempotent하며 `gold.active_factor_catalog` view는 현재 `APPROVED`인 버전만
노출한다. 팩터 값 계산 SQL은 호출자가 `factor_id`와 transaction을 관리한다.

## 연구 팩터 구현 실행

`trading_turnover_20d`와 `paid_in_capital_ratio`는
[`pipeline/gold/factors/manifest.json`](pipeline/gold/factors/manifest.json)에 등록된
read-only SQL로 계산한다. manifest는 연구 definition hash를 명시하며, 실행기는 APPROVED
상태, 실제 SQL SHA-256, 게시 config와 manifest의 definition hash, `predicted_sign`,
value/rank 계약을 모두 확인한다. 연구 parity와 운영 적재는 같은 query를 사용하고 운영
실행기만 날짜 파티션의 원자적 교체를 덧붙인다.

일별 v2 메타데이터는 기존 월별 버전을 덮어쓰지 않고 `CANDIDATE`로 별도 등록한다.

```bash
uv run python -m pipeline.gold.register_daily        # rollback 검증
uv run python -m pipeline.gold.register_daily --apply
```

```bash
# SQL과 계약을 검증하고 실행하되 마지막에 rollback
uv run python -m pipeline.gold.run \
  --factor trading_turnover_20d --as-of-date YYYY-MM-DD

# 명시적 승인 후에만 실제 적재
uv run python -m pipeline.gold.run \
  --factor trading_turnover_20d --as-of-date YYYY-MM-DD --apply
```

일별 v2 SQL은 인증된 RDS Silver 행과 PIT 식별자·공시만 사용하고 거래일마다 전체
유니버스의 값과 순위를 만든다. `--from-date`/`--to-date` 범위 실행도 지원하며 대상
날짜 파티션은 한 트랜잭션에서 완전히 교체한다. Gold 실행은 연구의 봉인 OOS 통과와
사람 승인 뒤 수행한다. Silver 재무제표 초기 적재가 끝나기 전에는 daily task에
자동 연결하지 않는다.
