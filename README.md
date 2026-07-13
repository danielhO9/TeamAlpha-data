# TeamAlpha-data

KRX·DART 데이터 파이프라인. **bronze**(raw → S3)로 수집하고 **silver**(정규화 → RDS)로 적재한다.
원칙: bronze 는 **API/데이터셋 응답을 수정 없이 저장**(필터·리네임·타입변환 없음). 정규화·수정주가·PIT는 silver 몫.

## 생애주기 (2축)

| | **backfill (초기 1회)** | **daily (증분)** |
|---|---|---|
| **bronze** | 2015~현재 대량 적재 | 전일치 적재 |
| **silver(RDS)** | bronze 전체 → 정규화 | 지정 날짜 가격 삭제 후 재적재 + 당해 재무 반영 |

`pipeline/jobs.py` 가 로컬/수동 모드를 묶는다. ECS/Fargate 운영은 `pipeline.daily_full` 을 EventBridge 로 화~토 08:30 KST에 실행해 전일 KRX 데이터를 적재한다.

## 데이터 × 소스

| 데이터 | backfill 소스 | daily 소스 | 저장 |
|---|---|---|---|
| 개별종목 시세 | **marcap** (전종목·상폐포함, 1995~) | **KRX OpenAPI** (공식·일별) | `stock/<source>/date=` |
| 지수 시세 | **KRX OpenAPI** (2010~) | **KRX OpenAPI** | `index/krxapi/date=` |
| 재무(주요계정) | **OpenDART** `fnlttMultiAcnt` (2015~) | **OpenDART** (당해 재실행=신규 공시) | `financials/dart/year=/corp=` |
| DART 회사코드 | **OpenDART** `corpCode.xml` | 기존 bronze 재사용 | `financials/dart/corpCode.xml` |

- **시세 백필=marcap**: pykrx는 스크래핑이라 불안정(과거 `by_ticker` 안티봇 401). marcap은 값이 KRX와 일치·안정적.
- **일별 시세·지수=KRX OpenAPI**: `data-dbg.krx.co.kr/svc/apis`, 헤더 `AUTH_KEY`. 공식·증분에 적합.
- **DART 유니버스=corpCode.xml**: bronze 에 저장한 `corpCode.xml` 을 silver 가 읽는다. 키 하나로 전 상장사(stock_code 보유, 상폐포함) → pykrx 불필요.

## 저장 구조 — `<데이터종류>/<소스>/`

```
data/                                                  # 로컬 출력(gitignore), S3로 미러링
  stock/marcap/date=YYYY-MM-DD/all.parquet             # 그 날짜 전종목 (18컬럼)
  index/krxapi/date=YYYY-MM-DD/<series>.parquet        # series ∈ kospi|kosdaq|krx
  financials/dart/corpCode.xml                         # DART 회사코드 원문 XML
  financials/dart/year=YYYY/corp=<ticker>/<reprt>.json # 한 회사·한 보고서 (CFS+OFS 주요계정)
```
- 벌크 응답을 날짜/회사별로 나눠 저장하되 **값은 무수정**.
- `reprt`: `11011`사업(FY) · `11012`반기 · `11013`1분기 · `11014`3분기.

## 코드 구조 (`pipeline/`)

```
pipeline/
  bronze/     stock_marcap.py(백필) · stock_krxapi.py(일별) · index.py(KRX OpenAPI) · financials.py(DART)
  silver/     assets · prices(adj_close) · financials(PIT) · load  — bronze→RDS 정규화
  common/     paths.py(경로·.env) · sink.py(parquet/JSON) · db.py(RDS 접속·upsert)
  jobs.py     로컬/수동 엔트리포인트: run_backfill / run_daily
  daily_full.py ECS 엔트리포인트: 전일 bronze 수집 → 변경분 silver incremental
deploy/       Dockerfile (ECS/Fargate 골격)
```
모든 수집기 **재개 가능**(이미 있는 파티션 스킵, 로컬·S3 공통). DART는 사용한도(020) 감지 시 중단 후 재개.

## 셋업

```bash
uv sync
cp .env.example .env    # KRX_API_KEY, DART_API_KEY, AWS_PROFILE, S3_BRONZE_BUCKET, SILVER_DB_URL
```
- **KRX_API_KEY**: [openapi.krx.co.kr](https://openapi.krx.co.kr/) 가입 → 지수·주식 서비스 **활용신청(승인)** 후 발급.
- **DART_API_KEY**: OpenDART 발급. · S3 적재 시 `aws sso login --profile <aws-profile>`.
- **SILVER_DB_URL**: silver 대상 PostgreSQL/RDS. 스키마 적용 `psql "$SILVER_DB_URL" -f sql/schema.sql`.

## 실행

```bash
# 초기 백필 (bronze 전체)
python -m pipeline.jobs --mode backfill --from 2015 --to 2026

# 매일 증분 수동 실행 (대상일 직접 지정 권장)
python -m pipeline.jobs --mode daily --dest s3

# ECS 운영 기본값: KST 기준 전일 데이터 수집. 특정일 재실행은 PIPELINE_DATE 사용.
PIPELINE_DATE=20260710 python -m pipeline.daily_full

# silver: bronze(로컬 ./data) → RDS 정규화 적재
python -m pipeline.silver.load --mode backfill
python -m pipeline.silver.load --mode incremental --date 20260710

# 개별 모듈 직접 실행도 가능
python -m pipeline.bronze.stock_marcap --from 2015 --to 2026          # 백필(marcap)
python -m pipeline.bronze.stock_krxapi --from 20260710 --to 20260710  # 일별(KRX OpenAPI)
python -m pipeline.bronze.index        --from 20150101 --to 20260707
python -m pipeline.bronze.financials   --from 2015 --to 2026

# S3 미러링 (로컬 적재 후 통째로)
aws s3 sync ./data s3://<S3_BRONZE_BUCKET>
```

## 알아둘 것

- **시세 2소스**: 백필=marcap(`stock/marcap/`), 일별=KRX OpenAPI(`stock/krxapi/`). silver 가 둘을 `price_daily`(source='KRX')로 통일.
- **adj_close**: 가격수정(분할·증자)은 등락률로 역산 계산됨(배당 미반영). 스키마·산출은 `schema_tables.md`.
- **silver 읽기는 현재 로컬 `./data`** — S3 직접읽기는 후속. `--mode incremental --date YYYYMMDD` 는 해당 거래일 `price_daily` 를 삭제 후 재적재하고, DART 재무는 해당 연도 JSON만 upsert 한다.
- **2015 재무는 사업보고서(연간)만** — DART가 2015 분기/반기 미제공(2016~ 완비). 소스 한계.
- 시세·지수 거래일 정합: 2015-01-02~2026-07-07 = 2,825일.
- 전체 인프라 설계(S3/RDS/ECS) → `../notes/data_pipeline_architecture.md`.
