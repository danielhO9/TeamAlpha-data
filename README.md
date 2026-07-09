# TeamAlpha-Ingest

KRX 시세·지수·DART 재무를 **bronze(raw)** 로 수집해 로컬(`./data`)에 쌓고 S3로 미러링하는 배치 파이프라인.
원칙: **API/데이터셋 응답을 수정 없이 저장**(필터·리네임·타입변환 없음). 정규화·수정주가·PIT는 silver(RDS) 몫.

## 데이터 × 소스

| 데이터 | 소스 | 유니버스 | 기간 | 비고 |
|---|---|---|---|---|
| 개별종목 시세 | **marcap** (FinanceData) | 전종목(KOSPI/KOSDAQ/KONEX, 상폐포함) | 1995~ | raw 체결가, 정적 parquet(스크래핑·한도 없음) |
| 지수 시세 | **KRX OpenAPI** | 전 지수(코스피200·코스닥150 등) | 2010~ | 공식 인증키 API |
| 재무(주요계정) | **OpenDART** `fnlttMultiAcnt` | 상장사(corpCode.xml, 상폐포함) | 2015~ | 연결+별도, 2015는 사업보고서만(소스 한계) |

- **시세는 marcap**: pykrx는 KRX 웹 스크래핑이라 차단·불안정(과거 `by_ticker`가 안티봇 401). marcap은 값이 KRX와 일치하며 안정적이라 채택.
- **지수는 KRX OpenAPI**: `data-dbg.krx.co.kr/svc/apis/idx/*`, 헤더 `AUTH_KEY`. 구성종목 API는 없음(필요 시 pykrx 별도).
- **DART 유니버스는 corpCode.xml**: 키 하나로 전 상장사(stock_code 보유)를 받아 풀로 사용 — pykrx 불필요.

## 저장 구조 — `<데이터종류>/<소스>/`

```
data/
  stock/marcap/date=YYYY-MM-DD/all.parquet          # 그 날짜 전종목 (18컬럼: OHLC·Volume·Marcap·Stocks…)
  index/krxapi/date=YYYY-MM-DD/<series>.parquet      # series ∈ kospi|kosdaq|krx (그 날짜 해당 시리즈 전 지수)
  financials/dart/year=YYYY/corp=<ticker>/<reprt>.json   # 한 회사·한 보고서 (CFS+OFS 주요계정 raw rows)
```
- 파티션 키로 형식 구분: 시세·지수는 `date=`(그날 전체), 재무는 `year=`+`corp=`(회사×보고서).
- 벌크 응답을 날짜/회사별로 나눠 저장하되 **값은 무수정**.
- `reprt`: `11011`사업(FY) · `11012`반기 · `11013`1분기 · `11014`3분기.

## 모듈 (`bronze/`)

| 파일 | 소스 | 저장 | 실행 인자 |
|---|---|---|---|
| `stock.py` | marcap | `stock/marcap/date=` | `--from/--to` (연도) |
| `index.py` | KRX OpenAPI | `index/krxapi/date=` | `--from/--to` (YYYYMMDD) |
| `financials.py` | OpenDART | `financials/dart/year=/corp=` | `--from/--to` (연도) |
| `common.py` | 공통 | 경로·날짜·.env 로드 | — |
| `sink.py` | 공통 | parquet/JSON 저장 (로컬·S3 동일 코드) | — |

모든 수집기 **재개 가능**(이미 있는 파티션 스킵, `sink.exists`, 로컬·S3 공통). DART는 사용한도(020) 감지 시 중단 후 재개.

## 셋업

```bash
uv sync
cp .env.example .env    # KRX_API_KEY, DART_API_KEY, AWS_PROFILE, S3_BRONZE_BUCKET 채우기
```
- **KRX_API_KEY**: [openapi.krx.co.kr](https://openapi.krx.co.kr/) 가입 → 지수·주식 서비스 **활용신청(승인)** 후 발급.
- **DART_API_KEY**: OpenDART 발급.
- S3 적재 시: `aws sso login --profile <aws-profile>`.

## 실행

```bash
# 개별종목 시세 (marcap)
uv run python -m bronze.stock      --from 2015 --to 2026

# 지수 (KRX OpenAPI)
uv run python -m bronze.index      --from 20150101 --to 20260707

# 재무 (OpenDART) — 전 상장사, ~1,920콜, 재개 가능
uv run python -m bronze.financials --from 2015 --to 2026

# S3 미러링 (방법 B): 로컬에 쌓은 뒤 통째로 동기화
aws s3 sync ./data s3://<S3_BRONZE_BUCKET>
#   각 수집기에 --dest s3 를 주면 수집과 동시에 S3 적재도 가능
```

## 알아둘 것

- **2015 재무는 사업보고서(연간)만** — DART가 2015 분기/반기 주요계정을 제공하지 않음(2016~ 4보고서 완비). 삼성전자도 동일 → 소스 한계.
- 시세·지수 **거래일 정합**: 2015-01-02~2026-07-07 = 2,825 거래일로 동일 정렬.
- 로컬 출력 `./data/`, `.env`, `docs_cache/` 는 gitignore.
- 전체 인프라 설계(S3/RDS/ECS) → `../notes/data_pipeline_architecture.md`.
