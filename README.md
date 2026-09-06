# TeamAlpha Data Pipeline

KRX·DART·FMP 데이터를 수집해 원천 데이터는 **S3 Bronze**에 보존하고,
분석 가능한 데이터는 **RDS PostgreSQL Silver**, 검증된 팩터 산출물은 같은
database의 **`gold` schema**에 저장하는 배치 파이프라인입니다.

## 프로젝트 개요

```text
KRX OpenAPI / OpenDART / marcap / FMP stable API
                    │
                    ▼
        bronze 수집기 -> S3 bronze
                    │
                    ▼
       ECS daily/backfill -> RDS public silver
                                      │
                                      ▼
                 research/manual -> RDS gold schema
```

- **bronze**: 소스의 원천 단위와 값을 S3에 보존합니다. FMP 응답은 byte-for-byte로
  저장합니다. ETF·펀드가 섞여 있어도 삭제하지 않으며 편입 필터와 타입 변환은
  Silver에서 수행합니다.
- **silver**: `asset` 중심으로 가격·재무·기업행사를 정규화하고, point-in-time 및
  source-aware 품질 게이트를 통과한 데이터만 publish합니다.
- **gold**: 팩터 정의·버전·평가와 종목별 값·순위, 팩터 간 상관관계를 저장합니다.
  레거시 12-1 모멘텀과 여섯 연구 후보 계산 SQL이 있으며, 임시 모멘텀 값은 제거된 상태입니다.
- **운영 스케줄**: 화~토 오전 08:30 KST cron을 사용합니다. 전체 복구 작업 중에는
  Scheduler를 일시 중지할 수 있지만, 정상 배포는 새 daily target을 등록한 뒤
  Scheduler를 `ENABLED`로 복원합니다.
- **자동 배포**: `main` 브랜치에 push하면 GitHub Actions가 ECR/ECS/Scheduler를 갱신합니다.
- **결과 알림**: daily ECS task가 종료되면 SNS 이메일로 성공/실패 결과를 받습니다.

현재 운영 스냅샷은 2026-08-05 기준 ruleset `1.22.1`이며, FMP 물리 원자재
연속선물 28종의 2015~2026 Bronze/Silver 백필이 인증 완료됐습니다. 최신 행 수,
기간, warning과 DQ run ID는
[`pipeline/silver_quality/QUALITY_STATUS.md`](pipeline/silver_quality/QUALITY_STATUS.md)에
기록합니다.

## 폴더 구조

```text
.
├── .github/workflows/            # 자동 배포·테스트·원자재 one-off
│   ├── deploy.yml
│   ├── test.yml
│   └── commodity-backfill.yml
├── deploy/Dockerfile             # ECS/Fargate 실행 이미지
├── pipeline/                     # Bronze 수집, Silver 적재, Gold 계산 코드
│   ├── bronze/                   # S3 bronze 원천 데이터 수집기
│   ├── common/                   # 경로, 저장, DB 공통 유틸
│   ├── gold/                     # 팩터 계산 구현
│   ├── silver/                   # RDS silver 후보 생성/적재
│   └── silver_quality/           # 품질 규칙, DQ 이력, staging/backfill
├── sql/schema.sql                # RDS silver schema
├── sql/gold_schema.sql           # 같은 RDS의 gold schema
├── schema_tables.md              # silver 테이블 설계 상세 문서
├── gold_schema.md                # gold 3테이블 설계 상세 문서
├── pyproject.toml                # Python 프로젝트/의존성 설정
└── uv.lock                       # 의존성 lock 파일
```

## AWS 구조

운영에 필요한 핵심 흐름만 요약하면 다음과 같습니다.

```text
EventBridge Scheduler
  -> ECS Fargate task
  -> S3 bronze 저장
  -> RDS public silver 적재
  -> SNS 이메일 알림

Research/manual factor job
  -> RDS public silver 조회
  -> RDS gold 적재
```

### 배포 흐름

코드가 `main` 브랜치에 push되면 GitHub Actions가 새 실행 이미지를 만들고 운영 ECS 설정을 갱신합니다.

```text
GitHub main push
  -> GitHub Actions 실행
  -> deploy/Dockerfile로 Docker 이미지 빌드
  -> ECR repository에 이미지 push
     - 태그 1: Git commit SHA
     - 태그 2: latest
  -> ECS task definition 새 revision 등록
     - 새 revision은 태그가 아니라 방금 push한 이미지의 SHA-256 digest를 바라봄
  -> EventBridge Scheduler target 갱신
     - 현재 상태를 검증하고 새 task definition으로 교체
     - 정상 배포가 끝나면 Scheduler를 ENABLED로 복원
```

즉, GitHub에 코드를 push하면 새 Docker 이미지가 ECR에 올라가고, ECS는 다음 실행부터 그 이미지를 받아 실행합니다.

### 스케줄 실행 흐름

매일 실행은 EventBridge Scheduler가 시작합니다. 배포 workflow는 새 이미지와 task
definition을 등록한 뒤 Scheduler를 자동으로 활성화합니다.

```text
EventBridge Scheduler
  -> ECS Fargate task 실행
  -> ECS가 ECR에서 Docker 이미지 pull
  -> 컨테이너에서 python -m pipeline.daily_full 실행
  -> KRX/DART/FMP API 호출
  -> S3 bronze 저장
  -> 전체 DART/KRX 증거를 /app/data로 동기화
  -> viewer/support family 갱신 및 v5 action snapshot 사전검증
  -> RDS raw silver 적재 (총수익 계약 BUILDING)
  -> local action/신규 가격 scale preview
  -> action-only 적재 -> v3 전체 총수익 rebuild -> 독립 audit
  -> fatal freshness 검사 (CERTIFIED가 아니면 exit nonzero)
  -> ECS task 종료
  -> EventBridge rule이 STOPPED 이벤트 감지
  -> SNS 이메일 알림
```

핵심 리소스 종류:

| 구분 | 설명 |
|---|---|
| 리전 | `ap-northeast-2` |
| S3 bronze bucket | 원천 데이터를 저장하는 S3 bucket |
| ECR repository | ECS에서 실행할 Docker 이미지를 저장하는 repository |
| ECS cluster | daily batch task를 실행하는 Fargate cluster |
| ECS task definition | 파이프라인 컨테이너, role, secret 주입 설정 |
| EFS pipeline cache | `/app/data`에 마운트해 인증된 DART 증빙을 보존하고 ETag가 바뀐 객체만 다시 받는 영구 캐시 |
| Scheduler | daily ECS task를 시작하는 EventBridge Scheduler |
| Scheduler 시간 | `cron(30 8 ? * TUE-SAT *)`, `Asia/Seoul` |
| RDS PostgreSQL | `public` Silver와 `gold` 팩터 테이블을 함께 저장하는 private database |
| SNS topic | daily task 결과 이메일 알림 |

운영 task에는 AWS Secrets Manager 값이 환경변수로 주입됩니다.

```text
KRX_API_KEY
DART_API_KEY
FMP_API_KEY
S3_BRONZE_BUCKET
SILVER_DB_URL
```

FMP key는 GitHub Actions repository variable `FMP_API_SECRET_ARN`이 가리키는
AWS Secrets Manager 값으로 ECS의 `FMP_API_KEY`에 주입합니다. 새 task definition에
이 secret이 없으면 배포 workflow가 중단됩니다.

`.env`, API key, DB 비밀번호, 로컬 `data/`는 커밋하면 안 됩니다.

운영 Fargate task의 `/app/data`는 암호화된 EFS에 마운트한다.
`pipeline.dart_silver_backfill_ecs`는 S3 LIST에서 받은 ETag와 크기를
`.teamalpha/s3_object_cache_v1.json`에 원자적으로 기록한다. 다음 task는 로컬
파일·ETag·크기가 모두 일치하는 객체를 재사용하고 새로 생성되거나 변경된 객체만
GET한다. 최종 action snapshot 생성은 재사용한 입력도 SHA-256으로 다시 묶으므로
전송 캐시가 연구용 인증 검증을 우회하지 않는다.

## S3 Bronze 구조

버킷:

```text
s3://<bronze-bucket>/
```

경로 구조:

```text
stock/
  marcap/
    date=YYYY-MM-DD/
      all.parquet

  krxapi/
    date=YYYY-MM-DD/
      kospi.parquet
      kosdaq.parquet

index/
  krxapi/
    date=YYYY-MM-DD/
      kospi.parquet
      kosdaq.parquet
      krx.parquet

financials/
  dart/
    corpCode.xml
    year=YYYY/
      corp=<ticker>/
        11011.json   # FY
        11013.json   # Q1
        11012.json   # Q2
        11014.json   # Q3
  dart_statement_lines/
    year=YYYY/corp=<ticker>/report=<보고서코드>/fs_type=<CFS|OFS>/
      sha256=<원문해시>/response.json
      latest.json

ownership/dart/
  disclosure_type=<EXECUTIVE_MAJOR_SHAREHOLDER|FIVE_PERCENT>/corp=<ticker>/
    sha256=<원문해시>/response.json
    latest.json

company_profiles/dart/corp=<ticker>/
  sha256=<원문해시>/
    response.json
    manifest.json  # 최초 관측시각·DART 업종코드
  latest.json

investor_flows/krx/
  sha256=<원문해시>/
    source.csv
    manifest.json  # 구매·활용승인 ID 및 SHA-256

short_balances/krx/
  sha256=<원문해시>/
    source.csv
    manifest.json  # 구매·활용승인 ID·최초 관측시각·SHA-256

corporate_actions/
  dart/
    disclosures/
      year=YYYY/date=YYYY-MM-DD/corp=<ticker>/
        rcept=<접수번호>.json
    structured/
      event=<행사종류>/year=YYYY/corp=<ticker>/
        rcept=<접수번호>.json
    documents/
      year=YYYY/corp=<ticker>/
        rcept=<접수번호>.zip
    documents_unavailable/
      year=YYYY/corp=<ticker>/
        rcept=<접수번호>.xml  # DART status=014 원문
    manifests/
      from=YYYYMMDD/to=YYYYMMDD/
        disclosures_v3.json
        structured_complete_v3.json
        documents_complete_v5.json
  krx/
    cash_adjustment_scale_source_evidence.json
    cash_adjustment_scale_price_objects.json
    kind/
      cash_adjustment_scale_support.json
      request_objects/sha256=<sha>.json
      body_objects/sha256=<sha>.html
    # 위 manifest들이 가리키는 KIND 요청/원문과 날짜별 KRX 가격 object

dividends/dart/alot-matter/
  year=YYYY/report=<보고서코드>/corp=<ticker>/rcept=<접수번호>/
    response.json                 # API 응답 byte-for-byte
    manifest.json                 # 요청정보·크기·SHA-256, API key 제외

stock/fmp/
  universe/                         # stock-list, screener, profile bulk 원문
  eod-bulk/date=YYYY-MM-DD/         # 글로벌 CSV 응답 전체
financials/fmp/                     # 글로벌 bulk + 변경 종목별 JSON 원문
corporate_actions/fmp/              # 배당·분할 calendar 전체 응답
fx/fmp/pair=USDKRW/                 # USD/KRW 원문
commodities/fmp/
  list/snapshot_date=YYYY-MM-DD/     # FMP 전체 commodities 목록 원문
  eod/symbol=<symbol>/               # 허용된 28개 연속선물 OHLCV 원문
market/fmp/                         # 미국 거래소 시간·휴일 원문
```

bronze 원칙:

- 가능한 한 원천 응답 단위에 맞춰 파티션을 나눕니다.
- 값은 원천 응답 그대로 저장합니다.
- FMP 글로벌/broad 응답을 미국 주식만 골라 다시 쓰지 않습니다. `response.*`와
  SHA-256·요청정보만 담은 별도 `manifest.json`을 저장하며 API key는 기록하지 않습니다.
- FMP commodities 전체 목록은 원문 그대로 보존합니다. 가격은 금융선물과
  micro 중복을 제외한 물리 원자재 연속선물 28개만 수집합니다.
- FMP 과거 가격은 XNYS의 실제 완료 거래일만 대상으로 `eod-bulk`를 날짜당 한 번
  호출합니다. 현재 미국 세션과 미래 날짜를 빈 immutable 파티션으로 확정하지 않습니다.
- 재개 시 S3 payload 전체를 다시 내려받지 않고 완료 manifest와 객체 크기로 빠르게
  판정합니다. 전체 SHA-256 재검증 함수는 별도 품질 감사에 사용할 수 있습니다.
- EOD Bulk가 `429`를 반환하면 엔드포인트별 실질 호출 간격을 학습하며, 재무·유니버스
  등 작은 endpoint의 속도는 별도로 유지합니다.
- FMP `company-screener`는 10,000행 페이지를 끝까지 순회하고, `delisted-companies`는
  실제 100행 페이지 크기로 순회합니다. `symbol-change`는 명시적 10,000행 limit으로
  전체 이력을 보존합니다. 현재연도 split 백필의 종료일은 실행 당일까지만 사용합니다.
- Bronze에서는 `isEtf`·`isFund` 조건으로 행을 제거하지 않습니다. ETF/fund 및
  비주식 상품 제외는 Silver 후보 생성과 DQ에서만 수행합니다.
- `stock/marcap`은 과거 주식 가격 백필에 사용합니다.
- `stock/krxapi`, `index/krxapi`는 daily 증분 적재에 사용합니다.
- `financials/dart/corpCode.xml`은 bronze에 저장하고 silver에서 재사용합니다.
- DART 공시 목록과 유상·무상증자, 감자, 합병·분할, 주식교환의
  구조화 API 응답은 JSON 원문으로 저장합니다.
- 무상증자 issuer family가 공식 viewer에는 존재하지만 OpenDART 구조화 endpoint에서
  누락된 경우, 구조화 행을 꾸며내지 않습니다. 검증된 terminal viewer HTML을
  content-addressed로 고정하고 별도 semantic source `DART_VIEWER`로만 게시합니다.
- 액면분할·병합, 권리락·배당락과 현금·현물배당결정처럼 효력일·금액 확인에
  원문이 필요한 공시는 `document.xml` 응답인 ZIP도 함께 저장합니다.
  변경상장, 거래정지·상장폐지는 목록 JSON만 보존합니다.
- 국내 보고기간별 배당은 OpenDART `alotMatter.json` 원문을 접수번호별로
  보존합니다. 과거 기본 백필은 실제 DART 사업보고서가 있는 종목만 호출하고,
  일일 증분은 새로 생기거나 정정된 분기·반기·사업보고서만 호출합니다.
  `status=013`도 완료 응답으로 저장해 같은 무데이터 조합을 반복 호출하지 않습니다.
- 목록에는 있지만 DART 원문 파일이 없는 `status=014` 응답은
  `documents_unavailable`에 원문 XML로 기록하고 재요청하지 않습니다.
- 기간별 manifest와 단계 완료 marker를 저장해 중단 후 완료된 API 단계를
  반복 호출하지 않고 재개합니다.

## RDS Silver 구조

핵심 Silver 원천과 연구 확장 입력, 파생 총수익 계약·감사 테이블로 구성됩니다.

```text
asset
asset_identifier
price_daily
fundamental
fundamental_statement_line  # DART 전체 숫자 원계정
ownership_disclosure_event  # 임원·주요주주·5% 보유 공시
investor_flow_daily         # 승인된 KRX 투자자별 종목 수급
industry_classification_observation  # 관측시각이 고정된 DART 현재 업종
short_position_balance_observation   # 승인된 KRX 공매도 잔고 vintage
corporate_action
dividend_history  # corporate_action의 cash_dividend 조회 view
dart_action_snapshot_contract  # v5 action/source-evidence snapshot 계약
cash_adjustment_scale_source_evidence  # scale-change cash event parent 증거
cash_adjustment_scale_support_action   # parent별 공식 support action lineage
dividend_event_resolution  # 배당 정정·배당락일·적용일 감사
price_return_contract      # total_return_close 방법론·인증 상태
```

관계:

```text
asset
  -> asset_identifier  # ticker·corp_code·CIK·CUSIP·ISIN·FX/원자재 심볼
  -> price_daily       # 주식/지수/FX/원자재 연속선물 일봉
  -> fundamental       # DART 재무 지표 long format
  -> fundamental_statement_line  # DART BS/IS/CIS/CF/SCE 원계정
  -> ownership_disclosure_event  # DART 지분 공시
  -> investor_flow_daily         # KRX 투자자별 일별 수급
  -> industry_classification_observation  # DART 현재 업종 관측 이력
  -> short_position_balance_observation   # KRX 공매도 잔고 관측 이력
  -> corporate_action  # DART/FMP 배당·분할·자본변동
```

| 테이블 | 역할 | 주요 키 |
|---|---|---|
| `asset` | 종목/지수 마스터 | `asset_id` |
| `asset_identifier` | KRX/DART/FMP 식별자 매핑과 유효기간 | `(asset_id, source, identifier_type, identifier, valid_from)` |
| `price_daily` | 주식·지수·FX·원자재 연속선물 일봉 | `(asset_id, source, trade_date)` |
| `fundamental` | DART/FMP 재무계정 long format | `(asset_id, source, statement_type, data_basis, period_end, fiscal_period, fs_type, revision_key, metric)` |
| `fundamental_statement_line` | DART 전체 숫자 원계정 및 공시 revision | `(asset_id, source, filing_id, fs_type, line_key)` |
| `ownership_disclosure_event` | 임원·주요주주·5% 보유 공시 이벤트 | `(asset_id, source, event_key)` |
| `investor_flow_daily` | 승인된 KRX 투자자유형별 종목 일별 수급 | `(asset_id, source, trade_date, market, investor_type)` |
| `industry_classification_observation` | DART 현재 업종코드의 관측 vintage | `(asset_id, source, taxonomy, observation_key)` |
| `short_position_balance_observation` | 승인된 KRX 종목별 공매도 잔고 vintage | `(asset_id, source, position_date, market, observation_key)` |
| `corporate_action` | 배당·분할·증자·감자 등 기업행사 | `(asset_id, source, action_key)` |
| `dividend_source_receipt` | DART 현금배당 접수 원문·정정 family·PIT 매핑의 append-only 영수증 | `(quality_run_id, receipt_no)` |
| `dart_action_snapshot_contract` | v5 원문·receipt·게시 action·scale evidence snapshot | `quality_run_id` |
| `cash_adjustment_scale_source_evidence` | 조정계수 변경일 현금배당의 content-addressed parent 증거 | `(action_snapshot_run_id, evidence_key)` |
| `cash_adjustment_scale_support_action` | parent가 참조한 주식배당·무상증자·권배락 공식 증거. 구조화 API 누락 무상증자는 `DART_VIEWER`로 구분 | `(action_snapshot_run_id, evidence_key, support_action_source, support_action_key, support_action_type)` |
| `dividend_event_resolution` | 원천 현금배당별 canonical/제외·배당락일·실제 적용일 감사 | `(quality_run_id, asset_id, source, action_key, resolution_version)` |
| `price_return_contract` | 자산군별 총수익 방법론과 현재 인증 상태 | `(source, asset_type, field_name)` |

배당은 별도 물리 테이블을 추가하지 않는다. 이벤트 날짜·원/분할조정 주당배당금·통화·
지급주기는 `corporate_action`에 저장하고 `dividend_history` view로 조회한다. DART
정기보고서의 배당총액·배당성향·배당수익률은 `fundamental`의 `DIVIDEND` 지표로
저장한다. 원천 가격과 결합해 다시 계산할 수 있는 시점별 배당수익률은
`corporate_action`에 저장하지 않는다.

KRX `adj_close`는 KRX 전일대비/기준가격 계수로 분할·증자·주식배당 같은 가격
조정만 누적한 **price-only** 값이며 일반 주권 현금배당은 포함하지 않는다. KRX
공식 [기준가격 산정 규정](https://global.krx.co.kr/contents/GLB/06/0602/0602020202/GLB0602020202T3.jsp)은
일반 주권의 현금배당이 아니라 주식배당 등 주식수 변화의 권리락 기준가격을 조정한다.
주식 `total_return_close`는 이 `adj_close`에 ISSUER 현금배당을 gross 기준으로
배당락일 종가 재투자해 계산한다. 명시된 배당락일이 없으면 KRX 결제 관행에 따라
기준일 이전 두 번째 시장 세션을 사용하고, 모든 원천행의 선택·제외 근거는
`dividend_event_resolution`에 남긴다. 새 KRX 가격이나 ISSUER 현금배당이 적재되면
`price_return_contract`는 즉시 `BUILDING`으로 강등되며, 전체 재구축·행 parity가
끝난 뒤에만 다시 `CERTIFIED`가 된다.
`total_return_close`는 최신 정정까지 소급 반영한 ex-post 실현수익 **forward label**로만
사용한다. bitemporal action vintage가 생기기 전에는 모멘텀 같은 과거 feature가 이를
읽으면 안 되며, `factor_price_feature_daily` view의 `adj_close`를 사용한다. 이 view는
`total_return_close`와 그 lineage 컬럼을 물리적으로 노출하지 않는다.

FMP Silver 편입 대상은 NASDAQ·NYSE·AMEX에서 거래되는 common stock,
preferred stock, ADR, REIT입니다. ETF, fund, ETN, warrant, unit, listed note는
Silver에서 제외하고 제외 건수·사유를 `dq_result`/`dq_metric`에 남깁니다.
FMP `close`는 `adj_close`(분할조정), `adjClose`는 `total_return_close`(배당조정)로
보존하고, 원 OHLC는 수집한 split ratio로 복원합니다. USD 가격·보고통화는 각
행의 `currency`에 기록하며 `USDKRW`도 FX asset의 `price_daily`로 적재합니다.

FMP 원자재는 `asset_type=commodity`,
`instrument_type=commodity_future_continuous`로 구분하며 `price_daily.source`는
`FMP_COMMODITY`를 사용합니다. FMP의 `USX`(미국 센트) 가격은 Silver에서 USD로
나눠 표준화하고 `asset.price_unit`에 `USD/barrel`, `USD/bushel` 같은 단위를
기록합니다. 선물에는 분할·배당 조정을 적용하지 않아 `adj_close=close`,
`total_return_close=NULL`입니다. FMP가 일요일 날짜로 제공하는 야간 선물 세션은
유지하고, 거래 세션이 아닌 토요일 행과 OHLC 불일치 행은 Bronze에 보존하되
Silver에서 제외해 `MODIFIED`로 기록합니다. 롤오버 가능 급변은 값을 고치지 않고
warning으로 남깁니다.

현재 2015~2026 원자재 이력은 2026년 일괄 백필이므로 과거 각 시점에 실제로
가용했던 PIT 입력으로 간주하지 않는다. 또한 롤 조정/선물 총수익 계약이 없으므로
팩터 연구의 hidden OOS·Gold 입력에는 바로 사용하지 않고, retrospective 진단이나
향후 별도 PIT·롤 방법론을 구축하는 원천으로만 사용한다.

컬럼별 상세 설계는 [schema_tables.md](schema_tables.md)와 [sql/schema.sql](sql/schema.sql)를 참고합니다.

## RDS Gold 구조

Gold는 별도 인스턴스를 만들지 않고 Silver와 같은 PostgreSQL database의 `gold`
schema에 둡니다. Silver의 `public.asset`을 FK로 직접 참조하므로 별도 종목 마스터
복제나 동기화가 필요 없습니다.

```text
gold.factor
  -> gold.factor_value
  -> gold.factor_correlation
```

| 테이블 | 역할 | 주요 키 |
|---|---|---|
| `gold.factor` | 팩터 정의, 구현 버전, 설정, 최신 평가와 상태 | `(factor_key, version)` |
| `gold.factor_value` | 승인 팩터의 종목×PIT 날짜별 원값과 순위 | `(factor_id, asset_id, as_of_date)` |
| `gold.factor_correlation` | 두 승인 팩터의 기간별 rank Spearman 상관 | `(left_factor_id, right_factor_id, period_start, period_end)` |

상태는 `CANDIDATE`, `APPROVED`, `REJECTED`, `RETIRED`이며, 값과 상관관계는
승인된 팩터만 적재할 수 있도록 DB trigger가 강제합니다. 동일한 `factor_key`에서
`APPROVED` 버전은 하나만 허용합니다.

레거시 첫 구현은 **12-1 모멘텀**이며, 연구 후보용 구현으로
`trading_turnover_20d`, `paid_in_capital_ratio`, `market_leverage`,
`operating_return_on_capital_employed`, `return_kurtosis_24m`,
`turnover_volatility_12m`가 추가되어 있습니다.

```text
value = adj_close[t-21 거래일] / adj_close[t-252 거래일] - 1
rank  = 같은 as_of_date KOSPI·KOSDAQ 유니버스 내 내림차순 순위
```

구현은 [`pipeline/gold/factors/`](pipeline/gold/factors/)의 allowlist된 read-only SQL에
있습니다. 연구 단계에서는 이 쿼리를 Python 정의와 대조하고, 운영 실행기는 검증된 동일 쿼리에
공통 INSERT/UPSERT를 감쌉니다. `python -m pipeline.gold.run --factor <key>
--as-of-month YYYY-MM`은 `--apply`가 없으면 rollback합니다. Gold는 현재 daily task에
자동으로 연결하지 않고 봉인 OOS 통과와 사람 승인 뒤 명시적으로 실행합니다.

상세 설계와 DDL은 [gold_schema.md](gold_schema.md),
[sql/gold_schema.sql](sql/gold_schema.sql)을 참고합니다.

## Daily 실행 흐름

운영 진입점:

```bash
python -m pipeline.daily_full
```

대상 날짜:

- `PIPELINE_DATE`가 있으면 해당 날짜를 사용합니다.
- 없으면 `Asia/Seoul` 기준 어제 날짜를 사용합니다.

실행 순서:

1. 공통 PostgreSQL session advisory lock을 먼저 잡아 다른 daily/one-off 인증
   epoch와 전체 실행을 직렬화합니다.
2. 대상 날짜의 KRX 주식/지수 Bronze를 S3에 저장합니다.
3. 당해 연도 DART 재무·정기보고서 배당과 기업행사를 확인하고 변경 원문만 저장합니다.
   새 action 객체는 S3 PUT 전에 총수익 계약을 `BUILDING`으로 먼저 내립니다.
4. complete DART/KRX 증거를 ECS 컨테이너의 `/app/data`로 동기화하고, official
   viewer/support family와 v5 action snapshot을 재생성·검증합니다.
5. KRX/DART Silver 후보를 생성하고 자동 품질 검사와 read-only preview를 수행합니다.
6. Critical/Error가 없을 때만 대상 날짜 교체와 upsert를 하나의 transaction으로
   반영하고, 즉시 action-only publish -> v3 전체 rebuild -> 독립 audit을 닫습니다.
7. 완료된 직전 미국 세션의 FMP 주식·재무·기업행사·USDKRW와 원자재 28종을
   Bronze/Silver 별도 transaction으로 처리합니다. 월요일 target에는 일요일 야간
   선물 세션도 함께 조회·교체합니다.
8. final freshness가 총수익 `CERTIFIED`를 포함한 모든 계약을 확인한 뒤 session
   advisory lock을 해제합니다. 실패는 exit nonzero이며, 이미 관측·게시된 새
   가격/action이 있으면 총수익 계약은 의도적으로 `BUILDING`에 남습니다.
9. 인증된 증분 warning은 `dq_warning_state`에 누적하고, 같은 변경 파티션의 재검사가
   PASS일 때만 해소합니다. 미해결 warning은 `dq_open_warning`에서 바로 조회합니다.

Critical/Error 중 단일 행 불변조건은 RDS CHECK·PK·UNIQUE·FK로도 강제합니다.
따라서 애플리케이션 품질검사를 우회한 쓰기도 DB에서 거부되며, 시계열·소스 간 대사와
Warning은 계속 Python 품질 게이트에서 검사합니다.

Gold 팩터는 평가 정책과 갱신 주기가 확정되기 전까지 daily 흐름에 포함하지 않습니다.

KRX OpenAPI는 당일 데이터를 안정적으로 제공하지 않기 때문에 다음날 오전에 전날 데이터를 가져옵니다.

```text
화요일 08:30 KST -> 월요일 데이터
수요일 08:30 KST -> 화요일 데이터
...
토요일 08:30 KST -> 금요일 데이터
```

## 로컬 설정

```bash
uv sync
cp .env.example .env
```

`.env` 예시:

```text
KRX_API_KEY=...
DART_API_KEY=...
FMP_API_KEY=...
DART_DIVIDENDS_ENABLED=true
AWS_PROFILE=<aws-profile>
S3_BRONZE_BUCKET=<bronze-bucket>
SILVER_DB_URL=postgresql://<user>:<password>@<rds-endpoint>:5432/<database>
```

AWS CLI 로그인:

```bash
aws sso login --profile <aws-profile>
```

## 자주 쓰는 명령

문법 확인:

```bash
uv run python -m compileall -q pipeline
```

Silver quality DB migration:

```bash
uv run python -m pipeline.silver_quality.migrate
```

`pipeline.daily_full`은 수집 전에 migration checksum을 읽기 전용으로 확인하며
미적용 DDL을 자동 실행하지 않습니다. 최초 v2 전환은 대형 `price_daily`·
`fundamental`의 타입/PK 변경을 포함하므로 스케줄을 중지하고 RDS snapshot을 만든
maintenance window에서 위 명령을 one-off로 먼저 실행해야 합니다.
일일 task는 KRX 가격 transaction 전에 최신 DART viewer/support family와 v5 action
snapshot을 완성하고 read-only preview를 통과시킨다. 가격 적재로 총수익 계약이
`BUILDING`이 되면 같은 ECS invocation에서 local scale preview, action-only 적재,
v3 전체 rebuild와 독립 audit까지 닫는다. freshness 오류는 경고로 삼키지 않으므로
성공 exit는 `CERTIFIED` 계약을 뜻한다.
새 DART action 객체는 S3 PUT 직전 PostgreSQL 계약을 먼저 `BUILDING`으로 강등한다.
따라서 새 정정을 관측한 뒤 viewer/family preflight가 실패해도 과거 label을
`CERTIFIED`로 계속 노출하지 않는다. viewer 공식 selector는 매 실행 다시 조회하고,
응답 body는 SHA-256 경로에 불변 저장한다. 생성된 viewer/support/action 산출물은
versioned bundle에 먼저 올린 뒤 단일 `quality/dart-total-return-snapshots/current.json`
pointer만 이전 ETag CAS로 전환하므로 중간 종료나 겹친 retry가 기존 snapshot을
부분 덮어쓰거나 coverage를 되돌릴 수 없다.
이전 task가 신규 가격 적재 뒤 실패해 계약이 이미 `BUILDING`이면 다음 task는 그 기존
가격 범위를 먼저 재인증한다. 재인증할 수 없으면 새 거래일을 적재하기 전에 중단한다.

직접 Silver backfill CLI는 비활성화되어 있습니다. 다음 두 진입점은 KRX
가격/기업행사를 쓴 뒤 v3 총수익 계약을 다시 `CERTIFIED`로 닫는
검증된 오케스트레이터가 없어, base·S3·RDS 접근 전에 즉시 실패합니다.

- `pipeline.silver_quality.backfill`
- `pipeline.silver_quality.s3_backfill`

일별 적재는 K2 인증 세션에서 원천 적재·action snapshot·총수익
재인증을 함께 닫는 `pipeline.daily_full`만 사용합니다. 최초/전체
backfill은 동일한 closed recertification 계약을 구현한 전용 운영
오케스트레이터가 추가될 때까지 금지됩니다.

기존 Silver 품질 감사:

```bash
uv run python -m pipeline.silver_quality.audit --scope all
```

보존된 legacy backfill 구현은 연도별 Silver 후보를 S3
`quality/candidates/silver-backfill/run=<run-id>/`에 고정하는 내부 함수를 유지하지만,
직접 CLI로는 실행할 수 없습니다. 일별 `pipeline.daily_full`과 수동 incremental도
동일한 품질 게이트를 자동 실행하며 우회 옵션은 없습니다. 규칙과 severity는
[`pipeline/silver_quality/README.md`](pipeline/silver_quality/README.md)에 정리되어 있습니다.
현재 운영 RDS의 인증 실행·OPEN warning·DB guard 결과 스냅샷은
[`pipeline/silver_quality/QUALITY_STATUS.md`](pipeline/silver_quality/QUALITY_STATUS.md)에서 확인합니다.

특정 날짜를 production daily 방식으로 실행:

```bash
PIPELINE_DATE=20260713 uv run python -m pipeline.daily_full
```

bronze 수집기 수동 실행:

```bash
uv run python -m pipeline.bronze.stock_marcap --from 2015 --to 2026 --dest s3
uv run python -m pipeline.bronze.stock_krxapi --from 20260713 --to 20260713 --dest s3
uv run python -m pipeline.bronze.index --from 20260713 --to 20260713 --dest s3
uv run python -m pipeline.bronze.financials --from 2026 --to 2026 --dest s3
uv run python -m pipeline.bronze.financials_full \
  --scope 004990:2015:11011:CFS --dest s3
uv run python -m pipeline.bronze.dart_full_statements --from 2015 --to 2026 --dest s3
uv run python -m pipeline.bronze.dart_ownership --dest s3
uv run python -m pipeline.bronze.dart_company_profiles --dest s3
# KRX 웹 자동수집 금지: 구매·활용승인 파일만 등록
uv run python -m pipeline.bronze.krx_investor_flows \
  --source-file ./authorized.csv --authorization-id <계약식별자> --dest s3
uv run python -m pipeline.bronze.krx_short_balances \
  --source-file ./authorized-short.csv --authorization-id <계약식별자> --dest s3
# 운영 S3 기업행사 직접 publication은 금지됩니다. pipeline.daily_full의
# fail-closed invalidation -> recertification 경로만 사용합니다.
uv run python -m pipeline.bronze.dividends --from 2015 --to 2026 --dest s3 --reports annual
uv run python -m pipeline.bronze.fmp --mode dividends --from 2015 --to 2026 --dest s3
uv run python -m pipeline.bronze.fmp --mode commodities --from 2015 --to 2026 --dest s3
uv run python -m pipeline.bronze.fmp --mode backfill --from 2015 --to 2026 --dest s3
uv run python -m pipeline.bronze.fmp --mode daily --date 20260713 --dest s3
```

FMP 전체 Bronze 이후 Silver를 연도별로 이어서 실행하는 ECS용 진입점:

```bash
uv run python -m pipeline.fmp_backfill_ecs --phase full --from 2015 --to 2026
uv run python -m pipeline.fmp_backfill_ecs --phase silver-range --from 2015 --to 2026
uv run python -m pipeline.fmp_backfill_ecs --phase commodities --from 2015 --to 2026
```

`silver-range`는 FMP API를 다시 호출하지 않고 S3 Bronze만 내려받아 연도별
RDS transaction을 순차 실행한다.

전체 재무·지분공시 Bronze와 승인된 투자자수급을 Silver에 원자적으로 반영:

```bash
uv run python -m pipeline.alternative_data_backfill_ecs \
  --phase full --from 2015 --to 2026
uv run python -m pipeline.alternative_data_backfill_ecs --phase silver
```

OpenDART 수집은 content-addressed pointer로 재개된다. 투자자수급·공매도 수집기는
KRX 웹페이지를 스크레이핑하지 않으며 승인 원본과 취득근거가 없으면 fail-closed한다.
현재 DART 업종과 오늘 받은 과거 공매도 파일은 최초 관측시각 이전으로 소급하지 않는다.

원자재 28종 전체 백필은 GitHub Actions의
[`commodity-backfill.yml`](.github/workflows/commodity-backfill.yml)을 수동 실행할
수도 있습니다. 운영 배포 role에 ECS `RunTask` 권한을 추가하지 않기 위해 workflow가
Scheduler를 잠시 one-off task definition으로 바꿔 한 번 실행한 뒤 원래 daily
스케줄로 복원합니다. workflow 성공은 **제출과 스케줄 복원 성공**을 뜻하며, 실제
적재 완료는 ECS exit code, SNS 알림과 `dq_run`의 `CERTIFIED` 상태로 확인합니다.

기존 DART 배당·기업행사 Bronze를 Silver 총수익에 반영하고 계약을 닫는 ECS용
단일 진입점:

```bash
uv run python -m pipeline.dart_silver_backfill_ecs --phase dart-extras
```

`krx-gap`은 가격을 쓴 뒤 총수익 계약을 `BUILDING`에 남길 수 있어 비활성화됐다.
누락 거래일은 날짜별 closed daily 흐름으로 재생해야 한다.

`dart-extras`는 총수익에 필요한 `cash_dividend`/실제일 `ex_dividend`와 scale-change가
참조하는 정확한 주식배당·무상증자·권배락 support action/source body까지 한정한다.
`DART_VIEWER/bonus_issue`는 official family 전 receipt에 구조화 행이 없고 terminal
issuer `주요사항보고서(무상증자결정)` body에서 보통주→보통주 양수 비율과
신주배정기준일을 정확히 하나씩 읽은 경우에만 `ADJUSTMENT_COMPONENT`가 된다.
body 경로·SHA, receipt/date/ratio와 `1/(1+r)` 계수는 family manifest와 Silver child,
synthetic `corporate_action` 사이에서 exact parity로 다시 검증한다.
TR 전용 v5 snapshot을 발행한 뒤
`total_return_close` 전체 rebuild와 독립 audit까지 성공해야 종료하며, 중간 실패 시
총수익 계약은 `BUILDING`에 남는다. `dividends/dart/alot-matter`의 정기보고서 배당
지표는 별도의 content-addressed completeness manifest가 없으므로 이 phase에서
`fundamental DIVIDEND/REPORTED`로 인증·적재하지 않는다. 해당 fundamental 복구는
전용 원문 manifest와 object/receipt parity 계약을 마련한 별도 작업으로 남긴다.

조정계수 변경 현금배당 331건의 로컬 증거 복구는 다음 순서로 준비한다. KIND 입력은
공식 고지를 검토한 두 외부 canonical artifact다. reference request는 KOSPI 99311과
KOSDAQ 70767 기준가 공시를, component request는 KRX에만 존재하는 61474 비현금
구성요소를 선언한다. 첫 명령은 두 raw request bytes와 각 공시의 공식 main identity,
선택된 body, 필요한 contents page를 모두 SHA-256 content-addressed object로 보존한다.
검증기는 main의 issuer·ticker·acceptance·selected document, form별 표 구조,
security·적용일·기준가·비현금 사유, component의 배당기준일·종류·비율을 다시 읽는다.
모든 request는 대상 현금 receipt/date의 정확히 한 KIND child에 소비되어야 하며
미사용·중복·우선주 혼입은 허용하지 않는다. 아래 수집 명령은 AWS/RDS에 쓰지 않으며,
`build`도 로컬 manifest만 원자적으로 게시한다.

먼저 `DART_API_KEY`를 argv나 문서에 쓰지 말고 실행 프로세스 환경에 secret으로
주입한다. 아래 세 명령은 하나의 로컬 base에 native disclosure/structured/document
완료 marker와 공식 cash/support revision-family manifest를 순서대로 만든다.
`corporate_actions`는 `--no-documents`나 document shard 없이 끝까지 실행해야
`disclosures_v3.json`, `structured_complete_v3.json`, `documents_complete_v5.json`을
완료 상태로 게시할 수 있다.

```bash
uv run python -m pipeline.bronze.corporate_actions \
  --from 20150101 --to 20260810 --dest local \
  --base /complete/dart/snapshot

uv run python -m pipeline.bronze.dart_viewer_corrections \
  --base /complete/dart/snapshot \
  --coverage-start 2015-01-01 --coverage-end 2026-08-10 \
  --apply

uv run python -m pipeline.bronze.dart_support_action_families \
  --base /complete/dart/snapshot \
  --coverage-start 2015-01-01 --coverage-end 2026-08-10 \
  --apply
```

종료일 전체를 덮는 v3/v3/v5 marker, viewer correction manifest, support-action
family manifest 중 하나라도 없거나 old marker만 있으면 이후 `build`는 fail-closed로
중단한다. Viewer/support 수집도 공식 DART HTTPS 원문만 로컬에 추가하며 AWS/RDS에는
쓰지 않는다.

```bash
uv run python -m pipeline.silver.cash_adjustment_scale_builder \
  download-kind \
  --base /complete/dart/snapshot \
  --reference-requests /reviewed/kind-reference-requests-v2.json \
  --component-requests /reviewed/kind-component-requests-v1.json

uv run python -m pipeline.silver.cash_adjustment_scale_builder \
  download-prices \
  --base /complete/dart/snapshot \
  --overlap /private/tmp/teamalpha-dividend-scale-overlap-20260812.csv \
  --expectations /private/tmp/teamalpha-dividend-final-gate-expectations-20260812.json \
  --s3-root s3://<bronze-bucket>/<optional-prefix> \
  --aws-profile <read-only-profile>

uv run python -m pipeline.silver.cash_adjustment_scale_builder \
  build \
  --base /complete/dart/snapshot \
  --overlap /private/tmp/teamalpha-dividend-scale-overlap-20260812.csv \
  --expectations /private/tmp/teamalpha-dividend-final-gate-expectations-20260812.json \
  --coverage-end 2026-08-10 \
  --s3-root s3://<bronze-bucket>/<optional-prefix>
```

KRX gross 배당재투자 총수익의 최초 구축·복구는 Scheduler가 `DISABLED`인
maintenance window에서 closed orchestrator 한 번으로 실행한다. orchestrator는
동일 PostgreSQL session advisory lock을 전체 epoch 동안 보유한 채 현재 v5 snapshot
준비·로컬 preview·action-only 적재·persisted preview·v3 전체 rebuild·독립 audit을
순서대로 수행한다. 어느 단계든 실패하면 exit nonzero이고 계약은 `BUILDING`에
남는다.

```bash
uv run python -m pipeline.silver_quality.migrate
DART_SNAPSHOT_EXPECTED_END=2026-08-10 \
  uv run python -m pipeline.dart_silver_backfill_ecs --phase dart-extras
```

`pipeline.silver.dart_extra_load --apply`와
`pipeline.silver.total_return_rebuild --apply`의 단독 CLI는 action generation parity와
최종 audit까지 원자적으로 보장하지 못하므로 비활성화됐다. 진단용 read-only preview는
계속 사용할 수 있지만 운영 write는 위 closed orchestrator 또는 `pipeline.daily_full`
안에서만 수행한다. DART snapshot은
generic 기업행사 전체가 아니라 총수익에 필요한 `cash_dividend`/실제일
`ex_dividend`와 manifest가 정확히 참조한 scale-support action의 완전성 계약이다.
2015-01-01부터 지정 종료일까지 native atomic
완료 marker와 TR 관련 receipt/body/ticker/정정 family를 검증하고 모든 증거의
크기·SHA-256을 고정한다. 겹치는 DART 목록에서 접수번호·종목코드·공시명 같은
불변 필드가 바뀌면 차단하고, `rm`·회사명·`corp_cls`처럼 후일 갱신되는 목록 표시는
명시적 manifest 종료일이 가장 늦은 관측만 선택해 conflict count/digest를 남긴다.

`corp_cls`는 포함 여부가 아니라 원문 provenance다. 모든 분류를 먼저 family의 최종
경제기준일에 유효한 `asset_identifier`로 연결하고, common stock이면서 그 날짜를
포괄하는 인증 KOSPI/KOSDAQ 가격 episode가 있을 때만 포함한다. 따라서 과거 상장사가
`E`로 오표기되어도 포함되고, 실제 KONEX·비상장·우선주는 PIT identity/가격/상품
사유로 제외된다. 포함·제외의 `corp_cls`별 건수는 감사 통계로만 보존한다.

인증 범위는 `2015-01-01+`, `KRX common_stock`, 날짜별 시장이
`KOSPI`/`KOSDAQ`인 행뿐이다. 우선주·KONEX와 2015년 이전 원 가격은 인증 범위가
아니다. 1995년부터 존재하는 원 가격 범위는 계약 metadata에 참고값으로만 남고
배당 포함 총수익으로 인증되지 않는다.
`price_daily.quality_run_id`는 원 가격 인증을 계속 가리키며, rebuild는
`total_return_quality_run_id`만 기록한다. `dividend_event_resolution`은 rebuild run별
append-only다. `dividend_source_receipt`는 제외된 접수까지 보존하고 각 family의
`terminal_receipt_no`·`terminal_announcement_date`를 명시한다. 최종 계약 metadata에는
원 action snapshot digest/count/coverage뿐 아니라 전체·terminal receipt row digest,
게시된 cash/ex 및 manifest 참조 support action row digest, 포함 receipt↔cash action과
parent/child support action exact parity digest,
PIT asset identity digest와 모든 2015+ 행의 run parity가 들어간다.

새 KRX 주가 또는 ISSUER `cash_dividend`/`ex_dividend`/참조 support action이 들어오면 계약은
즉시 `BUILDING`이 된다. 정기 `daily_full` 성공 경로는 같은 task 안에서 최신 complete
action snapshot으로 재구축과 audit을 완료해 다시 `CERTIFIED`로 닫는다. 신규 가격-scale
겹침에 대한 exact KIND/DART/KRX 증거가 아직 없으면 local preview에서 task가 실패하고
계약은 `BUILDING`으로 남으므로, 성공처럼 보이거나 과거 인증을 계속 노출하지 않는다.
writer와 rebuild는 같은 PostgreSQL advisory lock을 사용해 동시에 입력과 파생값을
변경하지 않는다.
`total_return_audit`는 언제나 `REPEATABLE READ, READ ONLY`이며 rebuild/action DQ run,
실제 첫·마지막 거래일의 정확한 일치, 총수익 전행 양수·비NULL, 원 가격 인증,
행별 총수익 run parity, append-only resolution의 버전·적용/제외 의미, snapshot
SHA·PIT 제외 partition·parsed row digest와 재계산한 asset identity를 함께 검사한다.
또 모든 실제 현금 적용일에 전일/당일 `adj_close/close` scale을 4자리 저장구간으로
재검산한다. scale이 바뀐 event는 exact parent 1개와 공식 support component를 요구하고
`PRE_EVENT_PRICE_SCALE`로 현금을 환산하며, stable event는 evidence를 허용하지 않는다.
첫 가격일 exclusion과 parent/child/action/body/가격 digest가 하나라도 맞지 않으면
exit code 2를 반환한다.
checksum-frozen migration 009 뒤의 migration 010은 이 증거가 없는 기존
v1/v2 `CERTIFIED` 계약을 자동으로 `BUILDING`으로
강등하며, 명시적인 전체 rebuild 전에는 다시 인증하지 않는다.

완료된 `response.*`/`manifest.json` 파티션은 재호출하지 않으므로 같은 범위로
재실행해도 이어서 진행합니다. 운영에서는 daily task와 겹치지 않도록 Scheduler를
중지한 maintenance window에서 one-off ECS task로 실행합니다. 전체 snapshot은 약
15만 파일을 동기화하고 official viewer/support family를 다시 확인하므로 수 시간이
걸릴 수 있다. structured DART 장애 시 대기열은 첫 terminal failure에 취소하고,
동시에 실행 중인 bounded worker만 종료를 기다린다. 배포 task는 120초 ECS
`stopTimeout`, 8 vCPU·48GiB memory·120GiB ephemeral storage를 사용하고 Scheduler
retry는 0회다. 이는 기존 전체 KRX rebuild envelope를 보수적으로 재사용한 값이며,
실제 digest의 최대 RSS·disk·벽시계 soak를 확인하기 전에는 Scheduler를 활성화하지
않는다. 실측 뒤 여유를 남겨 right-size한다.

로컬 `./data` 진단 및 source-scoped FMP 적재:

```bash
uv run python -m pipeline.silver.fmp_load --mode backfill --from 2015 --to 2026
uv run python -m pipeline.silver.fmp_load --mode backfill --resume <dq-run-uuid>
uv run python -m pipeline.silver.fmp_load --mode daily --date 20260713
uv run python -m pipeline.silver.fmp_load --mode commodities --from 2015 --to 2026
uv run python -m pipeline.silver.dart_extra_load \
  --total-return-actions-only --expected-coverage-end YYYY-MM-DD  # TR preview
# standalone --apply는 비활성화됨. write는 closed orchestrator만 사용한다.
```

KRX `pipeline.silver.load`의 직접 backfill/incremental CLI와 legacy
`pipeline.jobs`는 가격/action을 쓴 뒤 총수익 계약을 닫지 않으므로 비활성화됐다.
날짜별 KRX 적재는 `pipeline.daily_full`만 사용한다.

Gold schema 생성:

```bash
psql "$SILVER_DB_URL" -v ON_ERROR_STOP=1 -f sql/gold_schema.sql
```

Gold SQL은 호출자가 승인된 `factor_id`를 전달하고 transaction을 소유하는 형태입니다.
연구 후보는 allowlist·SQL hash·부호 계약까지 검증하며, OOS와 사람 승인이 끝나기 전에는
자동 배치에 연결하지 않습니다.

GitHub Actions를 쓰지 못할 때 수동 이미지 배포:

```bash
AWS_ACCOUNT_ID=<aws-account-id>
AWS_REGION=ap-northeast-2
ECR_REPOSITORY=<ecr-repository>
SOURCE_COMMIT="$(git rev-parse HEAD)"
SOURCE_REPOSITORY="$(git config --get remote.origin.url)"

test -z "$(git status --porcelain)"
test "${#SOURCE_COMMIT}" -eq 40

AWS_PROFILE=<aws-profile> aws ecr get-login-password --region ap-northeast-2 \
  | docker login --username AWS --password-stdin \
      "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

docker buildx build \
  --platform linux/amd64 \
  -f deploy/Dockerfile \
  --build-arg "SOURCE_COMMIT=${SOURCE_COMMIT}" \
  --build-arg "SOURCE_REPOSITORY=${SOURCE_REPOSITORY}" \
  -t "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY}:${SOURCE_COMMIT}" \
  --metadata-file image-metadata.json \
  --push \
  .

IMAGE_DIGEST="$(jq -r '."containerimage.digest"' image-metadata.json)"
IMAGE_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY}@${IMAGE_DIGEST}"
```

수동·자동 배포 모두 ECS task definition에는 `latest`나 commit 태그가 아니라
`IMAGE_URI`처럼 digest가 포함된 URI를 기록한다. 40자리 commit은 원격 저장소에
push된 clean tree여야 하며 이미지의 `org.opencontainers.image.revision` label에도
같은 값이 들어간다.

현재 `deploy/Dockerfile`의 `python:3.12-slim`과
`ghcr.io/astral-sh/uv:latest` upstream 참조는 mutable이다. 따라서 이미 push된
ECR digest의 실행 바이트는 불변이지만, 나중에 같은 source commit을 다시 build해
bit-for-bit 동일한 digest가 나온다고 보장하지는 않는다. 공식 upstream digest를
별도로 검증한 뒤 두 `FROM` 참조를 digest로 고정하기 전까지 이 제한을 배포
영수증에 기록한다.

## 자동 배포

자동 배포는 [.github/workflows/deploy.yml](.github/workflows/deploy.yml)에서,
수동 원자재 백필은
[.github/workflows/commodity-backfill.yml](.github/workflows/commodity-backfill.yml)에서
관리합니다.

`main` 브랜치에 push하면 다음 작업이 실행됩니다.

1. 외부 DB 접속이 필요 없는 전체 pytest를 실행합니다.
2. GitHub OIDC로 AWS deploy role을 assume합니다.
3. `linux/amd64` Docker 이미지를 빌드합니다.
4. ECR에 commit SHA 태그와 `latest` 태그를 push하고 image digest를 확인합니다.
5. ECS task definition 새 revision을 digest URI로 등록합니다.
6. EventBridge Scheduler의 현재 상태를 검증하고 새 task definition target으로
   교체한 뒤 `ENABLED`로 복원합니다. PostgreSQL certification advisory lock과
   Scheduler retry 0 정책이 이전 revision과 다음 실행의 중첩 쓰기를 차단합니다.

필요한 GitHub secret:

```text
AWS_DEPLOY_ROLE_ARN=arn:aws:iam::<aws-account-id>:role/<deploy-role-name>
```

GitHub Actions는 repo 이름을 기준으로 ECR/ECS/Scheduler 이름을 추론합니다. 실제 리소스 이름이 기본 naming convention과 다르면 아래 variables로 override합니다.

```text
ECR_REPOSITORY=<ecr-repository>
ECS_TASK_FAMILY=<ecs-task-definition-family>
CONTAINER_NAME=<ecs-container-name>
SCHEDULE_NAME=<eventbridge-scheduler-name>
FMP_API_SECRET_ARN=<fmp-api-key-secret-arn>
```

## 알림

daily task 결과 알림은 다음 흐름으로 동작합니다.

```text
ECS task STOPPED 이벤트
  -> EventBridge rule
  -> SNS topic
  -> 이메일 구독
```

메일에는 task 상태, exit code, 종료 이유, 시작/종료 시각, task ARN, task definition ARN이 포함됩니다.

```text
Exit code 0 -> 정상 종료
그 외 값 또는 exit code 없음 -> CloudWatch 로그 확인 필요
```

## Git 관리

커밋하지 않는 로컬/생성 파일:

```text
.env
data/
.venv/
__pycache__/
.DS_Store
docs_cache/
```

push 전 확인:

```bash
git status --short
uv run python -m compileall -q pipeline
uv run pytest -q
```
