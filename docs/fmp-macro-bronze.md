# FMP 한국 매크로: coverage 허용 목록 기반 Bronze 수집

`pipeline.bronze.fmp_macro`는 2015년부터의 이력을 확인한 **14개 계열**만 선별한다.
이는 history coverage 허용 목록이지 과거 최초 발표치/PIT 인증 목록이 아니다.
Silver 테이블·Gold·백테스트 입력은 만들지 않으며 DB 연결이나 migration도 필요 없다.
근거: [2026-09-19 후보 감사](fmp-korea-macro-candidates-20260919.md).

## 대상

| 국가 | 계열 | 개수 |
|---|---|---:|
| 한국 | 기준금리 결정 | 1 |
| 한국 | CPI YoY·MoM | 2 |
| 한국 | 무역수지 | 1 |
| 한국 | 산업생산 YoY·MoM | 2 |
| 한국 | 소매판매 MoM, 실업률 | 2 |
| 한국 | 소비자심리, 기업경기 | 2 |
| 한국 | PPI YoY·MoM, 경상수지 | 3 |
| 중국 | NBS 제조업 PMI | 1 |

한국 수출입 증가율, 한국 제조업 PMI, 외환보유액, M2/M3, 수출입물가,
GDP, 소매판매 YoY 등은 이번 선택 파일에서 제외한다.
미국 ISM/FOMC는 한국 중심의 이번 허용 목록에 포함하지 않았다.
`country`와 정확한 이벤트 이름을 함께 확인하며, `(Jan)`~`(Dec)`만 접미사로 허용한다.
Flash/Final, 다른 국가, 다른 PMI를 이름 일부가 같다는 이유로 합치지 않는다.

## 저장 계약

S3 루트는 `S3_BRONZE_BUCKET`, 로컬 루트는 프로젝트 `data/`다.

```text
macro/fmp/economic-calendar/korea-coverage-v1/snapshot=<id>/
  from=YYYY-MM-DD/to=YYYY-MM-DD/
    raw/response.json          # 전체 API 응답 원본, byte-for-byte 증빙
    raw/manifest.json          # 요청·수집시각·SHA256
    selected.json              # 허용 목록 14개 계열의 행만 담은 원문 projection
    selection_manifest.json    # 원문 연결·checksum·계열별 건수·null/중복 건수
  runs/from=YYYY-MM-DD/to=YYYY-MM-DD/manifest.json
```

API가 세계 캘린더를 함께 반환하므로 `raw/` 증빙에는 허용 목록 외의 국가·행도 들어 있다.
이 파일을 허용된 적재 데이터로 직접 읽으면 안 된다. 소비자는 **완료된 run manifest에 열거된
selection manifest와 selected.json만** 사용해야 한다. 허용 목록 외의 이벤트는 selected.json에 없다.
원본 증빙은 필터링·재인코딩하지 않는다.

`selected.json`의 각 항목:

```json
{
  "series_id": "KR_CPI_YOY",
  "source_row_index": 0,
  "payload": {"date": "2015-02-02 23:00:00", "country": "KR", "event": "Inflation Rate YoY (Jan)", "actual": 0.8}
}
```

위 payload는 구조 예시다. 실제로는 `previous`, `estimate`, `unit` 등 반환된 모든 필드를 그대로 보존한다.
`source_row_index`는 해당 원본 배열에서의 0-based 위치다.

- `received_at`은 API를 실제로 받은 시각이다. 재개 시 바뀌지 않는다.
- provider `date`는 원문 문자열로 유지한다. 통계 기준일·공식 발표시각·효력일로 추정 변환하지 않는다.
- 모든 manifest에 `pit_approved=false`, `publication_time_verified=false`,
  `revision_history_verified=false`, `silver_publish_allowed=false`를 남긴다.
- `complete=true`는 해당 범위의 수집/선택 파일 작성 완료이지, 경제통계/PIT 인증이 아니다.
- 값의 단위, null actual, 중복 이벤트, estimate/previous는 수정·보간·제거하지 않는다.
  null은 건수로 따로 기록하고, 중복은 시계열+제공 시각 기준으로 집계한다.
  null actual을 실제 발표값으로 사용하거나 estimate를 검증된 과거 컨센서스로 간주하면 안 된다.
- 한국 금리 결정의 시각 보정과 중복 제거, 무역수지의 통화 단위 보정 등은 후속 검증 단계의 작업이다.

## 실행

환경변수로 `FMP_API_KEY`를 설정한다. S3에는 `S3_BRONZE_BUCKET`와 기존 AWS 자격증명 체인
(환경변수/프로필/ECS task role)을 사용한다. 키는 코드·요청 URL·manifest에 넣지 않는다.

프로젝트 디렉터리에서:

```sh
# 2015년부터 S3 Bronze 백필 (새 snapshot ID를 명시하는 것을 권장)
uv run python -m pipeline.bronze.fmp_macro \
  --start 2015-01-01 --end 2026-09-18 --dest s3 --snapshot backfill-20260919

# 2015년 시작 시점의 알려진 상태를 준비하려면 --start 2014-12-01
# 같은 명령을 다시 실행하면 완료된 원문을 검증하고 API 재호출 없이 재개

# 로컬 검증
uv run python -m pipeline.bronze.fmp_macro \
  --start 2026-09-01 --end 2026-09-18 --dest local --snapshot local-check-20260919

# 한국 처리일 기준 일일 수집
uv run python -m pipeline.bronze.fmp_macro --day 20260919 --dest s3
```

`--end`는 필수이며 미래 UTC 날짜는 허용하지 않는다. 날짜 범위는 2014-12-01 이후만 허용한다.
`--snapshot` 생략 시 `backfill-v1`로 고정되므로, 최신 수정값을 다시 받으려면 **새 ID**를 사용한다.
일일 모드의 snapshot은 `daily-YYYYMMDD`다. 당일 재실행도 동일 receipt를 재사용한다.
같은 날 다시 조회할 필요가 있으면 날짜 범위 모드에서 별도의 snapshot ID로 실행한다.

## 재개·수정 이력·오류

- 월별 요청, 기존 FMP 429/5xx exponential backoff·Retry-After 재사용, 요청 간 최소 0.4초.
- 캘린더가 기존 4,000행 안전 한계에 도달하면 날짜 범위를 이분할한다.
  부모 응답은 증빙으로만 남고 선택 건수는 자식 구간에서만 계산한다. 하루 구간도 한계면 실패한다.
- 원문 및 선택 파일은 payload 먼저, checksum manifest 마지막으로 작성한다.
  원문 수집 후 중단돼도 선택 파일을 API 재호출 없이 재생성할 수 있다.
- 기존 완료 객체가 손상되거나 같은 ID의 선택 계약이 달라지면 덮어쓰지 않고 실패한다.
  조사 후 새 snapshot을 사용한다. 허용 목록 계약을 바꿀 때는 contract version도 올린다.
- JSON 오류 객체, 비정상 행, 잘못된 날짜/범위, 선택 행의 비정상 숫자는 실패한다.
- 28일 이상 구간에 허용된 이벤트가 하나도 없으면 실패한다.
  63일 이상 전체 요청에서 14개 중 어떤 계열이든 actual이 전혀 없으면 완료 run manifest를 만들지 않는다.
  이는 소스 장애/이름 변경 탐지이며 대상월 전체 coverage를 매번 인증하는 검사는 아니다.
- 실패 전 생성된 raw 및 개별 파티션은 재개용으로 남는다. run manifest가 없으면 전체 범위 완료로 보지 않는다.

## 일일 파이프라인 연결

`daily_full._run_fmp_incremental`은 주식 배치 인증 여부와 관계없이 이 Bronze 수집기를 호출한다.
주식의 미국 직전 평일이 아니라 **한국 처리일 전날의 UTC 날짜**까지 최근 93일을 다시 조회한다.
08:30 KST 실행에서 당일 08:00 KST 발표는 전날 UTC에 해당하므로 조회 범위에 들어간다.
범위 마지막 UTC 날짜는 실행 시 아직 진행 중일 수 있으며 미발표 null 이벤트도 원문대로 남긴다.
실제 전달 지연이 있는 행은 다음 날 겹침 수집으로 포착한다. 발표 지연을 임의로 보정하지 않는다.
매일 새 snapshot에 저장하므로 과거 수집본을 덮어쓰지 않는다.

기존 시장가격/금리 수집과 주식 Silver 작업은 기존 경로로 계속 동작하지만,
**새 매크로 선택 파일은 Silver 로더에 전달하지 않는다.** 새 모듈에는 `--apply` 옵션이 없다.

## 검증

- 허용 목록·제외 계열·null/중복 보존·불변 체크포인트·손상·빈 응답·행수 분할·실패 전파 테스트.
- 모의 S3 PUT/GET 및 429 재시도에서 키가 URL/객체에 포함되지 않는지 확인.
- 2014-12~2026-09 감사 원문 142개를 실제 receipt 시각 그대로 재생하여 로컬 선택본 검증.
  선택 1,948행(14개 계열), 이 중 actual=null 1행을 원문대로 보존했다.
  이 수치는 2014년 초기 상태와 중복을 포함하며, 독립적인 월간 관측 수와 다르다.
- 2026-09-01~18은 API를 새로 호출해 선택 7행을 로컬에 저장하고 원문/선택 checksum을 검증했다.
- `tests/bronze`, daily full 및 기존 regime/실제 PostgreSQL 테스트: 245개 통과.
- 2026-09-20 KST 운영 S3 실제 적재와 독립 재조회 검증을 완료했다(아래 실행 기록).
  일일 자동 실행을 위한 운영 코드 배포는 별도로 수행하지 않았다.

## 운영 S3 실제 적재 기록 — 2026-09-20 KST

| 항목 | 결과 |
|---|---|
| 조회 범위 (provider UTC date) | 2015-01-01 ~ 2026-09-18 |
| 스냅샷 | `backfill-20260920` |
| 선별 범위 | 한국 13개 계열 + 중국 NBS 제조업 PMI 1개 |
| 월별 파티션 | 141개, 조회 구간 공백 없음 |
| 선별 행 | 1,932행 (actual 비어 있지 않은 행 1,931개) |
| S3 객체 | 565개, 합계 72,842,777 bytes |
| API 호출 / 재시도 / 429 | 141 / 0 / 0 |
| 검증 완료 | 2026-09-20 00:32:33 KST |

운영 버킷은 ECS 설정으로 확인한 `soma-quant-bronze-31-159372032315-ap-northeast-2-an`이다.
새 스냅샷이 비어 있음을 확인하고 적재했으며 기존 스냅샷은 변경하지 않았다.
완료 run manifest:

```text
s3://soma-quant-bronze-31-159372032315-ap-northeast-2-an/macro/fmp/economic-calendar/korea-coverage-v1/snapshot=backfill-20260920/runs/from=2015-01-01/to=2026-09-18/manifest.json
```

| 계열 | 선별 행 | actual=null |
|---|---:|---:|
| KR_POLICY_RATE | 105 | 0 |
| KR_CPI_YOY | 140 | 0 |
| KR_CPI_MOM | 140 | 0 |
| KR_TRADE_BALANCE | 141 | 0 |
| KR_INDUSTRIAL_PRODUCTION_YOY | 141 | 1 |
| KR_INDUSTRIAL_PRODUCTION_MOM | 140 | 0 |
| KR_RETAIL_SALES_MOM | 141 | 0 |
| KR_UNEMPLOYMENT_RATE | 141 | 0 |
| KR_CONSUMER_CONFIDENCE | 140 | 0 |
| KR_BUSINESS_CONFIDENCE | 140 | 0 |
| KR_PPI_YOY | 141 | 0 |
| KR_PPI_MOM | 141 | 0 |
| KR_CURRENT_ACCOUNT | 140 | 0 |
| CN_NBS_MANUFACTURING_PMI | 141 | 0 |

S3를 다시 읽어 282개 원본·선별 payload의 SHA256/byte length, 전체 565개 객체 목록·크기,
모든 선별 행의 원본 인덱스·payload 일치, 독립적인 허용 목록 재선별 결과, 구간 연결 및
파티션/전체 집계 일치를 검증했다. 검증 시에는 FMP를 재호출하거나 S3를 수정하지 않았다.
전체 원본 증빙에는 세계 캘린더 254,037행이 있으며, 이는 적재 허용 목록의 관측 수가 아니다.

원문 보존 사항:

- actual=null 1행: 산업생산 YoY, provider date `2026-03-30 23:00:00`.
- 동일 계열·provider timestamp 중복의 초과 행은 3개: 위 산업생산 1개 및
  기준금리 `2026-04-10 01:00:00`, `2026-07-16 01:00:00` 각각 1개.
- 위 건수는 정제된 독립 관측 수가 아니다. null과 중복을 삭제하거나 보간하지 않았다.
- 조회 범위는 통계 대상월이나 검증된 발표일 범위를 의미하지 않는다.
  이번 스냅샷에서 CPI·경상수지의 첫 provider date는 2015년 2월이다.
  2015년 시작 시점의 알려진 상태를 만들기 위한 2014년 12월 seed는 이번 적재에 포함하지 않았다.

검증 증빙은 로컬 `data/audits/fmp-macro-s3-20260920/load_result.json` 및
`data/audits/fmp-macro-s3-20260920/verification.json`에 보관했다.
실행 중 수집기 테스트 28개도 다시 통과했다.
**Silver/Gold 적재와 운영 배포는 수행하지 않았다.** `pit_approved=false` 등 모든 보호 플래그를
그대로 유지했으며, 이 적재 완료는 발표일·최초 발표치·수정 이력 인증을 뜻하지 않는다.
