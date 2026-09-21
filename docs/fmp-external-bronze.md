# 한국 레짐 보완용 FMP FX·COT·ETF Bronze

`pipeline.bronze.fmp_external`의 기본 `core` 묶음은 [실측 후보 조사](fmp-additional-bronze-candidates-20260920.md)의
우선순위 1~3에 해당하는 14개 계열을 수집한다. **원본 확보 전용이며 역사적 PIT 인증은 아니다.**
기존 한국 매크로 14개 계열 및 `pipeline.fmp_regime` 시장 입력과 별도 경로다.
추가 `risk` 묶음 9개는 [대만·변동성·금리 포지션 Bronze 안내](fmp-risk-bronze.md)를 참고한다.

| 종류 | 계열 |
|---|---|
| FX 5개 | USDCNH, USDCNY, USDJPY, AUDUSD, EURUSD |
| COT 6개 | HG, CL, DX, J6, GC, VX |
| ETF 3개 | EWY, EEM, FXI |

위 `core` 14개 범위에는 VVIX·VIX3M·항셍·닛케이225가 없다.
VVIX·VIX3M·VIX9D는 별도 `risk` 묶음에 포함하며, 항셍·닛케이225는 여전히 조사 후보이다.

## PIT 및 데이터 보존 계약

- `raw/response.json`은 FMP 응답 bytes 그대로 보존한다. 정렬·반올림·주말 제거·결측 보간을 하지 않는다.
- `raw/manifest.json`에는 요청 endpoint/params, 실제 API `received_at`, 원문 SHA256을 남긴다.
- `observations.json`은 `payload` 원문과 0-based `source_row_index`를 유지하면서 다음을 분리한다.

| 필드 | 의미 |
|---|---|
| `provider_date` | 원문 날짜 문자열 |
| `observation_date` | 제공 날짜의 일자 부분. 공개일로 사용 금지 |
| `reference_date` | COT 보유 기준일에만 채움. FX/ETF에는 null |
| `released_at` | 공식 과거 발표시각 미검증이므로 null |
| `available_at` | 역사적 연구 입력 가능시각 미검증이므로 null |
| `observed_at`, `system_known_at` | 지금 실제 API를 받은 시각. 과거 날짜로 소급하지 않음 |
| `vintage` | 최초 발표/수정 버전을 인증할 수 없으므로 null |
| `payload_sha256` | 원문 행의 canonical JSON hash. 값 수정 감지용이며 PIT 인증 아님 |

모든 관측행·파티션·run에 `pit_approved=false`, `publication_time_verified=false`,
`revision_history_verified=false`, `silver_publish_allowed=false`, `historical_backtest_allowed=false`를 남긴다.
`complete=true`는 원본과 projection의 수집 완료를 의미할 뿐 연구 사용 승인이 아니다.
Silver/Gold 로더, DB 연결, migration, `--apply` 옵션은 없다.

COT의 날짜는 공개일이 아니라 보유 기준일이다. 일반적인 화요일 기준/금요일 발표 규칙을
일괄 적용하지 않고 **과거 released_at을 비워 둔다**. 공휴일·지연 공표·정정 예외를 포함한
공식 발표 이력을 검증한 별도 단계만 이를 채울 수 있다.
[CFTC 기준일과 발표일 설명](https://www.cftc.gov/MarketReports/CommitmentsofTraders/AbouttheCOTReports/cot_about.html).
FMP `noncomm`를 managed-money/leveraged-funds 분류로 바꾸지 않으며, report type도 추정하지 않는다.

FX의 날짜만으로 UTC/뉴욕 마감시각을 가정하지 않는다. ETF도 분할·배당 조정 및 세션 종료시각
검증 전에는 한국 과거 시가에 연결하지 않는다. 현재 수신시각 이후의 시스템 관측 이력과
과거 공개 정보를 재구성하는 작업을 구분한다.

## 저장·재개

```text
regime/fmp-external/korea-external-v1/snapshot=<id>/
  series=<symbol>/from=YYYY-MM-DD/to=YYYY-MM-DD/
    raw/response.json
    raw/manifest.json
    observations.json
    manifest.json
  runs/from=YYYY-MM-DD/to=YYYY-MM-DD/manifest.json
```

- 연도별·심볼별 작은 구간으로 요청한다. 가격은 연간 최대 366개 날짜, COT는 주간이다.
- 기존 FMP 클라이언트의 header 환경변수 인증, 429/5xx backoff를 사용한다. 기본 최소 간격 0.5초.
- 완료 raw receipt는 매번 SHA256·bytes·endpoint·params·타임존 있는 실제 수집시각을 확인한다.
- 이미 완료된 다른 bytes는 덮어쓰지 않는다. projection/manifest 중간 실패는 동일 raw로 재개한다.
- raw payload만 있고 receipt manifest가 없는 경우 수집시각을 만들어 내지 않고 실패한다.
  조사 후 새 snapshot을 쓰거나 검증된 `--cache-root`의 동일 bytes/receipt로 복구한다.
- `--cache-root`는 같은 상대 경로의 원문/manifest를 검증해 목적지로 복사한다.
  `received_at`을 업로드 시각으로 바꾸지 않고 `copied_from`과 원본 manifest hash를 기록한다.
- API 오류 객체, 잘못된 심볼·CFTC contract code·날짜, 날짜 중복, 비정상 숫자는 실패한다.
  온전히 요청한 달이 통째로 비면 성공 완료 처리하지 않는다.
- null 핵심 값·비정상 OHLC·주말 날짜는 원문을 유지하고 `quality_flags`로 표시한다.
  플래그가 없다는 사실도 완전성/PIT 인증은 아니다.

## 실행 및 일일 연결

`FMP_API_KEY`를 환경변수로 설정한다. S3에는 `S3_BRONZE_BUCKET`와 기존 AWS 자격증명 체인을 사용한다.

```sh
# 로컬 백필
uv run python -m pipeline.bronze.fmp_external \
  --start 2015-01-01 --end 2026-09-18 --snapshot backfill-20260920 --dest local

# 검증된 로컬 원본을 S3로 승격: 같은 snapshot/범위는 API 재호출 없이 복사
uv run python -m pipeline.bronze.fmp_external \
  --start 2015-01-01 --end 2026-09-18 --snapshot backfill-20260920 --dest s3 \
  --cache-root /absolute/path/to/TeamAlpha-data/data
```

새 값을 다시 받아야 하면 새 snapshot ID를 사용한다. 같은 ID의 재실행은 수정값 재조회가 아니다.
`daily_full._run_fmp_incremental`에 `fmp_external.run_daily(krx_day)`를 연결했다.
현재 코드는 `core` 14개와 별도 버전의 `risk` 9개를 각각 수집한다.
기존 `korea-external-v1` 계약·저장 경로는 변경하지 않는다.
한국 처리일 전날 UTC 날짜까지 최근 120일을 매일 새 snapshot에 재관측한다.
당일 진행 중인 UTC 데이터가 포함될 수 있으므로 historical PIT 허용 플래그는 항상 false다.
120일보다 오래된 정정이나 과거 지연 공표를 모두 포착한다는 보장은 없으며 필요하면 별도 백필한다.
기존 주식 배치 인증 여부와 무관하게 실행하고 실패를 전파한다. **운영 배포는 별도이며 미실행이다.**

## 2026-09-20 실행 기록

- 실제 FMP API 168회 호출, 429·재시도 0회.
- 로컬 Bronze `2015-01-01~2026-09-18`, 14개 계열, 168개 파티션, **28,191행** 수집 완료.
- FX 15,690행, COT 3,666행, ETF 8,835행.
- 주말 FX 날짜 433행을 원문 그대로 보존했다. 그 외 검사 대상 품질 플래그는 0개.
- 모든 행의 과거 `released_at`/`available_at`/`vintage`는 null이며 PIT 승인 행은 0개다.
- 관련 회귀 테스트 292개 통과. 신규 모듈 테스트 47개 포함.
- 실행 증빙: `data/audits/fmp-external-load-20260920/local_result.json`.
- 독립 재조회 검증: 총 673개 파일, 원본·관측 payload 336개 SHA256/bytes 및
  전 행의 원문 일치·source index·실제 수집시각·PIT 차단 필드 확인 완료.
  결과는 `data/audits/fmp-external-load-20260920/local_verification.json`에 보관했다.
- AWS 재로그인 후 **운영 S3 승격 및 독립 재조회 검증까지 완료했다**(아래).

## 운영 S3 승격 완료 — 2026-09-20 14:22 KST

| 항목 | 검증 결과 |
|---|---|
| 조회 범위 | 2015-01-01 ~ 2026-09-18 |
| 계열 / 행 | 14개 / 28,191행 |
| FX / COT / ETF | 15,690 / 3,666 / 8,835행 |
| S3 파티션 / 객체 | 168개 / 673개 |
| 총 크기 | 58,438,579 bytes |
| 승격 중 FMP 호출 | 0회 — 로컬 원본만 사용 |
| SHA256·byte length 전수 검사 | 원본·관측 payload 336개 통과 |
| 로컬 원본·관측 파일과 S3 비교 | 모든 bytes 일치 |
| 원래 API 수집시각 | 2026-09-20 11:00:25 ~ 11:01:56 KST 그대로 유지 |
| 과거 발표시각·사용 가능시각이 채워진 행 | 0개 |
| 역사적 PIT 승인 행 | 0개 |

운영 ECS 설정의 버킷과 소유 계정을 확인하고 비어 있는 신규 스냅샷에 저장했다.
API 재호출이 발생하려 하면 실패하는 cache-only 클라이언트로 실행했다.
원래 수집시각은 업로드 시각으로 바꾸지 않았고, source manifest hash와 `copied_from`도 대조했다.
완료 run manifest:

```text
s3://soma-quant-bronze-31-159372032315-ap-northeast-2-an/regime/fmp-external/korea-external-v1/snapshot=backfill-20260920/runs/from=2015-01-01/to=2026-09-18/manifest.json
```

S3의 정확한 673개 객체 목록·크기, 파티션/전체 건수, 모든 행의 원문·source index·실제 수집시각,
PIT 보호 플래그 및 로컬 원문/관측 파일과의 byte 일치를 독립적으로 확인했다.
증빙: `data/audits/fmp-external-load-20260920/s3_result.json`,
`data/audits/fmp-external-load-20260920/s3_verification.json`.
새 수집기 테스트 47개도 재실행해 모두 통과했다.

**이 완료는 원본의 S3 보관 완료이며 과거 PIT 인증 완료가 아니다.**
Silver/Gold 적재 및 일일 수집 운영 배포는 수행하지 않았다.
