# FMP 대만·변동성·미국 금리 포지션 Bronze

`pipeline.bronze.fmp_external --bundle risk`는 coverage가 확인된 추가 후보 중
우선순위 1~3의 9개 계열만 수집한다. 기존 `core` 14개 및 한국 매크로 원본은 그대로 둔다.
요청 범위는 `2015-01-01~2026-09-18`이다. 아래 첫 날짜는 조회 범위 안의 첫 관측이지
FMP 전체 서비스의 최초 제공일이 아니다.

| 계열 | 종류 | 최초 관측 | 최신 관측 | 행 수 |
|---|---|---|---|---:|
| EWT | 대만 주식 ETF 일봉 | 2015-01-02 | 2026-09-18 | 2,945 |
| USDTWD | 달러/대만달러 일봉 | 2015-01-01 | 2026-09-18 | 3,135 |
| ^VVIX | 변동성 지수 일봉 | 2015-01-02 | 2026-09-18 | 2,946 |
| ^VIX3M | 변동성 지수 일봉 | 2015-01-02 | 2026-09-18 | 2,945 |
| ^VIX9D | 변동성 지수 일봉 | 2015-01-02 | 2026-09-18 | 2,945 |
| ZT | 2년 국채 선물 COT | 2015-01-06 | 2026-09-15 | 611 |
| ZN | 10년 국채 선물 COT | 2015-01-06 | 2026-09-15 | 611 |
| ZB | 30년 국채 선물 COT | 2015-01-06 | 2026-09-15 | 611 |
| ZQ | 연방기금 선물 COT | 2015-01-06 | 2026-09-15 | 611 |
| 합계 | 9개 | | | **17,360** |

## PIT: 확보와 연구 사용 승인을 분리

- FMP 응답 bytes를 그대로 보존하고 각 요청의 `received_at` 및 SHA256을 기록한다.
- `provider_date`와 `observation_date`는 제공자가 붙인 기준 날짜다. 발표시각이 아니다.
- COT만 `reference_date`에 포지션 기준일을 기록한다. 화요일/월요일 날짜를 바꾸거나
  일괄 `기준일 + 3일`로 발표일을 생성하지 않는다.
- `observed_at = system_known_at = 실제 API received_at`이다. 2015년 자료를 지금 받았다는
  사실을 유지하며, S3 업로드 시각으로 교체하거나 과거로 소급하지 않는다.
- 공식 과거 공개시각·수정 이력이 미검증이므로 `released_at`, `available_at`, `vintage`는 null.
- 모든 관측·파티션·run에 `pit_approved=false`, `publication_time_verified=false`,
  `revision_history_verified=false`, `silver_publish_allowed=false`,
  `historical_backtest_allowed=false`를 저장한다.
- COT report type이나 `noncomm`를 managed money/leveraged funds로 추측하지 않는다.
  CFTC contract code는 ZT `042601`, ZN `043602`, ZB `020601`, ZQ `045601`과 대조한다.
- ETF·지수·FX의 세션 마감시각, 한국 판단시점 정렬, 가격 조정/정정 이력은 추가 검증 대상이다.
  거래 날짜만으로 한국 당일 시가에서 사용할 수 있다고 승인하지 않는다.
- `complete=true`는 Bronze 수집 완료다. 역사적 PIT 인증·Silver/Gold 게시가 아니다.

이는 [기존 외부 Bronze 계약](fmp-external-bronze.md)과 같은 보호 수준이다.
원본 확보 후 별도의 시점 검증 없이는 과거 레짐 백테스트에 투입하지 않는다.

## 재개·운영 코드

```text
regime/fmp-external/korea-risk-v1/snapshot=backfill-20260920/
  series=<symbol>/from=<year-start>/to=<year-end>/
    raw/response.json
    raw/manifest.json
    observations.json
    manifest.json
  runs/from=2015-01-01/to=2026-09-18/manifest.json
```

```sh
uv run python -m pipeline.bronze.fmp_external --bundle risk \
  --start 2015-01-01 --end 2026-09-18 --snapshot backfill-20260920 --dest local

uv run python -m pipeline.bronze.fmp_external --bundle risk \
  --start 2015-01-01 --end 2026-09-18 --snapshot backfill-20260920 --dest s3 \
  --cache-root /absolute/path/to/TeamAlpha-data/data
```

API key는 `FMP_API_KEY` 환경변수에서 읽고 header 인증한다. key를 URL·원문 manifest에 기록하지 않는다.
연도·계열별 체크포인트를 사용하고 원문 hash/params/수집시각을 재검증한 뒤 재개한다.
완료된 다른 bytes를 덮어쓰지 않으며 변경값 재관측은 새 snapshot에서만 한다.
가격 API는 `historical-price-eod/full`, COT는 `commitment-of-traders-report`다.
최소 요청 간격은 0.5초이며 기존 429/5xx exponential backoff를 유지한다.

`run_daily`는 core와 risk를 각각 별도 버전 경로에 저장하고, 어느 쪽이든 실패하면 전파한다.
최근 120일 중첩 재관측으로 관측 후 정정을 보존한다. 그보다 오래된 정정은 별도 백필이 필요하다.
**일일 연결은 코드에 반영했지만 운영 배포는 이번 작업 범위에 포함하지 않는다.**

## 2026-09-20 실행·검증

- 로컬 수집: 9개 계열, 108개 연도·계열 파티션, 433개 객체, 17,360행.
- 실제 FMP 요청 108회, 429·재시도 0회. 이전 전체 구간 탐색과 계열별 행 수가 일치했다.
- 141개월 각각에 데이터가 있고 날짜 중복은 없다. 이는 거래소 세션별 100% coverage 인증은 아니다.
- USDTWD의 주말 날짜 78행은 원본 유지 및 `provider_weekend_date` 플래그 처리했다.
  검사 대상 OHLC null·0 이하·가격 관계 이상과 COT 핵심 5개 필드 null 플래그는 없다.
- 원본·관측 payload 216개의 SHA256/bytes와 전 행의 원문·source index·수집시각·PIT 차단 검증 통과.
- 실제 API 수집시각: 2026-09-20 21:25:24~21:26:18 KST. 발표일/가용시각으로 오해하지 않는다.
- 관련 테스트 138개 통과(기존 외부 Bronze, 매크로, 레짐, 일일 처리, FMP 원본/백필 포함).
- 운영 S3 승격 및 독립 재조회까지 완료했다(아래).

증빙 디렉터리: `data/audits/fmp-risk-load-20260920/`.
`local_result.json`, `local_api_metrics.json`, `local_verification.json`에 수집 및 검증을 기록했다.
전수 검증 스크립트 `verify_load.py`는 원본/관측 payload hash, 전 행, 정확한 객체 목록·크기를 대조한다.
운영 버킷으로 승격할 때는 S3 관측·원문과 로컬의 byte equality 및 원래 receipt 시각도 확인한다.

## 운영 S3 적재 완료 — 2026-09-20 21:29 KST

| 항목 | 결과 |
|---|---|
| 계열 / 행 | 9개 / 17,360행 |
| 파티션 / 객체 | 108개 / 433개 |
| 저장 크기 | 37,348,319 bytes |
| S3 승격 중 FMP 호출 | 0회 — cache-only 클라이언트로 재호출 차단 |
| 원본·관측 payload 해시 | 216개 전수 통과 |
| S3와 로컬 원본·관측 bytes | 전부 일치 |
| 원문 행·source index·receipt 시각 | 17,360행 전수 일치 |
| 미검증 과거 발표시각/가용시각이 채워진 행 | 0행 |
| 역사적 PIT 승인 / Silver·백테스트 승인 | 0행 |

```text
s3://soma-quant-bronze-31-159372032315-ap-northeast-2-an/regime/fmp-external/korea-risk-v1/snapshot=backfill-20260920/runs/from=2015-01-01/to=2026-09-18/manifest.json
```

계정·버킷 소유자와 비어 있는 신규 snapshot 경로를 확인한 후 적재했다.
기존 한국 매크로와 core 외부 레짐의 완료 manifest hash도 전후 동일함을 확인했다.
기존 core 계약과 경로는 바꾸지 않았다. 수집 당시 수신시각은 S3 업로드 시각으로 바꾸지 않았다.

증빙: `s3_result.json`, `s3_api_metrics.json`, `s3_verification.json`,
`protected_manifests_verification.json` (위 감사 디렉터리).
**이번 완료는 Bronze 보유 완료다. PIT 인증, Silver/Gold 적재, 일일 운영 배포는 수행하지 않았다.**
