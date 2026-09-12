# KIS 수급·공매도 적재

구현 상태: 코드와 단위 테스트. 운영 migration, 수집, 배포 및 스케줄 활성화는 별도 실행 단계이다.

## 범위

2015-01-01 이후 factor-research 월별 패널의 `in_universe=True`였던 모든 asset_id의 합집합을 사용한다. 현재 제외·상장폐지 종목도 유지한다. 해당 자산의 요청 기간 전체를 조회하며 편입 기간으로 데이터를 자르지 않는다. 날짜별 ticker는 RDS asset_identifier로 연결한다. 예상 날짜는 XKRX 거래일 달력과 RDS asset의 상장·폐지 기간으로 독립 생성한다. 각 예상 날짜에 가격·CERTIFIED 인증·유일한 ticker가 모두 있어야 한다. 하나라도 빠지면 API 호출 전 중단한다. RDS에 없는 자산, 상장일 미상, 중복 식별자도 오류다. 거래량 0인 날짜도 포함한다. 기간 내 거래일이 없거나 상장 전·폐지 후인 자산만 정상 제외되며 assets_without_expected_dates에 기록한다. 상장·폐지 메타데이터와 거래일 달력 자체의 정확성은 별도의 원천 계약이다.

- 수급: 개인·기관·외국인의 매수/매도/순매수 수량 및 금액. 금액은 백만원에서 원으로 변환.
- 시장: KRX(J), 2025-03-04 이후 NXT(NX), 통합(UN). UN 응답과 J+NX를 대조한다.
- NXT는 manifest의 nxt_intervals에 종목별·날짜별 적격 여부와 evidence를 지정한다. ELIGIBLE 기간만 NX/UN을 조회한다. INELIGIBLE 기간에는 NX를 호출하지 않으며 통합 수급은 KRX 원본으로 구성하고 _derivation 및 _market_evidence를 기록한다. 이력이 없거나 중첩되면 API 호출 전 중단한다. 미제공 응답을 비대상 또는 0으로 간주하지 않는다.
- 공매도: 운영 수집은 J 요청의 KRX 자료를 사용한다. 비율의 분모는 일별 차트 API의 FID_ORG_ADJ_PRC=1 원거래량. 제공 비율·거래량도 별도 보존한다. short_market, volume_market, volume_adjustment, market_scope_evidence를 payload에 기록한다. 시장 범위 근거가 확인된 정책 구간만 비율을 계산하고 나머지는 NULL/MARKET_SCOPE_UNVERIFIED로 저장한다.

## 보관과 연구 사용

원본 응답과 수집 영수증은 S3 Bronze에 저장한다. 수량/금액, 날짜 커버리지, 자산 식별, 통합 대조 검사를 통과한 파티션만 RDS kis_market_observation에 원자적으로 게시한다. 실패한 데이터도 Bronze에는 남는다. CERTIFIED는 이 입력 검사 통과를 의미하며 팩터 연구 승인이나 과거 전체에 대한 KRX 외부 대조 인증을 뜻하지 않는다.

trade_date, 실제 first_observed_at, 정책상 research_available_at을 분리한다. 제공자가 실제 공개한 시각은 알 수 없어 provider_available_at=NULL이다. 예시 정책은 다음 달력일 08:30 KST이며 공급자의 공개 보장이 아니다. 과거 소급 수집은 historical_revision_risk=true이다. kis_market_latest는 사후 분석용, kis_market_asof(timestamp)는 실제 관측 전 데이터를 숨기는 엄격한 시점 조회다. 따라서 새로 수집한 2015년 자료가 엄격한 2015년 시점 조회에 나타나지 않는 것은 의도된 동작이다.

정정 값은 새 버전으로 추가한다. 같은 값 재수집은 중복 삽입하지 않는다. 90일 단위 파티션 완료 체크포인트는 Silver 성공 후 기록한다. --refresh는 체크포인트를 무시하고 정정을 확인한다.

## 실행 준비

1. factor-research에서 사용하는 전체 월별 패널을 CSV로 내보낸다. 필수 열: asset_id, Code, trade_date, in_universe. 최신 월만 내보내면 안 된다.
2. 역사적 종목 manifest를 만든다:

```sh
python -m pipeline.kis_flows export-universe --monthly-csv /path/monthly.csv --dest /path/universe.json
```

3. 아래 예시 정책을 저장한다. 확인되지 않은 시장 범위를 KRX로 임의 변경하지 않는다. KRX로 설정할 경우 short_market_evidence, short_market_verified_from, short_market_verified_through가 필수다. 검증 기간은 양 끝 날짜를 포함하며 기간 밖은 비율을 NULL로 유지한다.

```json
{"version":"kis-assumed-next-day-0830-v1","availability_lag_calendar_days":1,"availability_hour_kst":8,"availability_minute_kst":30,"short_market":"UNKNOWN"}
```

4. 기존 방식으로 RDS 연결 설정, KIS_APP_KEY/KIS_APP_SECRET, S3 쓰기 권한을 준비한다. 키는 파일/로그/커밋에 포함하지 않는다.
5. 병합 전에 승인된 점검 시간에 이 브랜치에서 `python -m pipeline.silver_quality.migrate`를 실행해 015까지 적용한다. 기존 daily_full도 시작할 때 모든 migration을 검사하므로, 015를 적용하지 않고 main에 병합하면 KIS 비활성 상태에서도 기존 일일 작업이 중단된다. 수집기와 배포 workflow는 migration을 자동 실행하지 않는다.
6. 완료된 날짜까지만 소규모 구간부터 실행한다. --publish 생략 시 원본 수집·검사만 하고 Silver에는 쓰지 않는다.

```sh
python -m pipeline.kis_flows backfill --manifest /path/universe.json --policy /path/policy.json --root s3://BUCKET/bronze --start 2015-01-01 --end 2015-01-31
```

검토 후 동일 명령에 --publish를 추가한다. 전체 backfill은 end를 마지막 완료 거래일까지 확장한다. 실행 요약과 failures를 확인해야 하며 실패가 있으면 CLI는 종료 코드 1을 반환한다. API 제한을 고려해 기본 요청 간격은 1.05초이며 제한/일시적 네트워크 오류를 재시도한다.

## 일일 자동화 연결

기존 daily_full의 정상 완료 및 이미 인증된 날의 재실행 경로에 연결했다. 기본 비활성화다. 기존 ECS/EventBridge 일정에서 다음 환경변수를 설정한 배포 후 동작한다:

- KIS_FLOWS_ENABLED=1
- KIS_UNIVERSE_URI: 전체 역사적 종목 manifest의 로컬/S3 경로
- KIS_POLICY_URI: 정책 JSON 경로
- KIS_BRONZE_ROOT: 원본 S3 루트
- KIS_APP_KEY / KIS_APP_SECRET: 기존 비밀 주입 경로 이용

달력 기준 최근 5거래일을 재조회한다. manifest가 40일 이상 오래되면 중단하므로 factor-research 월별 패널 갱신 시 전체 역사 manifest도 재생성해야 한다. NXT 미제공/미확인 구간을 포함한 실패가 하나라도 있으면 일일 작업을 성공으로 보고하지 않는다. 활성화 전 실제 소규모 API/RDS 통합 검증, 시장별 적격 이력 및 공매도 시장 범위 확인이 필요하다. 이 코드 변경으로 운영 일정이나 비밀 설정은 바뀌지 않는다.

## NXT 적용 이력 입력

전체 수집의 기본 venues는 J/NX/UN이다. KRX만 명시적으로 수집할 때는 정책에 `"venues":["J"]`를 지정하며, 이를 통합 수집 완료로 간주하지 않는다.

검증한 NXT 종목 적용 이력을 JSON 배열로 만들고 export-universe에 `--nxt-intervals /path/nxt-intervals.json`을 추가한다. 이 파일은 manifest 해시에 포함된다. start/end는 양 끝 날짜를 포함하고 각 자산·거래일에는 정확히 하나의 구간이 대응해야 한다. 아래는 형식 예시이며 실제 종목의 적격 여부를 주장하는 데이터가 아니다.

```json
[{"asset_id":123,"start":"2025-03-04","end":"2025-03-07","status":"INELIGIBLE","evidence":"s3://BUCKET/verified-nxt-eligibility/source.json"}]
```

운영 준비용 manifest에는 공식 자료로 대조한 NXT 이력을 포함했다. 검증 종료일 뒤의 수집에는 새로운 공식 자료로 manifest를 갱신해야 한다. 종목 목록에 없다는 이유만으로 INELIGIBLE 이력을 생성하면 안 된다. 이 수정에는 추가 RDS migration이 없다.

## 공식 NXT 이력 대조

`pipeline.nxt_eligibility.reconcile`은 출범일의 10종목 목록과 편입 이벤트가 일치하는지 확인하고, **하루씩 조회한** 변동내역과 일별 상태를 대조한다. 넓은 기간 조회는 종목의 이전 변동을 누락하는 사례가 확인되어 이력 원본으로 사용하지 않는다.

- 일별 허용 상태는 그 날짜의 직접 근거로 사용하며, 변동내역의 재편입 누락은 별도 집계한다.
- 거래 제한 상태와 공식 거래량 0이 함께 확인된 날은 KRX-only 통합 계산의 근거로 기록한다. 일부 시장만 허용된 날은 ELIGIBLE을 유지하며 session_state로 구분한다.
- 이벤트상 편입인데 일별 목록이 없으면 중단한다. 상장폐지 등 별도의 공식 제외 근거가 있을 때만 해소한다.
- 거래일 달력의 특별 휴장일은 정책의 calendar_exclusions에 date와 evidence를 명시한다. 달력에서 빠지지 않은 2026-06-03/2026-07-17에 공식 휴장 근거를 적용했다.
- 2025-03-04~2026-09-10, 연구 종목 2,816개, 374거래일을 대조한 4,866개 적용 구간을 운영 S3에 준비했다. 아직 확정하지 않은 당일 자료는 제외했다. 종목 합집합 자체의 기준일은 기존 패널의 2026-08-10을 유지하며, 최근 날짜로 허위 갱신하지 않았다.

운영 사전 점검에서 연구 종목의 asset.listed_from이 모두 비어 있어, 별도의 인증된 상장 구간 스냅샷을 만들었다. manifest의 listing_snapshot_id로 이 스냅샷을 선택하며, 스냅샷이 없을 때는 기존 상장일 누락 차단을 유지한다. NXT·상장 이력 대조 완료를 수급·공매도 전체 적재 완료로 해석하지 않는다.

## 인증된 상장·폐지 구간 (2026-09-12)

`pipeline.asset_lifecycle.candidate`는 공식 원본의 날짜와 정확히 일치하는 종목코드로 후보를 만든다. 가격의 최초 관측일을 상장일로 대체하지 않으며, 보통주의 상장일을 우선주에 복사하지 않는다. 원본 파일명·SHA256과 개별 사실을 보존한다. 이 함수는 준비 단계이며 RDS를 수정하거나 인증하지 않는다.

- KIND 현재 상장법인, 과거 상장폐지, 시장별 신규·이전·재상장 이력을 대조했다. 신규상장 다운로드의 3,000행 제한 때문에 기간을 나누어 수집했다.
- 전체 2,816종목의 상장 시작 후보와 현재 비상장 334종목의 종료 후보를 확보했다. 종류주식 00104K/37550L은 KIS 종목 마스터의 해당 종목별 날짜로 보완했다.
- 공식 상장폐지일은 제외 경계다. `asset.listed_to`에 대응하는 포함 경계는 그 전날이며, 휴장일 제거는 거래일 달력에서 수행한다.
- 코스닥→코스피 이전상장으로 과거 구간을 자르지 않는다. 코넥스만의 상장 구간은 요청한 KOSPI/KOSDAQ 범위에서 제외한다.
- 오상헬스케어(036220)와 우양에이치씨(101970)는 실제 폐지 후 재상장한 이력이 있어 `periods`로 구간을 나눈다. 단일 listed_from/listed_to로 평탄화하면 중간 비상장 기간에 허위 결측이 생긴다. 후보 단계의 REVIEW_REQUIRED는 구간 분리와 RDS 대조로 해소했다.

전체 2,816종목, 2,818개 구간을 2015-01-01~2026-09-11의 RDS 가격 6,314,605행과 대조했다. 예상 거래일 누락·상장 구간 밖 가격·중복·미인증 가격·종목 식별자 오류가 각각 0건이다. 검사한 거래일은 2,871일이다. 이는 가격 날짜와 상장 이력 검증이며 KIS 수급·공매도 값의 전기간 검증은 아니다.

migration 016의 `asset_listing_snapshot`에 공식 원본·RDS 감사 결과의 해시와 2,818개 구간을 함께 인증 발행했다. 기존 asset/asset_identifier와 연구 패널의 동일성 정보는 덮어쓰지 않는다. 스냅샷은 수정·삭제를 금지하고 새 버전으로만 교체하며, CERTIFIED 품질 실행이 없으면 DB에서 삽입을 거부한다. 실제 RDS에서 거부 동작과 재상장 경계의 예상 날짜 SQL을 롤백 테스트로 확인한 뒤 적용했다.

`pipeline.silver.asset_lifecycle.validate`는 전체 종목 합집합, 해시, 구간 중복과 요청 기간을 검사한다. 수집기의 `expected_partitions`는 선택한 RDS 스냅샷과 거래일 달력으로 날짜를 생성한 뒤 가격·인증·식별자를 검사한다. 스냅샷 검증 종료일 뒤의 날짜를 묵시적으로 허용하지 않는다. 갱신 시에는 새로운 공식 자료와 RDS 감사를 거쳐 스냅샷을 발행하고 manifest의 listing_snapshot_id 및 해시를 함께 변경한다.

운영에는 015·016 migration 및 상장 이력 스냅샷을 적용했다. KIS 전체 백필과 일일 활성화는 아직 수행하지 않았다. PR 병합은 자동 배포를 유발하므로, 새 이미지 배포 후 짧은 기간의 KIS 실제 수집·RDS 적재 검증을 먼저 수행한다. NXT 이력은 현재 2026-09-10까지, 상장 구간은 2026-09-11까지 확인되어 있으므로 그 이후 수집 전 각각 최신화해야 한다.

## 공매도 거래소 범위 검증 (2026-09-12)

한투 일별 차트 [공식 명세](https://apiportal.koreainvestment.com/apiservice-apiservice?/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice)는 J=KRX, NX=NXT, UN=통합과 FID_ORG_ADJ_PRC=1=원주가를 명시한다. 공매도 [공식 명세](https://apiportal.koreainvestment.com/apiservice-apiservice?/uapi/domestic-stock/v1/quotations/daily-short-sale)는 J만 설명하지만 실제 조회에서는 NX/UN도 서로 다른 수량을 반환했다. 이 추가 응답은 시장 범위 대조에 사용하며 운영 NX/UN 공매도 수집을 활성화하지 않는다.

실제 검증은 8종목·98개 종목-거래일, 한투 132회·KRX 30회 조회로 수행했다. 공매도 수량과 금액은 각각 98/98 일치했고, 세 시장 응답이 모두 있는 44개 종목-거래일에서는 J+NX=UN 수량·금액·거래량 검사가 모두 통과했다. 미제공 NX 행은 0으로 채우지 않고 합산 검증에서 제외했다. 출범 전, 출범 첫 주, 2025년 3월 말, 2026년 9월 표본을 포함한다.

2026-09-10 삼성전자 공매도 수량은 J 692,808주 + NX 787주 = UN 693,595주이며 UN 수량·금액은 KRX 공개 합산 원본과 일치했다. KRX J 원거래량 22,517,075주를 분모로 사용한 KRX 비율은 3.076811708…%다. UN 거래량 31,539,807주를 J 공매도 수량에 붙이지 않는다.

[KRX 공개 공매도 화면](https://data.krx.co.kr/comm/srt/srtLoader/index.cmd?screenId=MDCSTAT301)은 2025-03-04 이후 KRX+NXT 합산이다. 따라서 KRX 웹 합산 비율과 한투 J 비율이 다르다는 것만으로 오류라고 판단하지 않는다. 또 그 화면의 삼성전자 합산 분모 27,278,081주는 한투 UN 차트 분모와 다르다. 통합 비율까지 동일 정의라고 인증하지 않으며, 그 분모 차이의 원인은 이 검증에서 확정하지 않았다.

운영 정책의 KRX 범위 승인은 공식 거래소 코드 정의와 실제 J+NX=UN 대조, KRX 원본 수량·금액 대조에 근거한 경험적 검증이다. 공급자가 공매도 NX/UN 지원을 문서로 보장했다는 의미나 전체 종목·전기간 값의 완전성을 보장한다는 의미는 아니다. 원본·해시·대조 결과는 정책의 short_market_evidence가 가리키는 불변 S3 검증 자료에 보관한다. 전체 이력 수집 후 날짜별 결측·값 검사를 별도로 수행한다.

검증된 정책 파일은 [policy-krx-short-v1.json](../deploy/kis/policy-krx-short-v1.json)이다. 기존 UNKNOWN 정책을 계속 사용하면 비율은 계속 NULL이므로, 배포 후 KIS_POLICY_URI를 이 파일과 동일한 불변 S3 객체로 지정해야 한다. 이 파일 추가만으로 운영 일정이 변경되지는 않는다.
