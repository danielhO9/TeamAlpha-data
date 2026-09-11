# KIS 수급·공매도 적재

구현 상태: 코드와 단위 테스트. 운영 migration, 수집, 배포 및 스케줄 활성화는 별도 실행 단계이다.

## 범위

2015-01-01 이후 factor-research 월별 패널의 `in_universe=True`였던 모든 asset_id의 합집합을 사용한다. 현재 제외·상장폐지 종목도 유지한다. 해당 자산의 요청 기간 전체를 조회하며 편입 기간으로 데이터를 자르지 않는다. 날짜별 ticker는 RDS asset_identifier로 연결한다. 예상 날짜는 CERTIFIED KRX price_daily에 있는 날짜이며 거래량 0인 날짜도 제외하지 않는다. 따라서 이 작업만으로 price_daily 자체의 결측을 검증하지는 않는다. 기간 내 예상 날짜가 없는 자산은 실행 요약의 assets_without_expected_dates에 기록하며, 상장 전·폐지 후인지 기준 데이터 누락인지 별도 확인해야 한다.

- 수급: 개인·기관·외국인의 매수/매도/순매수 수량 및 금액. 금액은 백만원에서 원으로 변환.
- 시장: KRX(J), 2025-03-04 이후 NXT(NX), 통합(UN). UN 응답과 J+NX를 대조한다.
- NXT 비대상 종목의 빈 응답도 0으로 해석하지 않는다. 현재 시장별 적격 종목 이력 계약이 없으므로 해당 NX/UN 구간은 실패 기록으로 남고 Silver에 들어가지 않는다. 완전한 통합 데이터 운영 전 이 이력을 확보해야 한다.
- 공매도: J 요청만 수집. 비율의 분모는 일별 차트 API의 FID_ORG_ADJ_PRC=1 원거래량. 제공 비율·거래량도 별도 보존한다. 시장 범위 근거가 확인된 정책 구간만 비율을 계산하고 나머지는 NULL/MARKET_SCOPE_UNVERIFIED로 저장한다.

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

3. 아래 예시 정책을 저장한다. 확인되지 않은 시장 범위를 KRX로 임의 변경하지 않는다. KRX로 설정할 경우 short_market_evidence와 short_market_verified_through가 필수다.

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

최근 5개 인증 거래일을 재조회한다. manifest가 40일 이상 오래되면 중단하므로 factor-research 월별 패널 갱신 시 전체 역사 manifest도 재생성해야 한다. NXT 미제공/미확인 구간을 포함한 실패가 하나라도 있으면 일일 작업을 성공으로 보고하지 않는다. 활성화 전 실제 소규모 API/RDS 통합 검증, 시장별 적격 이력 및 공매도 시장 범위 확인이 필요하다. 이 코드 변경으로 운영 일정이나 비밀 설정은 바뀌지 않는다.
