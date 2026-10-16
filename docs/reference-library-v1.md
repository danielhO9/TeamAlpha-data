# 현상 연구용 레퍼런스 측정 라이브러리

SQL: `sql/reference_library_schema.sql`.
실행·검증: 형제 저장소 factor-research의 `engine/reference_gold.py`,
`scripts/reference_library.py`.

## 버전 구분

최신 확인(2026-10-16): 활성 라이브러리는 **108개 측정치·66개 dataset·27,754,980행**이다.
실제 DB 전 행 대조 후 108개 모두 입고를 통과했으며, 수익성 승인과는 별개다.
아래 7개/45개와 program hash는 초기 발행 당시의 보존 기록이다. 최신 목록은
factor-research의 `research/phenomena/program.json`, 입고 결과는
`docs/reference-admission-status-20261016.md`를 따른다. 원천 갱신 없는 전략 탐색은
검증 완료 파일의 동결 해시를 사용하는 별도 경로이며 live 조회 검증을 대체하지 않는다.

이 파일의 v1은 **2026-10-02 초기 가격 전용 7개 측정 dataset**의 계약이다. 기존 dataset과
definition hash는 보존한다. 2026-10-03 확장은 기존 7개 + 가격·거래활동 추가 13개 +
DART 연간 재무 25개, 총 **45개 측정 정의**다. 신규 38개·9,865,877행의 outer transaction
커밋과 그룹별 전 행 대조를 완료했으며, 기존 7개·2,119,881행과 합쳐
**45개·11,985,758행·14개 불변 dataset**이다. 이는 독립 알파 수나 수익성 승인 수가 아니다.

확장의 기준 파일은 형제 저장소 factor-research의
`research/phenomena/reference-catalog-expansion-20261003.json`,
`research/phenomena/reference-expansion-literature.json`,
`docs/reference-expansion-literature-20261003.md`다. 활성 program과 완료 영수증이 실제 사용 가능 목록을 결정한다.
전체 14개 dataset·11,985,758행 재조회 후 collection 활성 프로그램 v2 전환을 완료했다.
이전 program 원문을 보존하고 CAS 일치를 확인했으며, 전환 후 collection manifest 검증에서
45개 측정치·14개 dataset을 확인했다. 실제 사용 목록은 factor-research의
`research/phenomena/program.json`으로 확인한다. 이번 활성 `program_sha256`은
`0b94e084e73bd5d5c20761e0a8a46b34a69bb69ce09fd4d8639bbb56dad5de54`다.
[확장 적재 완료 보고서](../../factor-research/docs/reference-expansion-completion-20261003.md)에
실제 수치와 검증 범위를 기록한다.

이번 적재는 **2026-09 월말 기준 일회성 스냅샷**이며 일일 갱신 배포가 아니다.
`PIT_ASSUMED`로 취급하고 최초 발표 빈티지는 독립 입증하지 못했다. 측정치별 시작일과 종목별
history는 서로 다르다. 측정 재현성 검증은 알파 승인이 아니며 기존 레거시 연구·Gold는 보존한다.

## 계약

- 세대: `phenomenon-research-v1`.
- 역할: `MEASUREMENT_AND_CONTROL_NOT_ALPHA_APPROVAL`.
- 기존 `gold.factor` 및 기존 팩터 값과 별도인 `gold.reference_definition`,
  `gold.reference_dataset`, `gold.reference_value`를 사용한다.
- 초기 v1 dataset은 인증된 KRX 가격 입력만 사용한다. 확장 v2는 인증 KRX 가격과 정확 계정·범위의
  DART FY as-of 재무를 사용한다. 두 경로 모두 기존 Gold 신호·미래 수익 label을 입력에 섞지 않는다.
- 6자리 대문자 영숫자 ticker를 문자열 그대로 보존하며 역사적 asset_id로 계산한다.
- 원천의 보통주 분류와 충돌하는 비영(非0) 접미사 항목은 격리하고 사유를 남긴다.
  `public.asset` 분류를 자동으로 바꾸지 않는다.
- DQ 인증은 최초 발표 vintage의 독립 입증과 다르다. 가격 feature의 기존 수용 계약과
  관측일 종료 시각의 보수적 availability 가정을 명시한다.

확장 원천 유형은 `RDS_CERTIFIED_KRX_DART_FY_REFERENCE_V2`, 계약은
`KRX_PRICE_AND_EXACT_DART_FY_ASOF_V2`다. FY·KRW·CFS/OFS·filing/revision·가용시각을 확인하며
정확 전년 결산기를 대응한다. 최신 FY 경과일 550일 초과, 누락 계정·부적합 범위는 결측·사유로 남긴다.
값의 단위·추정식 사전도 정확 일치를 요구한다. 원형과 다른 총자본/종목시총·광의 유동채권 등은
새로운 명칭과 ID로 등록하며 정확한 보통주 B/M 또는 순수 매출채권이라고 부르지 않는다.

완전한 자산·부채·자본의 등식 잔차가 `max(1원, abs(자산)×10⁻⁶)`를 초과하면 현재·전년 FY
묶음을 불확실 격리한다. 원본과 공시 시점은 유지하고, 반올림 가능성을 원천 오류로 단정하지 않는다.

v2는 `PIT_ASSUMED_ACCEPTED_VINTAGE_NOT_FIRST_RELEASE_PROOF`와
`first_vintage_independently_verified=false`를 강제한다. 가용시각 연결은 수행하지만 최초 발표값임을
독립 증명하지 않는다. 가격·재무 capture의 빈티지도 개별 기록하며 공통 snapshot으로 가장하지 않는다.
초기 가격 전용 source-v1는 정확한 기존 7개 정의에만 사용할 수 있고 재무 확장의 인증으로 바꿔 붙일 수 없다.

## 불변성·발행

dataset은 입력·정의·코드·SQL 대조·출력 값 해시와 함께 고정한다.
Gold 값은 dataset 생성과 같은 DB transaction에서만 삽입할 수 있다.
UPDATE·DELETE·TRUNCATE는 trigger로 막는다. 정정은 새 dataset/version으로 수행한다.
DB 특권 소유자가 스키마를 바꾸는 공격까지 차단한다는 의미는 아니다.

전체 Python 결과를 독립 SQLite 공식과 대조하고, 성과나 오차를 보기 전에 고정한
10개 asset_id의 전 기간을 PostgreSQL 공식으로 다시 대조한다.
전송량을 줄이기 위해 임시 binary staging을 사용한 뒤 동일 transaction 안에서
정의·dataset 키를 붙여 삽입한다. 적재한 모든 행의 모든 필드를 DB 안에서 정렬한 뒤
10,000행 단위 binary SHA256을 계산하고, Python의 동일 binary 직렬화와 정확히 비교한다.
문자열 길이·UTF-8, int8 asset, date/timestamptz, float8 비트를 모두 포함하며 표본 검사가 아니다.
대량 원문을 SSM으로 다시 전송하지 않아도 전 행의 동일성을 확인할 수 있다.
영수증의 `readback_sha256`은 binary chunk 목록 해시이며 JSON `values_sha256`과 구분한다.
기존 Gold metadata가 변하지 않았는지 검사한 뒤 commit한다.

기본 실행은 dry-run이며 실제 적재에는 `--apply`가 필요하다. 완료된 입력 snapshot은
중복 원천 조회 없이 재계산·재검증할 수 있다. 부분 가격 조회를 완료로 추측하지 않는다.

확장 실행은 factor-research의 `scripts/publish_reference_expansion.py`가 담당한다.
`prepare` → `calculate` → 그룹별 `publish-group --apply` → `activation-plan` 순서이며,
초기 7개 dataset은 재발행하지 않고 그대로 결합한다. 신규 그룹은 **최대 3개 정의**로 나누며,
새 정의별 최소 100종목·전체 관측 월 60개 조건은 성과를 보지 않고 고정한 데이터 기준이다.
모든 종목의 60개월 완전 history나 30개 독립 알파를 보장하는 기준이 아니다.
각 그룹의 outer transaction commit과 실제 DB 전 행 재조회를 확인한 뒤에만 활성화 제안을 만든다.
기존 완료 checkpoint를 재사용할 때도 실제 DB readback은 생략하지 않는다.

## 사용

소비자는 factor-research의 `research/phenomena/program.json`이 지정한 **정확한 dataset ID**로
`engine.phenomenon_references.load_active_references()`를 사용한다. 최신값·레거시 fallback은 없다.
외부 프로그램에서도 명시적 dataset/generation/정의·값 해시 확인이 필요하다.

활성 program-v1는 단일 dataset, program-v2는 정확한 여러 dataset의 collection이다.
`load_active_references(conn, repo_root, reference_ids=[...], snapshot_path=...)`는 두 버전을 분기한다.
v1 캐시는 단일 parquet 파일이고, v2 캐시는 `<dataset_id>.parquet` 파일들의 디렉터리다.
선택 지표가 속한 각 dataset **전 행**을 live DB와 검증한 후 선택 결과를 반환한다.
캐시만으로 사용하거나 누락 그룹을 무시하지 않는다.

v2 반환 metadata의 `dataset_id`는 Gold 단일 dataset ID가 아니라 collection manifest 해시다.
`dataset_kind=REFERENCE_COLLECTION_NOT_A_GOLD_DATASET` 및 구성 `reference_datasets`·`source_versions`를
함께 기록한다. collection은 서로 다른 원천 snapshot을 합친 것이며 같은 빈티지를 주장하지 않는다.

측정값은 투자 유니버스, 체결시가, TR label, 실거래 승인, 독립 alpha가 아니다.
전략 입력에는 별도로 검토한 유니버스·거래일·가격·배당/기업행위·PIT 계약을 연결해야 한다.
누락 측정치를 임의로 0으로 채우거나 자동으로 종목을 제외하지 않는다.

초기 v1 실행 수치와 제약은 factor-research의 `docs/phenomenon-research-completion-20261002.md`에 기록한다.
새 확장의 실제 적재 수치·커버리지는 위 확장 완료 보고서와 영수증을 확인한다.
적재 완료와 활성 program 전환은 별도 상태이며 소비자는 항상 현재 program을 확인한다.
