# Bronze 계층 — 최종 결정사항

KRX·DART 시장/재무 데이터를 **원본 그대로(raw)** 저장하는 bronze 계층의 설계 기록.
silver(RDS)는 이 bronze를 읽어 정규화한다. bronze는 "API가 준 것을 수정 없이 박제"하는 곳.

## 핵심 원칙

1. **원본 무수정(raw)** — API 응답을 필터·리네임·타입변환 없이 저장. (과거 DB 적재처럼 17개 지표만 남기는 식의 축소 금지)
2. **append-only 파티션** — 날짜/연도 단위로 파티션 추가, 기존 파일 불변.
3. **소스 프리픽스** — 최상위를 소스로 나눠(`datago`/`pykrx`/`dart`) silver가 형식을 구분해 파싱.
4. **자격증명 하드코딩 없음** — S3는 boto3 기본 체인(로컬 `AWS_PROFILE`, ECS Task Role).
5. **재개 가능** — 모든 수집기가 이미 저장된 것을 건너뜀(`sink.exists`, 로컬·S3 공통). 중단(SSO 만료 등) 시
   재로그인 후 같은 명령을 재실행하면 이어서 진행. dart 는 사용한도(020) 감지 시 깔끔히 중단.

## 데이터 × 소스 배정

| 데이터 | 소스 | 유니버스 | 기간 |
|---|---|---|---|
| 주식·지수 시세 (옛날) | **pykrx** by_date (`hist.py`) | 구성종목 union | ~2019 |
| 주식·지수 시세 | **data.go.kr** 주식·지수시세정보 | 전 상장/지수 | **2020~2026.06** |
| 주식·지수 시세 (최근·매일) | **pykrx** by_ticker (`ingest.py`) | 전 상장/지수 | 2026.07~ |
| 지수 구성종목 | **pykrx** `get_index_portfolio_deposit_file` | KOSPI200·KOSDAQ150 | 분기(리밸) |
| DART 재무(주요계정 ~30) | **OpenDART** `fnlttMultiAcnt`(다중회사) | 구성종목 union(~750) | 2015~ |

- **경계**: data.go.kr 은 **2020년부터만** 제공 → 2015-2019 는 pykrx `hist`(by_date), 2026.07~ 는 pykrx `ingest`(by_ticker).
  이 환경에서 pykrx by_ticker 는 옛날 날짜가 빈 응답이라 옛 구간은 by_date 로만 받아진다.
- **DART 유니버스**는 구성종목 union → 구성종목이 DART 스코프이자 백테스트 유니버스로 이중 활용.
- 구성종목은 data.go.kr에 목록 API가 없어 pykrx 전담(소량·분기·저위험).

## 저장 구조

```
<base>/                                   # 로컬 ./data  또는  s3://<bucket>
  datago/
    stock/date=YYYY-MM-DD/all.parquet     # 전 종목 (OHLCV+시총+주식수 한 레코드)
    index/date=YYYY-MM-DD/all.parquet     # 전 지수
  pykrx/
    ohlcv/date=YYYY-MM-DD/<KOSPI|KOSDAQ>.parquet     # by_ticker (2026.07~)
    ohlcv/ticker=<code>.parquet                      # by_date  (~2019, hist)
    market_cap/date=YYYY-MM-DD/<KOSPI|KOSDAQ>.parquet
    market_cap/ticker=<code>.parquet
    index_ohlcv/date=YYYY-MM-DD/<KOSPI200|KOSDAQ150>.parquet
    index_ohlcv/index=<KOSPI200|KOSDAQ150>.parquet   # by_date (~2019)
    index_member/date=<리밸일>/<KOSPI200|KOSDAQ150>.parquet
  dart/
    year=YYYY/corp=<ticker>/<reprt>.json         # 한 회사·보고서 (CFS+OFS 주요계정 raw rows)
```

파티션 키로 형식 구분: 시세 `date=`=by_ticker 스냅샷 / `ticker=`·`index=`=by_date 시계열 / DART `year=`+`corp=`.
DART·datago 는 벌크 응답을 종목/날짜별로 나눠 저장(값 무수정, 파티션만 분할).

## 소스별 형식 (raw 그대로)

| 소스 | 형식 | 필드 | 타입 |
|---|---|---|---|
| datago | parquet (JSON레코드→표) | 영문(`clpr`,`trqu`,`lstgStCnt`,`mrktTotAmt`) | 문자열 |
| pykrx | parquet (DataFrame) | 한글(`종가`,`거래량`,`상장주식수`) | 숫자 |
| dart | **리터럴 JSON** | DART 원본(`account_id`,`thstrm_amount`,`rcept_no`...) | 문자열 |

정규화(공통 스키마·수정주가·표준지표 매핑)는 전부 **silver**의 몫.

## DART 세부

- 엔드포인트: `https://opendart.fss.or.kr/api/fnlttMultiAcnt.json` (다중회사 주요계정)
  - 한 콜에 **여러 회사(배치 100) × 연결(CFS)+별도(OFS) × ~15 주요계정** → 응답에 `stock_code`(티커) 포함
  - 주요계정 = 자산/부채/자본 총계, 매출·영업이익·순이익 등 대표 항목(팩터에 충분). 세부 하위계정(213개)은 제외.
- 파라미터: `corp_code`(콤마 구분 배치), `bsns_year`, `reprt_code` (fs_div 불필요 — 응답에 CFS/OFS 둘 다)
- `reprt_code`: **11011**=사업(FY) · **11013**=1분기 · **11012**=반기 · **11014**=3분기
- 호출량 ≈ (750/100)배치 × 12년 × 4보고서 ≈ **~400 콜**(수분). OpenDART 쿼터(2만/일) 여유, 재개 가능.
- `available_date`(PIT)는 silver에서 `rcept_no`(접수번호)로 접수일 조회해 계산.

## 구현 모듈 (bronze/)

| 파일 | 담당 | 상태 |
|---|---|---|
| `common.py` | 공통 헬퍼 (경로·날짜·.env 로드) | ✅ |
| `datago.py` | data.go.kr 주식·지수 (과거) | ✅ |
| `krx.py`·`ingest.py` | pykrx 시세 by_ticker — 일 배치(`--date`)+백필(`--from/--to`, 2026.07~) | ✅ |
| `hist.py` | pykrx 시세 by_date — 옛날 구간(~2019) 백필 | ✅ |
| `members.py` | pykrx 구성종목 (분기) + 유니버스 헬퍼 | ✅ |
| `dart.py` | OpenDART 재무 수집 (다중회사 주요계정, 재개·쿼터) | ✅ |
| `sink.py` | parquet/JSON 저장 (로컬/S3) | ✅ |

## 향후 확장 (현재 미수집)

멘토가 한국 알파 원천으로 꼽은 데이터. 필요해지면 pykrx로 추가 (data.go.kr엔 없음):
- **투자자 수급** (외국인·기관 순매수) `get_market_trading_value_by_date`
- **공매도** (거래량·잔고) `get_shorting_volume_by_date` · `get_shorting_balance_by_date`
- **외국인 소진율** `get_exhaustion_rates_of_foreign_investment_by_ticker`
- **KRX 펀더멘털** (PER/PBR/EPS) `get_market_fundamental_by_ticker`

범위 밖: ETF/ETN/ELW·선물, 분봉(증권사 API 필요).
