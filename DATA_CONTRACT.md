# Silver 데이터 계약 (Data Contract)

> 버전 2026-08-12 · ruleset 1.28.0 · 대상: 팩터를 생성하는 research agent 및 그 가드레일
>
> 이 문서는 **각 필드가 무엇이고, 어떤 팩터에 써도 되는지/쓰면 안 되는지**를 규정한다.
> 목적은 "오류 없음"을 넘어 **조용한 편향(silent bias)** 을 막는 것이다 — 겉보기엔
> 멀쩡하나 숨은 함정이 있는 필드를 agent가 모르고 써서 그럴듯한 가짜 팩터를 만드는 일.
> 기계 판독본은 [`field_reliability.yaml`](field_reliability.yaml).

---

## 0. 한눈 요약 — 절대 혼동 금지

| 필드 | 정체 | ✅ 안전 | ❌ 금기 |
|---|---|---|---|
| `close` | 원시 종가(무조정, 명목) | 당일 값 표시 | **시계열 수익률**(분할·배당 미조정) |
| `adj_close` | KRX 기준가격 계수 기반 **가격조정** 종가 | 가격 모멘텀·리버설·변동성 | **인증된 현금 총수익 라벨 대용** |
| `total_return_close` | KRX v3/v5 인증 **최신 정정 반영 ex-post 총수익 라벨** | forward label·실현 성과평가 | **팩터 입력/과거 feature** (`feature_pit_safe=false`) |
| `shares` | **총상장주식수** | 시총·발행주식 | **유동주식(free float) 아님** |

**adj_close ≠ 인증된 v3 총수익.** `adj_close`는 KRX 기준가격 조정을 반영하는
가격 계열이며 기업행사·현금배당일과 조정계수 변경이 겹칠 수도 있다. 그러나 이것만으로
감사된 현금 재투자 총수익 계약을 충족하지는 않는다. v3는 별도로 검증한 조정 현금배당을
더해 forward gross return을 계산한다.
반대로 `total_return_close`도 과거 팩터 입력으로 쓰면 안 된다. KRX 값은
`krx_gross_dividend_reinvested_v3` 방법론과
`dart_total_return_action_snapshot_v5`의 **최신 정정본**을 소급 반영한 ex-post
forward label이다. bitemporal action vintage가 없으므로 당시 이용 가능했던 배당정보를
재현하는 PIT feature가 아니다.

---

## 1. 커버리지 (있는 것)

| 소스 | 자산 | 기간 | 비고 |
|---|---|---|---|
| **KRX 주식** | ~6,677 | **1995-05-02 ~** | marcap(과거) + KRX OpenAPI(일별). **상장폐지 포함**(생존편향 없음) |
| KRX 지수 | 1028 KOSPI200·2203 KOSDAQ150 | 2010-01-04 ~ | 벤더 최소제공일 |
| FMP 미국주식 | ~8,720 (NYSE·NASDAQ·AMEX) | 2015-01-02 ~ | 상장폐지 포함 |
| FMP 원자재 | 28 | 2015 ~ | 연속선물(롤 주의) |
| FMP FX | USDKRW | 2015 ~ | |
| DART 재무 | BS·IS ~3,071 | 2015-03-31 ~ | 표준 핵심계정 |
| DART 전체 재무 원계정 | BS·IS·CIS·CF·SCE, CFS/OFS | 2015 ~ | 인증 백필 후 사용; 계정 의미는 팩터별 고정 필요 |
| DART 지분공시 | 임원·주요주주·5% 보유 | 공식 API 제공기간 | 공시 이벤트이며 실제 체결 테이프가 아님 |
| DART 업종코드 | 현재 기업개황 snapshot | 최초 인증 관측 이후 | 과거 업종 효력일은 제공되지 않음 |
| KRX 투자자 수급 | 투자자유형별 종목 일별 | 계약 파일 범위 | 구매·활용승인 export만 적재 |
| KRX 공매도 순보유잔고 | 종목별 수량·금액·비중 | 2016-06-30 ~ | 승인된 관측 vintage만; 보고기준 미만 잔고는 포함되지 않음 |
| DART 배당 | ~1,903 | 2013-09-30 ~ | 정기보고서 |
| FMP 재무 | BS·IS·CF ~8,490 | 2015 ~ | **벤더 신뢰(미검증)** |

## 1b. 제공하지 않음 (명시적 울타리 — 억지로 대체 금지)

- **유동주식수(free float)** — `shares`는 총상장주식뿐
- **호가(order book / bid-ask)**
- **최초 관측 이전 PIT 과거 업종분류** — 현재 DART 업종을 과거에 소급 적용 금지
- **최초 승인 관측 이전 공매도 잔고 vintage 및 대차** — 오늘 받은 수정본을 과거에 소급 적용 금지

> 위 항목이 필요한 팩터는 **만들 수 없다**(대체 필드로 우회 금지). 필요 시 벤더 조달 후 추가.

---

## 2. 필드 사전

### price_daily
| 필드 | 의미 | 단위 | 신뢰 | 금기/주의 |
|---|---|---|---|---|
| `close` | 원시 종가 | 통화 | 검증 | 시계열 수익률 금지 |
| `open/high/low` | 원시 시/고/저 | 통화 | 검증 | **거래량 0일은 NULL**(비거래) |
| `adj_close` | KRX 공식 기준가격/전일대비 계수로 재구성한 가격조정 종가 | 통화 | 검증 | 가격 feature용. 그 자체는 인증된 현금 총수익 라벨이 아님 |
| `total_return_close` | listing episode 첫 행을 `adj_close`에 고정하고 `(adj_close[t]+adjusted_cash[t])/adj_close[t-1]`을 순방향 복리한 KRX gross 총수익 지수 | 통화 | 조건부 인증 | `price_return_contract`가 `CERTIFIED`, 방법론이 `krx_gross_dividend_reinvested_v3`, action snapshot이 v5일 때만 forward label로 사용. **최신 정정 소급반영·feature_pit_safe=false** |
| `volume` | 거래량 | 주 | 검증 | 0=비거래(OHLC NULL) |
| `trading_value` | 거래대금 | 통화 | 검증 | |
| `shares` | **총상장주식수** | 주 | 검증 | **유동주식 아님** |
| `market_cap` | 시가총액(=close×shares) | 통화 | 검증(대사 게이트) | |
| `market` | KOSPI/KOSDAQ/KONEX | | 검증 | |
| `prev_diff` | KRX 전일대비(adj_close 산출 근거) | 통화 | 검증 | 내부용 |

KRX 총수익의 배당 적용일은 인증 rebuild가 남긴 append-only
`dividend_event_resolution`과 원문·action·가격 evidence로 감사한다. 연구 코드가
`record_date`에서 며칠을 임의로 빼 배당락일을 다시 추정해서는 안 된다. FMP의
`total_return_close`는 벤더의 배당조정 가격이며 위 KRX v3/v5 인증을 상속하지 않는다.

### fundamental (long: metric/value)
- **DART(한국) — 검증됨**(ERROR 게이트: 값타당성 total_assets>0·revenue≥0, 회계항등식 gross>10% 제외, 음수배당 제외).
  - BS: `total_assets, current_assets, noncurrent_assets, total_liabilities, current_liabilities, noncurrent_liabilities, total_equity, capital_stock, retained_earnings`
  - IS: `revenue, operating_income, pretax_income, net_income, comprehensive_income`
  - DIVIDEND(정기보고서): `cash_dividend_per_share, dividend_yield, stock_dividend_per_share, payout_ratio`
  - **없음: COGS, SG&A, cash, CFO, CAPEX, 감가상각** (상세계정 미적재)
  - `available_date` = 공시 다음날(PIT 준수, look-ahead 없음 — FUNDAMENTAL_PIT_ORDER CRITICAL)
  - `fs_type` CFS(연결)/OFS(별도) 구분 — 혼용 금지
- **FMP(미국) — 벤더 신뢰(내부검증 안 함).** BS/IS/CF 제공. total_assets=0·음수 revenue·회계 불일치가 관행상 존재하나 **그대로 둠**(정의 차이). US 팩터는 이 전제 하에 사용.

### fundamental_statement_line / ownership_disclosure_event / investor_flow_daily

- `fundamental_statement_line`은 OpenDART 전체재무제표의 숫자 원계정을
  BS·IS·CIS·CF·SCE 및 CFS/OFS 구분 그대로 보존한다. 표준 `metric`으로 자동
  합치지 않으므로 팩터마다 `account_id` 집합·단위·부호·커버리지를 사전 고정해야 한다.
- `ownership_disclosure_event`는 임원·주요주주 소유상황과 5% 대량보유 **공시**다.
  공시 다음 날부터 사용할 수 있지만 실제 매매 체결일·체결가는 제공하지 않는다.
- `investor_flow_daily`는 KRX Data Marketplace 구매 또는 Open API 활용승인을
  증명하는 `authorization_id`가 있는 원본만 받는다. 웹 화면 자동수집 원본은 금지한다.
- 세 테이블 모두 `quality_run_id`가 `CERTIFIED`인 행만 연구 입력으로 사용하고,
  `available_date/available_at <= :as_of`를 반드시 적용한다.

### industry_classification_observation / short_position_balance_observation

- DART 기업개황의 `induty_code`는 현재 분류만 제공하므로 최초 수집시각을
  `available_at`으로 저장한다. 과거 시점 업종중립화에 현재 코드를 소급 적용하지 않는다.
- KRX 공매도 순보유잔고는 보고의무 기준을 넘은 투자자의 잔고를 종목별로 합산한
  데이터다. 보고 지연과 사후 정정이 있어 원본을 실제 받은 시각별로 보존한다.
- 두 테이블 모두 과거 날짜가 적힌 파일을 오늘 받았다면 그 행은 오늘부터만
  PIT-safe하다. 향후 일별 snapshot을 누적하면 prospective 연구 구간이 생긴다.

### corporate_action / dividend_history
| 필드 | 의미 | 주의 |
|---|---|---|
| `action_type` | cash_dividend, stock_split, reverse_split, capital_reduction, bonus_issue, merger, … | |
| `ex_date` | 배당락/권리락일 | KRX 현금배당 원천행에는 NULL일 수 있음. 직접 추정하지 말고 인증된 `dividend_event_resolution`을 사용 |
| `record_date` | 배당기준일 | KRX 배당의 기준 날짜 |
| `cash_amount` | 주당 현금배당 | **KRX 커버리지 부분적**(~17,761/19,880) |
| `adjusted_cash_amount` | 분할조정 주당배당 | |

`corporate_action`과 그 조회 view인 `dividend_history`는 연구 관점에서 원천·감사용
최신 상태 이벤트 이력이다. 특히 KRX/DART 총수익 subset은 v5 최신 정정을 사용한다.
action-vintage별 as-of 상태를 보존하는 bitemporal 계약이 없으므로
**직접 배당 피처로는 PIT 인증되지 않았다**. 배당수익률·배당빈도·배당성장·carry 같은
팩터 입력에 직접 사용하지 말고, 인증된 `total_return_close`도 forward label로만 쓴다.

### asset / asset_identifier
- `asset` = asset_id, name, asset_type(stock/index/commodity/fx), exchange. **sector/업종 없음.**
- `asset_identifier` = 티커·ISIN·cik 매핑(valid_from/to). **재사용 티커는 다른 asset_id로 분리**(정체성 안전). cik는 1기업-N증권이라 중복 정상.

---

## 2b. Point-in-Time / 정정본(restatement) — look-ahead 방지 (필수)

DART 재무는 **정정본이 존재**한다(같은 period_end이 여러 revision으로 저장, 각자
`available_date` 보유 — 32,403 scope, 그중 98.6%가 서로 다른 available_date). 백테스트가
**최신 revision을 그냥 쓰면 미래 정보(look-ahead)** 가 새어든다.

**올바른 as-of 선택 (날짜 D 시점에 실제 이용 가능했던 값만):**
```sql
SELECT DISTINCT ON (asset_id, period_end, fs_type, metric) *
FROM fundamental
WHERE source='DART' AND available_date <= :as_of_date
ORDER BY asset_id, period_end, fs_type, metric,
         available_date DESC, revision_key DESC;
```
- `available_date <= as_of_date`로 자르고, 각 스코프에서 **available_date 최댓값(그 시점 최신 정정본)** 하나만 취한다.
- `FUNDAMENTAL_PIT_ORDER`(CRITICAL)가 `available_date > period_end`(공시 후 이용가능)를 보장하지만, **as-of 선택은 쿼리 쪽 책임**이다. "무조건 최신 revision" 금지.

## 3. 조용한-편향 함정 (반드시 인지)

1. **총수익 라벨 ≠ 가격 feature:** forward label은 `total_return_close`, 과거 가격 feature는 `adj_close`. 섞지 말 것.
2. **FMP 재무는 미검증:** total_assets=0/음수 revenue가 있을 수 있음(관행). US 재무 팩터는 자체 정제 권장.
3. **DART 전체계정은 비표준 원계정:** 이름만 보고 CFO·CAPEX·COGS·SG&A를 합치지 말고,
   동결된 `account_id`와 단위·부호·커버리지 검증을 통과한 정의만 사용한다.
4. **유동주식 없음:** float-adjusted 시총/가중은 불가(총주식뿐).
5. **극단 수익률은 대부분 진짜:** 동전주 ±수백% 등은 실제 이동 → **winsorize·유동성 필터 필수**(데이터 오류 아님).
6. **배당 적용일 재추정 금지:** `record_date`로 배당락일을 다시 계산하는 휴리스틱은 인증 계약이 아니다. `dividend_event_resolution`에 고정된 적용일만 신뢰한다.
7. **직접 배당 feature 금지:** `corporate_action`/`dividend_history`는 최신 정정 기준이며 bitemporal PIT 이력이 아니다.
8. **현재 업종 소급 금지:** DART `induty_code` 관측 이전 날짜의 업종으로 간주하지 않는다.
9. **공매도 잔고는 전시장 잔고가 아님:** 법정 보고기준 미만 포지션은 집계에 없고 후속 정정도 가능하다.

---

## 4. 품질 보증 (floor)

DQ 게이트가 보장하는 불변식(차단=CRITICAL/ERROR):
- null close 0, 비양수 가격 제외, `market_cap ≈ close×shares`(1%)
- 재무 PIT 순서(look-ahead 없음), 통화 일관성
- KRX `total_return_close`는 v3 방법론·v5 action snapshot·resolution/action/body/가격 digest·행별 run parity가 모두 맞고 `price_return_contract.status='CERTIFIED'`일 때만 인증
- **재무 값타당성**(total_assets>0, revenue≥0, 음수배당 제외), **회계항등식 gross(>10%) 제외**
- adj_close 재구성 일관성, 시장/벤치마크 완전성
- 중복 price 행 0, 증권 식별자(ticker/ISIN) 활성중복 0

**보증 안 함:** `corporate_action`/`dividend_history`의 feature PIT 안전성,
FMP 총수익의 KRX v3/v5 인증, 인코딩 안 된 오류 유형, WARNING(원천보존·검토대상),
FMP 재무 정확성, 위 "제공 안 함" 항목.

## 5. 아웃라이어·검토 플래그
- 극단값·구조적 warning은 `dq_warning_state` 워크리스트에 누적, `pipeline.silver_quality.review`로 조회/ack.
- 팩터 구축단에서 **스파이크 winsorize + 저유동 필터** 적용 권장.

## 6. 팩터유형별 안전표
| 팩터 유형 | 쓸 필드 | 피할 것 / 주의 |
|---|---|---|
| 가격 모멘텀·리버설·변동성 | `adj_close` | `close`(무조정) |
| forward 수익률 라벨·실현 성과평가 | 인증된 `total_return_close` | 팩터 입력으로 재사용 금지 |
| 배당수익률·빈도·성장·carry feature | — | `dividend_history`/`corporate_action` 직접 사용 금지(PIT 미인증) |
| 가치·퀄리티·투자·발생액(KR) | DART 핵심 + 인증된 `fundamental_statement_line` + `market_cap` | CFS/OFS 혼용, 이름 기반 임의 계정합산, FMP 혼용 |
| 지분변화(KR) | `ownership_disclosure_event` | 체결일·체결가로 해석 금지 |
| 외국인·기관수급(KR) | 인증된 `investor_flow_daily` | 무허가 웹수집, 가용시각 이전 사용 |
| 업종중립화(KR) | `industry_classification_observation` | 최초 관측 이전으로 소급 금지 |
| 공매도 잔고(KR) | `short_position_balance_observation` | 전체 시장 short interest로 해석 금지, 관측시각 이전 사용 금지 |
| 유동성·규모 | `trading_value`, `market_cap`, `shares` | `shares`≠유동주식 |
| US 팩터 | FMP price/fundamental | 벤더 신뢰 전제 |
| float 가중·공매도·호가 기반 | — | **불가(미제공)** |
