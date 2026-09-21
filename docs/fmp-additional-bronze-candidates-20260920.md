# FMP 추가 Bronze 원본 후보 — 2026-09-20

## 결론과 조사 범위

현재 수집 코드의 허용 목록에 없는 후보 18개를 실제 FMP API로 조회했다.
모두 요청 범위 `2015-01-01~2026-09-18` 내 장기 응답이 있으며, 141개 달 각각에 행이 있다.
이는 **월 단위 history 존재 확인**이지 개별 거래일·공식 발표 전체의 완전성이나 PIT 인증이 아니다.
아래 첫 날짜는 이번 조회 범위 내 첫 관측이며 FMP 전체 서비스의 최초 제공일이 아니다.

카탈로그 3회와 장기 데이터 18회, 총 21회 조회했다. `FMP_API_KEY` 환경변수와 header 인증을 사용했다.
원문·checksum·실제 수집 시각은 로컬 `data/audits/fmp-extra-bronze-20260920/`에 보존했다.
**이번 조사는 운영 S3 적재, 일일 파이프라인 변경, Silver/Gold 적재를 하지 않았다.**
AWS 인증 만료로 운영 버킷 전체를 다시 대조하지 못했으므로 '현재 코드에 없는 후보'와
'운영 어디에도 없는 데이터'는 구분한다.

## 우선순위 (한국 관련성과 입력 종류의 보완성을 고려한 판단)

| 순위 | 원본 후보 | 보완 목적 | 주의점 |
|---|---|---|---|
| 1 | USDCNH·USDCNY·USDJPY, 보조 AUDUSD·EURUSD | 역내외 위안화, 엔화, 달러 환경을 기존 USDKRW와 함께 관찰 | FX 일봉 마감시각·주말 행 검증 필요. CNH와 CNY를 같은 시장으로 합치지 않음 |
| 2 | COT HG·CL·DX·J6, 보조 GC·VX | 구리·원유·달러·엔화 등의 가격 외에 선물 참여자 포지션을 추가 | 보유 기준일과 공개일 분리, 공식 CFTC 보고서 종류·수정 이력 대조 필요 |
| 3 | EWY·EEM·FXI 일봉 | 미국 거래시간의 한국·신흥국·중국 주식 가격 반응 | 실제 외국인 순매수/펀드 유입액이 아님. ETF 가격조정·분배금 구분 |
| 4 | VVIX·VIX3M | 기존 VIX 외에 변동성의 변동성과 3개월 변동성 관찰 | 지수 정의·미국 세션 종료·한국 판단시점 정렬 필요 |
| 5 | HSI·N225 | 홍콩·일본 증시의 지역 공통 움직임 | 한국과 거래시간이 겹침. 당일 종가를 한국 당일 시가에 사용하지 않음 |

위 순위는 전략 성과 검증 결과가 아니며 매매 지시나 신호는 만들지 않았다.

## 실측 API 결과

가격 API는 `/stable/historical-price-eod/full`, 포지션 API는 `/stable/commitment-of-traders-report`를 사용했다.

| 계열 | 첫 관측 | 최신 관측 | 원본 행 | 행이 있는 달 |
|---|---|---|---:|---:|
| USDCNH | 2015-01-02 | 2026-09-18 | 3,162 | 141/141 |
| USDCNY | 2015-01-01 | 2026-09-18 | 3,134 | 141/141 |
| USDJPY | 2015-01-01 | 2026-09-18 | 3,142 | 141/141 |
| AUDUSD | 2015-01-01 | 2026-09-18 | 3,133 | 141/141 |
| EURUSD | 2015-01-02 | 2026-09-18 | 3,119 | 141/141 |
| ^VVIX | 2015-01-02 | 2026-09-18 | 2,946 | 141/141 |
| ^VIX3M | 2015-01-02 | 2026-09-18 | 2,945 | 141/141 |
| ^HSI | 2015-01-02 | 2026-09-18 | 2,889 | 141/141 |
| ^N225 | 2015-01-05 | 2026-09-18 | 2,862 | 141/141 |
| EWY | 2015-01-02 | 2026-09-18 | 2,945 | 141/141 |
| EEM | 2015-01-02 | 2026-09-18 | 2,945 | 141/141 |
| FXI | 2015-01-02 | 2026-09-18 | 2,945 | 141/141 |
| COT HG — 구리 | 2015-01-06 | 2026-09-15 | 611 | 141/141 |
| COT CL — WTI | 2015-01-06 | 2026-09-15 | 611 | 141/141 |
| COT GC — 금 | 2015-01-06 | 2026-09-15 | 611 | 141/141 |
| COT DX — 달러지수 | 2015-01-06 | 2026-09-15 | 611 | 141/141 |
| COT J6 — 엔화 | 2015-01-06 | 2026-09-15 | 611 | 141/141 |
| COT VX — VIX | 2015-01-06 | 2026-09-15 | 611 | 141/141 |

합계 39,833행. 모든 응답에서 날짜 중복 및 요청 범위 밖 날짜는 없었다.
가격 12개 계열의 close null/0은 없었다. 이는 OHLC 전체 품질이나 원천 정확성 인증이 아니다.

## 품질·시점 관련 확인 사항

- FX에는 주말 날짜가 포함된다: USDCNH 118, USDCNY 78, USDJPY 85, AUDUSD 77, EURUSD 75행.
  이를 0이나 결측으로 바꾸거나 자동 제거하지 않는다. Bronze는 원문 보존,
  이후 가격 세션 정의/동일값 반복/타임존을 검증한다.
- EWY·EEM·FXI는 로컬 XNYS 참고 캘린더와 날짜 집합이 일치했다.
  각 ETF 거래소의 공식 세션 및 가격·배당 조정 검증을 대신하지 않는다.
- COT 6개 계열은 각각 화요일 605행·월요일 6행이다. 핵심 5개 필드
  (`openInterestAll`, `noncommPositionsLongAll`, `noncommPositionsShortAll`,
  `commPositionsLongAll`, `commPositionsShortAll`)의 null은 없었다.
- COT 응답의 `date`를 발표일로 쓰면 안 된다. 조사 응답에 별도 발표시각 필드는 없다.
  CFTC 일반 일정은 화요일 기준 포지션을 금요일 15:30 미국 동부시간 공개하지만,
  공휴일·정부 업무중단·지연 공표의 예외가 있어 일괄 `date + 3일`로 만들지 않는다.
  월요일 기준 6행을 임의로 화요일로 바꾸지도 않는다.
- `noncomm`를 `managed money` 또는 `leveraged funds`와 동일한 분류로 가정하지 않는다.
  Futures-only/combined 여부 및 CFTC contract code 기준으로 공식 원문 대조가 필요하다.
- 모든 후보는 아직 `pit_approved=false`. 과거 거래일이 있다는 사실은 당시 수신했거나
  그 시점에 공개된 수정 전 값이라는 증거가 아니다.

## 중복 확보를 피할 항목

- 기존 한국 매크로 백필은 전체 세계 캘린더 원문을 S3 `raw/`에 보존했다.
  미국 ISM 제조업 PMI·FOMC·고용 및 중국 통계는 이미 원문 안에 포함돼 있어,
  새 API 수집보다 기존 원본의 허용 목록/추출 계약 검토가 먼저다.
  이들은 현재 한국 13개+중국 PMI 선택 파일에 자동 편입된 것은 아니다.
- VIX·달러지수·SOX·S&P500·HYG·IEF·LQD·TLT·미국 국채금리는 기존 레짐 코드 범위다.
  신규 후보와 혼동하지 말고 운영 장기 백필 완료 여부를 별도로 확인한다.
- USDKRW 및 금·구리·WTI·브렌트 등 물리 원자재 28종은 기존 수집 코드 범위다.
  구리/금 비율, 장단기 금리차, CNH-CNY 차이는 파생값이지 별도의 Bronze 원천이 아니다.
- 한국 제조업 PMI의 긴 null 구간, M2의 2020년 시작, 수출입 YoY의 2015년 초 누락 등
  기존 감사에서 발견한 제한은 그대로 유효하다. 목록 확대만으로 해결되지 않는다.

## 근거와 재현 자료

- 로컬 원문/manifest: `data/audits/fmp-extra-bronze-20260920/catalog/`, `probes/`.
- 장기 응답 집계: `data/audits/fmp-extra-bronze-20260920/candidate_probe_summary.json`.
- 품질 표본 검사: `data/audits/fmp-extra-bronze-20260920/quality_screen.json`.
- [FMP FX 일봉 API](https://site.financialmodelingprep.com/developer/docs/stable/forex-historical-price-eod-full).
- [FMP COT 원본 API](https://site.financialmodelingprep.com/developer/docs/stable/cot-report).
- [CFTC 보고서 기준일·발표 일정](https://www.cftc.gov/MarketReports/CommitmentsofTraders/AbouttheCOTReports/cot_about.html),
  [공표 지연 예외](https://www.cftc.gov/PressRoom/PressReleases/9138-25),
  [과거 특이 공지](https://www.cftc.gov/MarketReports/CommitmentsofTraders/HistoricalSpecialAnnouncements/index.htm).
- [iShares ETF 목록과 상품 식별](https://www.ishares.com/us/products/etf-investments),
  [FXI 상품 범위](https://www.ishares.com/us/products/239536/).
- [Cboe VVIX·VIX3M 설명](https://www.cboe.com/insights/posts/index-insights-july-2026).
