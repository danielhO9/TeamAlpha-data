"""silver 계층 — 로컬 bronze 를 읽어 정규화해 RDS 에 적재.

구현:
  - asset/assets_identifier: KRX 티커 + bronze corpCode.xml 기반 DART corp_code 매핑
  - price_daily: 주식/지수 일봉, 가격수정 adj_close 계산
  - fundamental: DART 주요계정 long 정규화 + PIT available_date 계산
  - backfill: 전체 bronze 반영
  - incremental(day): 해당 날짜 price_daily 삭제 후 재적재 + 해당 연도 fundamental upsert
"""
