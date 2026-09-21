# FMP 레짐 판독용 입력

수집 대상: VIX(`^VIX`), 달러지수(`DX-Y.NYB`), SOX(`^SOX`), S&P 500(`^GSPC`),
HYG·IEF·LQD·TLT, 미국 국채 만기별 금리(1개월~30년).
USD/KRW·원자재는 기존 파이프라인을 사용한다. CPI 등 수정되는 거시 지표는 이번 범위에 포함하지 않는다.

## 저장 및 품질

- Bronze: `regime/fmp/series=.../from=.../to=.../snapshot=.../response.json` 및 checksum manifest.
- Silver: `fmp_regime_observation`. 기존 주식 universe와 별도이며 ETF 제외 정책을 변경하지 않는다.
- 지수 단위는 `index_points`, ETF는 `USD`, 금리는 `percent`(4.5는 4.5%)다.
- 표준 가격 endpoint의 종가를 보존한다. ETF 배당 재투자 총수익으로 간주하지 않는다.
- 원문·요청 범위·실제 수집 시각을 보존한다. 오류 객체, 중복/범위 밖 날짜, 심볼 불일치,
  비유한 수치, 비정상 OHLC, 음수 거래량을 거부한다. 금리는 음수와 0을 허용한다.
- 3개월·2년·10년 금리는 필수다. 당시 존재하지 않는 다른 만기는 null을 0으로 채우지 않는다.
- DXY의 종가 0인 주말 placeholder는 Bronze에 보존하고 Silver에서 제외하며 DQ MODIFIED로 기록한다.
- 최소 7일 요청이 통째로 비어 있으면 실패한다. 일일 적재 후 9개 시계열 각각의 최신일이
  대상일보다 6일 넘게 오래되면 작업이 실패한다. 개별 거래일의 완전성을 보장하는 검사는 아니다.
- DB 관측행과 DQ 인증은 같은 transaction에서 게시한다. DB 지연 트리거가 인증 없는 행을 거부한다.

`fmp_regime_latest`는 최신 관측 revision을 선택하는 사후 분석용 view다.
`fmp_regime_asof(cutoff)`는 실제 수집 시각이 cutoff 이하인 관측만 선택한다.
2015년 자료를 지금 백필해도 2015년에 알려졌던 데이터라는 보장은 없으며,
과거 hidden OOS 입력으로 자동 편입하지 않는다. 원천 날짜는 미국 세션 날짜이며,
한국 장 시작 전 사용 시 그 시각까지 수집된 관측만 선택한다.

## 실행

환경변수: `FMP_API_KEY`, S3 사용 시 `S3_BRONZE_BUCKET`, Silver 적재 시 기존 DB 설정.
키는 코드·URL·manifest에 저장하지 않는다.

```sh
# 배포 시 기존 migration 절차로 017_fmp_regime.sql 적용
uv run python -m pipeline.silver_quality.migrate
# 원문 수집 및 검증만 (DB 쓰기 없음)
uv run python -m pipeline.fmp_regime --start 2015-01-01 --end 2026-09-17 --dest local
# S3 원문과 Silver 적재, 중단 후 같은 명령으로 재개 가능
uv run python -m pipeline.fmp_regime --start 2015-01-01 --end 2026-09-17 --dest s3 --apply
```

요청은 28일씩 분할하며 기존 FMP 재시도·429 backoff를 재사용한다.
검증된 Bronze receipt가 있으면 API 재호출을 생략한다.
기존 snapshot의 원문·manifest 중 하나라도 있으면 SHA256과 요청 정보를 먼저 검사한다.
손상되었거나 원문/manifest 하나만 남은 미완료 receipt는 재조회·덮어쓰기 없이 실패한다.
해당 증빙을 보존하고 새 `--refresh-id`로 다시 수집한다. 정상 재개는 원래 bytes와 수집시각을 유지한다.
정정값을 다시 수집할 때 `--refresh-id YYYYMMDD`로 새 snapshot을 만든다.
일일 `daily_full`은 완료된 미국 대상일 기준 10일을 겹쳐 조회하고 별도 snapshot을 남긴다.
주식 배치가 이미 인증됐더라도 레짐 데이터 수집·검사는 실행한다.
새 코드는 migration을 자동 실행하지 않으므로 migration 적용 후 daily 이미지를 배포한다.
