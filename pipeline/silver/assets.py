"""asset + asset_identifier 마스터 구축 (bronze → silver).

종목 유니버스: marcap + krxapi 전 날짜 union(상폐 포함) → 종목별 asset + KRX 티커 identifier.
DART corp_code: bronze corpCode.xml 로 티커→corp_code 매핑해 DART identifier 도 기록(있으면).
지수: 벤치마크(코스피200·코스닥150)를 asset_type='index' 로 등록, KRX 지수코드 identifier.

재실행 안전: 이미 있는 KRX identifier 는 건너뜀.
"""
from __future__ import annotations

import glob

import pandas as pd

from pipeline.bronze import financials

# IDX_NM → (asset name, KRX 지수코드)
BENCHMARKS = {"코스피 200": ("KOSPI200", "1028"), "코스닥 150": ("KOSDAQ150", "2203")}


def _stock_universe(base: str) -> dict[str, str]:
    """marcap + krxapi 전 날짜 union → {ticker: name} (최근 이름 우선, 상폐 포함)."""
    names: dict[str, str] = {}
    for f in sorted(glob.glob(f"{base}/stock/marcap/date=*/all.parquet")):
        df = pd.read_parquet(f, columns=["Code", "Name"])
        names.update(zip(df["Code"].astype(str), df["Name"].astype(str)))
    for f in sorted(glob.glob(f"{base}/stock/krxapi/date=*/*.parquet")):
        df = pd.read_parquet(f, columns=["ISU_CD", "ISU_NM"])
        names.update(zip(df["ISU_CD"].astype(str), df["ISU_NM"].astype(str)))
    return names


def build(conn, base: str) -> dict[str, int]:
    """asset·asset_identifier 적재. 반환: {ticker: asset_id} (KRX)."""
    names = _stock_universe(base)
    corp = {sc: cc for cc, sc in financials.load_listed_corps_from_bronze(base)}  # ticker→corp_code

    with conn.cursor() as cur:
        cur.execute("SELECT identifier, asset_id FROM asset_identifier WHERE source='KRX'")
        krx_map = dict(cur.fetchall())

    new_stocks = [(t, n) for t, n in names.items() if t not in krx_map]
    with conn.cursor() as cur:
        for t, n in new_stocks:
            cur.execute(
                "INSERT INTO asset (name, asset_type, exchange, currency) "
                "VALUES (%s, 'stock', 'KRX', 'KRW') RETURNING asset_id", (n,))
            aid = cur.fetchone()[0]
            cur.execute("INSERT INTO asset_identifier VALUES (%s, 'KRX', %s) "
                        "ON CONFLICT DO NOTHING", (aid, t))
            if t in corp:  # 공통주만 corp_code 매핑(우선주는 corpCode 에 없음)
                cur.execute("INSERT INTO asset_identifier VALUES (%s, 'DART', %s) "
                            "ON CONFLICT DO NOTHING", (aid, corp[t]))
            krx_map[t] = aid
        # 지수 벤치마크
        for _, (name, code) in BENCHMARKS.items():
            if code in krx_map:
                continue
            cur.execute(
                "INSERT INTO asset (name, asset_type, exchange, currency) "
                "VALUES (%s, 'index', 'KRX', 'KRW') RETURNING asset_id", (name,))
            aid = cur.fetchone()[0]
            cur.execute("INSERT INTO asset_identifier VALUES (%s, 'KRX', %s) "
                        "ON CONFLICT DO NOTHING", (aid, code))
            krx_map[code] = aid
    conn.commit()
    print(f"[assets] 종목 {len(names)} (신규 {len(new_stocks)}) + 지수 {len(BENCHMARKS)} → asset_identifier(KRX) {len(krx_map)}")
    return krx_map
