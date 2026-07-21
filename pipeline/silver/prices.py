"""price_daily 적재: 개별종목(marcap+krxapi) + 지수(벤치마크). adj_close(가격수정) 계산.

adj_close: KRX 등락률·전일대비가 조정기준 → 일별 계수 m = (close−전일대비)/이전close (정상일=1,
분할·증자일=조정비율) 누적곱 C, adj_close = close × C_last/C. (배당 미반영 — 소스 없음.)
소스는 marcap·krxapi 모두 같은 KRX 데이터라 price_daily.source='KRX' 로 통일.
"""
from __future__ import annotations

import glob
from datetime import date

import numpy as np
import pandas as pd

from pipeline.common import db
from pipeline.silver.assets import BENCHMARKS

COLS = ["asset_id", "source", "trade_date", "open", "high", "low", "close",
        "adj_close", "volume", "trading_value", "shares", "market_cap", "market"]

# marcap 은 코스닥 글로벌 세그먼트를 따로 표기하지만 krxapi 는 KOSDAQ 으로 합쳐 준다(1771+50=1821 일치).
# 두 소스를 같은 값으로 맞춘다.
MARKET_NORM = {"KOSDAQ GLOBAL": "KOSDAQ"}


def _num(s):
    return pd.to_numeric(s.astype(str).str.replace(",", "", regex=False), errors="coerce")


def _read_marcap(base: str) -> pd.DataFrame:
    frames = []
    for f in sorted(glob.glob(f"{base}/stock/marcap/date=*/all.parquet")):
        frames.append(pd.read_parquet(f, columns=[
            "Code", "Date", "Open", "High", "Low", "Close", "Volume", "Amount",
            "Stocks", "Marcap", "Changes", "Market"]))
    if not frames:
        return pd.DataFrame()
    m = pd.concat(frames, ignore_index=True)
    return pd.DataFrame({
        "ticker": m["Code"].astype(str),
        "trade_date": pd.to_datetime(m["Date"]).dt.date,
        "open": m["Open"], "high": m["High"], "low": m["Low"], "close": m["Close"],
        "volume": m["Volume"], "trading_value": m["Amount"],
        "shares": m["Stocks"], "market_cap": m["Marcap"], "prev_diff": m["Changes"],
        "market": m["Market"].astype(str).replace(MARKET_NORM),
    })


def _read_krxapi(base: str) -> pd.DataFrame:
    frames = []
    for f in sorted(glob.glob(f"{base}/stock/krxapi/date=*/*.parquet")):
        frames.append(pd.read_parquet(f))
    if not frames:
        return pd.DataFrame()
    k = pd.concat(frames, ignore_index=True)
    return pd.DataFrame({
        "ticker": k["ISU_CD"].astype(str),
        "trade_date": pd.to_datetime(k["BAS_DD"]).dt.date,
        "open": _num(k["TDD_OPNPRC"]), "high": _num(k["TDD_HGPRC"]),
        "low": _num(k["TDD_LWPRC"]), "close": _num(k["TDD_CLSPRC"]),
        "volume": _num(k["ACC_TRDVOL"]), "trading_value": _num(k["ACC_TRDVAL"]),
        "shares": _num(k["LIST_SHRS"]), "market_cap": _num(k["MKTCAP"]),
        "prev_diff": _num(k["CMPPREVDD_PRC"]),
        "market": k["MKT_NM"].astype(str).replace(MARKET_NORM),
    })


def _with_adj_close(df: pd.DataFrame) -> pd.DataFrame:
    """티커별 시계열에 adj_close 컬럼 추가 (가격수정 누적계수)."""
    df = df.sort_values(["ticker", "trade_date"]).reset_index(drop=True)
    prev = df.groupby("ticker")["close"].shift(1)
    adj_prev = df["close"] - df["prev_diff"].fillna(0)
    m = np.where((prev > 0) & (adj_prev > 0), adj_prev / prev, 1.0)
    m = np.where(np.abs(m - 1) < 1e-9, 1.0, m)  # 정상일 정수라 정확히 1 — 부동소수 잡음 제거
    df["_m"] = m
    C = df.groupby("ticker")["_m"].cumprod()
    C_last = C.groupby(df["ticker"]).transform("last")
    df["adj_close"] = (df["close"] * (C_last / C)).round(4)
    return df


def _rescale_history_for_events(conn, stock: pd.DataFrame, target_date: date, tol: float = 0.005) -> int:
    """target_date 에 조정 이벤트가 있으면 그 종목의 과거 adj_close 를 소급 조정.

    daily 경로는 대상일 bronze 만 내려받아 _with_adj_close 가 하루짜리 시계열을 보므로 누적계수가
    항상 1 이 된다(= adj_close 가 close 그대로). 그래서 분할·병합이 나면 백필 구간과 스케일이
    어긋나 시계열이 끊긴다. 전체 재계산 대신 소급 보정으로 잇는다:

      adj_close(d) = close(d) × C_last/C(d) 인데 새 이벤트 계수 k 는 C_last 만 k 배 한다
      → target_date 이전 모든 adj_close 에 k 를 곱하면 된다. (target_date 행은 close 그대로가 정답)

    k 는 KRX 전일대비가 조정기준이라는 성질로 구한다: k = (close − 전일대비) / 직전거래일 close.
    이미 반영된 경우(직전거래일 adj_close == 조정전일종가) 건너뛰어 재실행에 안전하다.
    """
    if stock.empty or "prev_diff" not in stock.columns:
        return 0
    fixed = 0
    with conn.cursor() as cur:
        for row in stock.itertuples(index=False):
            aid, close, pdiff = row.asset_id, row.close, row.prev_diff
            if pd.isna(aid) or pd.isna(close) or pd.isna(pdiff):
                continue
            adj_prev = float(close) - float(pdiff)  # KRX 조정전일종가
            if adj_prev <= 0:
                continue
            cur.execute(
                "SELECT close, adj_close FROM price_daily "
                "WHERE asset_id=%s AND source='KRX' AND trade_date < %s "
                "ORDER BY trade_date DESC LIMIT 1",
                (int(aid), target_date),
            )
            got = cur.fetchone()
            if not got or not got[0] or float(got[0]) <= 0:
                continue
            prev_close, prev_adj = float(got[0]), got[1]
            k = adj_prev / prev_close
            if abs(k - 1) <= tol:
                continue  # 평일 — 이벤트 없음
            if prev_adj and abs(float(prev_adj) - adj_prev) / adj_prev < tol:
                continue  # 이미 반영됨 (재실행)
            cur.execute(
                "UPDATE price_daily SET adj_close = adj_close * %s "
                "WHERE asset_id=%s AND source='KRX' AND trade_date < %s",
                (k, int(aid), target_date),
            )
            print(f"[prices] 조정이벤트 asset_id={int(aid)} k={k:.4f} → 과거 adj_close {cur.rowcount}행 소급조정")
            fixed += 1
    conn.commit()
    return fixed


def _read_index(base: str) -> pd.DataFrame:
    """벤치마크 지수(코스피200·코스닥150) → 종목시세와 같은 스키마. adj_close=close."""
    frames = []
    for f in glob.glob(f"{base}/index/krxapi/date=*/kospi.parquet") + \
             glob.glob(f"{base}/index/krxapi/date=*/kosdaq.parquet"):
        df = pd.read_parquet(f)
        frames.append(df[df["IDX_NM"].isin(BENCHMARKS)])
    if not frames:
        return pd.DataFrame()
    x = pd.concat(frames, ignore_index=True)
    code = x["IDX_NM"].map(lambda n: BENCHMARKS[n][1])
    close = _num(x["CLSPRC_IDX"])
    return pd.DataFrame({
        "code": code, "trade_date": pd.to_datetime(x["BAS_DD"]).dt.date,
        "open": _num(x["OPNPRC_IDX"]), "high": _num(x["HGPRC_IDX"]),
        "low": _num(x["LWPRC_IDX"]), "close": close, "adj_close": close,
        "volume": _num(x["ACC_TRDVOL"]), "trading_value": _num(x["ACC_TRDVAL"]),
        "shares": np.nan, "market_cap": _num(x["MKTCAP"]),
        "market": None,  # 지수는 어느 시장에 '상장'된 게 아니라 NULL
    })


def run(conn, base: str, krx_map: dict[str, int], target_date: date | None = None) -> None:
    # 개별종목: marcap + krxapi → adj_close
    stock = pd.concat([_read_marcap(base), _read_krxapi(base)], ignore_index=True)
    if not stock.empty:
        stock = _with_adj_close(stock)
        if target_date is not None:
            stock = stock[stock["trade_date"] == target_date]
    else:
        stock = pd.DataFrame(columns=[
            "ticker", "trade_date", "open", "high", "low", "close", "adj_close",
            "volume", "trading_value", "shares", "market_cap", "market",
        ])
    stock["asset_id"] = stock["ticker"].map(krx_map)
    # 지수
    idx = _read_index(base)
    if not idx.empty:
        if target_date is not None:
            idx = idx[idx["trade_date"] == target_date]
        idx["asset_id"] = idx["code"].map(krx_map)
    both = pd.concat([stock, idx], ignore_index=True)
    unmapped = int(both["asset_id"].isna().sum())
    both = both.dropna(subset=["asset_id"])
    if both.empty:
        print(f"[prices] price_daily upsert 0행 (미매핑 {unmapped} 스킵)")
        return
    both["asset_id"] = both["asset_id"].astype("int64")
    both["source"] = "KRX"
    for c in ("volume", "shares"):  # BIGINT — float 표현 방지 (nullable int)
        both[c] = both[c].round().astype("Int64")
    both = both.drop_duplicates(["asset_id", "source", "trade_date"], keep="last")

    rows = list(both[COLS].astype(object).where(pd.notna(both[COLS]), None).itertuples(index=False, name=None))
    n = db.upsert(conn, "price_daily", COLS, rows,
                  conflict=["asset_id", "source", "trade_date"],
                  update=["open", "high", "low", "close", "adj_close",
                          "volume", "trading_value", "shares", "market_cap", "market"])
    print(f"[prices] price_daily upsert {n}행 (미매핑 {unmapped} 스킵)")

    # daily 는 하루치만 보므로 분할·병합 계수를 못 잡는다 → 과거 adj_close 를 소급 보정해 시계열을 잇는다.
    if target_date is not None and not stock.empty:
        n_ev = _rescale_history_for_events(conn, stock.dropna(subset=["asset_id"]), target_date)
        if n_ev:
            print(f"[prices] 조정이벤트 {n_ev}종목 소급조정 완료")
