"""price_daily 적재: 개별종목(marcap+krxapi) + 지수(벤치마크). adj_close(가격수정) 계산.

adj_close: KRX ``CMPPREVDD_PRC``/marcap ``Changes``(전일대비)가 조정기준 →
일별 계수 m = (close−전일대비)/이전close (정상일=1, 분할·증자·주식배당 등
기준가격 조정일=조정비율) 누적곱 C, adj_close = close × C_last/C.
일반 주권의 현금배당은 KRX 기준가격 조정식에 포함되지 않으므로 이 값은
price-only이며 DART 현금을 별도로 한 번 가산한다. KRX 공식 기준가격 규정:
https://global.krx.co.kr/contents/GLB/06/0602/0602020202/GLB0602020202T3.jsp
소스는 marcap·krxapi 모두 같은 KRX 데이터라 price_daily.source='KRX' 로 통일.
"""
from __future__ import annotations

import glob
import re
from datetime import date
from uuid import UUID

import numpy as np
import pandas as pd

from pipeline.common import db
from pipeline.silver.return_contract import (
    acquire_return_writer_transaction_lock,
    invalidate_krx_total_return,
)
from pipeline.silver.assets import BENCHMARKS

COLS = [
    "asset_id", "source", "trade_date", "open", "high", "low", "close",
    "adj_close", "total_return_close", "currency", "vwap", "available_at",
    "volume", "trading_value", "shares", "market_cap", "market",
    "quality_run_id", "total_return_quality_run_id", "total_return_loaded_at",
]

# marcap 은 코스닥 글로벌 세그먼트를 따로 표기하지만 krxapi 는 KOSDAQ 으로 합쳐 준다(1771+50=1821 일치).
# 두 소스를 같은 값으로 맞춘다.
MARKET_NORM = {"KOSDAQ GLOBAL": "KOSDAQ"}
UNSUPPORTED_MARKETS = {"KONEX"}
LISTING_EPISODE_GAP_DAYS = 365
_PARTITION_DATE = re.compile(r"/date=(\d{4}-\d{2}-\d{2})(?:/|$)")


def _num(s):
    return pd.to_numeric(s.astype(str).str.replace(",", "", regex=False), errors="coerce")


def _normalize_incomplete_ohlc(df: pd.DataFrame) -> pd.DataFrame:
    """원천의 O/H/L 0 sentinel을 결측으로 보존한다.

    종가로 임의 보정하지 않는다. 세 값이 모두 결측인 행은 quality gate에서
    SOURCE_INCOMPLETE_OHLC Explained로 기록하고, 일부만 결측이면 차단한다.
    """
    for column in ("open", "high", "low"):
        numeric = pd.to_numeric(df[column], errors="coerce")
        df.loc[numeric.eq(0), column] = np.nan
    # 원천 OHLC 대소가 모순인 행(오래된 marcap 소수)은 O/H/L을 신뢰할 수 없어
    # 셋 다 결측 처리하고 close는 보존한다. quality gate는 이를 전부-결측
    # 형태로 보아 SOURCE_INCOMPLETE_OHLC로 기록한다(부분결측=차단은 유지).
    o = pd.to_numeric(df["open"], errors="coerce")
    h = pd.to_numeric(df["high"], errors="coerce")
    low = pd.to_numeric(df["low"], errors="coerce")
    c = pd.to_numeric(df["close"], errors="coerce")
    complete = o.notna() & h.notna() & low.notna()
    hi_ref = pd.concat([o, low, c], axis=1).max(axis=1)
    lo_ref = pd.concat([o, h, c], axis=1).min(axis=1)
    inconsistent = complete & (h.lt(hi_ref) | low.gt(lo_ref))
    for column in ("open", "high", "low"):
        df.loc[inconsistent, column] = np.nan
    return df


def _exclude_unsupported_markets(
    frame: pd.DataFrame,
) -> tuple[pd.DataFrame, dict]:
    """분석 유니버스 밖의 시장을 명시적으로 제외하고 대사 정보를 만든다."""
    if frame.empty:
        return frame, {
            "row_count": 0,
            "ticker_count": 0,
            "markets": {},
            "samples": [],
        }
    unsupported = frame[
        frame["asset_type"].eq("stock")
        & frame["market"].isin(UNSUPPORTED_MARKETS)
    ].copy()
    retained = frame.drop(index=unsupported.index).reset_index(drop=True)
    summary = (
        unsupported.groupby(["market", "identifier"], as_index=False)
        .agg(
            row_count=("identifier", "size"),
            first_trade_date=("trade_date", "min"),
            last_trade_date=("trade_date", "max"),
        )
        .sort_values(
            ["row_count", "market", "identifier"],
            ascending=[False, True, True],
        )
    )
    return retained, {
        "row_count": len(unsupported),
        "ticker_count": int(unsupported["identifier"].nunique()),
        "markets": {
            str(market): int(count)
            for market, count in unsupported.groupby("market").size().items()
        },
        "samples": (
            summary.head(20)
            .astype(object)
            .where(pd.notna(summary.head(20)), None)
            .to_dict("records")
        ),
    }


def _exclude_nonpositive_prices(
    frame: pd.DataFrame,
) -> tuple[pd.DataFrame, dict]:
    """close/상장주식수/시가총액이 비양수인 주식 행을 명시적으로 제외한다.

    정지·상장폐지 과정의 과거 marcap 행은 close/Stocks/Marcap가 0 또는 결측이라
    유효한 가격 시계열이 될 수 없고 DB CHECK(close>0, shares>0, market_cap>0)에
    걸린다. Bronze에는 원천 그대로 보존하고 Silver에서만 제외하며 사유·건수를
    기록한다. adj_close 누적계수가 오염되지 않도록 반드시 _with_adj_close 이전에
    적용한다. (현재 2015+ 구간에는 해당 행이 없어 기존 인증에 무영향.)
    """
    empty_detail = {"row_count": 0, "ticker_count": 0, "samples": []}
    if frame.empty:
        return frame, empty_detail
    close = pd.to_numeric(frame["close"], errors="coerce")
    shares = pd.to_numeric(frame["shares"], errors="coerce")
    market_cap = pd.to_numeric(frame["market_cap"], errors="coerce")
    invalid = ~((close > 0) & (shares > 0) & (market_cap > 0))
    if not invalid.any():
        return frame.reset_index(drop=True), empty_detail
    bad = frame[invalid]
    retained = frame[~invalid].reset_index(drop=True)
    summary = (
        bad.groupby("ticker", as_index=False)
        .agg(
            row_count=("ticker", "size"),
            first_trade_date=("trade_date", "min"),
            last_trade_date=("trade_date", "max"),
        )
        .sort_values(["row_count", "ticker"], ascending=[False, True])
    )
    return retained, {
        "row_count": int(invalid.sum()),
        "ticker_count": int(bad["ticker"].nunique()),
        "samples": (
            summary.head(20)
            .astype(object)
            .where(pd.notna(summary.head(20)), None)
            .to_dict("records")
        ),
    }


def _paths_in_period(
    pattern: str,
    start_date: date | None,
    end_date: date | None,
) -> list[str]:
    """Return deterministic date-partition paths without reading other years."""
    selected: list[str] = []
    for path in sorted(glob.glob(pattern)):
        match = _PARTITION_DATE.search(path)
        if match is None:
            continue
        partition_date = date.fromisoformat(match.group(1))
        if start_date is not None and partition_date < start_date:
            continue
        if end_date is not None and partition_date > end_date:
            continue
        selected.append(path)
    return selected


def available_years(base: str) -> list[int]:
    """List price partition years without materializing any price rows."""
    patterns = (
        f"{base}/stock/marcap/date=*/all.parquet",
        f"{base}/stock/krxapi/date=*/*.parquet",
        f"{base}/index/krxapi/date=*/*.parquet",
    )
    years: set[int] = set()
    for pattern in patterns:
        for path in glob.glob(pattern):
            match = _PARTITION_DATE.search(path)
            if match is not None:
                years.add(date.fromisoformat(match.group(1)).year)
    return sorted(years)


def _read_marcap(
    base: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pd.DataFrame:
    frames = []
    for f in _paths_in_period(
        f"{base}/stock/marcap/date=*/all.parquet",
        start_date,
        end_date,
    ):
        frame = pd.read_parquet(f, columns=[
            "Code", "Date", "Open", "High", "Low", "Close", "Volume", "Amount",
            "Stocks", "Marcap", "Changes", "Market"])
        frame["_source_file"] = f
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    m = pd.concat(frames, ignore_index=True)
    out = pd.DataFrame({
        "ticker": m["Code"].astype(str),
        "trade_date": pd.to_datetime(m["Date"]).dt.date,
        "open": m["Open"], "high": m["High"], "low": m["Low"], "close": m["Close"],
        "volume": m["Volume"], "trading_value": m["Amount"],
        "shares": m["Stocks"], "market_cap": m["Marcap"], "prev_diff": m["Changes"],
        "market": m["Market"].astype(str).replace(MARKET_NORM),
        "source_file": m["_source_file"],
    })
    previous = out["close"] - out["prev_diff"]
    out["fluc_rate"] = out["prev_diff"] / previous.replace(0, np.nan) * 100
    return out


def _read_krxapi(
    base: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pd.DataFrame:
    frames = []
    for f in _paths_in_period(
        f"{base}/stock/krxapi/date=*/*.parquet",
        start_date,
        end_date,
    ):
        frame = pd.read_parquet(f)
        frame["_source_file"] = f
        frames.append(frame)
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
        "fluc_rate": _num(k["FLUC_RT"]),
        "market": k["MKT_NM"].astype(str).replace(MARKET_NORM),
        "source_file": k["_source_file"],
    })


def _with_adj_close(df: pd.DataFrame) -> pd.DataFrame:
    """티커별 시계열에 adj_close 컬럼 추가 (가격수정 누적계수)."""
    df = df.sort_values(["ticker", "trade_date"]).reset_index(drop=True)
    previous_date = df.groupby("ticker")["trade_date"].shift(1)
    gap_days = (
        pd.to_datetime(df["trade_date"])
        - pd.to_datetime(previous_date)
    ).dt.days
    df["_listing_episode"] = (
        gap_days.gt(LISTING_EPISODE_GAP_DAYS)
        .groupby(df["ticker"])
        .cumsum()
    )
    series_keys = [df["ticker"], df["_listing_episode"]]
    prev = df.groupby(series_keys)["close"].shift(1)
    adj_prev = df["close"] - df["prev_diff"].fillna(0)
    m = np.where((prev > 0) & (adj_prev > 0), adj_prev / prev, 1.0)
    m = np.where(np.abs(m - 1) < 1e-9, 1.0, m)  # 정상일 정수라 정확히 1 — 부동소수 잡음 제거
    df["_m"] = m
    C = df.groupby(series_keys)["_m"].cumprod()
    C_last = C.groupby(series_keys).transform("last")
    df["adj_close"] = (df["close"] * (C_last / C)).round(4)
    return df


def _rescale_history_for_events(
    conn,
    stock: pd.DataFrame,
    target_date: date,
    apply_tolerance: float = 1e-9,
) -> int:
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
                "SELECT trade_date, close, adj_close FROM price_daily "
                "WHERE asset_id=%s AND source='KRX' AND trade_date < %s "
                "ORDER BY trade_date DESC LIMIT 1",
                (int(aid), target_date),
            )
            got = cur.fetchone()
            if (
                not got
                or (target_date - got[0]).days > LISTING_EPISODE_GAP_DAYS
                or not got[1]
                or float(got[1]) <= 0
            ):
                continue
            prev_close, prev_adj = float(got[1]), got[2]
            k = adj_prev / prev_close
            if abs(k - 1) <= apply_tolerance:
                continue  # 원천 조정계수가 정확히 1인 정상일
            already_tolerance = max(0.0002, abs(adj_prev) * 1e-7)
            if prev_adj and abs(float(prev_adj) - adj_prev) <= already_tolerance:
                continue  # 이미 반영됨 (재실행)
            cur.execute(
                "UPDATE price_daily SET adj_close = adj_close * %s "
                "WHERE asset_id=%s AND source='KRX' AND trade_date < %s",
                (k, int(aid), target_date),
            )
            print(f"[prices] 조정이벤트 asset_id={int(aid)} k={k:.4f} → 과거 adj_close {cur.rowcount}행 소급조정")
            fixed += 1
    return fixed


def _verify_adj_close_post_publish(
    conn,
    candidates: pd.DataFrame,
    target_date: date,
) -> None:
    """commit 전 RDS 수정종가의 최신 scale과 전일 연속성을 검증한다."""
    if candidates.empty:
        return
    asset_ids = sorted({
        int(value)
        for value in candidates["asset_id"].dropna().tolist()
    })
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT asset_id, close, adj_close
            FROM price_daily
            WHERE source='KRX' AND trade_date=%s
              AND asset_id = ANY(%s)
            """,
            (target_date, asset_ids),
        )
        current = {
            int(asset_id): (float(close), float(adj_close))
            for asset_id, close, adj_close in cur.fetchall()
        }
        cur.execute(
            """
            SELECT DISTINCT ON (asset_id)
                   asset_id, trade_date, close, adj_close
            FROM price_daily
            WHERE source='KRX' AND trade_date < %s
              AND asset_id = ANY(%s)
            ORDER BY asset_id, trade_date DESC
            """,
            (target_date, asset_ids),
        )
        previous = {
            int(asset_id): (trade_date, float(close), float(adj_close))
            for asset_id, trade_date, close, adj_close in cur.fetchall()
        }
        cur.execute(
            """
            SELECT DISTINCT asset_id
            FROM price_daily
            WHERE source='KRX' AND trade_date > %s
              AND asset_id = ANY(%s)
            """,
            (target_date, asset_ids),
        )
        has_future = {int(row[0]) for row in cur.fetchall()}

    failures: list[dict] = []
    for row in candidates.itertuples(index=False):
        asset_id = int(row.asset_id)
        stored = current.get(asset_id)
        if stored is None:
            failures.append({
                "asset_id": asset_id,
                "reason": "target row missing after publish",
            })
            continue
        stored_close, stored_adj_close = stored
        if asset_id not in previous:
            if asset_id in has_future and row.asset_type == "stock":
                failures.append({
                    "asset_id": asset_id,
                    "reason": "historical insertion has no previous adjustment anchor",
                })
            elif abs(stored_adj_close - stored_close) > max(
                0.0002,
                abs(stored_close) * 1e-7,
            ):
                failures.append({
                    "asset_id": asset_id,
                    "reason": "latest first row must have adj_close=close",
                    "close": stored_close,
                    "adj_close": stored_adj_close,
                })
            continue

        previous_date, _, previous_adj_close = previous[asset_id]
        if (target_date - previous_date).days > LISTING_EPISODE_GAP_DAYS:
            if abs(stored_adj_close - stored_close) > max(
                0.0002,
                abs(stored_close) * 1e-7,
            ):
                failures.append({
                    "asset_id": asset_id,
                    "reason": "listing episode first row must have adj_close=close",
                    "close": stored_close,
                    "adj_close": stored_adj_close,
                })
            continue
        if pd.isna(row.prev_diff):
            failures.append({
                "asset_id": asset_id,
                "reason": "prev_diff missing",
            })
            continue
        reference_price = stored_close - float(row.prev_diff)
        if reference_price <= 0 or previous_adj_close <= 0:
            failures.append({
                "asset_id": asset_id,
                "reason": "invalid reference or previous adjusted close",
                "reference_price": reference_price,
                "previous_adj_close": previous_adj_close,
            })
            continue
        actual_return = stored_adj_close / previous_adj_close - 1
        expected_return = float(row.prev_diff) / reference_price
        if abs(actual_return - expected_return) > 0.0005:
            failures.append({
                "asset_id": asset_id,
                "reason": "adjusted return differs from KRX reference return",
                "actual_return": actual_return,
                "expected_return": expected_return,
                "previous_adj_close": previous_adj_close,
                "adj_close": stored_adj_close,
            })

    if failures:
        raise RuntimeError(
            "ADJ_CLOSE_POST_PUBLISH failed: "
            f"count={len(failures)}, samples={failures[:20]}"
        )
    print(
        f"[prices] ADJ_CLOSE_POST_PUBLISH pass rows={len(candidates)}",
        flush=True,
    )


def _read_index(
    base: str,
    start_date: date | None = None,
    end_date: date | None = None,
) -> pd.DataFrame:
    """벤치마크 지수(코스피200·코스닥150) → 종목시세와 같은 스키마. adj_close=close."""
    frames = []
    for f in _paths_in_period(
        f"{base}/index/krxapi/date=*/*.parquet",
        start_date,
        end_date,
    ):
        df = pd.read_parquet(f)
        df["_source_file"] = f
        frames.append(df)
    if not frames:
        empty = pd.DataFrame()
        empty.attrs["input_rows"] = 0
        return empty
    raw = pd.concat(frames, ignore_index=True)
    x = raw[raw["IDX_NM"].isin(BENCHMARKS)].copy()
    code = x["IDX_NM"].map(lambda n: BENCHMARKS[n][1])
    close = _num(x["CLSPRC_IDX"])
    out = pd.DataFrame({
        "code": code, "trade_date": pd.to_datetime(x["BAS_DD"]).dt.date,
        "open": _num(x["OPNPRC_IDX"]), "high": _num(x["HGPRC_IDX"]),
        "low": _num(x["LWPRC_IDX"]), "close": close, "adj_close": close,
        "volume": _num(x["ACC_TRDVOL"]), "trading_value": _num(x["ACC_TRDVAL"]),
        "shares": np.nan, "market_cap": _num(x["MKTCAP"]),
        "market": None,  # 지수는 어느 시장에 '상장'된 게 아니라 NULL
        "prev_diff": _num(x["CMPPREVDD_IDX"]),
        "fluc_rate": _num(x["FLUC_RT"]),
        "source_file": x["_source_file"],
    })
    out.attrs["input_rows"] = len(raw)
    return out


def prepare(
    base: str,
    target_date: date | None = None,
    *,
    start_date: date | None = None,
    end_date: date | None = None,
) -> tuple[pd.DataFrame, dict]:
    """Bronze를 price 후보로 변환한다. 중복과 invalid row를 제거하지 않는다."""
    if target_date is not None:
        start_date = target_date
        end_date = target_date
    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValueError("start_date must be on or before end_date")
    source_files = (
        _paths_in_period(
            f"{base}/stock/marcap/date=*/all.parquet",
            start_date,
            end_date,
        )
        + _paths_in_period(
            f"{base}/stock/krxapi/date=*/*.parquet",
            start_date,
            end_date,
        )
        + _paths_in_period(
            f"{base}/index/krxapi/date=*/*.parquet",
            start_date,
            end_date,
        )
    )
    stock = pd.concat(
        [
            _read_marcap(base, start_date, end_date),
            _read_krxapi(base, start_date, end_date),
        ],
        ignore_index=True,
    )
    stock_input_rows = len(stock)
    nonpositive_price = {"row_count": 0, "ticker_count": 0, "samples": []}
    if not stock.empty:
        stock, nonpositive_price = _exclude_nonpositive_prices(stock)
        stock = _normalize_incomplete_ohlc(stock)
        stock = _with_adj_close(stock)
        if target_date is not None:
            stock = stock[stock["trade_date"] == target_date]
    else:
        stock = pd.DataFrame(columns=[
            "ticker", "trade_date", "open", "high", "low", "close", "adj_close",
            "volume", "trading_value", "shares", "market_cap", "market",
            "prev_diff", "fluc_rate", "source_file",
        ])
    stock["identifier"] = stock["ticker"].astype(str)
    stock["asset_type"] = "stock"
    # 지수
    idx = _read_index(base, start_date, end_date)
    index_input_rows = int(idx.attrs.get("input_rows", len(idx)))
    if not idx.empty:
        if target_date is not None:
            idx = idx[idx["trade_date"] == target_date]
        idx = _normalize_incomplete_ohlc(idx)
        idx["identifier"] = idx["code"].astype(str)
        idx["asset_type"] = "index"
    both = pd.concat([stock, idx], ignore_index=True)
    if both.empty:
        return both, {
            "input_rows": stock_input_rows + index_input_rows,
            "transformed_rows": 0,
            "excluded_rows": stock_input_rows + index_input_rows,
            "rejected_rows": 0,
            "source_file_count": len(source_files),
        }
    both["source"] = "KRX"
    for c in ("volume", "shares"):  # BIGINT — float 표현 방지 (nullable int)
        both[c] = pd.to_numeric(both[c], errors="coerce").round().astype("Int64")
    columns = [
        "identifier", "source", "trade_date", "open", "high", "low", "close",
        "adj_close", "volume", "trading_value", "shares", "market_cap", "market",
        "asset_type", "prev_diff", "fluc_rate", "source_file",
    ]
    both = both[columns]
    both, unsupported_market = _exclude_unsupported_markets(both)
    stats = {
        "input_rows": stock_input_rows + index_input_rows,
        "transformed_rows": len(both),
        "excluded_rows": stock_input_rows + index_input_rows - len(both),
        "rejected_rows": 0,
        "source_file_count": len(source_files),
        "unsupported_market": unsupported_market,
        "nonpositive_price": nonpositive_price,
    }
    return both, stats


def publish(
    conn,
    candidates: pd.DataFrame,
    krx_map: dict[str, int],
    quality_run_id: UUID,
    target_date: date | None = None,
) -> int:
    if candidates.empty:
        return 0
    both = candidates.copy()
    both["asset_id"] = both["identifier"].map(krx_map)
    if both["asset_id"].isna().any():
        missing = sorted(both.loc[both["asset_id"].isna(), "identifier"].astype(str).unique())
        raise RuntimeError(f"quality gate missed unmapped price identifiers: {missing[:20]}")
    both["asset_id"] = both["asset_id"].astype("int64")
    if "total_return_close" not in both:
        both["total_return_close"] = both["adj_close"]
    if "currency" not in both:
        both["currency"] = "KRW"
    if "vwap" not in both:
        both["vwap"] = None
    if "available_at" not in both:
        both["available_at"] = (
            pd.to_datetime(both["trade_date"])
            .dt.tz_localize("Asia/Seoul")
            + pd.Timedelta(days=1, hours=8, minutes=30)
        )
    both["quality_run_id"] = quality_run_id
    # This load certifies raw price fields only.  The derived total-return
    # placeholder is deliberately left without derived lineage until rebuild.
    both["total_return_quality_run_id"] = None
    both["total_return_loaded_at"] = None
    rows = list(
        both[COLS].astype(object).where(pd.notna(both[COLS]), None)
        .itertuples(index=False, name=None)
    )
    published_krx_stock = (
        "asset_type" in both
        and "source" in both
        and (
            both["asset_type"].eq("stock")
            & both["source"].eq("KRX")
        ).any()
    )
    if published_krx_stock:
        acquire_return_writer_transaction_lock(conn)
    n = db.upsert(conn, "price_daily", COLS, rows,
                  conflict=["asset_id", "source", "trade_date"],
                  update=["open", "high", "low", "close", "adj_close",
                          "total_return_close", "currency", "vwap", "available_at",
                          "volume", "trading_value", "shares", "market_cap", "market",
                          "quality_run_id", "total_return_quality_run_id",
                          "total_return_loaded_at", "loaded_at"],
                  temp_name="_stg_price_publish")
    print(f"[prices] price_daily upsert {n}행")

    if target_date is not None:
        stock = both[both["asset_type"] == "stock"]
        n_ev = _rescale_history_for_events(conn, stock, target_date)
        if n_ev:
            print(f"[prices] 조정이벤트 {n_ev}종목 소급조정 완료")
        _verify_adj_close_post_publish(conn, both, target_date)
    if n > 0 and published_krx_stock:
        invalidate_krx_total_return(
            conn,
            reason="KRX_PRICE_PUBLISHED",
            quality_run_id=quality_run_id,
        )
    return n


def run(conn, base: str, krx_map: dict[str, int], target_date: date | None = None,
        quality_run_id: UUID | None = None) -> None:
    """호환 wrapper. quality_run_id 없이 직접 적재할 수 없다."""
    if quality_run_id is None:
        raise RuntimeError("prices.run requires quality_run_id; use silver.load")
    candidates, _ = prepare(base, target_date)
    publish(conn, candidates, krx_map, quality_run_id, target_date)
