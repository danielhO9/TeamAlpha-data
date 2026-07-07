"""KRX raw 데이터 수집 (pykrx).

원칙: **가공하지 않는다.** pykrx 가 준 DataFrame(원본 컬럼명·인덱스·dtype)을 그대로 반환한다.
컬럼 리네임·행 필터·표준지표 매핑은 전부 silver 단계의 몫. 여기선 오직 "받아서 넘김"만.

pykrx 는 KRX 웹을 긁는 방식이라 연속 호출 시 빈 응답이 오기도 해서, 간단한 재시도를 감쌌다.
"""
from __future__ import annotations

import contextlib
import io
import time

import pandas as pd


def _load_stock():
    # pykrx import 시 나오는 로그인 관련 출력만 억제 (동작에는 영향 없음)
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        from pykrx import stock
    return stock


stock = _load_stock()

MARKETS = ["KOSPI", "KOSDAQ"]
# 벤치마크 지수: 이름 -> KRX 지수코드
INDEX_CODES = {"KOSPI200": "1028", "KOSDAQ150": "2203"}


def _retry_df(fn, tries: int = 4, base_delay: float = 1.5) -> pd.DataFrame:
    """빈 응답/에러 시 지수백오프로 재시도. 끝까지 실패하면 마지막 예외를 올린다."""
    last_err: Exception | None = None
    for attempt in range(tries):
        try:
            df = fn()
            if df is not None and len(df) > 0:
                return df
        except Exception as exc:  # noqa: BLE001
            last_err = exc
        time.sleep(base_delay * (attempt + 1))
    if last_err is not None:
        raise last_err
    return pd.DataFrame()


# --- 현 스키마에 필요한 4종 (전부 raw 원본 반환) ---

def fetch_ohlcv(date: str, market: str) -> pd.DataFrame:
    """주식 일봉 OHLCV — 날짜 스냅샷(그 날짜 전 종목). index=티커.
    컬럼: 시가/고가/저가/종가/거래량/거래대금/등락률"""
    return _retry_df(lambda: stock.get_market_ohlcv_by_ticker(date, market=market))


def fetch_market_cap(date: str, market: str) -> pd.DataFrame:
    """시가총액/상장주식수 — 날짜 스냅샷. index=티커.
    컬럼: 종가/시가총액/거래량/거래대금/상장주식수"""
    return _retry_df(lambda: stock.get_market_cap_by_ticker(date, market=market))


def fetch_index_ohlcv(date: str, index_code: str) -> pd.DataFrame:
    """지수 일봉 OHLCV — 해당 날짜 1행. index=날짜.
    컬럼: 시가/고가/저가/종가/거래량/거래대금/상장시가총액"""
    return _retry_df(lambda: stock.get_index_ohlcv_by_date(date, date, index_code))


def fetch_index_members(date: str, index_code: str) -> pd.DataFrame:
    """지수 구성종목 — 티커 리스트. raw 는 list 라서 최소 형태(1컬럼 DataFrame)로 감싼다."""
    def _call():
        tickers = stock.get_index_portfolio_deposit_file(index_code, date)
        return pd.DataFrame({"ticker": tickers})
    return _retry_df(_call)


def trading_days(fromdate: str, todate: str) -> list[str]:
    """[fromdate, todate] 사이 거래일 목록(YYYYMMDD). KOSPI200 지수 시세로 도출(거래일만 반환)."""
    df = _retry_df(lambda: stock.get_index_ohlcv_by_date(fromdate, todate, INDEX_CODES["KOSPI200"]))
    return [d.strftime("%Y%m%d") for d in df.index]


# --- by_date (종목/지수 고정, 기간) — by_ticker 스냅샷이 안 되는 옛날 구간 백필용. index=날짜 ---

def fetch_ohlcv_history(fromdate: str, todate: str, ticker: str) -> pd.DataFrame:
    """한 종목 기간 OHLCV. 컬럼: 시가/고가/저가/종가/거래량/등락률 (by_ticker 와 달리 거래대금·시총 없음)."""
    return _retry_df(lambda: stock.get_market_ohlcv_by_date(fromdate, todate, ticker))


def fetch_market_cap_history(fromdate: str, todate: str, ticker: str) -> pd.DataFrame:
    """한 종목 기간 시총/주식수. 컬럼: 시가총액/거래량/거래대금/상장주식수."""
    return _retry_df(lambda: stock.get_market_cap_by_date(fromdate, todate, ticker))


def fetch_index_ohlcv_history(fromdate: str, todate: str, index_code: str) -> pd.DataFrame:
    """한 지수 기간 OHLCV. index=날짜."""
    return _retry_df(lambda: stock.get_index_ohlcv_by_date(fromdate, todate, index_code))
