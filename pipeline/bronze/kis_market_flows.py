"""KIS Open API 투자자 수급·공매도 거래흐름을 immutable Bronze로 수집한다.

이 수집기는 한국투자증권이 공개한 두 공식 REST API만 호출한다.

* 종목별 투자자매매동향(일별):
  ``/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily``
* 국내주식 공매도 일별추이:
  ``/uapi/domestic-stock/v1/quotations/daily-short-sale``

공매도 데이터는 *순보유잔고*나 *대차잔고*가 아니라 실제 공매도 체결수량·
거래대금·비중이다. 응답 JSON bytes를 변형하지 않고 저장하고, 요청 범위와
응답 coverage를 별도 manifest로 고정한다. appkey/appsecret과 access token은
어떤 파일에도 기록하지 않는다.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any

import requests

from pipeline.common.paths import base_uri
from pipeline.common.sink import read_bytes, write_bytes, write_text_if_changed

API_ROOT = "https://openapi.koreainvestment.com:9443"
TOKEN_PATH = "/oauth2/tokenP"
INVESTOR_PATH = (
    "/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily"
)
SHORT_SALE_PATH = "/uapi/domestic-stock/v1/quotations/daily-short-sale"
INVESTOR_TR_ID = "FHPTJ04160001"
SHORT_SALE_TR_ID = "FHPST04830000"
SOURCE = "KIS_SECURITIES_OPEN_API"
REQUEST_INTERVAL_SECONDS = 1.0
RATE_LIMIT_RETRIES = 3
SOURCE_REFERENCES = {
    "investor-flow": (
        "https://github.com/koreainvestment/open-trading-api/blob/main/"
        "examples_llm/domestic_stock/investor_trade_by_stock_daily/"
        "investor_trade_by_stock_daily.py"
    ),
    "short-sale": (
        "https://github.com/koreainvestment/open-trading-api/blob/main/"
        "examples_llm/domestic_stock/daily_short_sale/daily_short_sale.py"
    ),
}
TICKER_RE = re.compile(r"^[0-9A-Z]{6}$")
DATE_RE = re.compile(r"^[0-9]{8}$")
INVESTOR_CATEGORY_PREFIXES = {
    "foreign": ("frgn_",),
    "individual": ("prsn_",),
    "institution_total": ("orgn_",),
    "securities": ("scrt_",),
    "investment_trust": ("ivtr_",),
    "private_fund": ("pe_fund_",),
    "bank": ("bank_",),
    "insurance": ("insu_",),
    "merchant_bank": ("mrbn_",),
    "pension_fund": ("fund_",),
    "other": ("etc_",),
}


def _credential(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(
            f"{name} is required; issue an appkey/appsecret for the user's "
            "own KIS Open API account"
        )
    return value


def _date(value: str, name: str) -> str:
    if DATE_RE.fullmatch(value) is None:
        raise ValueError(f"{name} must be YYYYMMDD")
    datetime.strptime(value, "%Y%m%d")
    return value


def _ticker(value: str) -> str:
    rendered = value.strip().upper()
    if TICKER_RE.fullmatch(rendered) is None:
        raise ValueError(f"invalid KRX ticker: {value!r}")
    return rendered


def issue_token(session: requests.Session | None = None) -> str:
    """Issue one 24-hour KIS REST access token without persisting credentials."""
    client = session or requests.Session()
    appkey = _credential("KIS_APP_KEY")
    appsecret = _credential("KIS_APP_SECRET")
    response = client.post(
        f"{API_ROOT}{TOKEN_PATH}",
        headers={"Content-Type": "application/json; charset=UTF-8"},
        json={
            "grant_type": "client_credentials",
            "appkey": appkey,
            "appsecret": appsecret,
        },
        timeout=30,
    )
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError(
            f"KIS token response is not JSON: status={response.status_code}"
        ) from exc
    if response.status_code != 200 or not payload.get("access_token"):
        code = payload.get("error_code") or payload.get("rt_cd") or "UNKNOWN"
        message = payload.get("error_description") or payload.get("msg1") or ""
        raise RuntimeError(
            f"KIS token issuance failed: status={response.status_code}, "
            f"code={code}, message={message}"
        )
    return str(payload["access_token"])


def _request(
    *,
    session: requests.Session,
    access_token: str,
    path: str,
    tr_id: str,
    params: dict[str, str],
) -> tuple[bytes, dict[str, Any], dict[str, str]]:
    appkey = _credential("KIS_APP_KEY")
    appsecret = _credential("KIS_APP_SECRET")
    for attempt in range(RATE_LIMIT_RETRIES + 1):
        response = session.get(
            f"{API_ROOT}{path}",
            headers={
                "Content-Type": "application/json; charset=UTF-8",
                "authorization": f"Bearer {access_token}",
                "appkey": appkey,
                "appsecret": appsecret,
                "tr_id": tr_id,
                "custtype": "P",
                "tr_cont": "",
            },
            params=params,
            timeout=30,
        )
        raw = response.content
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"KIS data response is not JSON: status={response.status_code}"
            ) from exc
        if response.status_code == 200 and str(payload.get("rt_cd")) == "0":
            break
        if (
            payload.get("msg_cd") == "EGW00201"
            and attempt < RATE_LIMIT_RETRIES
        ):
            time.sleep(REQUEST_INTERVAL_SECONDS * (attempt + 1))
            continue
        raise RuntimeError(
            "KIS data request failed: "
            f"status={response.status_code}, code={payload.get('msg_cd')}, "
            f"message={payload.get('msg1')}"
        )
    public_headers = {
        name: value
        for name, value in response.headers.items()
        if name.lower() in {"tr_cont", "content-type", "date"}
    }
    return raw, payload, public_headers


def _rows(payload: dict[str, Any], *, dataset: str) -> list[dict[str, Any]]:
    rows = payload.get("output2")
    if not isinstance(rows, list) or not rows:
        raise RuntimeError(f"KIS {dataset} response has no output2 rows")
    if not all(isinstance(row, dict) for row in rows):
        raise RuntimeError(f"KIS {dataset} output2 is not a row list")
    required = {"stck_bsop_date"}
    if dataset == "investor-flow":
        alternatives = {
            "frgn_ntby_qty", "orgn_ntby_qty", "prsn_ntby_qty",
            "frgn_ntby_tr_pbmn", "orgn_ntby_tr_pbmn", "prsn_ntby_tr_pbmn",
        }
    else:
        alternatives = {"ssts_cntg_qty", "ssts_tr_pbmn", "ssts_vol_rlim"}
    columns = set().union(*(row.keys() for row in rows))
    if not required.issubset(columns) or not alternatives.intersection(columns):
        raise RuntimeError(
            f"KIS {dataset} response shape changed: columns={sorted(columns)}"
        )
    return rows


def _coverage(
    rows: list[dict[str, Any]], *, dataset: str,
) -> dict[str, Any]:
    columns = sorted(set().union(*(row.keys() for row in rows)))
    dates = sorted({
        str(row.get("stck_bsop_date"))
        for row in rows
        if DATE_RE.fullmatch(str(row.get("stck_bsop_date") or ""))
    })
    if dataset == "investor-flow":
        categories = [
            category
            for category, prefixes in INVESTOR_CATEGORY_PREFIXES.items()
            if any(column.startswith(prefixes) for column in columns)
        ]
    else:
        categories = ["executed_short_sale"]
    return {
        "row_count": len(rows),
        "asset_count": 1,
        "date_min": dates[0] if dates else None,
        "date_max": dates[-1] if dates else None,
        "categories": categories,
        "column_names": columns,
    }


def _save(
    *,
    raw: bytes,
    dest: str,
    dataset: str,
    ticker: str,
    endpoint: str,
    tr_id: str,
    params: dict[str, str],
    response_headers: dict[str, str],
    coverage: dict[str, Any],
    fetched_at: datetime,
) -> str:
    digest = hashlib.sha256(raw).hexdigest()
    root = (
        f"{base_uri(dest)}/market_flows/kis/dataset={dataset}/"
        f"ticker={ticker}/sha256={digest}"
    )
    object_uri = f"{root}/response.json"
    manifest_uri = f"{root}/manifest.json"
    manifest = {
        "schema_version": "kis-market-flow-response-v1",
        "source": SOURCE,
        "source_reference": SOURCE_REFERENCES[dataset],
        "dataset": dataset,
        "endpoint": endpoint,
        "tr_id": tr_id,
        "request_params": params,
        "response_headers": response_headers,
        "ticker": ticker,
        "sha256": digest,
        "object_uri": object_uri,
        "fetched_at": fetched_at.isoformat(),
        "coverage": coverage,
        "pagination": {
            "tr_cont": response_headers.get("tr_cont", ""),
            "response_complete": response_headers.get("tr_cont", "")
            not in {"M", "F"},
        },
        "availability_contract": {
            "observed_at": "fetched_at",
            "row_level_available_at": "not_supplied_by_endpoint",
            "pit_use": "initial_observation_only_until_history_policy_is_certified",
        },
        "units": (
            {"volume": "shares", "value": "KRW_million"}
            if dataset == "investor-flow"
            else {"volume": "shares", "value": "KRW", "ratio": "percent"}
        ),
        "semantic_note": (
            "investor-category executed buy/sell/net flow"
            if dataset == "investor-flow"
            else "executed short-sale flow; not short balance or stock-loan balance"
        ),
    }
    existing = read_bytes(object_uri)
    if existing is not None and hashlib.sha256(existing).hexdigest() != digest:
        raise RuntimeError(f"immutable KIS Bronze body mismatch: {object_uri}")
    existing_manifest = read_bytes(manifest_uri)
    if existing_manifest is not None:
        previous = json.loads(existing_manifest.decode("utf-8"))
        immutable_fields = (
            "schema_version", "source", "dataset", "endpoint", "tr_id",
            "source_reference", "request_params", "ticker", "sha256",
            "object_uri", "coverage", "pagination", "availability_contract",
            "units", "semantic_note",
        )
        if any(previous.get(key) != manifest.get(key) for key in immutable_fields):
            raise RuntimeError(f"immutable KIS Bronze manifest mismatch: {manifest_uri}")
        if existing is None:
            raise RuntimeError(f"immutable KIS Bronze body missing: {object_uri}")
    else:
        write_bytes(raw, object_uri)
        write_text_if_changed(
            json.dumps(manifest, ensure_ascii=False, sort_keys=True), manifest_uri,
        )
    return object_uri


def collect_investor_flow(
    ticker: str,
    as_of: str,
    dest: str,
    *,
    session: requests.Session | None = None,
    access_token: str | None = None,
) -> dict[str, Any]:
    ticker = _ticker(ticker)
    as_of = _date(as_of, "as_of")
    client = session or requests.Session()
    token = access_token or issue_token(client)
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": ticker,
        "FID_INPUT_DATE_1": as_of,
        "FID_ORG_ADJ_PRC": "",
        "FID_ETC_CLS_CODE": "",
    }
    raw, payload, headers = _request(
        session=client,
        access_token=token,
        path=INVESTOR_PATH,
        tr_id=INVESTOR_TR_ID,
        params=params,
    )
    rows = _rows(payload, dataset="investor-flow")
    fetched_at = datetime.now(timezone.utc)
    coverage = _coverage(rows, dataset="investor-flow")
    uri = _save(
        raw=raw, dest=dest, dataset="investor-flow", ticker=ticker,
        endpoint=INVESTOR_PATH, tr_id=INVESTOR_TR_ID, params=params,
        response_headers=headers, coverage=coverage, fetched_at=fetched_at,
    )
    return {"uri": uri, "ticker": ticker, **coverage}


def collect_short_sale(
    ticker: str,
    from_date: str,
    to_date: str,
    dest: str,
    *,
    session: requests.Session | None = None,
    access_token: str | None = None,
) -> dict[str, Any]:
    ticker = _ticker(ticker)
    from_date = _date(from_date, "from_date")
    to_date = _date(to_date, "to_date")
    if from_date > to_date:
        raise ValueError("from_date must be on or before to_date")
    client = session or requests.Session()
    token = access_token or issue_token(client)
    params = {
        "FID_COND_MRKT_DIV_CODE": "J",
        "FID_INPUT_ISCD": ticker,
        "FID_INPUT_DATE_1": from_date,
        "FID_INPUT_DATE_2": to_date,
    }
    raw, payload, headers = _request(
        session=client,
        access_token=token,
        path=SHORT_SALE_PATH,
        tr_id=SHORT_SALE_TR_ID,
        params=params,
    )
    rows = _rows(payload, dataset="short-sale")
    fetched_at = datetime.now(timezone.utc)
    coverage = _coverage(rows, dataset="short-sale")
    uri = _save(
        raw=raw, dest=dest, dataset="short-sale", ticker=ticker,
        endpoint=SHORT_SALE_PATH, tr_id=SHORT_SALE_TR_ID, params=params,
        response_headers=headers, coverage=coverage, fetched_at=fetched_at,
    )
    return {"uri": uri, "ticker": ticker, **coverage}


def run(
    *,
    dataset: str,
    tickers: list[str],
    dest: str,
    as_of: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> list[dict[str, Any]]:
    client = requests.Session()
    token = issue_token(client)
    results = []
    for index, ticker in enumerate(tickers):
        if dataset == "investor-flow":
            if not as_of:
                raise ValueError("as_of is required for investor-flow")
            result = collect_investor_flow(
                ticker, as_of, dest, session=client, access_token=token,
            )
        else:
            if not from_date or not to_date:
                raise ValueError("from_date and to_date are required for short-sale")
            result = collect_short_sale(
                ticker, from_date, to_date, dest,
                session=client, access_token=token,
            )
        results.append(result)
        print(
            "[kis-market-flow] "
            + json.dumps(result, ensure_ascii=False, sort_keys=True),
            flush=True,
        )
        if index + 1 < len(tickers):
            time.sleep(REQUEST_INTERVAL_SECONDS)
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset", choices=("investor-flow", "short-sale"), required=True,
    )
    parser.add_argument(
        "--tickers", required=True,
        help="comma-separated six-character KRX tickers",
    )
    parser.add_argument("--as-of", help="investor-flow anchor date YYYYMMDD")
    parser.add_argument("--from", dest="from_date", help="YYYYMMDD")
    parser.add_argument("--to", dest="to_date", help="YYYYMMDD")
    parser.add_argument("--dest", choices=("local", "s3"), default="local")
    args = parser.parse_args()
    run(
        dataset=args.dataset,
        tickers=[value for value in args.tickers.split(",") if value.strip()],
        dest=args.dest,
        as_of=args.as_of,
        from_date=args.from_date,
        to_date=args.to_date,
    )


if __name__ == "__main__":
    main()
