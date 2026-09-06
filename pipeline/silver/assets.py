"""asset + asset_identifier 후보 생성 및 publish (bronze → silver).

종목 유니버스: marcap + krxapi 전 날짜 union(상폐 포함) → 종목별 asset + KRX 티커 identifier.
DART corp_code: bronze corpCode.xml 로 티커→corp_code 매핑해 DART identifier 도 기록(있으면).
지수: 벤치마크(코스피200·코스닥150)를 asset_type='index' 로 등록, KRX 지수코드 identifier.

후보 생성 단계는 DB를 변경하지 않는다. publish는 상위 quality transaction 안에서 실행한다.
"""
from __future__ import annotations

import glob
import re
from datetime import date

import pandas as pd
from uuid import UUID

from pipeline.bronze import financials

# IDX_NM → (asset name, KRX 지수코드)
BENCHMARKS = {"코스피 200": ("KOSPI200", "1028"), "코스닥 150": ("KOSDAQ150", "2203")}
_PREFERRED_SUFFIX = re.compile(r"(?:\d*우(?:[A-Z])?|우선주)$")


def _stock_universe(
    base: str,
    target_date: date | None = None,
) -> dict[str, str]:
    """Return the daily partition, or the full historical universe for backfill."""
    names: dict[str, str] = {}
    if target_date is not None:
        pattern = f"{base}/stock/krxapi/date={target_date.isoformat()}/*.parquet"
        paths = sorted(glob.glob(pattern))
        if not paths:
            available = sorted(glob.glob(f"{base}/stock/krxapi/date=*/*.parquet"))
            if available:
                latest_partition = max(
                    re.search(r"/date=(\d{4}-\d{2}-\d{2})/", path).group(1)
                    for path in available
                    if re.search(r"/date=(\d{4}-\d{2}-\d{2})/", path)
                )
                paths = [
                    path for path in available
                    if f"/date={latest_partition}/" in path.replace("\\", "/")
                ]
        for path in paths:
            frame = pd.read_parquet(path, columns=["ISU_CD", "ISU_NM"])
            names.update(zip(
                frame["ISU_CD"].astype(str), frame["ISU_NM"].astype(str),
            ))
        return names
    for f in sorted(glob.glob(f"{base}/stock/marcap/date=*/all.parquet")):
        df = pd.read_parquet(f, columns=["Code", "Name"])
        names.update(zip(df["Code"].astype(str), df["Name"].astype(str)))
    for f in sorted(glob.glob(f"{base}/stock/krxapi/date=*/*.parquet")):
        df = pd.read_parquet(f, columns=["ISU_CD", "ISU_NM"])
        names.update(zip(df["ISU_CD"].astype(str), df["ISU_NM"].astype(str)))
    return names


def prepare(
    base: str,
    target_date: date | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Bronze에서 asset/identifier 후보를 만든다.

    natural_key는 현재 KRX ticker다. identifier 충돌은 quality rule과 publish 양쪽에서 차단한다.
    """
    names = _stock_universe(base, target_date=target_date)
    corp = {sc: cc for cc, sc in financials.load_listed_corps_from_bronze(base)}  # ticker→corp_code

    asset_rows = [
        {
            "natural_key": ticker,
            "name": name,
            "asset_type": "stock",
            "instrument_type": (
                "preferred_stock" if _PREFERRED_SUFFIX.search(name) else "common_stock"
            ),
            "exchange": "KRX",
            "currency": "KRW",
            "country_code": "KR",
            "base_currency": "KRW",
            "listed_from": None,
            "listed_to": None,
            "source_file": None,
        }
        for ticker, name in names.items()
    ]
    identifier_rows = [
        {
            "natural_key": ticker,
            "source": "KRX",
            "identifier": ticker,
            "identifier_type": "ticker",
            "valid_from": date.min,
            "valid_to": None,
            "source_file": None,
        }
        for ticker in names
    ]
    identifier_rows.extend(
        {
            "natural_key": ticker,
            "source": "DART",
            "identifier": corp_code,
            "identifier_type": "corp_code",
            "valid_from": date.min,
            "valid_to": None,
            "source_file": "financials/dart/corpCode.xml",
        }
        for ticker, corp_code in corp.items()
        if ticker in names
    )
    for _, (name, code) in BENCHMARKS.items():
        asset_rows.append({
            "natural_key": code,
            "name": name,
            "asset_type": "index",
            "instrument_type": "index",
            "exchange": "KRX",
            "currency": "KRW",
            "country_code": "KR",
            "base_currency": "KRW",
            "listed_from": None,
            "listed_to": None,
            "source_file": None,
        })
        identifier_rows.append({
            "natural_key": code,
            "source": "KRX",
            "identifier": code,
            "identifier_type": "ticker",
            "valid_from": date.min,
            "valid_to": None,
            "source_file": None,
        })
    return pd.DataFrame(asset_rows), pd.DataFrame(identifier_rows)


def restrict_to_price_universe(
    asset_candidates: pd.DataFrame,
    identifier_candidates: pd.DataFrame,
    supported_price_identifiers: set[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """지원 시장 가격이 있는 주식과 벤치마크 지수만 후보로 유지한다."""
    supported = {str(value) for value in supported_price_identifiers}
    keep = (
        asset_candidates["asset_type"].eq("index")
        | asset_candidates["natural_key"].astype(str).isin(supported)
    )
    retained_assets = asset_candidates[keep].reset_index(drop=True)
    retained_keys = set(retained_assets["natural_key"].astype(str))
    retained_identifiers = identifier_candidates[
        identifier_candidates["natural_key"].astype(str).isin(retained_keys)
    ].reset_index(drop=True)
    return retained_assets, retained_identifiers


def preferred_share_issuer_map(
    asset_candidates: pd.DataFrame,
) -> dict[str, str]:
    """우선주 KRX ticker를 동일 이름의 보통주 ticker에 결정적으로 연결한다.

    DART corpCode는 통상 보통주 코드만 제공한다. 이름에서 명확한 우선주
    접미사를 제거한 결과가 정확히 하나의 보통주명과 일치할 때만 상속한다.
    모호하거나 보통주가 없는 경우에는 추정하지 않는다.
    """
    if asset_candidates.empty:
        return {}
    stocks = asset_candidates[
        asset_candidates["asset_type"].eq("stock")
    ][["natural_key", "name"]].copy()
    stocks["natural_key"] = stocks["natural_key"].astype(str)
    stocks["name"] = stocks["name"].astype(str).str.strip()
    common_by_name: dict[str, list[str]] = {}
    for row in stocks.itertuples(index=False):
        if not _PREFERRED_SUFFIX.search(row.name):
            common_by_name.setdefault(row.name, []).append(row.natural_key)

    mapping: dict[str, str] = {}
    for row in stocks.itertuples(index=False):
        base_name = _PREFERRED_SUFFIX.sub("", row.name).strip()
        if base_name == row.name:
            continue
        matches = common_by_name.get(base_name, [])
        if len(matches) == 1:
            mapping[row.natural_key] = matches[0]
    return mapping


def publish(
    conn,
    asset_candidates: pd.DataFrame,
    identifier_candidates: pd.DataFrame,
    quality_run_id: UUID,
) -> dict[str, int]:
    """후보를 현재 transaction에 반영하고 KRX identifier→asset_id를 반환한다."""
    asset_defaults = {
        "instrument_type": "unknown",
        "country_code": None,
        "base_currency": None,
        "listed_from": None,
        "listed_to": None,
    }
    identifier_defaults = {
        "identifier_type": "ticker",
        "valid_from": date.min,
        "valid_to": None,
    }
    asset_candidates = asset_candidates.copy()
    identifier_candidates = identifier_candidates.copy()
    for column, default in asset_defaults.items():
        if column not in asset_candidates:
            asset_candidates[column] = default
    asset_candidates["base_currency"] = asset_candidates[
        "base_currency"
    ].fillna(asset_candidates["currency"])
    for column, default in identifier_defaults.items():
        if column not in identifier_candidates:
            identifier_candidates[column] = default

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ai.identifier, ai.asset_id,
                   a.name, a.asset_type, a.instrument_type, a.exchange,
                   a.currency, a.country_code, a.base_currency,
                   a.listed_from, a.listed_to
            FROM asset_identifier ai
            JOIN asset a ON a.asset_id=ai.asset_id
            WHERE ai.source='KRX'
              AND ai.identifier_type='ticker'
              AND ai.valid_to IS NULL
            """
        )
        existing_assets = {
            str(row[0]): row[1:] for row in cur.fetchall()
        }
        krx_map = {
            identifier: int(values[0])
            for identifier, values in existing_assets.items()
        }
        natural_to_id: dict[str, int] = {}
        changed_assets = 0
        for row in asset_candidates.itertuples(index=False):
            natural_key = str(row.natural_key)
            if natural_key in krx_map:
                natural_to_id[natural_key] = krx_map[natural_key]
                existing = existing_assets[natural_key]
                desired = (
                    row.name, row.asset_type, row.instrument_type,
                    row.exchange, row.currency, row.country_code,
                    row.base_currency,
                    existing[8] if pd.isna(row.listed_from) else row.listed_from,
                    None if pd.isna(row.listed_to) else row.listed_to,
                )
                if tuple(existing[1:]) == desired:
                    continue
                cur.execute(
                    """
                    UPDATE asset
                    SET name=%s, asset_type=%s, instrument_type=%s,
                        exchange=%s, currency=%s, country_code=%s,
                        base_currency=%s, listed_from=COALESCE(%s, listed_from),
                        listed_to=%s,
                        quality_run_id=%s, loaded_at=now()
                    WHERE asset_id=%s
                    """,
                    (
                        row.name, row.asset_type, row.instrument_type,
                        row.exchange, row.currency, row.country_code,
                        row.base_currency, row.listed_from, row.listed_to,
                        quality_run_id, krx_map[natural_key],
                    ),
                )
                changed_assets += 1
                continue
            cur.execute(
                """
                INSERT INTO asset (
                    name, asset_type, instrument_type, exchange, currency,
                    country_code, base_currency, listed_from, listed_to,
                    quality_run_id, loaded_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
                RETURNING asset_id
                """,
                (
                    row.name, row.asset_type, row.instrument_type, row.exchange,
                    row.currency, row.country_code, row.base_currency,
                    row.listed_from, row.listed_to, quality_run_id,
                ),
            )
            aid = cur.fetchone()[0]
            natural_to_id[natural_key] = aid
            changed_assets += 1

        cur.execute(
            """
            SELECT source, identifier_type, identifier, asset_id,
                   valid_from, valid_to
            FROM asset_identifier
            WHERE valid_to IS NULL
            """
        )
        active_identifiers = {
            (str(source), str(identifier_type), str(identifier)): (
                int(asset_id), valid_from, valid_to,
            )
            for source, identifier_type, identifier, asset_id,
            valid_from, valid_to in cur.fetchall()
        }
        changed_identifiers = 0
        for row in identifier_candidates.itertuples(index=False):
            aid = natural_to_id.get(str(row.natural_key)) or krx_map.get(str(row.natural_key))
            if aid is None:
                continue
            identity = (
                str(row.source), str(row.identifier_type), str(row.identifier),
            )
            existing = active_identifiers.get(identity)
            if existing and existing[0] != aid:
                raise RuntimeError(
                    "identifier mapping conflict: "
                    f"source={row.source}, identifier={row.identifier}, "
                    f"existing_asset_id={existing[0]}, candidate_asset_id={aid}"
                )
            desired_valid_from = row.valid_from
            desired_valid_to = None if pd.isna(row.valid_to) else row.valid_to
            if existing == (aid, desired_valid_from, desired_valid_to):
                continue
            cur.execute(
                """
                INSERT INTO asset_identifier (
                    asset_id, source, identifier, identifier_type,
                    valid_from, valid_to, quality_run_id, loaded_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,now())
                ON CONFLICT (
                    asset_id, source, identifier_type, identifier, valid_from
                ) DO UPDATE SET
                    valid_to=EXCLUDED.valid_to,
                    quality_run_id=EXCLUDED.quality_run_id,
                    loaded_at=now()
                """,
                (
                    aid, row.source, str(row.identifier), row.identifier_type,
                    desired_valid_from, desired_valid_to, quality_run_id,
                ),
            )
            changed_identifiers += 1

        cur.execute(
            "SELECT identifier, asset_id FROM asset_identifier "
            "WHERE source='KRX' AND valid_to IS NULL"
        )
        krx_map = dict(cur.fetchall())
    print(
        f"[assets] candidates={len(asset_candidates)} "
        f"changed_assets={changed_assets} "
        f"changed_identifiers={changed_identifiers} "
        f"KRX identifiers={len(krx_map)}"
    )
    return krx_map


def build(conn, base: str, quality_run_id: UUID | None = None) -> dict[str, int]:
    """호환 wrapper. 새 적재 코드는 prepare/publish를 직접 사용한다."""
    if quality_run_id is None:
        raise RuntimeError("assets.build requires quality_run_id; use silver.load")
    a, i = prepare(base)
    return publish(conn, a, i, quality_run_id)
