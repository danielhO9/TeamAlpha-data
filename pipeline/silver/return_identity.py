"""PIT KRX identity binding used by the dividend total-return contract."""
from __future__ import annotations

import hashlib
import json
from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date

import pandas as pd

from pipeline.silver.return_contract import normalize_krx_ticker


ASSET_IDENTITY_CONTRACT = "krx_pit_ticker_asset_v3_price_scoped"
CERTIFIED_MARKETS = ("KOSPI", "KOSDAQ")

_PIT_MAP_CACHE_MAX_ENTRIES = 2
_PIT_MAP_CACHE: OrderedDict[
    tuple[str, str, str, bool],
    tuple[pd.DataFrame, "PitActionMapStats", pd.DataFrame | None],
] = OrderedDict()


def _cached_pit_map(key):
    cached = _PIT_MAP_CACHE.get(key)
    if cached is None:
        return None
    _PIT_MAP_CACHE.move_to_end(key)
    mapped, stats, audit = cached
    return (
        mapped.copy(deep=True),
        deepcopy(stats),
        audit.copy(deep=True) if audit is not None else None,
    )


def _remember_pit_map(key, mapped, stats, audit) -> None:
    _PIT_MAP_CACHE[key] = (
        mapped.copy(deep=True),
        deepcopy(stats),
        audit.copy(deep=True) if audit is not None else None,
    )
    _PIT_MAP_CACHE.move_to_end(key)
    while len(_PIT_MAP_CACHE) > _PIT_MAP_CACHE_MAX_ENTRIES:
        _PIT_MAP_CACHE.popitem(last=False)


@dataclass(frozen=True)
class AssetIdentityDigest:
    contract: str
    digest: str
    row_count: int
    asset_count: int


@dataclass(frozen=True)
class PitActionMapStats:
    input_count: int
    mapped_common_stock_count: int
    before_contract_count: int
    out_of_scope_instrument_count: int
    out_of_scope_market_count: int = 0
    out_of_scope_market_ticker_count: int = 0
    out_of_scope_market_classes: dict[str, int] = field(
        default_factory=dict
    )
    included_corp_cls_counts: dict[str, int] = field(default_factory=dict)
    excluded_corp_cls_counts: dict[str, int] = field(default_factory=dict)
    excluded_reason_counts: dict[str, int] = field(default_factory=dict)


def _normalize_ticker(value: object) -> str:
    return normalize_krx_ticker(value)


def _normalize_corp_cls(value: object) -> str | None:
    if pd.isna(value):
        return None
    rendered = str(value).strip().upper()
    return rendered or None


def action_event_dates(frame: pd.DataFrame) -> pd.Series:
    """Use the economic event date, not today's ticker assignment."""
    values = pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns]")
    event_type = frame.get(
        "event_type", pd.Series("", index=frame.index, dtype="object")
    ).astype(str)
    effective = pd.to_datetime(
        frame.get("effective_date"), errors="coerce"
    ) if "effective_date" in frame else values.copy()
    record = pd.to_datetime(
        frame.get("record_date"), errors="coerce"
    ) if "record_date" in frame else values.copy()
    announced = pd.to_datetime(
        frame.get("announcement_date"), errors="coerce"
    ) if "announcement_date" in frame else values.copy()
    cash = event_type.eq("cash_dividend")
    values.loc[cash] = record.loc[cash].fillna(effective.loc[cash]).fillna(
        announced.loc[cash]
    )
    values.loc[~cash] = effective.loc[~cash].fillna(record.loc[~cash]).fillna(
        announced.loc[~cash]
    )
    return values.dt.normalize()


def revision_family_event_dates(frame: pd.DataFrame) -> pd.Series:
    """Bind every cash revision family to one economic PIT event date."""
    values = action_event_dates(frame).copy()
    event_type = frame.get(
        "event_type", pd.Series("", index=frame.index, dtype="object")
    ).astype(str)
    cash = frame.loc[event_type.eq("cash_dividend")]
    if cash.empty:
        return values
    action_key_column = next(
        (name for name in ("rcept_no", "action_key", "filing_id") if name in cash),
        None,
    )
    if action_key_column is None:
        raise RuntimeError("cash actions require a receipt/action key")
    action_keys = cash[action_key_column].fillna("").astype(str)
    if action_keys.eq("").any():
        raise RuntimeError("cash revision family has a missing action key")
    if "revision_root_action_key" in cash:
        roots = cash["revision_root_action_key"].fillna("").astype(str)
        roots = roots.where(roots.ne(""), action_keys)
    else:
        roots = action_keys
    grouped = cash.assign(_family_root=roots).groupby(
        "_family_root", sort=False,
    )
    for root, family in grouped:
        identifiers = family["identifier"].astype(str).unique().tolist()
        if len(identifiers) != 1:
            raise RuntimeError(
                "DART revision family spans multiple tickers: "
                f"root={root} identifiers={identifiers}"
            )
        attachment = (
            family.get(
                "revision_kind",
                pd.Series("", index=family.index, dtype="object"),
            ).fillna("").astype(str).eq("ATTACHMENT_ONLY")
            | family.get(
                "cash_amount_status",
                pd.Series("", index=family.index, dtype="object"),
            ).fillna("").astype(str).eq("ATTACHMENT_ONLY")
        )
        economic = family.loc[~attachment].copy()
        if economic.empty:
            raise RuntimeError(f"DART revision family has no economic row: {root}")
        economic["_announcement"] = pd.to_datetime(
            economic["announcement_date"], errors="coerce",
        )
        economic["_action_key"] = economic[action_key_column].astype(str)
        economic = economic.sort_values(
            ["_announcement", "_action_key"], kind="mergesort",
        )
        terminal = economic.iloc[-1]
        terminal_status = str(terminal.get("cash_amount_status") or "")
        terminal_record = pd.to_datetime(
            terminal.get("record_date"), errors="coerce",
        )
        if terminal_status == "POSITIVE_PENDING_RECORD_DATE":
            raise RuntimeError(
                f"DART revision family terminal is incomplete: {root}"
            )
        known_records = pd.to_datetime(
            economic.get("record_date"), errors="coerce",
        ).dropna()
        if pd.notna(terminal_record):
            anchor = pd.Timestamp(terminal_record).normalize()
        elif not known_records.empty:
            # A terminal withdrawal/zero decision remains bound to the latest
            # prior positive decision's economic record date.
            anchor = pd.Timestamp(known_records.iloc[-1]).normalize()
        else:
            anchor = pd.Timestamp(terminal["_announcement"]).normalize()
        values.loc[family.index] = anchor
    return values


def map_actions_to_pit_assets(
    conn,
    frame: pd.DataFrame,
    *,
    coverage_start: date,
    include_audit: bool = False,
    verified_snapshot_sha256: str | None = None,
    asset_identity_digest: str | None = None,
) -> tuple[pd.DataFrame, PitActionMapStats] | tuple[
    pd.DataFrame, PitActionMapStats, pd.DataFrame
]:
    """Resolve each action at its event date; never drop a failed mapping.

    Overlapping/reused ticker episodes are hard failures.  Inclusion is based
    only on event-date KRX identity, common-share type and the certified
    KOSPI/KOSDAQ price range.  DART ``corp_cls`` is retained for audit counts
    but is never a market-scope gate (historical listed issuers can be ``E``).
    """
    cache_key = None
    if verified_snapshot_sha256 is not None and asset_identity_digest is not None:
        cache_key = (
            str(verified_snapshot_sha256),
            str(asset_identity_digest),
            coverage_start.isoformat(),
            include_audit,
        )
        cached = _cached_pit_map(cache_key)
        if cached is not None:
            mapped, stats, audit = cached
            print("[corporate-actions] reused verified PIT mapping cache", flush=True)
            return (mapped, stats, audit) if include_audit else (mapped, stats)
    if frame.empty:
        result = (frame.copy(), PitActionMapStats(0, 0, 0, 0))
        if cache_key is not None:
            _remember_pit_map(
                cache_key, result[0], result[1],
                frame.copy() if include_audit else None,
            )
        return (*result, frame.copy()) if include_audit else result
    required = {"identifier", "event_type", "announcement_date"}
    missing = required - set(frame.columns)
    if missing:
        raise RuntimeError(f"action frame missing PIT fields: {sorted(missing)}")
    scoped = frame.copy()
    scoped["identifier"] = scoped["identifier"].map(_normalize_ticker)
    scoped["_event_date"] = revision_family_event_dates(scoped)
    missing_date = scoped[scoped["_event_date"].isna()]
    if not missing_date.empty:
        sample = missing_date["identifier"].head(10).tolist()
        raise RuntimeError(f"DART actions have no event date: {sample}")
    scoped["pit_mapping_status"] = None
    scoped["pit_excluded_reason"] = None
    before = scoped["_event_date"].dt.date < coverage_start
    before_count = int(before.sum())
    scoped.loc[before, "pit_mapping_status"] = "EXCLUDED"
    scoped.loc[before, "pit_excluded_reason"] = "BEFORE_CONTRACT"
    eligible_scope = scoped.loc[~before].copy()
    if eligible_scope.empty:
        audit = scoped.rename(columns={"_event_date": "pit_event_date"})
        mapped = eligible_scope.drop(columns="_event_date")
        excluded_classes = scoped.get(
            "corp_cls", pd.Series(None, index=scoped.index, dtype="object")
        ).map(_normalize_corp_cls).fillna("UNKNOWN")
        result = (mapped, PitActionMapStats(
            len(frame), 0, before_count, 0,
            excluded_corp_cls_counts={
                str(key): int(value)
                for key, value in excluded_classes.value_counts().to_dict().items()
            },
            excluded_reason_counts={"BEFORE_CONTRACT": before_count},
        ))
        if cache_key is not None:
            _remember_pit_map(
                cache_key, result[0], result[1],
                audit if include_audit else None,
            )
        return (*result, audit) if include_audit else result

    all_corp_cls = scoped.get(
        "corp_cls", pd.Series(None, index=scoped.index, dtype="object")
    ).map(_normalize_corp_cls)
    corp_cls = eligible_scope.get(
        "corp_cls", pd.Series(None, index=eligible_scope.index, dtype="object")
    ).map(_normalize_corp_cls)
    # corp_cls is provenance only.  Do not turn new/incorrect raw values into
    # an inclusion gate; PIT identity and certified price episodes decide.
    eligible_scope["_row_no"] = range(len(eligible_scope))
    row_numbers = eligible_scope["_row_no"].astype(int).tolist()
    identifiers = eligible_scope["identifier"].astype(str).tolist()
    event_dates = eligible_scope["_event_date"].dt.date.tolist()
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH requested AS (
                SELECT *
                FROM unnest(%s::bigint[], %s::text[], %s::date[])
                    AS r(row_no, identifier, event_date)
            )
            SELECT r.row_no, ai.asset_id, a.instrument_type,
                   CASE
                     WHEN pc.last_on_or_before IS NULL THEN false
                     ELSE pc.first_on_or_after IS NOT NULL
                       OR pc.last_trade >= r.event_date - 7
                   END AS certified_market_episode_covers_event
            FROM requested r
            LEFT JOIN asset_identifier ai
              ON ai.source='KRX'
             AND ai.identifier_type='ticker'
             AND ai.identifier=r.identifier
             AND ai.valid_from <= r.event_date
             AND (ai.valid_to IS NULL OR ai.valid_to >= r.event_date)
            LEFT JOIN asset a
              ON a.asset_id=ai.asset_id
             AND a.asset_type='stock'
             AND a.exchange='KRX'
            LEFT JOIN LATERAL (
                SELECT max(p.trade_date) FILTER (
                           WHERE p.trade_date <= r.event_date
                       ) AS last_on_or_before,
                       min(p.trade_date) FILTER (
                           WHERE p.trade_date >= r.event_date
                       ) AS first_on_or_after,
                       max(p.trade_date) AS last_trade
                FROM price_daily p
                JOIN dq_run q ON q.run_id=p.quality_run_id
                WHERE p.asset_id=ai.asset_id
                  AND p.source='KRX'
                  AND p.market IN ('KOSPI','KOSDAQ')
                  AND q.status='CERTIFIED'
                  AND p.trade_date >= %s
            ) pc ON true
            ORDER BY r.row_no, ai.asset_id, ai.valid_from
            """,
            (
                row_numbers, identifiers, event_dates, coverage_start,
            ),
        )
        rows = cur.fetchall()
    candidates: dict[int, list[tuple[int, str, bool]]] = {
        row_number: [] for row_number in row_numbers
    }
    for row_number, asset_id, instrument_type, in_market_range in rows:
        if asset_id is not None and instrument_type is not None:
            candidates[int(row_number)].append(
                (int(asset_id), str(instrument_type), bool(in_market_range))
            )
    ambiguous = [
        {
            "identifier": identifiers[row_number],
            "event_date": event_dates[row_number].isoformat(),
            "match_count": len(candidates[row_number]),
        }
        for row_number in row_numbers
        if len(candidates[row_number]) > 1
    ]
    if ambiguous:
        raise RuntimeError(
            "DART PIT ticker mapping is ambiguous: "
            f"failure_count={len(ambiguous)} samples={ambiguous[:20]}"
        )
    eligible_scope["_identity_match_count"] = eligible_scope["_row_no"].map(
        lambda row_number: len(candidates[int(row_number)])
    )
    eligible_scope["asset_id"] = eligible_scope["_row_no"].map(
        lambda row_number: (
            candidates[int(row_number)][0][0]
            if candidates[int(row_number)] else None
        )
    )
    eligible_scope["_instrument_type"] = eligible_scope["_row_no"].map(
        lambda row_number: (
            candidates[int(row_number)][0][1]
            if candidates[int(row_number)] else None
        )
    )
    eligible_scope["_market_in_range"] = eligible_scope["_row_no"].map(
        lambda row_number: (
            candidates[int(row_number)][0][2]
            if candidates[int(row_number)] else False
        )
    )
    no_identity = eligible_scope["_identity_match_count"].eq(0)
    wrong_instrument = (
        ~no_identity & ~eligible_scope["_instrument_type"].eq("common_stock")
    )
    no_market_range = (
        ~no_identity & ~wrong_instrument & ~eligible_scope["_market_in_range"]
    )
    included = ~no_identity & ~wrong_instrument & ~no_market_range
    exclusion_reason_counts = {
        "BEFORE_CONTRACT": before_count,
        "NO_EVENT_DATE_PIT_IDENTITY": int(no_identity.sum()),
        "NON_COMMON_INSTRUMENT": int(wrong_instrument.sum()),
        "NO_CERTIFIED_KOSPI_KOSDAQ_PRICE_EPISODE": int(
            no_market_range.sum()
        ),
    }
    exclusion_reason_counts = {
        reason: count
        for reason, count in exclusion_reason_counts.items()
        if count
    }
    excluded = eligible_scope.loc[~included]
    excluded_classes = corp_cls.loc[excluded.index].fillna("UNKNOWN")
    all_excluded_classes = pd.concat([
        all_corp_cls.loc[scoped.index[before]].fillna("UNKNOWN"),
        excluded_classes,
    ])
    included_classes = corp_cls.loc[
        eligible_scope.loc[included].index
    ].fillna("UNKNOWN")
    market_excluded = no_identity | no_market_range
    market_ticker_count = int(
        eligible_scope.loc[market_excluded, "identifier"].astype(str).nunique()
    )
    eligible_scope.loc[included, "pit_mapping_status"] = "INCLUDED"
    eligible_scope.loc[no_identity, "pit_mapping_status"] = "EXCLUDED"
    eligible_scope.loc[no_identity, "pit_excluded_reason"] = (
        "NO_EVENT_DATE_PIT_IDENTITY"
    )
    eligible_scope.loc[wrong_instrument, "pit_mapping_status"] = "EXCLUDED"
    eligible_scope.loc[wrong_instrument, "pit_excluded_reason"] = (
        "NON_COMMON_INSTRUMENT"
    )
    eligible_scope.loc[no_market_range, "pit_mapping_status"] = "EXCLUDED"
    eligible_scope.loc[no_market_range, "pit_excluded_reason"] = (
        "NO_CERTIFIED_KOSPI_KOSDAQ_PRICE_EPISODE"
    )
    mapped = eligible_scope.loc[included].drop(
        columns=[
            "_event_date", "_row_no", "_instrument_type",
            "_identity_match_count", "_market_in_range",
        ]
    ).reset_index(drop=True)
    mapped["asset_id"] = mapped["asset_id"].astype("int64")
    stats = PitActionMapStats(
        input_count=len(frame),
        mapped_common_stock_count=len(mapped),
        before_contract_count=before_count,
        out_of_scope_instrument_count=int(wrong_instrument.sum()),
        out_of_scope_market_count=int(market_excluded.sum()),
        out_of_scope_market_ticker_count=market_ticker_count,
        out_of_scope_market_classes={
            str(key): int(value)
            for key, value in excluded_classes.value_counts().to_dict().items()
        },
        included_corp_cls_counts={
            str(key): int(value)
            for key, value in included_classes.value_counts().to_dict().items()
        },
        excluded_corp_cls_counts={
            str(key): int(value)
            for key, value in all_excluded_classes.value_counts().to_dict().items()
        },
        excluded_reason_counts=exclusion_reason_counts,
    )
    if (
        stats.input_count
        != stats.mapped_common_stock_count
        + sum(stats.excluded_reason_counts.values())
    ):
        raise RuntimeError("PIT action mapping partition parity failed")
    audit = pd.concat([
        scoped.loc[before], eligible_scope.drop(columns=[
            "_row_no", "_instrument_type", "_identity_match_count",
            "_market_in_range",
        ]),
    ], sort=False).sort_index().rename(
        columns={"_event_date": "pit_event_date"}
    )
    result = (mapped, stats)
    if cache_key is not None:
        _remember_pit_map(
            cache_key, mapped, stats,
            audit.reset_index(drop=True) if include_audit else None,
        )
    return (*result, audit.reset_index(drop=True)) if include_audit else result


def krx_common_stock_identity_digest(
    conn,
    *,
    coverage_start: date,
    coverage_end: date,
) -> AssetIdentityDigest:
    """Digest every common-stock KRX ticker episode overlapping the contract."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ai.asset_id, ai.identifier, ai.valid_from, ai.valid_to
            FROM asset_identifier ai
            JOIN asset a ON a.asset_id=ai.asset_id
            WHERE ai.source='KRX'
              AND ai.identifier_type='ticker'
              AND a.asset_type='stock'
              AND a.instrument_type='common_stock'
              AND a.exchange='KRX'
              AND ai.valid_from <= %s
              AND (ai.valid_to IS NULL OR ai.valid_to >= %s)
              AND EXISTS (
                  SELECT 1
                  FROM price_daily p
                  JOIN dq_run q ON q.run_id=p.quality_run_id
                  WHERE p.asset_id=ai.asset_id
                    AND p.source='KRX'
                    AND p.market IN ('KOSPI','KOSDAQ')
                    AND q.status='CERTIFIED'
                    AND p.trade_date BETWEEN %s AND %s
              )
            ORDER BY ai.asset_id, ai.identifier, ai.valid_from, ai.valid_to
            """,
            (coverage_end, coverage_start, coverage_start, coverage_end),
        )
        rows = cur.fetchall()
    if not rows:
        raise RuntimeError("KRX common-stock identity scope is empty")
    digest = hashlib.sha256()
    asset_ids: set[int] = set()
    for asset_id, identifier, valid_from, valid_to in rows:
        asset_ids.add(int(asset_id))
        canonical = [
            int(asset_id),
            str(identifier),
            valid_from.isoformat(),
            valid_to.isoformat() if valid_to is not None else None,
        ]
        digest.update(
            json.dumps(
                canonical,
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        digest.update(b"\n")
    return AssetIdentityDigest(
        contract=ASSET_IDENTITY_CONTRACT,
        digest=digest.hexdigest(),
        row_count=len(rows),
        asset_count=len(asset_ids),
    )
