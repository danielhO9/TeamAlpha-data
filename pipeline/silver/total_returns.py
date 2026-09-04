"""Deterministic KRX gross total-return reconstruction.

The functions in this module are intentionally database-free.  They turn
certified Silver price/action candidates into auditable resolved dividend
events and a gross (pre-tax) total-return close.  Callers own persistence and
transaction boundaries.

Contract
--------
* one canonical cash-dividend decision per security and record date;
* an explicit ex-dividend date wins, otherwise the second most recent market
  session on or before the record date is used (KRX T+2 convention);
* a non-trading ex-date is applied on the security's first subsequent trade;
* events never cross a listing-episode gap;
* cash is converted to the terminal split-adjusted price scale before use;
* each listing episode starts at ``adj_close`` and compounds gross returns as
  ``(adj_close[t] + adjusted_cash[t]) / adj_close[t-1]``.
"""
from __future__ import annotations

import math
from decimal import Decimal, ROUND_HALF_UP
from typing import Iterable

import numpy as np
import pandas as pd

from pipeline.silver.dividend_evidence import assert_verified_cash_evidence
from pipeline.silver.cash_adjustment_scale_evidence import (
    PRE_EVENT_PRICE_SCALE,
    STABLE_PRICE_SCALE,
)
from pipeline.silver.prices import LISTING_EPISODE_GAP_DAYS


# Independently filed ordinary/capital-reduction dividends with the same
# record date are genuinely additive.  Every set is receipt-specific; an
# unknown independent-root collision fails closed instead of reviving the old
# record-date dedupe bug or silently double-counting a source typo.
REVIEWED_ADDITIVE_ROOT_SETS = frozenset({
    frozenset({"20260225801107", "20260225801133"}),  # 002100
    frozenset({"20260213901085", "20260213901121"}),  # 079000
})

# A complete scan of the certified 2015-2026 KRX history found that repeated
# source/persistence rounding can move an otherwise stable adjusted-price
# scale by at most 4.53e-8 relatively.  Keep the admission ceiling just above
# that observed lineage bound and forty times below the 2 ppm regression that
# must remain a real scale change.
MAX_STABLE_SCALE_LINEAGE_DRIFT = 5e-8


def _column(frame: pd.DataFrame, *names: str) -> str | None:
    return next((name for name in names if name in frame.columns), None)


def _dates(values: pd.Series) -> pd.Series:
    return pd.to_datetime(values, errors="coerce").dt.normalize()


def stored_adjustment_scales_may_match(
    *,
    previous_close: float,
    previous_adj_close: float,
    applied_close: float,
    applied_adj_close: float,
) -> bool:
    """Compare scales within exact storage or certified lineage bounds."""
    previous_low, previous_high = _stored_scale_interval(
        close=previous_close, adjusted_close=previous_adj_close,
    )
    applied_low, applied_high = _stored_scale_interval(
        close=applied_close, adjusted_close=applied_adj_close,
    )
    # Equal underlying scales are possible exactly when the two intervals
    # implied by NUMERIC(...,4) rounding overlap.  Only float ULP expansion is
    # admitted; no broad absolute floor can hide a small real rights reset.
    interval_overlap = (
        previous_low <= applied_high and applied_low <= previous_high
    )
    previous_scale = previous_adj_close / previous_close
    applied_scale = applied_adj_close / applied_close
    lineage_drift = abs(previous_scale / applied_scale - 1.0)
    return interval_overlap or lineage_drift <= MAX_STABLE_SCALE_LINEAGE_DRIFT


def _stored_scale_interval(
    *, close: float, adjusted_close: float, uncertainty: float = 0.00005,
) -> tuple[float, float]:
    if not all(
        math.isfinite(value) and value > 0
        for value in (close, adjusted_close)
    ):
        raise RuntimeError("price scale inputs must be finite and positive")
    low = (adjusted_close - uncertainty) / close
    high = (adjusted_close + uncertainty) / close
    ulp = max(math.ulp(low), math.ulp(high))
    return low - ulp, high + ulp


def stored_price_factor_interval(
    *,
    previous_close: float,
    previous_adj_close: float,
    applied_close: float,
    applied_adj_close: float,
) -> tuple[float, float]:
    """Return every price factor compatible with four-decimal stored prices.

    Silver's historical price lineage contains two four-decimal rounding
    stages, so each value represents a closed one-quantum interval around the
    unknown pre-rounding value. Price factors are positive, so their extrema
    are the previous-scale low divided by the applied-scale high and vice
    versa. Only one float ULP is added at the computed endpoints.
    """
    previous_low, previous_high = _stored_scale_interval(
        close=previous_close,
        adjusted_close=previous_adj_close,
        # The evidence comparison crosses both the historical adjusted-price
        # observation and Silver's later rescale/persistence rounding stage.
        # Their two half-quantum errors add to one 0.0001 lineage quantum.
        uncertainty=0.0001,
    )
    applied_low, applied_high = _stored_scale_interval(
        close=applied_close,
        adjusted_close=applied_adj_close,
        uncertainty=0.0001,
    )
    low = previous_low / applied_high
    high = previous_high / applied_low
    ulp = max(math.ulp(low), math.ulp(high))
    return low - ulp, high + ulp


def _same_stored_price(left: object, right: object) -> bool:
    quantum = Decimal("0.0001")
    try:
        return Decimal(str(left)).quantize(quantum) == Decimal(
            str(right)
        ).quantize(quantum)
    except Exception as exc:  # noqa: BLE001 - invalid price must fail closed
        raise RuntimeError("invalid stored price for exact parity") from exc


def _stored_scale_and_cash(
    cash_amount: object,
    selected_scale: object,
) -> tuple[float, float]:
    """Apply the exact NUMERIC(28,12) scale and NUMERIC(28,8) cash contract."""
    scale = Decimal(str(selected_scale)).quantize(
        Decimal("0.000000000001"), rounding=ROUND_HALF_UP,
    )
    cash = (
        Decimal(str(cash_amount)) * scale
    ).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
    return float(scale), float(cash)


def classify_cash_dividend_revisions(actions: pd.DataFrame) -> pd.DataFrame:
    """Classify every cash-action row for append-only resolution auditing.

    The returned rows retain their source action identity and add
    ``revision_group_key``, ``is_canonical`` and ``excluded_reason``.  This is
    the shape needed to persist both the selected event and every superseded
    or incomplete source action in ``dividend_event_resolution``.
    """
    if actions.empty:
        result = actions.copy()
        result["revision_group_key"] = pd.Series(dtype="object")
        result["is_canonical"] = pd.Series(dtype="bool")
        result["excluded_reason"] = pd.Series(dtype="object")
        result["dividend_key"] = pd.Series(dtype="object")
        return result

    action_type = _column(actions, "event_type", "action_type")
    if action_type is None or "identifier" not in actions:
        raise ValueError("actions require identifier and event_type/action_type")
    required = {"record_date", "cash_amount"}
    missing = required - set(actions.columns)
    if missing:
        raise ValueError(f"cash-dividend actions missing columns: {sorted(missing)}")

    frame = actions[actions[action_type].eq("cash_dividend")].copy()
    frame["identifier"] = frame["identifier"].astype(str)
    frame["record_date"] = _dates(frame["record_date"])
    frame["cash_amount"] = pd.to_numeric(
        frame["cash_amount"], errors="coerce",
    )
    announcement = _column(frame, "announcement_date")
    frame["_revision_announcement"] = (
        _dates(frame[announcement]) if announcement is not None else pd.NaT
    )
    action_key = _column(frame, "rcept_no", "action_key", "filing_id")
    frame["dividend_key"] = (
        frame[action_key].fillna("").astype(str)
        if action_key is not None
        else ""
    )
    missing_key = frame["dividend_key"].eq("")
    frame.loc[missing_key, "dividend_key"] = [
        f"source-row:{index}" for index in frame.index[missing_key]
    ]
    if frame.duplicated(["identifier", "dividend_key"]).any():
        duplicates = frame.loc[
            frame.duplicated(["identifier", "dividend_key"], keep=False),
            ["identifier", "dividend_key"],
        ].head(10).to_dict("records")
        raise RuntimeError(f"duplicate cash source action keys: {duplicates}")

    # DART corrections can change the record date itself.  Consequently a
    # record date is not a revision key.  The official viewer's family/root is
    # the only valid grouping evidence; an original receipt naturally roots
    # to itself until a later correction names it.
    root_column = _column(frame, "revision_root_action_key")
    if root_column is not None:
        roots = frame[root_column].fillna("").astype(str).str.strip()
        roots = roots.where(roots.ne(""), frame["dividend_key"])
    else:
        # Backwards-compatible database-free helper mode.  Persisted actions
        # always carry the root column; older callers cannot express a
        # changed-record-date correction and retain record-date grouping.
        roots = frame["record_date"].dt.strftime("%Y-%m-%d").fillna(
            frame["dividend_key"]
        )
    frame["revision_group_key"] = frame["identifier"] + ":" + roots

    revision_kind_column = _column(frame, "revision_kind")
    revision_kind = (
        frame[revision_kind_column].fillna("").astype(str)
        if revision_kind_column is not None
        else pd.Series("", index=frame.index, dtype="object")
    )
    cash_status_column = _column(frame, "cash_amount_status")
    if cash_status_column is None:
        cash_status = pd.Series(
            np.where(
                frame["cash_amount"].notna() & frame["cash_amount"].gt(0),
                "POSITIVE",
                "UNRESOLVED",
            ),
            index=frame.index,
            dtype="object",
        )
    else:
        cash_status = frame[cash_status_column].fillna("").astype(str)
    source_status_column = _column(frame, "source_evidence_status")
    if cash_status_column is not None:
        if source_status_column is None or root_column is None:
            raise RuntimeError(
                "persisted cash actions require source evidence and an "
                "official revision root"
            )
        assert_verified_cash_evidence(
            frame,
            action_key_column="dividend_key",
            root_key_column=root_column,
        )

    attachment = (
        revision_kind.eq("ATTACHMENT_ONLY")
        | cash_status.eq("ATTACHMENT_ONLY")
        | (
            frame[source_status_column].eq("VERIFIED_ATTACHMENT_CORRECTION")
            if source_status_column is not None
            else False
        )
    )
    positive = cash_status.eq("POSITIVE")
    pending_positive = cash_status.eq("POSITIVE_PENDING_RECORD_DATE")
    no_common = cash_status.eq("NO_COMMON_CASH_DIVIDEND")
    no_event = cash_status.eq("NO_ECONOMIC_EVENT")
    recognized = attachment | positive | pending_positive | no_common | no_event
    # Legacy unit-level callers without the explicit status column retain the
    # old numeric inference, while persisted production actions must state an
    # exact terminal status.
    legacy_invalid = pd.Series(False, index=frame.index, dtype="bool")
    if cash_status_column is None:
        # The pure helper predates explicit source statuses.  Preserve its
        # useful audit behavior for malformed numeric rows, while production
        # rows (which always carry cash_amount_status) remain fail-closed.
        legacy_invalid = ~attachment & ~positive
        recognized = attachment | positive | legacy_invalid
    if (~recognized).any():
        sample = frame.loc[
            ~recognized, ["identifier", "dividend_key"]
        ].assign(cash_amount_status=cash_status[~recognized]).head(10)
        raise RuntimeError(
            "unresolved/unsupported cash action status: "
            f"{sample.to_dict('records')}"
        )

    frame["is_canonical"] = False
    frame["excluded_reason"] = None
    frame.loc[attachment, "excluded_reason"] = "ATTACHMENT_CORRECTION"
    frame.loc[legacy_invalid, "excluded_reason"] = "INVALID_CASH_AMOUNT"
    invalid_positive = positive & (
        frame["record_date"].isna()
        | frame["cash_amount"].isna()
        | frame["cash_amount"].le(0)
    )
    if invalid_positive.any():
        sample = frame.loc[
            invalid_positive,
            ["identifier", "dividend_key", "record_date", "cash_amount"],
        ].head(10).to_dict("records")
        raise RuntimeError(
            f"positive cash decisions are incomplete: {sample}"
        )

    economic = frame[~attachment & ~legacy_invalid].sort_values(
        ["revision_group_key", "_revision_announcement", "dividend_key"],
        kind="mergesort",
        na_position="first",
    )
    for _, group in economic.groupby("revision_group_key", sort=False):
        latest_index = group.index[-1]
        older_indices = group.index[:-1]
        if len(older_indices):
            frame.loc[older_indices, "excluded_reason"] = "SUPERSEDED_REVISION"
        if positive.loc[latest_index]:
            frame.loc[latest_index, "is_canonical"] = True
        elif pending_positive.loc[latest_index]:
            raise RuntimeError(
                "latest economic revision is still missing its record date: "
                f"{frame.loc[latest_index, 'dividend_key']}"
            )
        elif no_common.loc[latest_index]:
            frame.loc[latest_index, "excluded_reason"] = (
                "NO_COMMON_CASH_DIVIDEND"
            )
        elif no_event.loc[latest_index]:
            frame.loc[latest_index, "excluded_reason"] = "NO_ECONOMIC_EVENT"
        else:
            raise RuntimeError(
                "latest economic revision has no terminal decision: "
                f"{frame.loc[latest_index, 'dividend_key']}"
            )

    if root_column is not None:
        canonical = frame[frame["is_canonical"]]
        for _, same_date in canonical.groupby(
            ["identifier", "record_date"], dropna=False, sort=False,
        ):
            roots_on_date = frozenset(
                same_date["revision_group_key"].str.split(":", n=1).str[-1]
            )
            if (
                len(roots_on_date) > 1
                and roots_on_date not in REVIEWED_ADDITIVE_ROOT_SETS
            ):
                sample = same_date[[
                    "identifier", "record_date", "dividend_key",
                    "revision_group_key", "cash_amount",
                ]].to_dict("records")
                raise RuntimeError(
                    "unreviewed independent dividend roots collide on one "
                    f"record date: {sample}"
                )
    return frame.drop(columns="_revision_announcement").reset_index(drop=True)


def canonicalize_cash_dividends(actions: pd.DataFrame) -> pd.DataFrame:
    """Select the terminal positive decision in each official DART root.

    Record dates are values that a correction may change, never revision
    identifiers.  Attachment-only corrections are audit evidence only, while
    a terminal zero/withdrawal decision prevents an older positive DPS from
    being revived.
    """
    classified = classify_cash_dividend_revisions(actions)
    canonical = classified[classified["is_canonical"]].copy().reset_index(
        drop=True,
    )
    superseded = classified["excluded_reason"].eq("SUPERSEDED_REVISION").sum()
    rejected = (~classified["is_canonical"]).sum() - superseded
    canonical.attrs["canonicalization"] = {
        "input_rows": len(classified),
        "eligible_rows": int(classified["excluded_reason"].isin([
            None, "SUPERSEDED_REVISION",
        ]).sum()),
        "canonical_rows": len(canonical),
        "superseded_rows": int(superseded),
        "rejected_rows": int(rejected),
    }
    return canonical


def _market_session_index(
    market_sessions: pd.DataFrame | pd.Series | Iterable,
) -> pd.DatetimeIndex:
    if isinstance(market_sessions, pd.DataFrame):
        column = _column(market_sessions, "trade_date", "session_date")
        if column is None:
            raise ValueError("market_sessions require trade_date/session_date")
        values = market_sessions[column]
    elif isinstance(market_sessions, pd.Series):
        values = market_sessions
    else:
        values = pd.Series(list(market_sessions))
    return pd.DatetimeIndex(_dates(pd.Series(values)).dropna().unique()).sort_values()


def resolve_dividend_ex_dates(
    dividends: pd.DataFrame,
    actions: pd.DataFrame,
    market_sessions: pd.DataFrame | pd.Series | Iterable,
    *,
    notice_window_days: int = 15,
) -> pd.DataFrame:
    """Resolve cash events to an ex-date with explicit evidence first."""
    resolved = dividends.copy()
    if resolved.empty:
        for column in ("resolved_ex_date", "ex_date_basis"):
            resolved[column] = pd.Series(dtype="object")
        return resolved

    sessions = _market_session_index(market_sessions)
    cash_ex_column = _column(resolved, "ex_date", "effective_date")
    action_type = _column(actions, "event_type", "action_type")
    notice_ex_column = _column(actions, "effective_date", "ex_date")
    action_key = _column(actions, "rcept_no", "action_key", "filing_id")

    notices_by_identifier: dict[str, list[tuple[pd.Timestamp, str]]] = {}
    if action_type is not None and notice_ex_column is not None:
        notices = actions[actions[action_type].eq("ex_dividend")].copy()
        if not notices.empty:
            notices["_notice_date"] = _dates(notices[notice_ex_column])
            notices = notices[notices["_notice_date"].notna()]
            notices["_notice_key"] = (
                notices[action_key].fillna("").astype(str)
                if action_key is not None
                else ""
            )
            for identifier, group in notices.groupby(
                notices["identifier"].astype(str), sort=False,
            ):
                notices_by_identifier[str(identifier)] = sorted(
                    zip(group["_notice_date"], group["_notice_key"]),
                    key=lambda item: (item[0], item[1]),
                )

    resolved_dates: list[pd.Timestamp | pd.NaT] = []
    bases: list[str] = []
    for row in resolved.itertuples(index=False):
        direct = (
            pd.to_datetime(getattr(row, cash_ex_column), errors="coerce")
            if cash_ex_column is not None
            else pd.NaT
        )
        if pd.notna(direct):
            resolved_dates.append(pd.Timestamp(direct).normalize())
            bases.append("KRX_NOTICE")
            continue

        record_date = pd.to_datetime(row.record_date, errors="coerce")
        eligible_sessions = sessions[sessions <= record_date]
        inferred = (
            pd.Timestamp(eligible_sessions[-2])
            if len(eligible_sessions) >= 2
            else pd.NaT
        )
        identifier = str(row.identifier)
        candidates: list[tuple[int, pd.Timestamp, str]] = []
        for notice_date, notice_key in notices_by_identifier.get(identifier, []):
            if notice_date > record_date:
                continue
            anchor = inferred if pd.notna(inferred) else record_date
            distance = abs((notice_date - anchor).days)
            if distance <= notice_window_days:
                candidates.append((distance, notice_date, notice_key))
        if candidates:
            _, notice_date, _ = min(
                candidates,
                key=lambda item: (item[0], item[1], item[2]),
            )
            resolved_dates.append(notice_date)
            bases.append("KRX_NOTICE")
        elif pd.notna(inferred):
            resolved_dates.append(inferred)
            bases.append("KRX_T2_INFERRED")
        else:
            resolved_dates.append(pd.NaT)
            bases.append(None)

    resolved["resolved_ex_date"] = pd.to_datetime(resolved_dates)
    resolved["ex_date_basis"] = bases
    return resolved


def apply_dividends_to_prices(
    prices: pd.DataFrame,
    dividends: pd.DataFrame,
    *,
    scale_source_evidence: pd.DataFrame | None = None,
    listing_gap_days: int = LISTING_EPISODE_GAP_DAYS,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply resolved dividends and calculate episode-local gross TR closes."""
    required_prices = {"identifier", "trade_date", "close", "adj_close"}
    missing = required_prices - set(prices.columns)
    if missing:
        raise ValueError(f"prices missing columns: {sorted(missing)}")
    if not dividends.empty and "resolved_ex_date" not in dividends:
        raise ValueError("dividends require resolved_ex_date")

    output = prices.copy()
    output["identifier"] = output["identifier"].astype(str)
    output["trade_date"] = _dates(output["trade_date"])
    output["close"] = pd.to_numeric(output["close"], errors="coerce")
    output["adj_close"] = pd.to_numeric(
        output["adj_close"], errors="coerce",
    )
    if output[["trade_date", "close", "adj_close"]].isna().any().any():
        raise ValueError("prices require finite trade_date, close and adj_close")
    if output[["close", "adj_close"]].le(0).any().any():
        raise ValueError("stock close and adj_close must be positive")
    if output.duplicated(["identifier", "trade_date"]).any():
        raise ValueError("duplicate identifier/trade_date price rows")

    output["_input_order"] = np.arange(len(output))
    output = output.sort_values(
        ["identifier", "trade_date"], kind="mergesort",
    ).reset_index(drop=True)
    gaps = output.groupby("identifier", sort=False)["trade_date"].diff().dt.days
    output["listing_episode"] = gaps.gt(listing_gap_days).groupby(
        output["identifier"], sort=False,
    ).cumsum().astype(int)
    output["adjusted_cash_dividend"] = 0.0

    event_output = dividends.copy().reset_index(drop=True)
    if "cash_amount" in event_output:
        event_output["cash_amount"] = pd.to_numeric(
            event_output["cash_amount"], errors="coerce",
        )
    event_output["applied_trade_date"] = pd.NaT
    event_output["adjusted_cash_amount"] = np.nan
    event_output["application_status"] = "unresolved_ex_date"
    for column in (
        "previous_trade_date", "previous_close", "previous_adj_close",
        "applied_close", "applied_adj_close", "previous_price_scale",
        "applied_price_scale", "selected_cash_scale",
        "cash_adjustment_scale_basis", "scale_change_detected",
        "scale_evidence_action_snapshot_run_id", "scale_evidence_key",
        "scale_price_factor_observed", "scale_price_factor_reference",
        "scale_price_factor_parity",
    ):
        event_output[column] = None

    evidence = (
        pd.DataFrame() if scale_source_evidence is None
        else scale_source_evidence.copy()
    )
    if not evidence.empty:
        required_evidence = {
            "asset_id", "ticker", "cash_receipt_no",
            "action_snapshot_run_id", "evidence_key",
            "previous_trade_date", "adjustment_trade_date",
            "raw_previous_close", "raw_applied_close",
            "raw_reference_price", "expected_price_factor",
            "cash_scale_basis",
        }
        missing_evidence = required_evidence - set(evidence.columns)
        if missing_evidence:
            raise RuntimeError(
                "cash-scale source evidence columns missing: "
                f"{sorted(missing_evidence)}"
            )
        evidence["ticker"] = evidence["ticker"].astype(str)
        evidence["cash_receipt_no"] = evidence[
            "cash_receipt_no"
        ].astype(str)
        evidence["previous_trade_date"] = _dates(
            evidence["previous_trade_date"]
        )
        evidence["adjustment_trade_date"] = _dates(
            evidence["adjustment_trade_date"]
        )
        evidence_identity = [
            "asset_id", "cash_receipt_no", "adjustment_trade_date",
        ]
        if evidence.duplicated(evidence_identity).any():
            raise RuntimeError("cash-scale source evidence is not one-to-one")
    used_evidence: set[int] = set()

    groups = {
        identifier: group.index.to_numpy()
        for identifier, group in output.groupby("identifier", sort=False)
    }
    for event_index, event in event_output.iterrows():
        ex_date = pd.to_datetime(
            event.get("resolved_ex_date"), errors="coerce",
        )
        if pd.isna(ex_date):
            continue
        indices = groups.get(str(event.get("identifier")))
        if indices is None or len(indices) == 0:
            event_output.at[event_index, "application_status"] = "no_price_series"
            continue
        dates = output.loc[indices, "trade_date"].to_numpy(
            dtype="datetime64[ns]",
        )
        position = int(np.searchsorted(dates, np.datetime64(ex_date), side="left"))
        if position >= len(indices):
            event_output.at[event_index, "application_status"] = "pending_future_trade"
            continue
        applied_index = int(indices[position])
        if position == 0:
            event_output.at[event_index, "application_status"] = (
                "before_listing_or_episode_start"
            )
            continue
        previous_index = int(indices[position - 1])
        if (
            output.at[applied_index, "listing_episode"]
            != output.at[previous_index, "listing_episode"]
        ):
            event_output.at[event_index, "application_status"] = "listing_episode_gap"
            continue

        exact_trade = output.at[applied_index, "trade_date"] == ex_date.normalize()
        previous_close = float(output.at[previous_index, "close"])
        previous_adj_close = float(output.at[previous_index, "adj_close"])
        applied_close = float(output.at[applied_index, "close"])
        applied_adj_close = float(output.at[applied_index, "adj_close"])
        previous_scale = previous_adj_close / previous_close
        applied_scale = applied_adj_close / applied_close
        stable_scale = stored_adjustment_scales_may_match(
            previous_close=float(output.at[previous_index, "close"]),
            previous_adj_close=float(
                output.at[previous_index, "adj_close"]
            ),
            applied_close=float(output.at[applied_index, "close"]),
            applied_adj_close=float(output.at[applied_index, "adj_close"]),
        )
        observed_factor = previous_scale / applied_scale
        receipt = str(
            event.get("dividend_key")
            or event.get("action_key")
            or event.get("rcept_no")
            or ""
        )
        asset_value = event.get("asset_id")
        event_evidence = pd.DataFrame()
        if not evidence.empty and pd.notna(asset_value):
            event_evidence = evidence[
                evidence["asset_id"].astype("int64").eq(int(asset_value))
                & evidence["cash_receipt_no"].eq(receipt)
                & evidence["adjustment_trade_date"].eq(
                    output.at[applied_index, "trade_date"]
                )
            ]
        if stable_scale:
            if not event_evidence.empty:
                raise RuntimeError(
                    "stable-scale cash event must not consume external "
                    f"evidence: asset_id={asset_value} receipt={receipt}"
                )
            selected_scale = applied_scale if exact_trade else previous_scale
            scale_basis = STABLE_PRICE_SCALE
            evidence_run_id = None
            evidence_key = None
            reference_factor = 1.0
            factor_parity = True
        else:
            if len(event_evidence) != 1:
                raise RuntimeError(
                    "cash dividend share base is ambiguous: it coincides "
                    "with an adjustment-scale change "
                    "and requires exactly one source-evidence row: "
                    f"identifier={event.get('identifier')} receipt={receipt} "
                    f"ex_date={pd.Timestamp(ex_date).date()} "
                    f"applied_trade_date="
                    f"{output.at[applied_index, 'trade_date'].date()} "
                    f"matches={len(event_evidence)}"
                )
            evidence_index = int(event_evidence.index[0])
            support = event_evidence.iloc[0]
            expected_values = (
                (
                    support["previous_trade_date"],
                    output.at[previous_index, "trade_date"],
                    "previous trade date",
                ),
                (
                    support["adjustment_trade_date"],
                    output.at[applied_index, "trade_date"],
                    "adjustment trade date",
                ),
            )
            for declared, observed, label in expected_values:
                if declared != observed:
                    raise RuntimeError(
                        f"cash-scale evidence {label} parity failed"
                    )
            for declared, observed, label in (
                (support["raw_previous_close"], previous_close, "previous close"),
                (support["raw_applied_close"], applied_close, "applied close"),
            ):
                if not _same_stored_price(declared, observed):
                    raise RuntimeError(
                        f"cash-scale source/RDS {label} parity failed"
                    )
            if support["cash_scale_basis"] != PRE_EVENT_PRICE_SCALE:
                raise RuntimeError("changed-scale source selected a forbidden basis")
            reference_factor = float(support["expected_price_factor"])
            declared_factor = (
                float(support["raw_reference_price"])
                / float(support["raw_previous_close"])
            )
            if Decimal(str(reference_factor)).quantize(
                Decimal("0.000000000001")
            ) != Decimal(str(declared_factor)).quantize(
                Decimal("0.000000000001")
            ):
                raise RuntimeError(
                    "cash-scale expected factor/source-price parity failed"
                )
            factor_low, factor_high = stored_price_factor_interval(
                previous_close=previous_close,
                previous_adj_close=previous_adj_close,
                applied_close=applied_close,
                applied_adj_close=applied_adj_close,
            )
            factor_parity = factor_low <= reference_factor <= factor_high
            if not factor_parity:
                raise RuntimeError(
                    "cash-scale observed/reference factor lies outside the "
                    "stored-price quantization interval: "
                    f"observed={observed_factor} reference={reference_factor} "
                    f"interval=[{factor_low},{factor_high}]"
                )
            selected_scale = previous_scale
            scale_basis = PRE_EVENT_PRICE_SCALE
            evidence_run_id = support["action_snapshot_run_id"]
            evidence_key = str(support["evidence_key"])
            used_evidence.add(evidence_index)
        cash_amount = event.get("cash_amount")
        if pd.isna(cash_amount) or float(cash_amount) <= 0:
            event_output.at[event_index, "application_status"] = "invalid_cash_amount"
            continue
        selected_scale, adjusted_cash = _stored_scale_and_cash(
            cash_amount, selected_scale,
        )
        output.at[applied_index, "adjusted_cash_dividend"] += adjusted_cash
        event_output.at[event_index, "applied_trade_date"] = output.at[
            applied_index, "trade_date"
        ]
        event_output.at[event_index, "adjusted_cash_amount"] = adjusted_cash
        event_output.at[event_index, "application_status"] = "applied"
        event_output.at[event_index, "previous_trade_date"] = output.at[
            previous_index, "trade_date"
        ]
        event_output.at[event_index, "previous_close"] = previous_close
        event_output.at[event_index, "previous_adj_close"] = previous_adj_close
        event_output.at[event_index, "applied_close"] = applied_close
        event_output.at[event_index, "applied_adj_close"] = applied_adj_close
        event_output.at[event_index, "previous_price_scale"] = previous_scale
        event_output.at[event_index, "applied_price_scale"] = applied_scale
        event_output.at[event_index, "selected_cash_scale"] = selected_scale
        event_output.at[
            event_index, "cash_adjustment_scale_basis"
        ] = scale_basis
        event_output.at[event_index, "scale_change_detected"] = not stable_scale
        event_output.at[
            event_index, "scale_evidence_action_snapshot_run_id"
        ] = evidence_run_id
        event_output.at[event_index, "scale_evidence_key"] = evidence_key
        event_output.at[
            event_index, "scale_price_factor_observed"
        ] = observed_factor
        event_output.at[
            event_index, "scale_price_factor_reference"
        ] = reference_factor
        event_output.at[
            event_index, "scale_price_factor_parity"
        ] = factor_parity

    if len(used_evidence) != len(evidence):
        unused = evidence.loc[
            ~evidence.index.isin(used_evidence),
            ["asset_id", "cash_receipt_no", "adjustment_trade_date",
             "evidence_key"],
        ].head(20).to_dict("records")
        raise RuntimeError(
            "cash-scale source evidence contains unused rows: "
            f"count={len(evidence) - len(used_evidence)} sample={unused}"
        )

    output["total_return_close"] = np.nan
    for _, indices_frame in output.groupby(
        ["identifier", "listing_episode"], sort=False,
    ):
        indices = indices_frame.index.to_numpy()
        adjusted = output.loc[indices, "adj_close"].to_numpy(dtype=float)
        cash = output.loc[
            indices, "adjusted_cash_dividend"
        ].to_numpy(dtype=float)
        total_return = np.empty(len(indices), dtype=float)
        total_return[0] = adjusted[0]
        for offset in range(1, len(indices)):
            gross_return = (adjusted[offset] + cash[offset]) / adjusted[offset - 1]
            total_return[offset] = total_return[offset - 1] * gross_return
        output.loc[indices, "total_return_close"] = total_return

    output = output.sort_values("_input_order").drop(
        columns="_input_order",
    ).reset_index(drop=True)
    return output, event_output


def build_total_return_close(
    prices: pd.DataFrame,
    actions: pd.DataFrame,
    market_sessions: pd.DataFrame | pd.Series | Iterable,
    *,
    notice_window_days: int = 15,
    listing_gap_days: int = LISTING_EPISODE_GAP_DAYS,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Canonicalize, resolve and apply KRX cash dividends in one pure call."""
    canonical = canonicalize_cash_dividends(actions)
    resolved = resolve_dividend_ex_dates(
        canonical,
        actions,
        market_sessions,
        notice_window_days=notice_window_days,
    )
    return apply_dividends_to_prices(
        prices,
        resolved,
        listing_gap_days=listing_gap_days,
    )
