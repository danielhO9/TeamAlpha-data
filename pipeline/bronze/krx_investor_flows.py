"""정식 KRX 투자자별 거래실적 export를 immutable Bronze에 등록한다.

KRX Data Marketplace 웹 화면을 자동화해 내려받지 않는다. 사용자가 구매했거나
Open API 활용 승인을 받은 CSV/TSV 파일만 ``--source-file``로 받는다. 원본 bytes와
SHA-256, 취득 근거 식별자를 함께 보존하며 Silver 파서는 원본을 다시 읽는다.

필수 열(영문 또는 괄호 안 한글 별칭):
``trade_date``(일자), ``ticker``(종목코드), ``market``(시장),
``investor_type``(투자자구분), 매도/매수/순매수 거래량 또는 거래대금.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from pipeline.common.paths import base_uri
from pipeline.common.sink import write_bytes, write_text_if_changed


def _read_tabular(raw: bytes, suffix: str) -> pd.DataFrame:
    if suffix.lower() not in {".csv", ".tsv", ".txt"}:
        raise ValueError("KRX investor-flow input must be CSV/TSV/TXT")
    separator = "\t" if suffix.lower() in {".tsv", ".txt"} else ","
    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "cp949"):
        try:
            from io import BytesIO

            return pd.read_csv(BytesIO(raw), sep=separator, encoding=encoding)
        except UnicodeDecodeError as exc:
            last_error = exc
    raise ValueError("KRX investor-flow file encoding is not UTF-8/CP949") from last_error


def validate_export(raw: bytes, suffix: str) -> dict:
    """Perform a source-shape preflight without changing or canonicalizing bytes."""
    frame = _read_tabular(raw, suffix)
    aliases = {
        "trade_date": {"trade_date", "일자", "거래일"},
        "ticker": {"ticker", "종목코드", "단축코드"},
        "market": {"market", "시장", "시장구분"},
        "investor_type": {"investor_type", "투자자구분", "투자자"},
    }
    rendered = {str(column).strip() for column in frame.columns}
    missing = [
        canonical for canonical, names in aliases.items()
        if not rendered.intersection(names)
    ]
    volume_names = {
        "sell_volume", "buy_volume", "net_volume",
        "매도거래량", "매수거래량", "순매수거래량",
    }
    value_names = {
        "sell_value", "buy_value", "net_value",
        "매도거래대금", "매수거래대금", "순매수거래대금",
    }
    if not rendered.intersection(volume_names | value_names):
        missing.append("volume_or_value_fields")
    if missing:
        raise ValueError(f"KRX investor-flow export missing columns: {missing}")
    if frame.empty:
        raise ValueError("KRX investor-flow export is empty")
    return {"row_count": len(frame), "columns": sorted(rendered)}


def ingest(
    source_file: str,
    dest: str,
    *,
    authorization_id: str,
) -> str:
    """Preserve one licensed export and return its immutable Bronze URI."""
    if not authorization_id.strip():
        raise ValueError("authorization_id is required")
    path = Path(source_file)
    raw = path.read_bytes()
    shape = validate_export(raw, path.suffix)
    digest = hashlib.sha256(raw).hexdigest()
    base = base_uri(dest)
    object_uri = (
        f"{base}/investor_flows/krx/sha256={digest}/"
        f"source{path.suffix.lower()}"
    )
    manifest_uri = f"{base}/investor_flows/krx/sha256={digest}/manifest.json"
    write_bytes(raw, object_uri)
    manifest = {
        "schema_version": "krx-investor-flow-export-v1",
        "source": "KRX_DATA_MARKETPLACE_AUTHORIZED_EXPORT",
        "authorization_id": authorization_id.strip(),
        "original_filename": path.name,
        "sha256": digest,
        "object_uri": object_uri,
        "row_count": shape["row_count"],
        "columns": shape["columns"],
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }
    write_text_if_changed(
        json.dumps(manifest, ensure_ascii=False, sort_keys=True), manifest_uri,
    )
    print(
        f"[krx-investor-flows] saved rows={shape['row_count']} sha256={digest}",
        flush=True,
    )
    return object_uri


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-file", required=True)
    parser.add_argument("--authorization-id", required=True)
    parser.add_argument("--dest", choices=["local", "s3"], default="local")
    args = parser.parse_args()
    ingest(
        args.source_file,
        args.dest,
        authorization_id=args.authorization_id,
    )


if __name__ == "__main__":
    main()
