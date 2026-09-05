"""승인된 KRX 공매도 순보유잔고 export를 immutable Bronze로 등록한다.

KRX 화면을 자동화하지 않는다. 구매했거나 Open API 활용승인을 받은 CSV/TSV/TXT만
받으며 최초 관측시각을 immutable manifest에 고정한다. 오늘 받은 과거 파일은 오늘
이전 시점의 PIT feature로 소급 사용하지 않는다.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from pipeline.common.paths import base_uri
from pipeline.common.sink import read_bytes, write_bytes, write_text_if_changed

CORE_ALIASES = {
    "position_date": {"position_date", "일자", "기준일"},
    "ticker": {"ticker", "종목코드", "단축코드"},
    "market": {"market", "시장", "시장구분"},
}
BALANCE_FIELDS = {
    "short_balance_quantity", "공매도순보유잔고수량", "순보유잔고수량",
    "short_balance_value", "공매도순보유잔고금액", "순보유잔고금액",
    "short_balance_ratio", "공매도순보유잔고비중", "순보유잔고비중", "비중",
}


def _read_tabular(raw: bytes, suffix: str) -> pd.DataFrame:
    if suffix.lower() not in {".csv", ".tsv", ".txt"}:
        raise ValueError("KRX short-balance input must be CSV/TSV/TXT")
    separator = "\t" if suffix.lower() in {".tsv", ".txt"} else ","
    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "cp949"):
        try:
            return pd.read_csv(io.BytesIO(raw), sep=separator, encoding=encoding)
        except UnicodeDecodeError as exc:
            last_error = exc
    raise ValueError("KRX short-balance encoding is not UTF-8/CP949") from last_error


def validate_export(raw: bytes, suffix: str) -> dict:
    frame = _read_tabular(raw, suffix)
    columns = {str(column).strip() for column in frame.columns}
    missing = [
        canonical for canonical, aliases in CORE_ALIASES.items()
        if not columns.intersection(aliases)
    ]
    if not columns.intersection(BALANCE_FIELDS):
        missing.append("short_balance_quantity_or_value_or_ratio")
    if missing:
        raise ValueError(f"KRX short-balance export missing columns: {missing}")
    if frame.empty:
        raise ValueError("KRX short-balance export is empty")
    return {"row_count": len(frame), "columns": sorted(columns)}


def ingest(source_file: str, dest: str, *, authorization_id: str) -> str:
    if not authorization_id.strip():
        raise ValueError("authorization_id is required")
    path = Path(source_file)
    raw = path.read_bytes()
    shape = validate_export(raw, path.suffix)
    digest = hashlib.sha256(raw).hexdigest()
    base = base_uri(dest)
    root = f"{base}/short_balances/krx/sha256={digest}"
    object_uri = f"{root}/source{path.suffix.lower()}"
    manifest_uri = f"{root}/manifest.json"
    manifest = {
        "schema_version": "krx-short-balance-export-v1",
        "source": "KRX_DATA_MARKETPLACE_AUTHORIZED_EXPORT",
        "authorization_id": authorization_id.strip(),
        "original_filename": path.name,
        "sha256": digest,
        "object_uri": object_uri,
        "row_count": shape["row_count"],
        "columns": shape["columns"],
        "observed_at": datetime.now(timezone.utc).isoformat(),
    }
    existing = read_bytes(manifest_uri)
    if existing is not None:
        previous = json.loads(existing.decode("utf-8"))
        immutable = {
            key: previous.get(key)
            for key in ("schema_version", "source", "authorization_id", "sha256", "object_uri")
        }
        expected = {
            key: manifest[key]
            for key in ("schema_version", "source", "authorization_id", "sha256", "object_uri")
        }
        if immutable != expected:
            raise RuntimeError(f"immutable KRX short-balance manifest mismatch: {manifest_uri}")
        existing_body = read_bytes(object_uri)
        if existing_body is None or hashlib.sha256(existing_body).hexdigest() != digest:
            raise RuntimeError(f"immutable KRX short-balance object mismatch: {object_uri}")
    else:
        write_bytes(raw, object_uri)
        write_text_if_changed(
            json.dumps(manifest, ensure_ascii=False, sort_keys=True), manifest_uri,
        )
    print(
        f"[krx-short-balances] saved rows={shape['row_count']} sha256={digest}",
        flush=True,
    )
    return object_uri


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-file", required=True)
    parser.add_argument("--authorization-id", required=True)
    parser.add_argument("--dest", choices=["local", "s3"], default="local")
    args = parser.parse_args()
    ingest(args.source_file, args.dest, authorization_id=args.authorization_id)


if __name__ == "__main__":
    main()
