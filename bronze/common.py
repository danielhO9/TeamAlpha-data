"""공통 헬퍼 — 저장 루트 경로, 날짜 포맷, .env 로드. 모든 수집기가 공유한다."""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


def ymd_to_dash(date: str) -> str:
    """YYYYMMDD → YYYY-MM-DD (파티션 경로용)."""
    return datetime.strptime(date, "%Y%m%d").strftime("%Y-%m-%d")


def base_uri(dest: str) -> str:
    """저장 루트. 버킷 자체가 bronze 계층이라 bronze/ 프리픽스는 두지 않는다.
    local -> ./data,  s3 -> s3://<bucket>"""
    if dest == "s3":
        bucket = os.environ.get("S3_BRONZE_BUCKET")
        if not bucket:
            raise SystemExit("S3_BRONZE_BUCKET 환경변수가 없습니다 (.env 확인)")
        return f"s3://{bucket}"
    return str(PROJECT_ROOT / "data")
