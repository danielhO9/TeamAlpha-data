"""Parquet 저장 — 로컬 또는 S3. 목적지 URI 로 분기.

같은 코드로 로컬 테스트(파일)와 S3 적재를 모두 지원한다:
  - dest 가 s3://... 로 시작하면 boto3 로 업로드
  - 아니면 로컬 파일로 저장

S3 자격증명은 boto3 기본 체인을 따른다(로컬은 AWS_PROFILE, ECS 는 Task Role). 코드에 프로필 하드코딩 없음.
"""
from __future__ import annotations

import io
from pathlib import Path

import pandas as pd


def _split_s3(uri: str) -> tuple[str, str]:
    without = uri[len("s3://"):]
    bucket, _, key = without.partition("/")
    return bucket, key


def write_parquet(df: pd.DataFrame, dest: str) -> str:
    """DataFrame 을 parquet 로 dest 에 저장. index 는 보존한다(pykrx 인덱스가 의미 있음)."""
    if dest.startswith("s3://"):
        import boto3

        bucket, key = _split_s3(dest)
        buf = io.BytesIO()
        df.to_parquet(buf, index=True)
        buf.seek(0)
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    else:
        path = Path(dest)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=True)
    return dest


def exists(dest: str) -> bool:
    """dest 에 이미 저장돼 있는지 (재개용). 로컬=파일 존재, S3=객체 head."""
    if dest.startswith("s3://"):
        import boto3
        from botocore.exceptions import ClientError

        bucket, key = _split_s3(dest)
        try:
            boto3.client("s3").head_object(Bucket=bucket, Key=key)
            return True
        except ClientError:
            return False
    return Path(dest).exists()


def write_text(text: str, dest: str) -> str:
    """리터럴 텍스트(예: DART raw JSON 응답)를 그대로 저장 — 로컬 또는 S3."""
    if dest.startswith("s3://"):
        import boto3

        bucket, key = _split_s3(dest)
        boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=text.encode("utf-8"))
    else:
        path = Path(dest)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    return dest
