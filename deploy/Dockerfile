# TeamAlpha-data 파이프라인 이미지 (ECS/Fargate 배치)
# EventBridge 스케줄 → ECS Task 로 전일 bronze 수집 + silver 증분 적재를 실행한다.
FROM python:3.12-slim

# uv 설치
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY pipeline/ ./pipeline/

# 자격증명: 로컬 .env 아님. ECS Task Role(S3/RDS) + Secrets(KRX/DART 키) 로 주입.
ENTRYPOINT ["uv", "run", "python", "-m", "pipeline.daily_full"]
