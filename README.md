# TeamAlpha Data Pipeline

KRX와 DART 데이터를 수집해 원천 데이터는 **S3 bronze**에 저장하고, 분석/조회용 데이터는 **RDS PostgreSQL silver**에 정규화해 적재하는 배치 파이프라인입니다.

## 프로젝트 개요

```text
KRX OpenAPI / OpenDART / marcap
  -> bronze 수집기
  -> S3 bronze 저장소
  -> ECS daily task
  -> silver 적재 로직
  -> RDS PostgreSQL
```

- **bronze**: API/데이터셋 응답을 최대한 그대로 저장합니다. 필터링, 리네임, 타입 변환은 silver에서 처리합니다.
- **silver**: 팀원이 SQL로 조회하기 좋도록 `asset` 중심으로 정규화합니다.
- **운영 스케줄**: 화~토 오전 08:30 KST에 실행해 전날 KRX 데이터를 적재합니다.
- **자동 배포**: `main` 브랜치에 push하면 GitHub Actions가 ECR/ECS/Scheduler를 갱신합니다.
- **결과 알림**: daily ECS task가 종료되면 SNS 이메일로 성공/실패 결과를 받습니다.

## 폴더 구조

```text
.
├── .github/workflows/deploy.yml  # GitHub Actions 자동 배포
├── deploy/Dockerfile             # ECS/Fargate 실행 이미지
├── pipeline/                     # 수집, 동기화, silver 적재 코드
│   ├── bronze/                   # S3 bronze 원천 데이터 수집기
│   ├── common/                   # 경로, 저장, DB 공통 유틸
│   └── silver/                   # RDS silver 정규화/적재
├── sql/schema.sql                # RDS silver schema
├── schema_tables.md              # silver 테이블 설계 상세 문서
├── pyproject.toml                # Python 프로젝트/의존성 설정
└── uv.lock                       # 의존성 lock 파일
```

## AWS 구조

운영에 필요한 핵심 흐름만 요약하면 다음과 같습니다.

```text
EventBridge Scheduler
  -> ECS Fargate task
  -> S3 bronze 저장
  -> RDS silver 적재
  -> SNS 이메일 알림
```

### 배포 흐름

코드가 `main` 브랜치에 push되면 GitHub Actions가 새 실행 이미지를 만들고 운영 ECS 설정을 갱신합니다.

```text
GitHub main push
  -> GitHub Actions 실행
  -> deploy/Dockerfile로 Docker 이미지 빌드
  -> ECR repository에 이미지 push
     - 태그 1: Git commit SHA
     - 태그 2: latest
  -> ECS task definition 새 revision 등록
     - 새 revision이 방금 push한 ECR 이미지를 바라봄
  -> EventBridge Scheduler target 갱신
     - 다음 스케줄 실행부터 새 task definition 사용
```

즉, GitHub에 코드를 push하면 새 Docker 이미지가 ECR에 올라가고, ECS는 다음 실행부터 그 이미지를 받아 실행합니다.

### 스케줄 실행 흐름

매일 실행은 EventBridge Scheduler가 시작합니다.

```text
EventBridge Scheduler
  -> ECS Fargate task 실행
  -> ECS가 ECR에서 Docker 이미지 pull
  -> 컨테이너에서 python -m pipeline.daily_full 실행
  -> KRX/DART API 호출
  -> S3 bronze 저장
  -> 필요한 S3 객체를 /app/data로 다운로드
  -> RDS silver 적재
  -> ECS task 종료
  -> EventBridge rule이 STOPPED 이벤트 감지
  -> SNS 이메일 알림
```

핵심 리소스 종류:

| 구분 | 설명 |
|---|---|
| 리전 | `ap-northeast-2` |
| S3 bronze bucket | 원천 데이터를 저장하는 S3 bucket |
| ECR repository | ECS에서 실행할 Docker 이미지를 저장하는 repository |
| ECS cluster | daily batch task를 실행하는 Fargate cluster |
| ECS task definition | 파이프라인 컨테이너, role, secret 주입 설정 |
| Scheduler | daily ECS task를 시작하는 EventBridge Scheduler |
| Scheduler 시간 | `cron(30 8 ? * TUE-SAT *)`, `Asia/Seoul` |
| RDS PostgreSQL | silver 테이블을 저장하는 private database |
| SNS topic | daily task 결과 이메일 알림 |

운영 task에는 AWS Secrets Manager 값이 환경변수로 주입됩니다.

```text
KRX_API_KEY
DART_API_KEY
S3_BRONZE_BUCKET
SILVER_DB_URL
```

`.env`, API key, DB 비밀번호, 로컬 `data/`는 커밋하면 안 됩니다.

## S3 Bronze 구조

버킷:

```text
s3://<bronze-bucket>/
```

경로 구조:

```text
stock/
  marcap/
    date=YYYY-MM-DD/
      all.parquet

  krxapi/
    date=YYYY-MM-DD/
      kospi.parquet
      kosdaq.parquet

index/
  krxapi/
    date=YYYY-MM-DD/
      kospi.parquet
      kosdaq.parquet
      krx.parquet

financials/
  dart/
    corpCode.xml
    year=YYYY/
      corp=<ticker>/
        11011.json   # FY
        11013.json   # Q1
        11012.json   # Q2
        11014.json   # Q3
```

bronze 원칙:

- 가능한 한 원천 응답 단위에 맞춰 파티션을 나눕니다.
- 값은 원천 응답 그대로 저장합니다.
- `stock/marcap`은 과거 주식 가격 백필에 사용합니다.
- `stock/krxapi`, `index/krxapi`는 daily 증분 적재에 사용합니다.
- `financials/dart/corpCode.xml`은 bronze에 저장하고 silver에서 재사용합니다.

## RDS Silver 구조

silver는 PostgreSQL 테이블 4개로 구성됩니다.

```text
asset
asset_identifier
price_daily
fundamental
```

관계:

```text
asset
  -> asset_identifier  # KRX ticker, DART corp_code 등 외부 식별자
  -> price_daily       # 주식/지수 일봉, 수정종가, 거래량, 시가총액
  -> fundamental       # DART 재무 지표 long format
```

| 테이블 | 역할 | 주요 키 |
|---|---|---|
| `asset` | 종목/지수 마스터 | `asset_id` |
| `asset_identifier` | KRX/DART 식별자 매핑 | `(asset_id, source, identifier)` |
| `price_daily` | 주식/벤치마크 지수 일봉 | `(asset_id, source, trade_date)` |
| `fundamental` | DART 주요 재무계정 long format | `(asset_id, source, period_end, fiscal_period, fs_type, metric)` |

컬럼별 상세 설계는 [schema_tables.md](schema_tables.md)와 [sql/schema.sql](sql/schema.sql)를 참고합니다.

## Daily 실행 흐름

운영 진입점:

```bash
python -m pipeline.daily_full
```

대상 날짜:

- `PIPELINE_DATE`가 있으면 해당 날짜를 사용합니다.
- 없으면 `Asia/Seoul` 기준 어제 날짜를 사용합니다.

실행 순서:

1. 대상 날짜의 주식/지수 bronze 데이터를 S3에 저장합니다.
2. 당해 연도 DART 데이터를 확인하고, 변경된 JSON만 S3에 다시 저장합니다.
3. 필요한 S3 객체만 ECS 컨테이너의 `/app/data`로 다운로드합니다.
4. RDS에서 대상 날짜의 기존 `price_daily`를 삭제합니다.
5. `price_daily`와 변경된 `fundamental`을 upsert합니다.

KRX OpenAPI는 당일 데이터를 안정적으로 제공하지 않기 때문에 다음날 오전에 전날 데이터를 가져옵니다.

```text
화요일 08:30 KST -> 월요일 데이터
수요일 08:30 KST -> 화요일 데이터
...
토요일 08:30 KST -> 금요일 데이터
```

## 로컬 설정

```bash
uv sync
cp .env.example .env
```

`.env` 예시:

```text
KRX_API_KEY=...
DART_API_KEY=...
AWS_PROFILE=<aws-profile>
S3_BRONZE_BUCKET=<bronze-bucket>
SILVER_DB_URL=postgresql://<user>:<password>@<rds-endpoint>:5432/<database>
```

AWS CLI 로그인:

```bash
aws sso login --profile <aws-profile>
```

## 자주 쓰는 명령

문법 확인:

```bash
uv run python -m compileall -q pipeline
```

특정 날짜를 production daily 방식으로 실행:

```bash
PIPELINE_DATE=20260713 uv run python -m pipeline.daily_full
```

bronze 수집기 수동 실행:

```bash
uv run python -m pipeline.bronze.stock_marcap --from 2015 --to 2026 --dest s3
uv run python -m pipeline.bronze.stock_krxapi --from 20260713 --to 20260713 --dest s3
uv run python -m pipeline.bronze.index --from 20260713 --to 20260713 --dest s3
uv run python -m pipeline.bronze.financials --from 2026 --to 2026 --dest s3
```

로컬 `./data`에서 silver 적재:

```bash
uv run python -m pipeline.silver.load --mode backfill
uv run python -m pipeline.silver.load --mode incremental --date 20260713
```

GitHub Actions를 쓰지 못할 때 수동 이미지 배포:

```bash
AWS_ACCOUNT_ID=<aws-account-id>
AWS_REGION=ap-northeast-2
ECR_REPOSITORY=<ecr-repository>

AWS_PROFILE=<aws-profile> aws ecr get-login-password --region ap-northeast-2 \
  | docker login --username AWS --password-stdin \
      "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

docker buildx build \
  --platform linux/amd64 \
  -f deploy/Dockerfile \
  -t "${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPOSITORY}:latest" \
  --push \
  .
```

## 자동 배포

자동 배포는 [.github/workflows/deploy.yml](.github/workflows/deploy.yml)에서 관리합니다.

`main` 브랜치에 push하면 다음 작업이 실행됩니다.

1. GitHub OIDC로 AWS deploy role을 assume합니다.
2. `linux/amd64` Docker 이미지를 빌드합니다.
3. ECR에 commit SHA 태그와 `latest` 태그를 push합니다.
4. ECS task definition 새 revision을 등록합니다.
5. EventBridge Scheduler target을 새 task definition으로 갱신합니다.

필요한 GitHub secret:

```text
AWS_DEPLOY_ROLE_ARN=arn:aws:iam::<aws-account-id>:role/<deploy-role-name>
```

GitHub Actions는 repo 이름을 기준으로 ECR/ECS/Scheduler 이름을 추론합니다. 실제 리소스 이름이 기본 naming convention과 다르면 아래 variables로 override합니다.

```text
ECR_REPOSITORY=<ecr-repository>
ECS_TASK_FAMILY=<ecs-task-definition-family>
CONTAINER_NAME=<ecs-container-name>
SCHEDULE_NAME=<eventbridge-scheduler-name>
```

## 알림

daily task 결과 알림은 다음 흐름으로 동작합니다.

```text
ECS task STOPPED 이벤트
  -> EventBridge rule
  -> SNS topic
  -> 이메일 구독
```

메일에는 task 상태, exit code, 종료 이유, 시작/종료 시각, task ARN, task definition ARN이 포함됩니다.

```text
Exit code 0 -> 정상 종료
그 외 값 또는 exit code 없음 -> CloudWatch 로그 확인 필요
```

## Git 관리

커밋하지 않는 로컬/생성 파일:

```text
.env
data/
.venv/
__pycache__/
.DS_Store
docs_cache/
```

push 전 확인:

```bash
git status --short
uv run python -m compileall -q pipeline
```
