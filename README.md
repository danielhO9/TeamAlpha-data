# TeamAlpha-Ingest

KRX 시세·DART 재무 데이터를 **S3 bronze**(raw)로 수집하는 배치 파이프라인.
과거는 공식 API(data.go.kr)로 대량 수집, 최근·매일은 pykrx, 재무는 OpenDART.

- **bronze 계층 상세 설계**(소스 배정·저장구조·형식) → [`bronze/README.md`](bronze/README.md)
- **전체 인프라 설계**(S3/RDS/ECS) → `../notes/data_pipeline_architecture.md`

## 모듈 (`bronze/`)

| 파일 | 역할 |
|---|---|
| `common.py` | 공통 헬퍼 (경로·날짜·.env 로드) |
| `datago.py` | data.go.kr 공식 — 주식·지수 시세 (과거 ~2026.06) |
| `krx.py` · `ingest.py` | pykrx — 시세 (일 배치 `--date` + 백필 `--from/--to`) |
| `members.py` | pykrx — 지수 구성종목 (분기) |
| `dart.py` | OpenDART — 재무 주요계정 (다중회사) |
| `sink.py` | parquet/JSON 저장 (로컬·S3 동일 코드) |

## 셋업

```bash
uv sync
cp .env.example .env            # KRX·DART·data.go.kr 키, AWS 설정 채우기
aws sso login --profile <aws-profile>   # S3 적재 시
```
data.go.kr 키는 공공데이터포털에서 **주식시세정보·지수시세정보 각각 활용신청** 후 발급.

## 실행

```bash
# 과거 대량 (~2026.06)
uv run python -m bronze.datago  --from 20150101 --to 20260630     # 주식·지수 시세(공식)
uv run python -m bronze.members --from 20150101 --to 20260630     # 지수 구성종목(pykrx)
uv run python -m bronze.dart    --from 20150101 --to 20260630     # DART 재무(주요계정)

# 최근·매일 (2026.07~)
uv run python -m bronze.ingest --date 20260706                    # pykrx 일 배치
uv run python -m bronze.ingest --from 20260701 --to 20260706      # pykrx 백필(재개)

# S3 적재는 --dest s3
```

- 로컬 출력은 `./data/`(gitignore), 최종은 S3.
- 자세한 저장 경로·형식·주의사항은 [`bronze/README.md`](bronze/README.md).
