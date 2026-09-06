from pathlib import Path

import pandas as pd

from pipeline.silver import assets


def _write_market(root: Path, day: str, ticker: str) -> None:
    path = root / f"stock/krxapi/date={day}/kospi.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"ISU_CD": ticker, "ISU_NM": f"name-{ticker}"}]).to_parquet(path)


def test_daily_asset_universe_reads_only_target_partition(tmp_path: Path):
    _write_market(tmp_path, "2026-08-31", "000001")
    _write_market(tmp_path, "2026-09-01", "005930")

    assert assets._stock_universe(
        str(tmp_path), target_date=pd.Timestamp("2026-09-01").date(),
    ) == {"005930": "name-005930"}


def test_holiday_asset_universe_reuses_latest_local_partition(tmp_path: Path):
    _write_market(tmp_path, "2026-08-31", "000001")
    _write_market(tmp_path, "2026-09-01", "005930")

    assert assets._stock_universe(
        str(tmp_path), target_date=pd.Timestamp("2026-09-02").date(),
    ) == {"005930": "name-005930"}
