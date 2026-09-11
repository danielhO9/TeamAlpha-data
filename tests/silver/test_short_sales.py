import pandas as pd
import pytest

from pipeline.silver.short_sales import prepare


def inputs():
    return pd.DataFrame([{
        "ticker": "001020", "trade_date": "20150102", "ssts_cntg_qty": "200",
        "acml_vol": "100", "ssts_vol_rlim": "200", "stnd_vol_smtn": "9000",
        "short_qty_verified": True, "short_qty_evidence": "official:001020:20150102",
    }]), pd.DataFrame([{
        "ticker": "001020", "trade_date": "2015-01-02", "raw_total_volume": "1,000",
        "volume_basis": "UNADJUSTED_SHARES", "raw_volume_verified": True,
        "volume_evidence": "raw-volume:001020:20150102",
    }])


def test_mixed_adjustment_and_cumulative_columns_never_used():
    sales, volumes = inputs()
    original = sales.copy(deep=True)
    result = prepare(sales, volumes).iloc[0]
    assert result.short_sale_ratio_pct == 20
    assert result.ratio_value_verified
    assert result.acml_vol == "100"
    assert result.ssts_vol_rlim == "200"
    pd.testing.assert_frame_equal(sales, original)


@pytest.mark.parametrize("field,value,status", [
    ("raw_total_volume", "", "MISSING_RAW_VOLUME"),
    ("raw_volume_verified", "False", "UNVERIFIED_RAW_VOLUME"),
    ("volume_basis", "ADJUSTED_SHARES", "UNVERIFIED_RAW_VOLUME"),
    ("volume_evidence", "", "UNVERIFIED_RAW_VOLUME"),
    ("raw_total_volume", "100", "QUANTITY_EXCEEDS_RAW_VOLUME"),
    ("trade_date", "20150105", "MISSING_RAW_VOLUME"),
    ("ticker", "000040", "MISSING_RAW_VOLUME"),
])
def test_invalid_denominator_never_falls_back(field, value, status):
    sales, volumes = inputs()
    volumes = volumes.astype(object)
    volumes.loc[0, field] = value
    result = prepare(sales, volumes).iloc[0]
    assert pd.isna(result.short_sale_ratio_pct)
    assert result.ratio_status == status
    assert not result.ratio_value_verified


def test_zero_short_is_real_zero_only_with_positive_denominator():
    sales, volumes = inputs()
    sales.loc[0, "ssts_cntg_qty"] = "0"
    assert prepare(sales, volumes).iloc[0].short_sale_ratio_pct == 0
    volumes.loc[0, "raw_total_volume"] = "0"
    result = prepare(sales, volumes).iloc[0]
    assert result.ratio_status == "ZERO_RAW_VOLUME"
    assert pd.isna(result.short_sale_ratio_pct)


def test_computing_ratio_does_not_certify_quantity():
    sales, volumes = inputs()
    sales.loc[0, "short_qty_verified"] = False
    result = prepare(sales, volumes).iloc[0]
    assert result.short_sale_ratio_pct == 20
    assert not result.ratio_value_verified


@pytest.mark.parametrize("side", ["sales", "volumes"])
def test_duplicate_keys_cannot_multiply_or_choose_vintages(side):
    sales, volumes = inputs()
    if side == "sales":
        sales = pd.concat([sales, sales])
    else:
        volumes = pd.concat([volumes, volumes])
    with pytest.raises(ValueError, match="duplicate"):
        prepare(sales, volumes)


@pytest.mark.parametrize("bad", ["-1", "Infinity", "1.5", "broken"])
def test_invalid_quantities_rejected(bad):
    sales, volumes = inputs()
    sales.loc[0, "ssts_cntg_qty"] = bad
    with pytest.raises(ValueError):
        prepare(sales, volumes)
