from datetime import date

import pandas as pd
import pytest

from pipeline.silver.prices import _with_adj_close
from pipeline.silver.reviewed_price_references import apply_reviewed_references
from pipeline.silver_quality.streaming_adj_close import _factor_for_row, _SeriesState


def source():
    return pd.DataFrame([
        dict(ticker="052670",trade_date=date(2026,2,6),close=2080.,shares=29129064.,prev_diff=0.,fluc_rate=0.),
        dict(ticker="052670",trade_date=date(2026,2,9),close=625000.,shares=19419.,prev_diff=622920.,fluc_rate=29948.08),
        dict(ticker="052670",trade_date=date(2026,2,10),close=503000.,shares=19419.,prev_diff=-122000.,fluc_rate=-19.52),
    ])


def test_consolidation_batch_price_return_and_source_preservation():
    raw=source(); untouched=raw.copy(deep=True)
    fixed=apply_reviewed_references(raw)
    pd.testing.assert_frame_equal(raw,untouched)
    assert fixed.loc[1,"quoted_prev_diff"]==622920
    result=_with_adj_close(fixed)
    assert result.adj_close.iloc[0]==3120000
    assert result.adj_close.iloc[1]==625000
    assert result.adj_close.iloc[1]/result.adj_close.iloc[0]-1==pytest.approx(-.7996794871794872)
    assert result.adj_close.iloc[2]/result.adj_close.iloc[1]-1==pytest.approx(-.1952)


def test_daily_and_streaming_use_same_reference():
    one=apply_reviewed_references(source().iloc[[1]])
    _,factor=_factor_for_row(date(2026,2,9),625000,one.prev_diff.iloc[0],_SeriesState(date(2026,2,6),2080,0))
    assert factor==1500


def test_idempotent():
    fixed=apply_reviewed_references(source())
    pd.testing.assert_frame_equal(fixed,apply_reviewed_references(fixed))


@pytest.mark.parametrize("column,value",[("close",625001),("shares",20000),("prev_diff",622000)])
def test_changed_source_rejected(column,value):
    raw=source();raw.loc[1,column]=value
    with pytest.raises(ValueError,match="changed"):
        apply_reviewed_references(raw)


def test_no_guessing_for_unreviewed_spikes():
    raw=source();raw.ticker="999999"
    pd.testing.assert_frame_equal(raw,apply_reviewed_references(raw))


def test_already_economically_adjusted_upstream_is_not_double_corrected():
    raw=source();raw.loc[1,'prev_diff']=625000-3120000
    fixed=_with_adj_close(apply_reviewed_references(raw))
    assert fixed.adj_close.iloc[0]==3120000
