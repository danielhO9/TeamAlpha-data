from datetime import date
from decimal import Decimal
import json

import pandas as pd
import pytest

from pipeline.silver.reviewed_financial_units import correct_candidates
from pipeline.silver.fx_rates import parse_quotes
from tools.repair_financial_units import evidence_match


def fixture():
    common = dict(identifier="007720", revision_key="20241114002786",
                  source="DART", fs_type="CFS", currency="KRW")
    return pd.DataFrame([{**common, "metric":"total_assets", "value":159266074177000000.0},
                         {**common, "metric":"net_income", "value":374241407000000.0}])


def test_reviewed_scale_repair_is_exact_scoped_and_idempotent():
    frame = fixture()
    fixed, log = correct_candidates(frame)
    assert fixed.iloc[0].value == pytest.approx(159266074177)
    assert fixed.iloc[1].value == pytest.approx(374241407)
    assert len(log)==1
    again, log = correct_candidates(fixed)
    pd.testing.assert_frame_equal(fixed,again)
    assert log==[]
    assert frame.iloc[0].value == 159266074177000000.0


@pytest.mark.parametrize("field,value",[("revision_key","other"),("identifier","other"),
                                      ("currency","USD"),("source","FMP")])
def test_never_scales_unreviewed_scope(field,value):
    frame=fixture();frame[field]=value
    fixed,log=correct_candidates(frame)
    pd.testing.assert_frame_equal(frame,fixed)
    assert log==[]


def test_changed_or_missing_anchor_blocks():
    f=fixture();f.loc[0,'value']=123
    with pytest.raises(ValueError,match="anchor changed"):correct_candidates(f)
    with pytest.raises(ValueError,match="lacks assets"):correct_candidates(fixture().iloc[1:])


def test_document_match_preserves_negative_sign_and_account():
    rows=[["영업손익","(203,617,264)","233,654,482"]]
    assert evidence_match(rows,"operating_income",Decimal('-203617264'))
    assert not evidence_match(rows,"operating_income",Decimal('203617264'))
    assert not evidence_match(rows,"total_assets",Decimal('-203617264'))


def quote(**kw):
    return dict(symbol="JPYKRW", date="2024-01-02", open=9,high=10,low=8,close=9.5,**kw)


def test_fx_is_per_one_unit_not_100_yen():
    rows=parse_quotes(json.dumps([quote()]).encode(),'JPY',date(2024,1,1),date(2024,1,5))
    assert rows==[(date(2024,1,2),Decimal('9.5'))]


@pytest.mark.parametrize('field,value', [('symbol','KRWJPY'),('close',0),('close',float('nan')),('high',8)])
def test_bad_fx_blocks(field,value):
    r=quote();r[field]=value
    with pytest.raises(ValueError):
        parse_quotes(json.dumps([r]).encode(),'JPY',date(2024,1,1),date(2024,1,5))


def test_every_financial_gold_path_converts_currency():
    from pathlib import Path
    root=Path(__file__).parents[2]/'pipeline/gold/factors'
    for name in ('legacy_daily_bulk','market_leverage','paid_in_capital_ratio',
                 'operating_return_on_capital_employed'):
        assert 'public.factor_fx_rate' in (root/f'{name}.sql').read_text()
