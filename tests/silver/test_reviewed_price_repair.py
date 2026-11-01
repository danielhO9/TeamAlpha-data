from datetime import date
from decimal import Decimal

import pytest

from pipeline.reviewed_price_repair_ecs import validate_prices


def history(scale=1):
    return [
        dict(trade_date=date(2026, 2, 6),close=Decimal(2080),
             adj_close=Decimal(2080)*scale,shares=29129064),
        dict(trade_date=date(2026, 2, 9),close=Decimal(625000),
             adj_close=Decimal(625000),shares=19419),
    ]


def test_repair_state_is_idempotent_and_rejects_unreviewed_scale():
    assert validate_prices(history())=='needs_repair'
    assert validate_prices(history(1500))=='already_corrected'
    with pytest.raises(ValueError,match='Unexpected adjustment'):
        validate_prices(history(1499))


@pytest.mark.parametrize('bad',[0,-1,float('nan'),float('inf')])
def test_invalid_history_cannot_be_published(bad):
    rows=history()
    rows.insert(0,dict(trade_date=date(2026,2,5),close=2080,adj_close=bad,shares=29129064))
    with pytest.raises(ValueError,match='Invalid historical'):
        validate_prices(rows)


def test_raw_anchor_drift_and_duplicate_rows_are_rejected():
    rows=history();rows[1]['shares']=19420
    with pytest.raises(ValueError,match='anchors changed'):
        validate_prices(rows)
    rows=history();rows.append(rows[0].copy())
    with pytest.raises(ValueError,match='Duplicate'):
        validate_prices(rows)
