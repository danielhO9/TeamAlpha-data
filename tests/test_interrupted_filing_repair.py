import hashlib
from datetime import date

import pandas as pd
import pytest

from tools import repair_interrupted_filing_20261021 as repair


@pytest.mark.parametrize('mutation', [None, 'hash', 'filing', 'ticker', 'availability'])
def test_repair_rejects_changed_source_or_expanded_scope(tmp_path, monkeypatch, mutation):
    key = 'financials/dart/year=2026/corp=071950/11012.json'
    path = tmp_path / key
    path.parent.mkdir(parents=True)
    path.write_bytes(b'changed' if mutation == 'hash' else b'original')
    monkeypatch.setattr(repair, 'OBJECTS', {key: hashlib.sha256(b'original').hexdigest()})
    row = {'filing_id': repair.FILING, 'identifier': '071950',
           'available_date': date(2026, 10, 22)}
    if mutation == 'filing':
        row['filing_id'] = 'another'
    if mutation == 'ticker':
        row['identifier'] = '000660'
    if mutation == 'availability':
        row['available_date'] = date(2026, 10, 21)
    frame = pd.DataFrame([row])
    stats = {'input_rows': 1, 'transformed_rows': 1, 'excluded_rows': 0, 'rejected_rows': 0}
    monkeypatch.setattr(repair.financials, 'prepare', lambda *a, **k: (frame, stats))
    monkeypatch.setattr(repair.dividends, 'prepare', lambda *a, **k: (frame.iloc[:0], {}))
    if mutation:
        with pytest.raises(ValueError):
            repair.prepare(tmp_path)
    else:
        assert len(repair.prepare(tmp_path).fundamentals) == 1
