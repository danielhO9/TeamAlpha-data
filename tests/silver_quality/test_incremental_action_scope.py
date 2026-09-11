import pandas as pd

from pipeline.silver_quality import registry
from pipeline.silver_quality.models import CandidateBundle


def test_registered_rules_check_only_changed_action_receipts(monkeypatch):
    captured: list[pd.DataFrame] = []
    monkeypatch.setattr(
        registry,
        "check_actions",
        lambda frame, _partition_key: captured.append(frame.copy()) or [],
    )
    actions = pd.DataFrame([
        {"identifier": "005930", "rcept_no": "old"},
        {"identifier": "000660", "rcept_no": "changed"},
    ])
    identifiers = pd.DataFrame([
        {"source": "KRX", "identifier": "005930"},
        {"source": "KRX", "identifier": "000660"},
    ])

    registry.run_registered_rules(
        CandidateBundle(actions=actions, identifiers=identifiers),
        changed_action_receipts={"changed"},
    )

    assert len(captured) == 1
    assert captured[0]["rcept_no"].tolist() == ["changed"]
