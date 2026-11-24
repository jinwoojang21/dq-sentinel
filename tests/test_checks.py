from dq_sentinel.checks import compute_metrics, run_checks, validate_batch
from dq_sentinel.producer import generate_batch


def failed(contract, scenario, history=(500,) * 6):
    recs = generate_batch(9, 500, scenario)
    vr = validate_batch(recs, contract)
    m = compute_metrics("b", vr, contract)
    return {(c.check, c.field) for c in run_checks(m, contract, list(history)) if c.status == "fail"}, vr


def test_healthy_batch_passes(contract):
    f, vr = failed(contract, "healthy")
    assert f == set()
    assert len(vr.quarantined) == 0


def test_rename_flags_schema_drift(contract):
    f, _ = failed(contract, "schema_drift_rename")
    assert ("schema_missing_field", "amount") in f
    assert ("schema_unexpected_field", "order_amount") in f


def test_type_change_quarantines_with_type_reason(contract):
    _, vr = failed(contract, "type_change")
    assert vr.quarantined and all(any(r.startswith("type:amount") for r in rs) for _, rs in vr.quarantined)


def test_duplicates_detected(contract):
    f, vr = failed(contract, "duplicates")
    assert ("duplicate_rate", None) in f
    assert vr.duplicates > 0


def test_null_spike(contract):
    f, _ = failed(contract, "null_spike")
    assert ("null_rate", "channel") in f


def test_volume_needs_history(contract):
    f, _ = failed(contract, "volume_drop", history=())
    assert ("volume", None) not in f
    f, _ = failed(contract, "volume_drop")
    assert ("volume", None) in f


def test_naive_timestamp_rejected(contract):
    rec = generate_batch(0, 1)[0]
    rec["created_at"] = "2026-01-01T00:00:00"
    vr = validate_batch([rec], contract)
    assert vr.quarantined[0][1] == ["type:created_at:timestamp missing timezone"]
