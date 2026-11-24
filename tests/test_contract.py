import pytest

from dq_sentinel.contract import PatchError, apply_patch, yaml_diff


def test_rename_preserves_order_and_bumps_version(contract):
    new = apply_patch(contract.raw, [{"op": "rename_field", "from": "amount", "to": "order_amount"}])
    assert list(new["fields"])[2] == "order_amount"
    assert new["version"] == contract.version + 1
    assert contract.raw["fields"].get("amount")  # original untouched
    assert "+  order_amount:" in yaml_diff(contract.raw, new, "orders")


def test_add_allowed_value(contract):
    new = apply_patch(contract.raw, [{"op": "add_allowed_value", "field": "currency", "value": "JPY"}])
    assert "JPY" in new["fields"]["currency"]["allowed"]


@pytest.mark.parametrize("ops", [
    [{"op": "drop_table"}],
    [{"op": "rename_field", "from": "nope", "to": "x"}],
    [{"op": "rename_field", "from": "amount", "to": "currency"}],
    [{"op": "set_field_property", "field": "amount", "key": "owner", "value": 1}],
    [{"op": "set_slo", "key": "made_up", "value": 1}],
    [{"op": "add_field", "field": "x", "spec": {"type": "blob"}}],
])
def test_invalid_patches_rejected(contract, ops):
    with pytest.raises(PatchError if ops[0]["op"] != "add_field" else ValueError):
        apply_patch(contract.raw, ops)
