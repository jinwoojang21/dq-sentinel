# INC-0001 — new category on `orders`

| Batch | Severity | Upstream bug | Investigator |
|---|---|---|---|
| `b00006` | sev2 | no — contract change | rule-based baseline |

## Summary
Rule-based baseline classified this as new_category.

## Failed checks
- **quarantine_rate**: 0.23 (threshold 0.02) — 115/500 records failed record-level validation

## Evidence
- 115/500 records quarantined (23.0%)
- sample `currency` values: ['JPY', 'JPY', 'JPY']

## Recommended actions
1. Review the evidence and confirm with the producing team.

## Proposed contract change
_new value(s) ['JPY'] for `currency`_ — written to `INC-0001-orders.yaml`

```diff
--- contracts/orders.yaml
+++ contracts/orders.yaml (proposed)
@@ -1,5 +1,5 @@
 name: orders
-version: 3
+version: 4
 owner: commerce-data
 description: Order lifecycle events emitted by the checkout service.
 fields:
@@ -22,6 +22,7 @@
     - USD
     - EUR
     - GBP
+    - JPY
   status:
     type: string
     required: true
```

## Investigation trace
| # | Tool | Input | ok | ms |
|---|---|---|---|---|
| 1 | `get_failed_checks` | `{}` | ✓ | 2 |
| 2 | `sample_quarantine` | `{'limit': 3, 'reason_contains': 'allowed:'}` | ✓ | 59 |
| 3 | `propose_contract_patch` | `{'operations': [{'op': 'add_allowed_value', 'field': 'currency', 'value': 'JPY'}], 'ration` | ✓ | 4 |
| 4 | `submit_incident_report` | `{'severity': 'sev2', 'root_cause': 'new_category', 'summary': 'Rule-based baseline classif` | ✓ | 0 |
