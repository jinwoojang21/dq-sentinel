# INC-0001 — out of range on `orders`

| Batch | Severity | Upstream bug | Investigator |
|---|---|---|---|
| `b00006` | sev2 | yes | rule-based baseline |

## Summary
Rule-based baseline classified this as out_of_range.

## Failed checks
- **quarantine_rate**: 0.13 (threshold 0.02) — 39/300 records failed record-level validation

## Evidence
- 39/300 records quarantined (13.0%)
- sample `amount` values: [12026.0, 10035.0, 11489.0]

## Recommended actions
1. Review the evidence and confirm with the producing team.

## Investigation trace
| # | Tool | Input | ok | ms |
|---|---|---|---|---|
| 1 | `get_failed_checks` | `{}` | ✓ | 1 |
| 2 | `sample_quarantine` | `{'limit': 3, 'reason_contains': 'range:'}` | ✓ | 60 |
| 3 | `submit_incident_report` | `{'severity': 'sev2', 'root_cause': 'out_of_range', 'summary': 'Rule-based baseline classif` | ✓ | 0 |
