# INC-0001 — format change on `orders`

| Batch | Severity | Upstream bug | Investigator |
|---|---|---|---|
| `b00006` | sev2 | yes | claude (claude-sonnet-5-5) |

## Summary
Batch b00006 was 100% quarantined (300/300). The producer began emitting currency codes in lowercase ("usd", "eur", "gbp") instead of the uppercase codes the contract allows. The values are the same, only the case differs. Batches b00000–b00005 were clean, so the change was sudden. No valid rows from this batch reached the orders table.

## Failed checks
- **quarantine_rate**: 1.0 (threshold 0.02) — 300/300 records failed record-level validation

## Evidence
- quarantine_rate 1.0 vs threshold 0.02; 300/300 records quarantined, valid=0
- Only quarantine reason: 'allowed:currency:value not in contract' (300 records)
- Quarantined currency values in b00006: usd=201, eur=69, gbp=30. All are lowercase forms of the allowed USD/EUR/GBP.
- Samples such as ord-ea57cc54e917 have currency 'eur' and ord-c137fe605136 has 'gbp'. Their other fields look normal.
- Batches b00000–b00005 had quarantine_rate 0.0, so this is a sudden change, not a trend.
- No missing or unexpected fields, duplicate_rate 0.0, null rates normal. The only problem is the currency case.

## Recommended actions
1. Commerce-data / checkout service owners: find the recent deploy or serializer change that lowercases currency, and restore uppercase ISO 4217 codes.
1. Do not add lowercase values to the contract's allowed list. Keep the contract strict and fix the producer.
1. After the producer fix, replay or reprocess the 300 quarantined b00006 records from orders_quarantine. If a fix is delayed, normalize with upper(currency) on those records and then reprocess.
1. Confirm the next batch returns to a quarantine rate of 0.
1. Consider adding a producer-side contract test for currency casing.

## Investigation trace
| # | Tool | Input | ok | ms |
|---|---|---|---|---|
| 1 | `get_failed_checks` | `{}` | ✓ | 1 |
| 2 | `get_contract` | `{}` | ✓ | 1 |
| 3 | `sample_quarantine` | `{'limit': 5}` | ✓ | 38 |
| 4 | `get_batch_history` | `{}` | ✓ | 1 |
| 5 | `run_sql` | `{'query': "select json_extract_string(raw_json,'$.currency') c, count(*) n from orders_qua` | ✓ | 1 |
| 6 | `submit_incident_report` | `{'severity': 'sev2', 'root_cause': 'format_change', 'summary': 'Batch b00006 was 100% quar` | ✓ | 0 |

_Model turns: 3, tokens in/out: 11631/1041_
