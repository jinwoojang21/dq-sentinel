# INC-0001 — unit change on `orders`

| Batch | Severity | Upstream bug | Investigator |
|---|---|---|---|
| `b00006` | sev1 | yes | claude (claude-sonnet-5-5) |

## Summary
In b00006 every `amount` value is about 100x larger than in b00000–b00005, which looks like a switch from dollars to cents. The quarantine check caught only the 39 rows (13%) that crossed the max of 10000. The other 261 rows passed validation with wrong amounts, so corrupted values have probably reached consumers. This is an upstream producer bug, so I did not propose a contract change.

## Failed checks
- **quarantine_rate**: 0.13 (threshold 0.02) — 39/300 records failed record-level validation

## Evidence
- All 39 quarantined rows have the single reason 'range:amount:above max 10000'. Their amounts run from 10035 to 34341, across all currencies (USD, EUR, GBP) and all channels.
- Validated `amount` in b00000–b00005 had a median of about 34–38, a mean of about 45–56, a minimum of 1.56–3.6 and a maximum of 309–778.
- Validated `amount` in b00006 has a median of 3187, a mean of 3662, a minimum of 267 and a maximum of 9737. The whole distribution moved up about 100x, not just a tail.
- The b00006 amounts are whole numbers (for example 12026.0, 10035.0 and 17997.0), whereas earlier batches had decimals (for example 1.56 and 308.84). This fits integer minor units (cents).
- Batches b00000–b00005 had a quarantine rate of 0.0. In b00006 the duplicate rate is 0.0, there are no missing or unexpected fields, and the batch size is stable at 300. The change is sudden and affects only `amount`.
- The contract (v3) sets amount min 0 and max 10000 as a double with no unit change declared.

## Recommended actions
1. Ask the checkout service owners (commerce-data) whether a deploy before b00006 changed `amount` to minor units (cents). Have them roll it back or fix it so amounts go back to major units.
1. Treat the 261 validated b00006 rows in `orders` as corrupted. Quarantine them or tag them, and keep downstream revenue and finance consumers from using them until they are reprocessed.
1. After the producer fix, replay b00006, including the 39 quarantined rows, with amounts divided by 100 or re-emitted from source.
1. Do not raise the contract max for `amount`. That would hide the bug and let the wrong values through.
1. Add a distribution-drift check on `amount` (median or mean against a rolling baseline). The range check alone only caught the rows that crossed the max.

## Investigation trace
| # | Tool | Input | ok | ms |
|---|---|---|---|---|
| 1 | `get_failed_checks` | `{}` | ✓ | 1 |
| 2 | `get_batch_history` | `{}` | ✓ | 1 |
| 3 | `sample_quarantine` | `{'limit': 10}` | ✓ | 41 |
| 4 | `get_contract` | `{}` | ✓ | 1 |
| 5 | `run_sql` | `{'query': 'select _batch_id, count(*) n, min(amount) mn, avg(amount) av, quantile_cont(amo` | ✓ | 2 |
| 6 | `run_sql` | `{'query': "select json_extract_string(raw_json,'$.currency') c, json_extract_string(raw_js` | ✓ | 1 |
| 7 | `submit_incident_report` | `{'severity': 'sev1', 'root_cause': 'unit_change', 'summary': 'In b00006 every `amount` val` | ✓ | 0 |

_Model turns: 3, tokens in/out: 13453/1536_
