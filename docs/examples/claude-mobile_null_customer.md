# INC-0001 — null spike on `orders`

| Batch | Severity | Upstream bug | Investigator |
|---|---|---|---|
| `b00006` | sev2 | yes | claude (claude-sonnet-5-5) |

## Summary
In batch b00006, 71 of 300 records (23.7%) were quarantined because the required field customer_id was null. The change was sudden: b00000–b00005 had 0% quarantine. All 71 bad records have channel=mobile, so the mobile checkout path likely stopped populating customer_id. This is a producer bug, not a contract change. The orders are being held in quarantine and aren't reaching consumers.

## Failed checks
- **quarantine_rate**: 0.2367 (threshold 0.02) — 71/300 records failed record-level validation

## Evidence
- quarantine_rate 0.2367 (71/300) vs threshold 0.02; the only failing check
- Top quarantine reason is required:customer_id:null, with 71 of 71 records. customer_id null_rate is 0.2367
- No missing or unexpected fields. The key is present with a null value, so this is not a rename or schema drift
- All 71 quarantined records have channel=mobile (placed 39, shipped 24, refunded 5, cancelled 3). Sample order_ids: ord-586ff292e07e, ord-fa8d4bd45415. Amounts and currencies look normal
- Only 25 mobile orders in b00006 passed validation, so about 74% of mobile orders (71 of 96) lack a customer_id. Web and store orders were unaffected in the valid set
- Batches b00000–b00005 all had quarantine_rate 0.0 and 300 records each, so this is a sudden onset. Duplicate rate is 0

## Recommended actions
1. Page the checkout/mobile service owners (commerce-data). Check recent mobile app or API releases, and whether guest checkout or an auth/session change is dropping customer_id
1. Do not relax the required:true rule on customer_id in the contract. Nothing indicates that null customers are legitimate
1. Keep the 71 quarantined records from b00006 and replay them once the producer is fixed or customer_id is backfilled
1. Watch the next batches for quarantine_rate returning to about 0. Consider an alert on customer_id null rate segmented by channel
1. If mobile guest orders are intended, have the producer owner confirm and then agree a contract change explicitly, for example a guest ID

## Investigation trace
| # | Tool | Input | ok | ms |
|---|---|---|---|---|
| 1 | `get_failed_checks` | `{}` | ✓ | 1 |
| 2 | `sample_quarantine` | `{}` | ✓ | 40 |
| 3 | `get_batch_history` | `{}` | ✓ | 1 |
| 4 | `get_contract` | `{}` | ✓ | 1 |
| 5 | `run_sql` | `{'query': "select json_extract_string(raw_json,'$.channel') ch, json_extract_string(raw_js` | ✓ | 1 |
| 6 | `run_sql` | `{'query': "select channel, status, count(*) n from orders where _batch_id='b00006' group b` | ✓ | 0 |
| 7 | `submit_incident_report` | `{'severity': 'sev2', 'root_cause': 'null_spike', 'summary': "In batch b00006, 71 of 300 re` | ✓ | 0 |

_Model turns: 3, tokens in/out: 11874/1296_
