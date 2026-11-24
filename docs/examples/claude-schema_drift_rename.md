# INC-0001 — schema drift on `orders`

| Batch | Severity | Upstream bug | Investigator |
|---|---|---|---|
| `b00006` | sev2 | no — contract change | claude (claude-sonnet-5-5) |

## Summary
Starting with batch b00006, the producer renamed `amount` to `order_amount`. Every record failed the contract's required `amount` check, so 100% of the batch (300/300) was quarantined and no valid orders reached consumers. The rename looks deliberate and clean: the values are unchanged and no other field changed. I proposed a contract rename (v4), which needs human review. Whether the producer announced the rename is unconfirmed.

## Failed checks
- **quarantine_rate**: 1.0 (threshold 0.02) — 300/300 records failed record-level validation
- **schema_unexpected_field** `order_amount`: 1.0 (threshold 0.5) — field `order_amount` is not in the contract
- **schema_missing_field** `amount`: 1.0 (threshold 0.5) — contract field `amount` absent from events

## Evidence
- quarantine_rate 1.0 (300/300) vs threshold 0.02; the only quarantine reason is required:amount:missing (300 records)
- schema_missing_field: `amount` is missing in 100% of events. schema_unexpected_field: `order_amount` is present in 100% of events.
- Batches b00000-b00005 each had 300 records and 0.0 quarantine, so this is a sudden change at b00006, not a gradual trend.
- Sample quarantined records, e.g. ord-f18387c9a505, have order_amount=91.25 and ord-dc426553daa3 has order_amount=29.27. All other fields are populated and valid.
- order_amount in b00006 has mean 60.7 and range 3.81-492.77. Historical amount has mean 53.3 and range 1.56-777.89 over 1800 rows. The scale and unit are consistent, so this is not a unit change.
- No duplicates (duplicate_rate 0.0). The channel null rate is 1.3%, within its 5% limit.

## Recommended actions
1. commerce-data: review and approve proposal INC-0001-orders.yaml (contract v4, rename amount -> order_amount) after confirming the rename with the checkout service owners.
1. Checkout service owners: confirm the rename was intentional. If it wasn't, revert the producer to `amount` instead of merging the contract change.
1. After the contract is updated, replay the 300 quarantined b00006 records and any later quarantined batches from orders_quarantine into orders.
1. Update downstream consumers and dashboards that read `amount` to use `order_amount`, or add an alias.
1. Ask the producer to announce schema changes ahead of time, or version them, so a rename doesn't quarantine a whole batch.

## Proposed contract change
_From b00006 on, 300/300 events carry `order_amount` and none carry `amount`. The values look the same as before: mean 60.7 and range 3.81-492.77, against a historical mean of 53.3 and range 1.56-777.89. All other fields are unchanged, so this looks like a clean 1:1 rename._ — written to `INC-0001-orders.yaml`

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
@@ -10,7 +10,7 @@
   customer_id:
     type: string
     required: true
-  amount:
+  order_amount:
     type: double
     required: true
     min: 0
```

## Investigation trace
| # | Tool | Input | ok | ms |
|---|---|---|---|---|
| 1 | `get_failed_checks` | `{}` | ✓ | 1 |
| 2 | `get_contract` | `{}` | ✓ | 1 |
| 3 | `sample_quarantine` | `{'limit': 5}` | ✓ | 39 |
| 4 | `get_batch_history` | `{'limit': 8}` | ✓ | 1 |
| 5 | `run_sql` | `{'query': "select 'hist' k, avg(amount) a, min(amount) mn, max(amount) mx, count(*) n from` | ✓ | 2 |
| 6 | `propose_contract_patch` | `{'operations': [{'op': 'rename_field', 'from': 'amount', 'to': 'order_amount'}], 'rational` | ✓ | 3 |
| 7 | `submit_incident_report` | `{'severity': 'sev2', 'root_cause': 'schema_drift', 'summary': "Starting with batch b00006,` | ✓ | 0 |

_Model turns: 4, tokens in/out: 17833/1548_
