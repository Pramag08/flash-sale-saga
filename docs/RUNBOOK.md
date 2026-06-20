# RUNBOOK.md — Flash Sale Saga Operational Procedures

## Table of Contents

- [Alert Response Procedures](#alert-response-procedures)
- [CloudWatch Logs Insights — Saved Queries](#cloudwatch-logs-insights--saved-queries)
- [DLQ Operations](#dlq-operations)
- [Saga Timeout Monitor](#saga-timeout-monitor)
- [Financial Reconciliation](#financial-reconciliation)
- [Escalation Matrix](#escalation-matrix)

---

## Alert Response Procedures

### P1 — Critical: Refund DLQ Has Messages

**Alarm:** `dlq-payment-rollback-alarm`  
**Response SLA:** 15 minutes  
**Impact:** Users have been charged but NOT refunded. Money is stuck.

**Steps:**

1. Confirm the alarm in CloudWatch. Note the DLQ message count.
2. Inspect the DLQ messages:
   ```bash
   ./scripts/drain-dlq.sh --queue payment-rollback-dlq.fifo --action inspect
   ```
3. Check the Payment Rollback Lambda logs for errors:
   - Open CloudWatch Logs Insights and run **Query 1** (below) filtered by `service = "payment-rollback"`.
4. Identify root cause:
   - **PostgreSQL connection failure** → Check NeonDB status. If healthy, proceed to redrive.
   - **Schema mismatch / bad payload** → Do NOT redrive. Fix the bug first.
   - **Serialization failure** → Safe to redrive after 30 seconds.
5. Redrive messages back to the source queue:
   ```bash
   ./scripts/drain-dlq.sh --queue payment-rollback-dlq.fifo --action redrive
   ```
6. Monitor the source queue until `ApproximateNumberOfMessagesVisible` drops to 0.
7. Run the financial reconciliation script to verify no double-refunds or orphaned charges:
   ```bash
   python scripts/reconcile.py --event-id <EVENT_ID> --fail-on-violation
   ```
8. Close the incident.

---

### P2 — High: Payment or Inventory DLQ Has Messages

**Alarm:** `dlq-payment-processing-alarm` or `dlq-inventory-processing-alarm`  
**Response SLA:** 30 minutes  
**Impact:** Sagas are stuck. Users may be charged without receiving tickets.

**Steps:**

1. Confirm the alarm. Note which DLQ has messages.
2. Inspect the DLQ:
   ```bash
   ./scripts/drain-dlq.sh --queue <DLQ_NAME> --action inspect
   ```
3. Check Lambda logs using **Query 2** (below).
4. Common causes:
   - **NeonDB cold start** → Wait 2 minutes, then redrive.
   - **DynamoDB throttling** → Rare with On-Demand. Check AWS Health Dashboard.
   - **Poison message** → If the payload is malformed, do NOT redrive. Purge after confirming no data loss.
5. Redrive:
   ```bash
   ./scripts/drain-dlq.sh --queue <DLQ_NAME> --action redrive
   ```
6. Verify saga completion by polling `/status/{transaction_id}` for affected transactions.

---

### P2 — High: Lambda Error Rate Elevated

**Alarm:** `lambda-error-rate-alarm` (any function)  
**Response SLA:** 30 minutes

**Steps:**

1. Identify which Lambda is erroring from the alarm details.
2. Run **Query 2** in CloudWatch Logs Insights to see recent errors.
3. Check dependencies:
   - Payment Lambda → NeonDB connectivity, SQS permissions.
   - Inventory Lambda → DynamoDB throttling, SQS permissions.
4. If a dependency is down, wait for recovery and redrive any DLQ messages.
5. If errors persist, check for recent deployments. Roll back if necessary.

---

### P3 — Medium: Saga Status DLQ Has Messages

**Alarm:** `dlq-saga-status-alarm`  
**Response SLA:** 2 hours  
**Impact:** Users won't see status updates. Non-financial.

**Steps:**

1. Inspect and redrive at convenience:
   ```bash
   ./scripts/drain-dlq.sh --queue saga-status-dlq.fifo --action redrive
   ```

---

## CloudWatch Logs Insights — Saved Queries

Pin these as **Saved Queries** in CloudWatch Logs Insights for instant access during incidents.

### Query 1: Trace a Complete Saga by Transaction ID

Reconstructs the full timeline of a saga across all services.

```
fields @timestamp, service, event, @message
| filter transaction_id = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
| sort @timestamp asc
| limit 50
```

**Use when:** A user reports their purchase is stuck. Replace the UUID with their `transaction_id`.

---

### Query 2: Find All Failed Payments in the Last Hour

Surfaces payment processing errors for triage.

```
fields @timestamp, transaction_id, user_id, amount_cents, @message
| filter service = "payment-processor" AND level = "error"
| filter event IN ["payment_charge_failed", "insufficient_funds", "db_connection_timeout"]
| sort @timestamp desc
| limit 100
```

**Use when:** The payment DLQ alarm fires or users report charge failures.

---

### Query 3: Detect Stale Idempotency Locks

Identifies sagas where the idempotency store has a stale `PROCESSING` lock.

```
fields @timestamp, transaction_id, idempotency_key, @message
| filter event = "stale_idempotency_lock_detected"
| sort @timestamp desc
| limit 20
```

**Use when:** The saga timeout monitor reports recoveries but sagas remain stuck. Indicates potential lock contention.

---

### Query 4: Cold Start Frequency

Measures how often Lambda cold starts occur per service.

```
stats count(*) as invocations, sum(cold_start) as cold_starts by service
| filter cold_start = 1
```

**Use when:** Latency is elevated. Cold starts add 200-500ms (Python). If frequency is high, consider increasing Provisioned Concurrency.

---

### Query 5: p50/p90/p99 Latency Per Service

Identifies which service is the latency bottleneck.

```
filter ispresent(duration_ms)
| stats percentile(duration_ms, 50) as p50,
        percentile(duration_ms, 90) as p90,
        percentile(duration_ms, 99) as p99
  by service
```

**Use when:** Users report slow purchases. Target: p99 < 5 seconds end-to-end.

---

## DLQ Operations

### Using `drain-dlq.sh`

The `scripts/drain-dlq.sh` script provides three actions for DLQ management.

#### Inspect (Read Without Deleting)

```bash
./scripts/drain-dlq.sh --queue payment-processing-dlq.fifo --action inspect
```

Outputs message bodies, receive counts, and first-received timestamps. Use this for diagnosis before deciding to redrive or purge.

#### Redrive (Move Back to Source Queue)

```bash
./scripts/drain-dlq.sh --queue payment-rollback-dlq.fifo --action redrive
```

Uses the SQS `StartMessageMoveTask` API to move all messages from the DLQ back to the source queue. The underlying bug MUST be fixed before redriving, or messages will return to the DLQ.

Add `--dry-run` to preview without executing:

```bash
./scripts/drain-dlq.sh --queue payment-rollback-dlq.fifo --action redrive --dry-run
```

#### Purge (Delete All Messages — DANGEROUS)

```bash
./scripts/drain-dlq.sh --queue saga-status-dlq.fifo --action purge --confirm
```

Permanently deletes all messages in the DLQ. Only use after manual reconciliation confirms no data loss. The `--confirm` flag is required.

### DLQ Alert Severity Reference

| DLQ | Severity | Response SLA | Escalation |
|---|---|---|---|
| `payment-processing-dlq.fifo` | P2 — High | 30 min | On-call engineer |
| `inventory-processing-dlq.fifo` | P2 — High | 30 min | On-call engineer |
| `payment-rollback-dlq.fifo` | P1 — Critical | 15 min | Immediate page |
| `saga-status-dlq.fifo` | P3 — Medium | 2 hours | Reprocess at convenience |

---

## Saga Timeout Monitor

The saga timeout monitor runs every 5 minutes as a scheduled Lambda. It detects sagas where:

- Payment succeeded (`idempotency_store.status = 'COMPLETED'`) but no reservation exists in DynamoDB after 10 minutes.
- The monitor re-emits a `PaymentSuccess` event to the inventory queue with a new idempotency key.
- Sagas orphaned for > 30 minutes are logged as `CRITICAL` and emit the `SagaPermanentlyStuck` metric.

### Manual Invocation

```bash
aws lambda invoke \
  --function-name flash-sale-saga-monitor \
  --payload '{}' \
  /tmp/saga-monitor-response.json && cat /tmp/saga-monitor-response.json
```

### Interpreting Output

```json
{
  "orphaned_recovered": 2,
  "permanently_stuck": 0
}
```

- `orphaned_recovered > 0`: The monitor found and re-emitted stranded sagas. Check CloudWatch Logs for `saga_timeout_recovery` events.
- `permanently_stuck > 0`: **Immediate action required.** These sagas could not be auto-recovered. Investigate manually using Query 1.

---

## Financial Reconciliation

Run after every load test, chaos experiment, or DLQ incident:

```bash
python scripts/reconcile.py \
  --pg-url "$DATABASE_URL" \
  --dynamodb-endpoint "$LOCALSTACK_ENDPOINT" \
  --event-id "$EVENT_ID" \
  --fail-on-violation
```

The script verifies 6 invariants:

1. **ZERO-SUM:** Every charge has a reservation or refund.
2. **NO DOUBLE CHARGE:** At most 1 charge per transaction.
3. **NO DOUBLE REFUND:** At most 1 refund per transaction.
4. **REFUND <= CHARGE:** Refund amount matches original charge.
5. **INVENTORY CONSERVATION:** `total_qty = available_qty + active reservations`.
6. **WALLET CONSISTENCY:** Balance matches ledger-derived total.

---

## Escalation Matrix

| Scenario | Severity | Action | Escalation |
|---|---|---|---|
| Refund DLQ has messages | P1 | Immediate redrive after fix | Page on-call if not resolved in 15 min |
| Payment DLQ has messages | P2 | Inspect, fix root cause, redrive | Escalate if > 30 min |
| Inventory DLQ has messages | P2 | Inspect, fix root cause, redrive | Escalate if > 30 min |
| Lambda error rate > 5% | P2 | Check dependencies, roll back deploy | Escalate if > 30 min |
| Saga permanently stuck | P2 | Manual investigation via Logs Insights | Escalate if > 1 hour |
| DynamoDB throttling | P2 | Check AWS Health, review access patterns | Escalate to AWS support if > 15 min |
| Overselling detected | P1 | Halt flash sale, investigate immediately | Page engineering lead |
| Status DLQ has messages | P3 | Redrive at convenience | No escalation needed |
