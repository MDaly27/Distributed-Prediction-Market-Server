# Future Session Handoff

## Current state (as of this handoff)

- Authoritative schema: `tables-create-dsql.sql` (single source of truth).
- Services:
  - `client/client-listener` (order ingest)
  - `matchmaker` (distributed matching)
  - `executor` (distributed settlement)
- End-to-end concurrent test exists: `test/system_test.py`.
- Last known result on EC2: `SYSTEM TEST PASSED`.

## What the current test validates

`test/system_test.py` starts multiple instances of each process simultaneously:

- 2 listener nodes
- 2 matchmaker nodes
- 2 executor nodes

Then it validates:

1. Order ingress across multiple markets.
2. Matching behavior with crossing and non-crossing orders.
3. Partial fill behavior.
4. Open unmatched orders persist (are not lost or force-closed).
5. Resolved markets settle with expected payouts.
6. Settlement run status completes (`market_settlement_runs`).
7. Cleanup of test-created rows.

## What is still missing / not fully covered

### Product behavior gaps

1. No explicit cancel-order API and cancel processing path.
2. No IOC/FOK behavior tests (listener accepts these values, but no dedicated execution logic/tests for rejection/expiry semantics).
3. No full cash lifecycle (deposit/withdrawal flow and reconciliation around `cash_transactions`).
4. No market lifecycle admin flows (DRAFT->ACTIVE->HALTED->CLOSED->RESOLVED with authorization/rules).
5. No fee model and fee ledgering.

### Matching/settlement correctness gaps

1. Matching price policy is basic for binary crossing (no advanced tie-breaking policy documented beyond current implementation).
2. No strict invariants checker after each run (e.g., aggregate conservation checks for cash/shares/locks across all tables).
3. Settlement uses project-level simplified payout semantics for `CANCELLED` markets; needs explicit policy definition.

### Distributed/system gaps

1. No durable supervisor/daemon strategy (systemd units, restart policy, health checks).
2. No central metrics/alerts/log aggregation.
3. No load/stress test for high throughput and long-running contention.
4. No chaos/recovery tests (instance kill mid-transaction, network partitions).

### Security/ops gaps

1. Static token auth still used for client listener test path; move to Secrets Manager mode by default in deployed envs.
2. No least-privilege IAM policy doc for listener/matchmaker/executor roles.
3. No runbooks for operational incidents.

## Recommended next tasks (in order)

1. Add a DB invariants checker script.
   - New file: `test/invariants_check.py`
   - Must verify after test runs:
     - `accounts.available_cash_cents >= 0`, `locked_cash_cents >= 0`
     - `orders.remaining_qty` in range and status consistency
     - `positions` non-negative values
     - no duplicate settlement rows per `(market_id, account_id)`

2. Implement order cancel endpoint + logic.
   - Listener action: `cancel_order`
   - Write to `order_cancels`, update `orders.status`, unlock reserved funds/shares, append `ledger_entries` (`ORDER_UNLOCK`).

3. Add IOC/FOK enforcement in matchmaker path with tests.
   - IOC: fill available now; cancel remainder.
   - FOK: fill all immediately or cancel fully.

4. Add a stress test runner.
   - New file: `test/stress_test.py`
   - Run multiple listeners + matchmakers + executors with larger order volumes and random distributions.

5. Add deployment scripts.
   - `deploy/systemd/*.service` files for each node type.
   - Include environment file templates and restart policy.

6. Add observability basics.
   - Structured JSON logging for each service.
   - Counters/timers (ingest rate, match rate, settlement rate, OCC retries, failures).

## How to run the current system test

From EC2 (or any host with network access and Python env):

1. Generate IAM auth token and DSN.
2. Run:

```bash
python3 test/system_test.py --db-dsn '<dsn>' --auth-token dev-shared-token
```

Expected success output:

- `SYSTEM TEST PASSED`

## Important note for future sessions

Do not reintroduce multiple schema sources of truth.

- Keep all schema evolution in `tables-create-dsql.sql`.
- If a migration is needed, add a proper migration mechanism, but keep one authoritative target schema definition.
