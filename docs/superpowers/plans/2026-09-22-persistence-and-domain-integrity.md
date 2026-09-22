# Persistence and Domain Integrity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prevent valid large financial histories from overflowing, reject impossible rehydrated state, recover incomplete inbox rows explicitly, and enforce cross-table financial ownership in PostgreSQL.

**Architecture:** PostgreSQL performs reconciliation arithmetic in `NUMERIC` and returns only the bounded net result. Domain snapshots validate the same invariant families as constructors plus state-specific persisted fields. Additive migration `000006` installs constraints and cross-wallet ownership keys after compatibility tests seed pre-existing rows.

**Tech Stack:** Go 1.27.1, PostgreSQL 17, pgx 5.11.0, golang-migrate 4.20.1.

**Spec:** `docs/superpowers/specs/2026-09-22-reliability-and-coverage-remediation-design.md`

## Global Constraints

- Do not edit migrations `000001` through `000005`.
- Preserve existing valid wallet, wager, ledger, and inbox behavior.
- PostgreSQL remains the source of truth for SQL arithmetic and constraints; do not introduce SQL mocks.
- Validate legacy data before installing each new constraint and provide a reversible `down` migration.
- Cyclomatic complexity must remain at most 6; cognitive complexity must remain at most 8.

---

### Task 1: Reconcile wide histories without intermediate overflow

**Files:**
- Modify: `internal/app/ports.go`
- Modify: `internal/app/wallet_service.go`
- Modify: `internal/app/wallet_service_test.go`
- Modify: `internal/app/memstore_test.go`
- Modify: `internal/adapter/postgres/queries.go`
- Modify: `internal/adapter/postgres/db_test.go`
- Create: `test/integration/reconciliation_test.go`

**Interfaces:**
- Changes: `ReconciliationSnapshot` replaces `Credits`/`Debits int64` with `CalculatedMinor int64`.
- Preserves: public `Reconciliation` JSON and `WalletService.Reconcile` behavior.

- [ ] **Step 1: Add failing PostgreSQL overflow regression**

Seed a wallet whose valid credit total and debit total individually exceed `math.MaxInt64`, but whose net balance fits `int64`. Require reconciliation to return the correct net and report consistency. Build the history with SQL fixtures that still satisfy ledger-chain constraints, and update every in-memory `ReconciliationSnapshot` fake from `Credits`/`Debits` to `CalculatedMinor`.

- [ ] **Step 2: Run focused integration test and verify RED**

Run: `go test -count=1 -tags=integration ./test/integration -run TestReconciliationAllowsWideIntermediateTotals`

Expected: FAIL with PostgreSQL bigint overflow from the current filtered `SUM(... )::BIGINT` expressions.

- [ ] **Step 3: Return only the bounded net from PostgreSQL**

Compute in `NUMERIC` and cast after subtraction:

```sql
(COALESCE(SUM(CASE WHEN l.direction = 'CREDIT' THEN l.amount_minor::NUMERIC ELSE 0 END), 0)
 - COALESCE(SUM(CASE WHEN l.direction = 'DEBIT' THEN l.amount_minor::NUMERIC ELSE 0 END), 0))::BIGINT
```

Scan the result into `CalculatedMinor`. In the app, construct one `money.Money` from that value and remove `signedSum`. This intentionally narrows only after the net is known; a genuinely out-of-domain net still fails.

- [ ] **Step 4: Update unit fakes and verify GREEN**

Run: `go test -count=1 ./internal/app ./internal/adapter/postgres && go test -count=1 -tags=integration ./test/integration -run TestReconciliation`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/app/ports.go internal/app/wallet_service.go internal/app/wallet_service_test.go internal/adapter/postgres/queries.go internal/adapter/postgres/db_test.go test/integration/reconciliation_test.go
git commit -m "fix: reconcile balances with wide arithmetic"
```

---

### Task 2: Validate every rehydrated wager invariant

**Files:**
- Modify: `internal/domain/wager/types.go`
- Modify: `internal/domain/wager/transaction.go`
- Modify: `internal/domain/wager/transaction_test.go`
- Modify: `internal/adapter/postgres/scan_internal_test.go`

**Interfaces:**
- Produces: `func (o Origin) Valid() bool`.
- Produces: `func (c FailureCode) Valid() bool`.
- Produces: small `Snapshot` validators for identity/core, external metadata, terminal result, retry schedule, and timestamps.

- [ ] **Step 1: Add a table of invalid persisted snapshots**

Cover unknown origin, invalid amount policy, malformed external metadata, unexpected internal metadata, invalid failure code, missing/unexpected result balance, inconsistent resolved reference, negative attempts, retry schedule outside `PENDING_REFERENCE`, zero `UpdatedAt`, and `UpdatedAt < CreatedAt`.

```go
func TestRehydrateRejectsImpossibleSnapshots(t *testing.T) {
    tests := []struct {
        name   string
        mutate func(*Snapshot)
    }{
        {"unknown origin", func(s *Snapshot) { s.Origin = "LEGACY" }},
        {"negative attempts", func(s *Snapshot) { s.Attempts = -1 }},
        {"updated before created", func(s *Snapshot) { s.UpdatedAt = s.CreatedAt.Add(-time.Second) }},
    }
    // Require errors.Is(err, ErrInvalidTransaction) for each case.
}
```

- [ ] **Step 2: Run focused domain tests and verify RED**

Run: `go test -count=1 ./internal/domain/wager -run TestRehydrateRejectsImpossibleSnapshots`

Expected: FAIL because current validation checks only identity, kind/status, origin-kind pairing, and external reference shape.

- [ ] **Step 3: Implement composable snapshot validation**

Reuse `validateCore`, `validateAmount`, `External.validate`, and `checkResolvedReference` where their creation semantics apply. Keep state-specific rules in flat helpers such as `validateTerminalState` and `validateSchedule`; do not put one large switch inside `Snapshot.validate`.

Define `FailureCode.Valid` from the existing constants. Require:

- `PROCESSED`: valid result balance, no failure code, reference shape matches kind;
- `REJECTED`: valid result balance and valid failure code;
- `FAILED`: valid failure code and no result balance;
- `PENDING_REFERENCE`: attempts positive, next attempt set, accepted external reference, no terminal fields;
- `PENDING`: no retry or terminal fields.

- [ ] **Step 4: Re-run domain and scan tests and verify GREEN**

Run: `go test -count=1 -race ./internal/domain/wager ./internal/adapter/postgres`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/domain/wager/types.go internal/domain/wager/transaction.go internal/domain/wager/transaction_test.go internal/adapter/postgres/scan_internal_test.go
git commit -m "fix: validate persisted wager state"
```

---

### Task 3: Prevent wallet time regression

**Files:**
- Modify: `internal/domain/wallet/wallet.go`
- Modify: `internal/domain/wallet/wallet_test.go`

**Interfaces:**
- Preserves: `func (w *Wallet) Apply(m Movement) (LedgerEntry, error)`.

- [ ] **Step 1: Add a failing non-monotonic movement test**

Rehydrate a wallet at `updatedAt = T`, apply a movement at `T-1ns`, and require `ErrInvalidMovement` without changing balance, version, or timestamp.

- [ ] **Step 2: Run focused test and verify RED**

Run: `go test -count=1 ./internal/domain/wallet -run TestApplyRejectsTimestampRegression`

Expected: FAIL because `checkMovement` does not compare `Now` with `updatedAt`.

- [ ] **Step 3: Enforce monotonic aggregate time**

Reject zero `Now` and times before `w.updatedAt`; allow equal instants because multiple durable operations may share one database-resolution timestamp. Check before constructing the ledger entry so the aggregate is unchanged on error.

- [ ] **Step 4: Re-run wallet tests and verify GREEN**

Run: `go test -count=1 -race ./internal/domain/wallet`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/domain/wallet/wallet.go internal/domain/wallet/wallet_test.go
git commit -m "fix: preserve wallet timestamp ordering"
```

---

### Task 4: Make incomplete inbox rows explicit and recoverable

**Files:**
- Modify: `internal/app/ports.go`
- Modify: `internal/app/consume.go`
- Modify: `internal/app/harness_test.go`
- Modify: `internal/adapter/postgres/inbox.go`
- Modify: `internal/adapter/postgres/db_test.go`
- Create: `test/integration/inbox_test.go`

**Interfaces:**
- Produces: `ErrInboxIncomplete` in `internal/app/errors.go`.
- Preserves: the normal register/process/complete transaction and completed-duplicate replay.

- [ ] **Step 1: Add failing adapter and service tests**

Seed an inbox row with `processed_at IS NULL` outside the normal unit-of-work path. Require `Register` to return an explicit incomplete state and the consumer flow to retry/alert rather than acknowledge it as a duplicate. Keep completed rows idempotent.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `go test -count=1 ./internal/app ./internal/adapter/postgres && go test -count=1 -tags=integration ./test/integration -run TestIncompleteInboxRow`

Expected: FAIL because the port comment assumes the state cannot exist and callers do not distinguish it.

- [ ] **Step 3: Represent and handle incomplete state explicitly**

Keep `InboxEntry.Completed`, but return `ErrInboxIncomplete` when an existing row is not completed. The application treats this as retryable infrastructure state, logs consumer/message identity, and never returns duplicate success. Do not silently delete or overwrite the row; automated repair would need a separate ownership/age policy.

- [ ] **Step 4: Update port documentation and verify GREEN**

Correct the invariant comment: normal transactions do not expose incomplete rows, but operational or legacy state can. Run:

`go test -count=1 -race ./internal/app ./internal/adapter/postgres && go test -count=1 -tags=integration ./test/integration -run TestInbox`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/app/errors.go internal/app/ports.go internal/app/consume.go internal/app/harness_test.go internal/adapter/postgres/inbox.go internal/adapter/postgres/db_test.go test/integration/inbox_test.go
git commit -m "fix: surface incomplete inbox records"
```

---

### Task 5: Add compatible database invariants in migration 000006

**Files:**
- Create: `internal/adapter/postgres/migrations/000006_domain_integrity.up.sql`
- Create: `internal/adapter/postgres/migrations/000006_domain_integrity.down.sql`
- Modify: `internal/adapter/postgres/migrate_test.go`
- Create: `test/integration/migrations_test.go`
- Create: `test/integration/persistence_constraints_test.go`

**Interfaces:**
- Produces: additive checks for wager state/timestamps and wallet timestamp order.
- Produces: one-to-one transaction-to-ledger ownership and a composite wallet association foreign key.

- [ ] **Step 1: Seed valid pre-000006 legacy rows**

Migrate only through version 5, insert representative valid rows for every
persistible wager status (`PENDING_REFERENCE`, `PROCESSED`, `REJECTED`, and
`FAILED`), opening and external transactions, wallets, and ledger entries,
then apply version 6. `PENDING` remains an in-memory-only domain state after
migration 000004. Require all seeded rows to remain readable.

- [ ] **Step 2: Add failing constraint expectations**

Attempt impossible rows: mismatched ledger wallet/transaction wallet, one transaction linked to two ledger rows, `updated_at < created_at`, terminal state without its required result/failure fields, and pending-reference state without a retry schedule. Require PostgreSQL constraint errors.

- [ ] **Step 3: Run focused migration tests and verify RED**

Run: `go test -count=1 -tags=integration ./test/integration -run 'TestMigration000006AcceptsExistingValidRows|TestDomainIntegrityConstraints'`

Expected: FAIL because migration 000006 and the constraints do not exist.

- [ ] **Step 4: Implement migration 000006**

Add and name constraints explicitly so the down migration can remove them deterministically:

```sql
ALTER TABLE wager_transactions
    ADD CONSTRAINT wager_transactions_id_wallet_unique UNIQUE (id, wallet_id);
ALTER TABLE ledger_entries
    ADD CONSTRAINT ledger_entries_transaction_unique UNIQUE (transaction_id),
    ADD CONSTRAINT ledger_entries_transaction_wallet_fk
        FOREIGN KEY (transaction_id, wallet_id)
        REFERENCES wager_transactions (id, wallet_id);
```

Add compatible `CHECK` constraints for origin/external-field presence, amount policy, status-field matrix, non-negative attempts, retry scheduling, and timestamp order. Use `NOT VALID` then `VALIDATE CONSTRAINT` for checks/FKs where PostgreSQL permits it; unique indexes require a preflight query and regular creation. The test must prove a clear migration error on conflicting legacy data.

- [ ] **Step 5: Verify down/up reversibility**

Apply 000006, migrate down one step, confirm the version and absence of only the new constraints, then migrate up again. Never weaken earlier migrations.

- [ ] **Step 6: Re-run integration suite and verify GREEN**

Run: `go test -count=1 -race -tags=integration ./test/integration`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/adapter/postgres/migrations/000006_domain_integrity.up.sql internal/adapter/postgres/migrations/000006_domain_integrity.down.sql internal/adapter/postgres/migrate_test.go test/integration/migrations_test.go test/integration/persistence_constraints_test.go
git commit -m "feat: enforce persistent domain integrity"
```

---

### Task 6: Document persistent invariants and run verification

**Files:**
- Modify: `README.md`
- Modify: `ARCHITECTURE.md`
- Create: `docs/operations.md`

- [ ] **Step 1: Document reconciliation and recovery behavior**

Explain that reconciliation aggregates in wide SQL arithmetic, only the net must fit the money domain, incomplete inbox rows are retryable/alertable rather than successful duplicates, and migration 000006 validates existing data before installing constraints.

- [ ] **Step 2: Document every new database constraint**

List its invariant, owning aggregate/table, and operational failure meaning. Include the pre-migration query operators should run when upgrading a database containing manually modified data.

- [ ] **Step 3: Run package and repository checks**

Run:

```bash
go test -count=1 -race ./internal/domain/... ./internal/app ./internal/adapter/postgres
go test -count=1 -race -tags=integration ./test/integration
go vet ./...
golangci-lint run
```

Expected: all commands exit 0.

- [ ] **Step 4: Commit**

```bash
git add README.md ARCHITECTURE.md docs/operations.md
git commit -m "docs: describe persistent integrity guarantees"
```
