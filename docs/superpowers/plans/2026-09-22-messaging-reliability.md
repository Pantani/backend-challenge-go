# Messaging Reliability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make SQS consumption and outbox publication uphold their documented timing, ordering, retry, and durable-finalization guarantees.

**Architecture:** SQS validates FIFO identity and protects the entire received batch budget. The outbox uses acquisition-specific fencing, claims immediately before work, counts actual publish attempts, and finalizes through a context independent from broker publication.

**Tech Stack:** Go 1.27.1, AWS SDK for Go v2 SQS 1.52.0, PostgreSQL 17, pgx 5.11.0, LocalStack 4.4.

**Spec:** `docs/superpowers/specs/2026-09-22-reliability-and-coverage-remediation-design.md`

## Global Constraints

- Preserve at-least-once delivery and per-wallet ordering.
- Preserve existing valid SQS message bodies and successful processing responses.
- Validate `MessageGroupId = walletId` and `MessageDeduplicationId = messageId`.
- Use separate contexts for business processing, broker acknowledgement, and durable outbox finalization.
- Add migration `000005`; never rewrite migrations `000001` through `000004`.
- Cyclomatic complexity must remain at most 6; cognitive complexity must remain at most 8.

---

### Task 1: Validate SQS envelope and FIFO identity

**Files:**
- Modify: `internal/adapter/sqs/message.go`
- Modify: `internal/adapter/sqs/consumer.go`
- Modify: `internal/adapter/sqs/sqs_test.go`
- Modify: `test/integration/messaging_test.go`

**Interfaces:**
- Produces: `func DecodeMessage(consumer, body string) (app.InboundMessage, error)` with message ID and RFC3339 timestamp validation.
- Produces: consumer validation of message group and deduplication system attributes.

- [ ] **Step 1: Add failing message-contract tests**

```go
func TestDecodeMessageRejectsInvalidEnvelopeMetadata(t *testing.T) {
    tests := []struct{ name, messageID, occurredAt string }{
        {"blank id", " ", "2026-09-22T12:00:00Z"},
        {"oversized id", strings.Repeat("x", 129), "2026-09-22T12:00:00Z"},
        {"missing time", "message-1", ""},
        {"invalid time", "message-1", "yesterday"},
    }
    // Encode each envelope and require errors.Is(err, ErrInvalidMessage).
}
```

Add consumer cases where group differs from wallet and deduplication ID differs from message ID; require DLQ and zero processor calls.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `go test -count=1 ./internal/adapter/sqs -run 'TestDecodeMessageRejectsInvalidEnvelopeMetadata|TestConsumerRejectsInvalidFIFOIdentity'`

Expected: FAIL because metadata is not validated and the deduplication attribute is not requested.

- [ ] **Step 3: Implement envelope validation**

```go
const maxMessageIDBytes = 128

func validateEnvelope(env Envelope) error {
    if id := strings.TrimSpace(env.MessageID); id == "" || len(env.MessageID) > maxMessageIDBytes || !printableASCII(env.MessageID) {
        return fmt.Errorf("%w: invalid messageId", ErrInvalidMessage)
    }
    if _, err := time.Parse(time.RFC3339, env.OccurredAt); err != nil {
        return fmt.Errorf("%w: invalid occurredAt: %v", ErrInvalidMessage, err)
    }
    if env.Type != MessageType { return fmt.Errorf("%w: unsupported type %q", ErrInvalidMessage, env.Type) }
    return nil
}
```

Keep the message ID byte-for-byte stable; reject rather than normalize it.

- [ ] **Step 4: Request and validate FIFO attributes**

Add `MessageDeduplicationId` to `MessageSystemAttributeNames`. After decoding, compare:

```go
if groupID(m) != msg.Command.WalletID.String() {
    return app.InboundMessage{}, fmt.Errorf("%w: MessageGroupId must equal walletId", ErrInvalidMessage)
}
if dedupID(m) != msg.MessageID {
    return app.InboundMessage{}, fmt.Errorf("%w: MessageDeduplicationId must equal messageId", ErrInvalidMessage)
}
```

- [ ] **Step 5: Verify unit and LocalStack behavior**

Run: `go test -race -count=1 ./internal/adapter/sqs && go test -race -count=1 -tags integration ./test/integration/... -run 'InvalidMessages|Sender|FIFO'`

Expected: PASS; malformed metadata reaches DLQ with no financial effect.

- [ ] **Step 6: Commit**

```bash
git add internal/adapter/sqs/message.go internal/adapter/sqs/consumer.go internal/adapter/sqs/sqs_test.go test/integration/messaging_test.go
git commit -m "fix: validate SQS FIFO message identity"
```

### Task 2: Protect the full SQS receive batch

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`
- Modify: `internal/adapter/sqs/consumer.go`
- Modify: `internal/adapter/sqs/sqs_test.go`
- Modify: `test/testenv/testenv.go`
- Modify: `test/e2e/main_test.go`
- Modify: `.env.example`
- Modify: `README.md`
- Modify: `ARCHITECTURE.md`

**Interfaces:**
- Produces: overflow-safe `func batchBudget(maxMessages int, process, ack time.Duration) (time.Duration, bool)`.
- Requires: `SQS_VISIBILITY_TIMEOUT > SQS_MAX_MESSAGES * (SQS_PROCESS_TIMEOUT + SQS_ACK_TIMEOUT)`.

- [ ] **Step 1: Add failing configuration boundary tests**

```go
func TestRejectsVisibilityShorterThanWholeBatch(t *testing.T) {
    lookup := validLookup(map[string]string{
        "SQS_MAX_MESSAGES": "10", "SQS_PROCESS_TIMEOUT": "20s",
        "SQS_ACK_TIMEOUT": "5s", "SQS_VISIBILITY_TIMEOUT": "30s",
    })
    _, err := Load(lookup)
    require.ErrorContains(t, err, "whole receive batch")
}
```

Add a maximum-duration case that proves multiplication cannot overflow.

- [ ] **Step 2: Run config tests and verify RED**

Run: `go test -count=1 ./internal/config -run 'WholeBatch|Overflow'`

Expected: FAIL because validation only covers one message.

- [ ] **Step 3: Implement overflow-safe budget validation**

```go
func productBelow(limit time.Duration, count int, parts ...time.Duration) bool {
    if count <= 0 { return false }
    perItem := time.Duration(0)
    for _, part := range parts {
        if part <= 0 || part >= limit-perItem { return false }
        perItem += part
    }
    return perItem < limit/time.Duration(count)
}
```

Use strict comparison so acknowledgement still fits before visibility expiry. Set the local default visibility to `5m`, which covers `10 * (20s + 5s)`.

Raise the Docker testenv and E2E visibility fixtures to cover their configured
maximum batch (`10 * (process + ack)`) while retaining enough headroom for
runner jitter. Do not reduce the batch to one, because the slow-batch regression
must exercise the production batching path.

- [ ] **Step 4: Add a two-consumer slow-batch integration test**

Receive two messages in one batch, hold the first near its processing budget, run a second consumer, and assert the second message is not delivered concurrently before the first consumer reaches it.

- [ ] **Step 5: Run focused and full SQS tests**

Run: `go test -race -count=1 ./internal/config ./internal/adapter/sqs && go test -race -count=1 -tags integration ./test/integration/... -run 'Visibility|Batch'`

Expected: PASS.

- [ ] **Step 6: Update operational documentation**

Document that receive visibility covers the worst-case serial batch and that a larger batch increases crash redelivery latency. Remove the claim that the per-message inequality alone prevents overlap.

- [ ] **Step 7: Commit**

```bash
git add internal/config internal/adapter/sqs .env.example README.md ARCHITECTURE.md test/integration/messaging_test.go
git commit -m "fix: protect complete SQS receive batches"
```

### Task 3: Preserve group order when DLQ source deletion fails

**Files:**
- Modify: `internal/adapter/sqs/consumer.go`
- Modify: `internal/adapter/sqs/sqs_test.go`

**Interfaces:**
- Produces: `func (*Consumer) delete(context.Context, types.Message) bool`.
- Consumes: `deadLetter` uses the returned boolean to decide whether the group tail may advance.

- [ ] **Step 1: Add a failing group-tail test**

Configure the fake so DLQ `SendMessage` succeeds and source `DeleteMessage` fails. Supply a permanently invalid head and valid tail with the same group. Assert the tail is released and the processor is never called for it.

- [ ] **Step 2: Run the test and verify RED**

Run: `go test -count=1 ./internal/adapter/sqs -run TestDLQDeleteFailureBlocksGroupTail`

Expected: FAIL because `deadLetter` currently returns true after ignoring delete failure.

- [ ] **Step 3: Return durable deletion status**

```go
func (c *Consumer) delete(ctx context.Context, m types.Message) bool {
    _, err := c.api.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: aws.String(c.cfg.QueueURL), ReceiptHandle: m.ReceiptHandle})
    if err != nil {
        c.logger.WarnContext(ctx, "sqs delete failed; redelivery will be deduplicated", "error", err)
        return false
    }
    return true
}
```

Normal committed processing may still return success after delete failure because inbox deduplication is safe. `deadLetter` must return the delete result because advancing the group is a different guarantee.

- [ ] **Step 4: Run the SQS package tests**

Run: `go test -race -count=1 ./internal/adapter/sqs`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/adapter/sqs/consumer.go internal/adapter/sqs/sqs_test.go
git commit -m "fix: retain FIFO group after failed DLQ deletion"
```

### Task 4: Add acquisition-specific outbox fencing

**Files:**
- Create: `internal/adapter/postgres/migrations/000005_outbox_claim_token.up.sql`
- Create: `internal/adapter/postgres/migrations/000005_outbox_claim_token.down.sql`
- Modify: `internal/app/ports.go:217-254`
- Modify: `internal/adapter/postgres/outbox.go`
- Modify: `internal/adapter/postgres/db_test.go`
- Modify: `internal/bootstrap/bootstrap_internal_test.go`
- Modify: `test/integration/failures_test.go`
- Modify: `test/integration/schema_test.go`
- Modify: `test/integration/messaging_test.go`

**Interfaces:**
- Adds: `OutboxMessage.ClaimID uuid.UUID`.
- Produces: `Claim(ctx, owner string, claimID uuid.UUID, now time.Time, lease time.Duration) (OutboxMessage, bool, error)` for one immediately publishable partition head.
- Produces: `StartAttempt(ctx, eventID, claimID uuid.UUID) (int, bool, error)`.
- Changes: `MarkPublished`, `MarkFailed`, and `MarkDead` use `claimID`; failure/dead methods return `(bool, error)`.

- [ ] **Step 1: Add failing schema and stale-owner integration tests**

Test two successive claims by the same `INSTANCE_ID` with different claim IDs. After the first lease expires and the second claim succeeds, assert that the first claim cannot publish, fail, or dead-letter the row.

Also migrate a version-4 database containing an active legacy lease. Require
version 5 to release that lease safely rather than reject the migration or
invent an unverifiable claim identity.

- [ ] **Step 2: Run focused integration and verify RED**

Run: `go test -race -count=1 -tags integration ./test/integration/... -run 'Outbox.*Claim|Lease'`

Expected: FAIL because ownership is currently only `(event_id, locked_by)`.

- [ ] **Step 3: Add migration 000005**

```sql
ALTER TABLE outbox_events ADD COLUMN claim_id UUID;
UPDATE outbox_events SET locked_by = NULL, locked_until = NULL
WHERE locked_by IS NOT NULL;
ALTER TABLE outbox_events DROP CONSTRAINT outbox_events_lease_pair;
ALTER TABLE outbox_events ADD CONSTRAINT outbox_events_lease_triplet CHECK (
    (locked_by IS NULL) = (locked_until IS NULL)
    AND (locked_by IS NULL) = (claim_id IS NULL)
);
```

The migration must run with relays stopped; releasing legacy leases makes every
pre-version-5 owner prove ownership again under a claim token. The down migration
drops the triplet constraint and column, then restores the original lease-pair
constraint. Document that both directions invalidate active claims.

- [ ] **Step 4: Update the store contract and SQL**

`Claim` sets `locked_by`, `locked_until`, and `claim_id` without incrementing attempts. `StartAttempt` increments attempts only when `event_id` and `claim_id` still match, then returns the new count. All terminal mutations clear the three lease fields and require the claim ID.

- [ ] **Step 5: Run migration and store tests**

Run: `go test -race -count=1 ./internal/adapter/postgres && go test -race -count=1 -tags integration ./test/integration/... -run 'Migration|Outbox|Lease'`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/adapter/postgres/migrations/000005* internal/app/ports.go internal/adapter/postgres test/integration
git commit -m "fix: fence outbox claims by acquisition"
```

### Task 5: Claim immediately before publish and finalize with an independent budget

**Files:**
- Modify: `internal/worker/relay.go`
- Modify: `internal/worker/worker_test.go`
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`
- Modify: `internal/bootstrap/bootstrap.go`
- Modify: `test/integration/messaging_test.go`
- Modify: `README.md`
- Modify: `ARCHITECTURE.md`

**Interfaces:**
- Adds: `RelayConfig.FinalizeTime time.Duration`.
- Relay claims one partition head immediately before starting it and processes up to `BatchSize` records per round.
- Publication uses `PublishTime`; `StartAttempt`, confirmation, retry, and dead-letter use `FinalizeTime`.

- [ ] **Step 1: Add failing deadline-sensitive relay tests**

Use a publisher that blocks until `ctx.Done()` and returns `ctx.Err()`. Assert that `MarkFailed` is called with a live context and persists backoff. At `MaxAttempts`, assert dead-letter metrics occur only after `MarkDead` returns `ok=true`.

- [ ] **Step 2: Add a cancellation-with-unstarted-claim test**

Cancel between records and assert no later event increments `attempts`. Assert every claimed record either begins publication immediately or remains unclaimed.

- [ ] **Step 3: Run relay tests and verify RED**

Run: `go test -count=1 ./internal/worker -run 'Relay.*Deadline|Relay.*Attempt|Relay.*Dead'`

Expected: FAIL because the current relay reuses the expired publish context and increments on batch claim.

- [ ] **Step 4: Implement separate publication and finalization phases**

```go
publishCtx, cancelPublish := Detach(parent, r.cfg.PublishTime)
err := r.publisher.Publish(publishCtx, message)
cancelPublish()

finalizeCtx, cancelFinalize := Detach(parent, r.cfg.FinalizeTime)
defer cancelFinalize()
if err != nil { r.recordFailure(finalizeCtx, message, err); return }
r.confirm(finalizeCtx, message)
```

`Detach` is a small package-private wrapper around
`context.WithTimeout(context.WithoutCancel(parent), timeout)`. It preserves
trace/log values while giving finalization its own cancellation budget.

Call `StartAttempt` immediately before `Publish`, update `message.Attempts`, and abandon processing when the claim was lost.

- [ ] **Step 5: Add and validate `OUTBOX_FINALIZE_TIMEOUT`**

Default it to `5s`. Require it positive and validate, without addition overflow,
`PublishTime + FinalizeTime < Lease`. Since claims are now immediate and
singular, no batch multiplication applies to the lease. Update every fake store
and bootstrap constructor affected by the port/config change.

- [ ] **Step 6: Add slow-publisher integration coverage**

Run two relays with the same owner but unique claim IDs. Force the first publisher to cross its deadline and verify durable backoff/dead-letter, no stale mutation, correct metrics, and later partition progress.

- [ ] **Step 7: Run messaging verification**

Run: `go test -race -count=1 ./internal/worker ./internal/config && go test -race -count=1 -tags integration ./test/integration/... -run 'Outbox|Relay|Publisher'`

Expected: PASS.

- [ ] **Step 8: Update docs and commit**

```bash
git add internal/worker internal/config internal/bootstrap test/integration .env.example README.md ARCHITECTURE.md
git commit -m "fix: finalize outbox attempts durably"
```

### Task 6: Verify messaging reliability

**Files:**
- Modify only if verification exposes a defect in files owned by Tasks 1-5.

- [ ] **Step 1: Run unit/race/lint**

Run: `go test -race -count=1 ./internal/adapter/sqs ./internal/adapter/postgres ./internal/worker ./internal/config && golangci-lint run ./...`

Expected: PASS.

- [ ] **Step 2: Run Docker-backed messaging tests twice**

Run: `go test -race -count=2 -tags integration ./test/integration/... -run 'SQS|Inbox|Outbox|Relay|Message|Publisher'`

Expected: PASS twice without leaked queues or stale leases.

- [ ] **Step 3: Run coverage**

Run: `make coverage`

Expected: every package remains at or above 90.0%.

- [ ] **Step 4: Commit verification-only fixes, if any**

```bash
git add internal test README.md ARCHITECTURE.md .env.example
git commit -m "test: finalize messaging reliability coverage"
```
