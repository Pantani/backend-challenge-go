# Reliability and Per-Package Coverage Remediation

Date: 2026-09-22

## Context

The repository implements a distributed wallet and wagering service with HTTP
and SQS inputs, PostgreSQL-backed financial state, an inbox/outbox design, and
multi-process workers. The existing architecture has strong separation between
domain, application, adapter, and bootstrap packages. The verified baseline is
healthy: unit, race, integration, and end-to-end suites pass; combined statement
coverage is 99.6%; lint and vet are clean.

The audit nevertheless found gaps in temporal and distributed guarantees that
statement coverage does not expose. Most defects arise where a timeout or lease
is validated for one operation but the implementation applies it to a batch or
multi-phase lifecycle. The test harness also contains false-green and cleanup
risks that reduce confidence in the existing coverage figure.

## Goals

1. Correct the verified reliability defects in SQS consumption, outbox relay,
   startup, and shutdown without changing documented successful HTTP or SQS
   contracts.
2. Correct functional defects in JWKS refresh, escaped HTTP routes,
   reconciliation, configuration conversion, and migration error redaction.
3. Strengthen persistent invariants with additive migrations and compatible
   domain validation.
4. Make integration and end-to-end tests deterministic, re-entrant, bounded,
   and incapable of calling fatal test methods from helper goroutines.
5. Enforce at least 90% combined statement coverage for every Go package,
   including command and test-support packages.
6. Align README, architecture documentation, GoDoc, and invariant comments with
   the behavior actually guaranteed by the implementation.

## Non-goals

- Replacing the existing domain/application/adapter architecture.
- Introducing a new framework, message bus, ORM, or repository abstraction.
- Rewriting existing migration history.
- Tightening JSON duplicate/case/null handling in this change.
- Changing normalization rules for previously accepted idempotency keys.
- Requiring a new JWT `typ` profile before the Keycloak token contract is
  separately confirmed.
- Changing wallet-opening conflict semantics or silently treating an ambiguous
  commit as a successful replay.
- Optimizing code without a benchmark or profile demonstrating a hot path.

## Compatibility policy

Successful HTTP routes, statuses, response bodies, and documented SQS messages
remain compatible. Enforcement of `MessageGroupId = walletId` and the documented
FIFO message identity is considered validation of the existing contract, not a
new contract. Inputs that currently work but violate that published contract may
be rejected or dead-lettered with an explicit reason.

Potentially breaking hardening, including stricter JSON semantics, a different
idempotency-key normalization policy, a mandatory JWT token type, and a new
wallet-opening idempotency contract, is deferred and documented separately.

## Architecture

The current dependency direction remains unchanged:

```text
domain <- app ports <- adapters <- bootstrap/cli
```

Interfaces remain small and consumer-owned. Changes are localized to the
component responsible for the guarantee:

- the SQS adapter owns broker visibility and FIFO metadata validation;
- the outbox store and relay own claims, leases, publishing, and durable
  finalization;
- bootstrap and CLI own application lifecycle budgets;
- auth owns shared JWKS refresh behavior;
- PostgreSQL owns wide reconciliation arithmetic and durable constraints;
- domain constructors and rehydration own in-memory invariants;
- the test harness owns resource lifetimes and assertion routing.

## SQS consumer design

The consumer must not start work whose visibility protection can expire while
waiting behind earlier messages. The implementation will use an explicit
visibility-management strategy for received messages rather than assuming
`process timeout + acknowledgement timeout < visibility timeout` protects the
entire batch.

The selected implementation must preserve ordering within one message group and
allow progress across independent groups. It must also:

- validate `MessageGroupId` against the command wallet ID;
- request and validate `MessageDeduplicationId` against the envelope message ID;
- validate message ID length and syntax before logging or persistence;
- validate the envelope timestamp according to the documented wire contract;
- return a non-successful group outcome when copying to the DLQ succeeds but
  deletion from the source queue fails, so the group tail is not advanced under
  a false acknowledgement;
- keep processing and broker-finalization contexts separate.

The initial design favors correctness and bounded work over maximum receive
batch throughput. Any concurrency introduced is limited and partition-aware.

## Outbox relay design

A claim needs an identity distinct from the human-readable instance ID. Durable
mutations after publication must prove ownership of the current claim, not only
reuse `locked_by`. Claim identity acts as a fencing token for confirmation,
failure scheduling, and dead-lettering.

Claims must be acquired or renewed close enough to publication that a later item
cannot spend most of its lease waiting behind earlier items. Publication and
durable finalization use separate contexts:

1. claim an eligible event;
2. publish using the publication budget;
3. cancel the publication context;
4. confirm, reschedule, or dead-letter using a separate bounded database budget;
5. increment success/dead-letter metrics only after the durable mutation
   succeeds for the current claim.

`attempts` must represent actual publication attempts. Merely claiming an event
or abandoning the remainder of a batch during shutdown must not consume a
publication attempt.

Ordering remains per wallet partition. At-least-once delivery remains the public
contract; FIFO deduplication is a mitigation, not the sole correctness boundary.

## Lifecycle design

Startup receives an explicit deadline from the configured startup budget. I/O
performed while constructing the application is moved to lifecycle hooks or is
otherwise given the same bounded, cancelable context. Configuration parsing
that can fail without I/O occurs before resource allocation.

Shutdown is coordinated in phases instead of relying on one Fx context to
serially drain every hook:

1. stop admitting new HTTP work and stop worker polling;
2. allow in-flight HTTP and worker operations to finish within explicit phase
   budgets;
3. release unstarted SQS messages safely;
4. close the database pool even if an earlier drain phase times out.

An unexpected `http.Server.Serve` error other than normal server closure is
reported and initiates coordinated application shutdown. Tests cover retained
HTTP work and in-flight worker work, not only idle shutdown.

## Authentication and HTTP design

JWKS refresh is shared work with its own timeout. A caller may stop waiting when
its request context is canceled, but that cancellation does not poison the
shared cache or record a failed refresh as a fresh empty cache. Failed refreshes
retain bounded retry protection so an unavailable identity provider cannot be
amplified into one request per token.

Operational JWKS failure remains distinguishable internally from invalid token
claims. Changing the public status from 401 to 503 is deferred unless it can be
done without altering the approved compatibility boundary.

HTTP route canonicalization remains under `http.ServeMux`; wrappers must not
invoke a wildcard handler in a way that bypasses `PathValue` population. Tests
cover escaped slash and dot segments in valid external identifiers.

Readiness probes recover panics inside their own goroutines and report the
dependency as unavailable without terminating the process.

## Persistence and domain integrity

Reconciliation performs signed arithmetic in PostgreSQL `NUMERIC` or an
equivalent wide representation and converts only the final balance-sized result
to the domain `int64` representation. Tests build valid histories whose gross
credit and debit totals exceed `MaxInt64` while the net balance remains valid.

New constraints are introduced only through additive migrations. Before adding
each constraint, an integration test seeds representative pre-existing rows and
proves the migration behavior. Existing migration files are immutable.

Domain rehydration reuses construction validators where doing so preserves the
distinction between creation and persisted state. It validates origin, external
metadata, amount policy, state-dependent result/failure/schedule fields,
attempts, references, and timestamp order. The schema adds compatible
constraints for impossible persisted combinations and wallet/transaction ledger
association.

An incomplete inbox row is treated as an explicit recoverable or alertable
state, not as a completed duplicate. The normal register/process/complete path
remains one transaction.

## Configuration and error handling

Configuration validates the unit and representable range used by each adapter:

- PostgreSQL timeouts are at least one millisecond and convert without becoming
  zero;
- SQS durations represented as integer seconds are exact and within API limits;
- duration relationships are compared without overflow-prone addition;
- connection counts fit the target `int32` type;
- shutdown and startup budgets are validated against their complete phases.

Errors are wrapped with operation context using `%w`, preserving `errors.Is`.
External messages remain stable unless explicitly covered by this design. DSN
errors are redacted before reaching stderr or logs. Migration close errors are
joined with operation errors instead of discarded.

## Test harness design

Test callbacks running in helper goroutines return values and errors. They never
call `require`, `Fatal`, or another operation that invokes `FailNow`; assertions
run in the test goroutine after join. Polling helpers similarly return state and
errors without fatal assertions inside `Eventually` callbacks.

Integration resources use execution-unique database names, queue names, and FIFO
deduplication IDs, with cleanup registered immediately after acquisition. The
suite must support repeated execution in one process.

The end-to-end harness:

- owns every process and temporary directory immediately after creation;
- removes compiled temporary binaries during cleanup;
- uses bounded HTTP clients and context-aware polling;
- detects early process exit;
- verifies signal and `Wait` results;
- makes final-consistency assertions non-vacuous and independent of test order;
- emits per-instance logs when a scenario fails;
- avoids deriving the repository root from trimmed caller paths.

Crash tests distinguish persistence after a completed response from the
commit-to-ack and publish-to-confirm windows. Where deterministic process-level
barriers are too invasive, integration tests use controlled wrappers and assert
the exact durable state at the failure boundary.

## Coverage policy

Coverage is measured from the combined unit, integration, and end-to-end
coverage data. Every package containing non-test Go statements must report at
least 90% statement coverage, including `cmd/wallet`, `internal/testutil`, and
`test/testenv`. The `test/integration` and `test/e2e` packages contain test
drivers rather than separately shipped statements; their execution contributes
coverage to the packages they exercise.

The coverage target fails when:

- any package is absent from the combined report;
- any package reports less than 90.0%; or
- coverage data cannot be parsed.

`internal/testutil` receives direct concurrent tests for `SyncBuffer` and
`FakeClock`, including `Set`. Production code is not expanded merely to make it
easier to cover. PostgreSQL behavior remains tested against PostgreSQL rather
than duplicated with low-value SQL mocks.

The CI policy for running Docker-backed coverage on every pull request remains a
separate cost decision. The local `make coverage` gate is deterministic and is
the authoritative full check. Fast pull-request jobs continue to run unit race,
vet, lint, build, and module checks unless the CI policy is explicitly changed.

## Documentation and comments

README and `ARCHITECTURE.md` are updated with the implementation that changes the
guarantee. They describe batch-level visibility, lease ownership, finalization
budgets, shutdown phases, SQS identity validation, combined per-package coverage,
and migration rollback risks.

GoDoc is added or corrected for exported APIs. Private comments explain only
non-obvious invariants, concurrency ownership, timing budgets, and reasons for
specific SQL. Comments that repeat the implementation or promise behavior not
enforced by code are removed or corrected.

## Implementation order

1. Repair test helpers and add the per-package coverage gate.
2. Add failing temporal tests for SQS, outbox, and lifecycle.
3. Correct SQS visibility/FIFO handling and outbox claim/finalization.
4. Correct startup, shutdown, and server failure propagation.
5. Correct JWKS refresh and escaped route handling.
6. Correct reconciliation, configuration conversions, and DSN redaction.
7. Add domain/schema hardening through additive migrations.
8. Strengthen integration/E2E oracles, resource ownership, and crash coverage.
9. Update README, architecture documentation, GoDoc, and invariant comments.
10. Run the full verification and independent final code review.

## Verification

Completion requires fresh evidence from all of the following:

```sh
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
go vet -tags integration,e2e ./...
golangci-lint run ./...
make test-integration
make test-e2e
make coverage
govulncheck ./...
```

The combined coverage output must show every package at or above 90%. Tests for
each bug must demonstrate the expected red failure before the production fix and
pass after it. The final review checks requirements, architecture, error
handling, distributed timing, documentation, and test oracles.
