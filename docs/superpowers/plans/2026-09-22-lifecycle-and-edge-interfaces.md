# Lifecycle and Edge Interfaces Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make configuration conversion, application startup/shutdown, JWKS refresh, HTTP routing, and CLI error handling bounded, cancellation-safe, and operationally diagnosable.

**Architecture:** Configuration validates the exact representation consumed by each adapter. A bootstrap-owned runtime coordinator starts and stops HTTP/workers/resources as one lifecycle boundary. JWKS refresh is shared work with an independent timeout, while each caller retains its own cancellation. HTTP requests always traverse `ServeMux`, preserving canonicalization and path binding.

**Tech Stack:** Go 1.27.1, Uber Fx 1.24.0, `net/http`, go-oidc 3.21.0, go-jose 4.1.5, pgx 5.11.0.

**Spec:** `docs/superpowers/specs/2026-09-22-reliability-and-coverage-remediation-design.md`

## Global Constraints

- Preserve successful HTTP status codes and response bodies.
- Preserve the current CLI commands and exit-code contract.
- Do not expose credentials from `DATABASE_URL` in errors or logs.
- A canceled verifier request may stop waiting, but must not cancel refresh work shared by other requests.
- The database pool must close after every successful construction, including timed-out shutdown.
- Cyclomatic complexity must remain at most 6; cognitive complexity must remain at most 8.

---

### Task 1: Validate adapter units and representable ranges

**Files:**
- Modify: `internal/config/config.go`
- Modify: `internal/config/config_test.go`
- Modify: `README.md`

**Interfaces:**
- Produces: `func wholeSeconds(name string, value time.Duration) error`.
- Produces: `func durationSumLessThan(left, right, limit time.Duration) bool` without overflow-prone addition.
- Produces: `Config.StartupTimeout`, loaded from `STARTUP_TIMEOUT` with a `25s` default.

- [ ] **Step 1: Add failing table-driven configuration tests**

Cover `DB_MAX_CONNS > math.MaxInt32`, positive PostgreSQL timeouts below `1ms`, fractional SQS durations, SQS limits above their API maximum, and values near `time.Duration` overflow. Require errors to name the exact environment variable.

```go
func TestLoadRejectsUnrepresentableAdapterValues(t *testing.T) {
    tests := []struct{ name, key, value, want string }{
        {"pool count", "DB_MAX_CONNS", "2147483648", "DB_MAX_CONNS"},
        {"pg timeout truncates", "DB_LOCK_TIMEOUT", "999us", "DB_LOCK_TIMEOUT"},
        {"fractional SQS second", "SQS_WAIT_TIME", "1500ms", "SQS_WAIT_TIME"},
    }
    // Load the smallest relevant subset and require the named validation error.
}
```

- [ ] **Step 2: Run focused tests and verify RED**

Run: `go test -count=1 ./internal/config -run 'TestLoadRejectsUnrepresentableAdapterValues|TestLoadRejectsOverflowingDurationRelationships'`

Expected: FAIL because current validation accepts values later truncated by `Milliseconds`, `Seconds`, or `int32` conversion and adds durations directly.

- [ ] **Step 3: Implement exact-unit and overflow-safe validation**

Use division/remainder checks rather than float conversion:

```go
func wholeUnit(value, unit time.Duration) bool {
    return value >= 0 && value%unit == 0
}

func sumLessThan(a, b, limit time.Duration) bool {
    return a < limit && b < limit-a
}
```

Require database timeouts to be at least `time.Millisecond`, integer-second SQS values where AWS receives seconds, `DB_MAX_CONNS <= math.MaxInt32`, and a positive `STARTUP_TIMEOUT`. The `25s` default covers the sequential worst case of the current `10s` queue-resolution phase plus `10s` database-ping phase and startup overhead. Keep parsing and semantic validation separate; a deliberately short startup budget remains valid and is tested as a cancellation path.

Raise the default `SHUTDOWN_TIMEOUT` to `40s`. Reserve the final `5s` for
resource cleanup and validate that the concurrent drain window covers the
largest of HTTP's `30s` write bound, SQS `process + ack`, outbox
`publish + finalize`, and the database statement timeout. Compare each budget
to the drain window without overflow-prone addition.

- [ ] **Step 4: Re-run config tests and verify GREEN**

Run: `go test -count=1 ./internal/config`

Expected: PASS.

- [ ] **Step 5: Document the accepted units and ranges**

Update the README configuration table with millisecond PostgreSQL precision, whole-second SQS precision, maximum pool size, and the reason fractional values are rejected rather than rounded.

- [ ] **Step 6: Commit**

```bash
git add internal/config/config.go internal/config/config_test.go README.md
git commit -m "fix: validate adapter configuration units"
```

---

### Task 2: Decouple shared JWKS refresh from caller cancellation

**Files:**
- Modify: `internal/adapter/auth/keyset.go`
- Modify: `internal/adapter/auth/keyset_internal_test.go`
- Modify: `internal/adapter/auth/auth.go`
- Modify: `internal/adapter/auth/auth_test.go`

**Interfaces:**
- Produces: refresh state guarded by `keySet.mu`, with one `done` channel per in-flight fetch.
- Produces: `func (k *keySet) waitRefresh(ctx context.Context) ([]jose.JSONWebKey, error)`.
- Reuses: `auth.Config.JWKSTimeout` and the existing bounded `http.Client`; the shared refresh starts from a non-caller context.
- Produces: `ErrJWKSUnavailable`, preserved by keyset refresh/cooldown paths for internal classification while the public HTTP response remains 401.

- [ ] **Step 1: Add failing concurrent refresh tests**

Start two verification calls for an unknown key. Cancel the first request while the test JWKS handler is blocked, then release the handler. Require the first call to return `context.Canceled`, the second call to verify successfully, and the handler to observe exactly one request. Add a failed-refresh case proving the old keys remain cached and retries remain cooldown-limited.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `go test -count=1 ./internal/adapter/auth -run 'TestKeySetCallerCancellationDoesNotPoisonSharedRefresh|TestKeySetFailedRefreshRetainsCachedKeys'`

Expected: FAIL because the first caller currently owns the network context and records its cancellation as the shared refresh result.

- [ ] **Step 3: Implement a single-flight refresh state machine**

Create the network request with `context.WithTimeout(context.Background(), k.fetchTimeout)`. Under `mu`, either join the current `done` channel or install a new refresh. Each caller waits with:

```go
select {
case <-ctx.Done():
    return nil, ctx.Err()
case <-refresh.done:
    return refresh.keys, refresh.err
}
```

Only successful fetches replace `keys`. Record the last attempt separately from the last successful cache update so failures throttle retries without making an empty cache appear successful. Wrap fetch failures and cooldown reuse with `ErrJWKSUnavailable`, and assert `errors.Is` in package-internal tests. go-oidc 3.21.0 stringifies keyset failures at its verifier boundary, so this classification intentionally remains inside the auth adapter; changing the public 401 contract is deferred. Do not add `singleflight`; the state is small and needs caller-specific cancellation semantics.

- [ ] **Step 4: Re-run auth tests and verify GREEN**

Run: `go test -count=1 -race ./internal/adapter/auth`

Expected: PASS with one IdP request for concurrent misses.

- [ ] **Step 5: Commit**

```bash
git add internal/adapter/auth/keyset.go internal/adapter/auth/keyset_internal_test.go internal/adapter/auth/auth.go internal/adapter/auth/auth_test.go
git commit -m "fix: isolate shared JWKS refresh cancellation"
```

---

### Task 3: Preserve ServeMux canonicalization and contain readiness panics

**Files:**
- Modify: `internal/adapter/httpapi/server.go`
- Modify: `internal/adapter/httpapi/routes_test.go`
- Modify: `internal/adapter/httpapi/server_internal_test.go`

**Interfaces:**
- Preserves: `func NewHandler(d Deps) http.Handler`.
- Produces: panic-safe readiness result collection inside each probe goroutine.

- [ ] **Step 1: Add failing escaped-route and readiness tests**

Cover external IDs containing escaped slash and dot segments. Assert that valid escaped values reach the use case with the decoded `PathValue`, while truly unclean paths receive the mux redirect. Add a readiness probe that panics and require a JSON `503` response while the process remains alive.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `go test -count=1 ./internal/adapter/httpapi -run 'TestEscapedExternalIDPreservesPathValues|TestReadinessProbePanicReturnsUnavailable'`

Expected: FAIL because classification through `mux.Handler` can select a route without binding `PathValue`, and probe panics escape their worker goroutine.

- [ ] **Step 3: Route all requests through ServeMux**

Use `mux.Handler` only to classify unmatched and redirect cases; never invoke the returned wildcard handler directly. Every matched request, including a path-cleaning redirect, must execute through `mux.ServeHTTP`, which owns wildcard binding and canonicalization. Preserve the JSON 404/405 adapter and bounded metrics labels.

- [ ] **Step 4: Recover inside every readiness probe goroutine**

Wrap the check invocation, not only the outer HTTP handler:

```go
func runCheck(ctx context.Context, check HealthCheck) (err error) {
    defer func() {
        if recovered := recover(); recovered != nil {
            err = fmt.Errorf("readiness %s panicked: %v", check.Name, recovered)
        }
    }()
    return check.Check(ctx)
}
```

Keep the returned public body generic; log the dependency name and recovered value internally.

- [ ] **Step 5: Re-run HTTP tests and verify GREEN**

Run: `go test -count=1 -race ./internal/adapter/httpapi`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/adapter/httpapi/server.go internal/adapter/httpapi/routes_test.go internal/adapter/httpapi/server_internal_test.go
git commit -m "fix: preserve HTTP routing and readiness isolation"
```

---

### Task 4: Bound application construction and startup

**Files:**
- Modify: `internal/bootstrap/bootstrap.go`
- Modify: `internal/bootstrap/bootstrap_internal_test.go`
- Modify: `internal/cli/cli.go`
- Modify: `internal/cli/cli_test.go`
- Modify: `internal/cli/cli_internal_test.go`
- Modify: `test/integration/app_test.go`

**Interfaces:**
- Changes: `func bootstrap.New(ctx context.Context, cfg config.Config, extra ...fx.Option) *fx.App`.
- Changes: CLI `newApp` to accept the bounded startup context.
- Produces: a CLI-owned `application` interface exposing `Err`, `Start`, `Stop`, and `Wait` for deterministic tests.

- [ ] **Step 1: Add failing startup-deadline tests**

Inject an app whose `Start` blocks until its context expires and require the configured start budget to cancel it. Add bootstrap tests where queue resolution blocks and where PostgreSQL ping blocks; both must stop at the same startup deadline.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `go test -count=1 ./internal/cli ./internal/bootstrap -run 'TestServeAppliesStartupDeadline|TestConstructionUsesStartupContext'`

Expected: FAIL because the CLI passes its long-lived signal context to `Start`, and constructors use `context.Background()`.

- [ ] **Step 3: Thread one bounded startup context through construction**

Create `startCtx` from `Config.StartupTimeout` before building the Fx app. Supply that context under a private bootstrap wrapper type so it cannot be confused with request contexts. Use it in `postgres.NewPool`, SQS client construction, queue resolution, and auth construction. Continue using Fx `StartTimeout` as a secondary framework guard, but do not rely on it when calling `App.Start(ctx)`.

- [ ] **Step 4: Keep configuration-only failures before allocation**

Preserve the current CLI order: parse command, load/validate only its configuration subset, then create resources. Add allocation counters in tests proving malformed configuration does not invoke migrator, SQS, or app constructors.

- [ ] **Step 5: Re-run lifecycle startup tests and verify GREEN**

Run: `go test -count=1 -race ./internal/cli ./internal/bootstrap`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/bootstrap/bootstrap.go internal/bootstrap/bootstrap_internal_test.go internal/cli/cli.go internal/cli/cli_test.go
git commit -m "fix: apply one bounded startup context"
```

---

### Task 5: Coordinate HTTP, workers, and resource shutdown

**Files:**
- Modify: `internal/bootstrap/bootstrap.go`
- Modify: `internal/bootstrap/bootstrap_internal_test.go`
- Modify: `internal/worker/worker.go`
- Modify: `internal/worker/worker_test.go`
- Create: `test/integration/lifecycle_test.go`

**Interfaces:**
- Produces: one bootstrap `runtime` lifecycle hook that owns listener, worker group, and pool shutdown ordering.
- Produces: asynchronous HTTP serve result reporting that ignores only `http.ErrServerClosed`.

- [ ] **Step 1: Add failing retained-work and serve-error tests**

Test an in-flight HTTP request and one in-flight worker operation during stop. Require admission to close first, both operations to receive their drain budget, and the pool-close spy to run even when either drain times out. Force `Serve` to return a non-closure error and require coordinated application shutdown with exit code 1.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `go test -count=1 ./internal/bootstrap ./internal/worker && go test -count=1 -tags integration ./test/integration -run 'TestShutdownCoordinatesRetainedWork|TestServeErrorRequestsShutdown'`

Expected: FAIL because independent reverse-order Fx hooks share one expiring context and the Serve goroutine discards its result.

- [ ] **Step 3: Replace independent stop hooks with one runtime coordinator**

The coordinator must:

1. close the listener and cancel worker polling;
2. wait for `http.Server.Shutdown` and `Group.Stop` concurrently under bounded child contexts;
3. release unstarted SQS messages through the consumer's stop path;
4. call `pool.Close()` exactly once in a `defer`, even after drain errors;
5. join drain errors with `errors.Join`.

The pool constructor no longer registers a separate `OnStop`; ownership transfers to the coordinator after successful construction. Keep each helper below the repository complexity limits.

- [ ] **Step 4: Report unexpected HTTP server termination**

Inject `fx.Shutdowner` into the runtime. The serve goroutine sends non-`http.ErrServerClosed` errors to a buffered channel, logs them once, and invokes `Shutdown(fx.ExitCode(1))`. The CLI waits on both its signal context and `application.Wait()`, propagating a non-zero Fx shutdown code as an error before calling `Stop`. Tests must confirm normal shutdown does not emit a false error.

- [ ] **Step 5: Re-run lifecycle tests and verify GREEN**

Run: `go test -count=1 -race ./internal/bootstrap ./internal/worker && go test -count=1 -race -tags integration ./test/integration`

Expected: PASS and no leaked goroutines under repeated execution.

- [ ] **Step 6: Commit**

```bash
git add internal/bootstrap/bootstrap.go internal/bootstrap/bootstrap_internal_test.go internal/worker/worker.go internal/worker/worker_test.go test/integration/lifecycle_test.go
git commit -m "fix: coordinate runtime shutdown phases"
```

---

### Task 6: Redact migration errors and preserve close failures

**Files:**
- Modify: `internal/adapter/postgres/migrate.go`
- Modify: `internal/adapter/postgres/migrate_test.go`
- Modify: `internal/cli/cli.go`
- Modify: `internal/cli/cli_test.go`

**Interfaces:**
- Produces: migration errors that contain operation context but never the raw DSN user info.
- Produces: `migrateCmd` named return that joins `Migrator.Close` failure with the operation failure.

- [ ] **Step 1: Add failing redaction and close-error tests**

Use a DSN with a unique password marker. Force parse/open/migration failures and assert the marker never appears in `Run` stderr. Cover operation-only, close-only, and combined failure; require `errors.Is` to find both underlying errors.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `go test -count=1 ./internal/adapter/postgres ./internal/cli -run 'TestMigratorErrorsRedactCredentials|TestMigrateJoinsCloseError'`

Expected: FAIL because raw parser errors can contain the DSN and deferred `Close` is discarded.

- [ ] **Step 3: Implement structured redaction and error joining**

Do not wrap a raw DSN parser/driver error directly: `%w` would preserve its
sensitive `Error()` text. Introduce a redacted wrapper whose `Error` returns
only a safe operation message while `Unwrap` preserves `errors.Is/As` without
delegating its string representation:

```go
type redactedError struct { operation string; cause error }
func (e redactedError) Error() string { return e.operation }
func (e redactedError) Unwrap() error { return e.cause }
```

Test the complete outer error chain and stderr for absence of the synthetic
password marker. In the CLI:

```go
func migrateCmd(lookup config.Lookup, args []string, stdout io.Writer) (retErr error) {
    // construct migrator
    defer func() { retErr = errors.Join(retErr, m.Close()) }()
    return mig.run(m, stdout)
}
```

Wrap operations with `%w` so callers retain error identity.

- [ ] **Step 4: Re-run CLI and migration tests and verify GREEN**

Run: `go test -count=1 -race ./internal/adapter/postgres ./internal/cli`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/adapter/postgres/migrate.go internal/adapter/postgres/migrate_test.go internal/cli/cli.go internal/cli/cli_test.go
git commit -m "fix: redact migration failures"
```

---

### Task 7: Document and verify the edge-boundary guarantees

**Files:**
- Modify: `README.md`
- Modify: `ARCHITECTURE.md`

- [ ] **Step 1: Update lifecycle, readiness, and authentication documentation**

Document startup and shutdown phases, unexpected HTTP-server failure behavior, JWKS cache/cooldown semantics, readiness panic containment, exact adapter units, and DSN redaction. Keep public 401 behavior unchanged and state that IdP-vs-token status differentiation remains deferred.

- [ ] **Step 2: Run package and repository checks**

Run:

```bash
go test -count=1 -race ./internal/config ./internal/adapter/auth ./internal/adapter/httpapi ./internal/bootstrap ./internal/cli ./internal/worker
go vet ./...
golangci-lint run
```

Expected: all commands exit 0.

- [ ] **Step 3: Commit**

```bash
git add README.md ARCHITECTURE.md
git commit -m "docs: describe lifecycle and edge guarantees"
```
