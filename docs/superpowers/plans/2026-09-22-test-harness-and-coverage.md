# Test Harness and Per-Package Coverage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the integration/E2E harness deterministic and enforce at least 90% combined statement coverage for every package containing non-test Go code.

**Architecture:** Test helpers return values and errors from worker goroutines; assertions stay on the test goroutine. Resource owners register cleanup immediately. Coverage data from unit, integration, and E2E runs is checked against the package inventory rather than only reporting a global percentage.

**Tech Stack:** Go 1.27.1, `testing`, Testify 1.11.1, testcontainers-go 0.44.0, GNU Make, POSIX `awk`, Go coverage data tools.

**Spec:** `docs/superpowers/specs/2026-09-22-reliability-and-coverage-remediation-design.md`

## Global Constraints

- Combined statement coverage must be at least 90.0% for every package containing non-test Go statements.
- Production code must not be expanded merely to improve coverage.
- Fatal test operations must run only on the goroutine that owns `testing.T`.
- All network and process waits must have explicit deadlines.
- Existing HTTP and SQS success contracts remain unchanged.
- Cyclomatic complexity must remain at most 6; cognitive complexity must remain at most 8.

---

### Task 1: Give shared test utilities direct behavioral coverage

**Files:**
- Create: `internal/testutil/testutil_test.go`
- Modify: `internal/testutil/testutil.go:61-89`

**Interfaces:**
- Consumes: `SyncBuffer`, `NewFakeClock`, `FakeClock.Now`, `Advance`, and `Set`.
- Produces: direct race-instrumented tests covering all statements in `internal/testutil` and a corrected `Advance` contract.

- [ ] **Step 1: Write direct concurrency and clock tests**

```go
func TestSyncBufferIsSafeForConcurrentReadersAndWriters(t *testing.T) {
    var buf SyncBuffer
    var wg sync.WaitGroup
    errs := make(chan error, 32)
    for i := range 32 {
        wg.Go(func() {
            _, err := fmt.Fprintf(&buf, "line-%d\n", i)
            errs <- err
            _ = buf.String()
        })
    }
    wg.Wait()
    close(errs)
    for err := range errs { require.NoError(t, err) }
    for i := range 32 {
        assert.Contains(t, buf.String(), fmt.Sprintf("line-%d\n", i))
    }
}

func TestFakeClockAdvanceAndSet(t *testing.T) {
    start := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
    clock := NewFakeClock(start)
    clock.Advance(2 * time.Second)
    assert.Equal(t, start.Add(2*time.Second), clock.Now())
    clock.Set(start.Add(-time.Hour))
    assert.Equal(t, start.Add(-time.Hour), clock.Now())
}
```

- [ ] **Step 2: Run the tests with coverage and race instrumentation**

Run: `go test -race -count=1 -cover ./internal/testutil`

Expected: PASS and `coverage: 100.0% of statements`.

- [ ] **Step 3: Correct the misleading `Advance` comment**

```go
// Advance moves the clock by d; a negative duration moves it backwards.
func (c *FakeClock) Advance(d time.Duration) { /* existing implementation */ }
```

- [ ] **Step 4: Re-run the package checks**

Run: `go test -race -count=1 ./internal/testutil && golangci-lint run ./internal/testutil/...`

Expected: PASS and zero lint issues.

- [ ] **Step 5: Commit**

```bash
git add internal/testutil/testutil.go internal/testutil/testutil_test.go
git commit -m "test: cover shared test utilities"
```

### Task 2: Make parallel helpers return errors instead of failing worker goroutines

**Files:**
- Modify: `test/testenv/client.go:167-182`
- Create: `test/testenv/client_test.go`
- Modify: `test/integration/concurrency_test.go`
- Modify: `test/integration/messaging_test.go`
- Modify: `test/integration/app_test.go`
- Modify: `test/e2e/scenarios_test.go`

**Interfaces:**
- Produces: `func Parallel[T any](n int, fn func(int) (T, error)) ([]T, error)`.
- Consumers: integration and E2E concurrency helpers; callers assert the joined error after `Parallel` returns.
- Requires: new `test/testenv` tests start with `//go:build integration || e2e`, matching the package implementation files.

- [ ] **Step 1: Write a failing test for worker error collection**

```go
func TestParallelReturnsWorkerErrorsInCallOrder(t *testing.T) {
    values, err := Parallel(3, func(i int) (int, error) {
        if i == 1 {
            return 0, errors.New("worker 1")
        }
        return i, nil
    })
    assert.Equal(t, []int{0, 0, 2}, values)
    require.ErrorContains(t, err, "worker 1")
}
```

- [ ] **Step 2: Run the test and verify the API mismatch fails**

Run: `go test -tags integration -run TestParallelReturnsWorkerErrors ./test/testenv`

Expected: FAIL to compile because `Parallel` still accepts `func(int) T` and returns one value.

- [ ] **Step 3: Implement the error-returning helper**

```go
func Parallel[T any](n int, fn func(int) (T, error)) ([]T, error) {
    values := make([]T, n)
    errs := make([]error, n)
    var wg, ready sync.WaitGroup
    ready.Add(n)
    start := make(chan struct{})
    for i := range n {
        wg.Go(func() {
            ready.Done()
            <-start
            values[i], errs[i] = fn(i)
        })
    }
    ready.Wait()
    close(start)
    wg.Wait()
    return values, errors.Join(errs...)
}
```

- [ ] **Step 4: Update every caller to return errors and assert after join**

```go
results, err := testenv.Parallel(n, func(i int) (testenv.Response, error) {
    return client.Submit(ctx, wallet, provider, externalID(i), "BET", "1.00", "")
})
require.NoError(t, err)
```

Replace helpers that accept `*testing.T` inside the callback with error-returning variants. Do not use `require`, `assert`, `Fatal`, or `FailNow` in the callback.

- [ ] **Step 5: Replace fatal assertions inside `Eventually` callbacks**

```go
require.EventuallyWithT(t, func(collect *assert.CollectT) {
    got, err := loadState(context.Background())
    if !assert.NoError(collect, err) { return }
    assert.Equal(collect, want, got)
}, 30*time.Second, 200*time.Millisecond)
```

When a helper cannot accept `assert.TestingT`, return `(value, error)` and assert outside it instead.

Search every `Eventually`, `EventuallyWithT`, `Parallel`, and explicit `go`
callback in `test/`; require that no reachable helper invokes `require`,
`Fatal`, `FailNow`, or another fatal assertion outside the test goroutine.

- [ ] **Step 6: Run focused and race tests**

Run: `go test -race -count=1 -tags integration ./test/testenv ./test/integration/...`

Expected: PASS with no `FailNow` call reachable from helper goroutines.

- [ ] **Step 7: Commit**

```bash
git add test/testenv/client.go test/testenv/client_test.go test/integration test/e2e/scenarios_test.go
git commit -m "test: keep fatal assertions on test goroutines"
```

### Task 3: Make integration resources re-entrant and strengthen false-green oracles

**Files:**
- Modify: `test/integration/main_test.go`
- Modify: `test/integration/cli_test.go`
- Modify: `test/integration/schema_test.go`
- Modify: `test/integration/messaging_test.go`
- Modify: `test/integration/failures_test.go`

**Interfaces:**
- Produces: execution-unique database, queue, external, message, and deduplication identifiers.
- Produces: `func sqlState(error) (string, error)` that never conflates success with a non-PostgreSQL error.

- [ ] **Step 1: Capture the existing re-entrancy failure**

Run: `go test -race -count=2 -tags integration ./test/integration/...`

Expected: FAIL in migration databases and FIFO scenarios because names and deduplication IDs repeat.

- [ ] **Step 2: Make SQL-state assertions preserve the original error**

```go
func sqlState(err error) (string, error) {
    if err == nil {
        return "", nil
    }
    var pgErr *pgconn.PgError
    if !errors.As(err, &pgErr) {
        return "", err
    }
    return pgErr.Code, nil
}
```

For valid SQL, assert `require.NoError(t, err)`. For rejected SQL, require a non-empty expected SQLSTATE and the expected original error.

- [ ] **Step 3: Generate and clean execution-unique resources**

```go
suffix := strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
databaseName := "cli_check_" + suffix
messageID := "bad-" + uuid.NewString()
dedupID := uuid.NewString()
```

Register `DROP DATABASE`/queue cleanup immediately after successful creation. Close the migrator with `defer` and join any close error into setup failure.

- [ ] **Step 4: Make the HTTP+SQS integration test traverse actual HTTP**

Start the application fixture, call `testenv.Client.Submit`, send the SQS envelope concurrently, and assert all of:

```go
require.Equal(t, 1, debitCount)
require.Equal(t, 1, transactionCount)
require.Equal(t, 1, completedInboxCount)
require.Equal(t, 0, inputVisible+inputInFlight)
require.Equal(t, 0, dlqVisible+dlqInFlight)
```

- [ ] **Step 5: Exercise global idempotency without a wallet lock**

Submit the same provider/idempotency key concurrently against two wallets, then
repeat with the same provider/external ID and different keys. Require exactly
one durable transaction/effect and the documented replay or conflict outcome.
This proves the global unique indexes rather than only the per-wallet lock.

- [ ] **Step 6: Upgrade representative data through every migration boundary**

For each version, migrate to `vN`, insert the valid edge rows affected by
`vN+1`, apply the next migration, and verify data plus named indexes, triggers,
and constraints. Derive the latest version from embedded migration filenames;
do not hardcode it.

- [ ] **Step 7: Rename or replace misleading failure tests**

Keep the canceled-context test but name it for cancellation. Add a real broken-connection test by terminating a disposable backend connection, then prove the pool recovers on a subsequent query.

- [ ] **Step 8: Run the repeated suite**

Run: `go test -race -count=2 -tags integration ./test/integration/...`

Expected: PASS twice in the same process with no name, FIFO deduplication, or cleanup collision.

- [ ] **Step 9: Commit**

```bash
git add test/integration
git commit -m "test: make integration fixtures reentrant"
```

### Task 4: Give the E2E harness explicit ownership, deadlines, and non-vacuous assertions

**Files:**
- Modify: `test/testenv/testenv.go`
- Modify: `test/integration/main_test.go`
- Modify: `test/e2e/main_test.go`
- Modify: `test/e2e/scenarios_test.go`
- Create: `test/testenv/testenv_test.go`

**Interfaces:**
- Produces: `func RepoRoot(start string) (string, error)` that walks upward for `go.mod`.
- Produces: `func (*Env) Stop(context.Context) error`.
- Produces: a bounded HTTP client stored on `testenv.Client`.
- Produces: idempotent E2E cleanup that owns the temporary directory and every process immediately.
- Requires: `testenv_test.go` starts with `//go:build integration || e2e`.

- [ ] **Step 1: Preserve the `-trimpath` reproduction**

Run: `go test -trimpath -count=1 -timeout 5m -tags e2e ./test/e2e/...`

Expected: FAIL because `RepoRoot` currently derives a trimmed module path from `runtime.Caller`.

- [ ] **Step 2: Add a root-search test**

```go
func TestRepoRootWalksUpToGoMod(t *testing.T) {
    root, err := RepoRoot(filepath.Join("..", "e2e"))
    require.NoError(t, err)
    _, err = os.Stat(filepath.Join(root, "go.mod"))
    require.NoError(t, err)
}
```

- [ ] **Step 3: Implement bounded root discovery and cleanup errors**

```go
func RepoRoot(start string) (string, error) {
    dir, err := filepath.Abs(start)
    if err != nil { return "", err }
    for {
        if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil { return dir, nil }
        parent := filepath.Dir(dir)
        if parent == dir { return "", errors.New("go.mod not found") }
        dir = parent
    }
}

func (e *Env) Stop(ctx context.Context) error {
    var errs []error
    for _, c := range e.containers { errs = append(errs, c.Terminate(ctx)) }
    return errors.Join(errs...)
}
```

Update `Env.Start` and both suite entrypoints to propagate root-discovery and
cleanup failures; no caller may discard the new `Stop` error.

- [ ] **Step 4: Make E2E setup an owned fixture**

Create an `e2eFixture` that stores `tempDir`, `pool`, and instances. Register cleanup immediately after each acquisition. Add an instance to the owned set immediately after `cmd.Start`, before readiness polling.

```go
type e2eFixture struct {
    tempDir string
    pool *pgxpool.Pool
    instances []*instance
}

func (f *e2eFixture) Close(ctx context.Context) error {
    // stop/kill and Wait every process, close pool, then os.RemoveAll(tempDir)
}
```

- [ ] **Step 5: Bound HTTP and process waits**

Use `http.Client{Timeout: 10 * time.Second}`, context-aware timers, `stop(ctx)` with TERM followed by Kill after deadline, and always call `Wait`. Detect an early process exit while readiness is polling.

- [ ] **Step 6: Remove the port-reservation race**

Start each wallet with `HTTP_ADDR=127.0.0.1:0`, parse the structured
`"http server listening"` log record from the owned stdout/stderr stream, and
publish that bound address to readiness. Do not reserve and close a listener
before the child binds.

- [ ] **Step 7: Validate token endpoint status before decoding**

Make the testenv token client reject non-2xx responses with a bounded, redacted
body excerpt and the HTTP status. A 4xx/5xx must never degrade into a misleading
JSON decode error.

- [ ] **Step 8: Remove order-dependent final consistency**

Track wallets created by each scenario or reconcile inside each test. Require at least one wallet before a global check. Replace ignored SQL errors inside polling with collected assertions.

- [ ] **Step 9: Verify cleanup and portability**

Run: `go test -trimpath -count=1 -timeout 15m -tags e2e ./test/e2e/...`

Expected: PASS. The run leaves no `wallet-e2e*` directory created by that run and no child wallet process.

- [ ] **Step 10: Commit**

```bash
git add test/testenv/testenv.go test/testenv/testenv_test.go test/e2e
git commit -m "test: make e2e lifecycle deterministic"
```

### Task 5: Enforce combined coverage per package

**Files:**
- Create: `scripts/check-go-coverage.sh`
- Create: `scripts/testdata/coverage-pass.txt`
- Create: `scripts/testdata/coverage-low.txt`
- Create: `scripts/testdata/packages.txt`
- Modify: `Makefile`
- Modify: `.github/workflows/ci.yml`
- Modify: `README.md`

**Interfaces:**
- Produces: `scripts/check-go-coverage.sh REPORT MINIMUM EXPECTED_PACKAGES`, returning non-zero for a missing package, malformed report, or package below the threshold.
- Produces: `make coverage` with combined instrumentation for `./cmd/...`, `./internal/...`, and `./test/testenv`.

- [ ] **Step 1: Write executable fixture checks that fail with no checker**

```sh
scripts/check-go-coverage.sh scripts/testdata/coverage-pass.txt 90 scripts/testdata/packages.txt
! scripts/check-go-coverage.sh scripts/testdata/coverage-low.txt 90 scripts/testdata/packages.txt
```

Expected: FAIL because the checker and fixtures do not exist.

- [ ] **Step 2: Implement the portable checker**

```sh
#!/bin/sh
set -eu
report=$1
minimum=$2
expected=$3
actual=$(mktemp)
trap 'rm -f "$actual"' EXIT
awk -v minimum="$minimum" '
NF {
  if (NF != 5 || $2 != "coverage:" || $4 != "of" || $5 != "statements") {
    printf "malformed coverage row: %s\n", $0 > "/dev/stderr"
    failed=1
    next
  }
  found=1
  package=$1; percent=$3; sub(/%$/, "", percent)
  if (percent !~ /^[0-9]+([.][0-9]+)?$/) {
    printf "invalid coverage percentage: %s\n", $3 > "/dev/stderr"
    failed=1
    next
  }
  print package
  if ((percent + 0) < minimum) {
    printf "%s coverage %.1f%% is below %.1f%%\n", package, percent, minimum > "/dev/stderr"
    failed=1
  }
}
END { if (!found || failed) exit 1 }
' "$report" | sort -u > "$actual"
missing=$(comm -23 "$expected" "$actual")
test -z "$missing" || { printf 'missing coverage: %s\n' "$missing" >&2; exit 1; }
```

- [ ] **Step 3: Extend `make coverage` and enforce the threshold**

Generate the expected inventory with:

```sh
go list -tags integration,e2e -f '{{if .GoFiles}}{{.ImportPath}}{{end}}' ./cmd/... ./internal/... ./test/testenv | sed '/^$/d' | sort -u > coverage/packages.expected
go tool covdata percent -i=coverage/unit,coverage/integration,coverage/e2e > coverage/packages.txt
scripts/check-go-coverage.sh coverage/packages.txt 90 coverage/packages.expected
```

Add `./test/testenv` to `-coverpkg` for the integration run.

- [ ] **Step 4: Make the Docker CI job run the authoritative target once**

Replace separate integration/E2E steps in the scheduled/manual job with `make coverage`, then upload `coverage/coverage.out` and `coverage/packages.txt`. Keep the existing PR policy unchanged.

- [ ] **Step 5: Run the complete coverage target**

Run: `make coverage`

Expected: PASS with every listed package at or above 90.0%.

- [ ] **Step 6: Update the README contract**

Document the combined per-package threshold, the exact `make coverage` command, and that Docker-backed coverage runs in scheduled/manual CI.

- [ ] **Step 7: Commit**

```bash
git add scripts Makefile .github/workflows/ci.yml README.md
git commit -m "ci: enforce per-package coverage floor"
```

### Task 6: Verify the test-infrastructure plan

**Files:**
- Modify only if verification exposes a defect in files owned by Tasks 1-5.

**Interfaces:**
- Produces: a clean test and coverage foundation consumed by the remaining remediation plans.

- [ ] **Step 1: Run formatting, vet, lint, and unit race**

Run: `gofmt -w internal/testutil test/testenv test/integration test/e2e && go vet -tags integration,e2e ./... && golangci-lint run ./... && go test -race -count=1 ./...`

Expected: PASS with zero lint/vet issues.

- [ ] **Step 2: Run repeated integration and E2E**

Run: `go test -race -count=2 -tags integration ./test/integration/... && go test -count=1 -timeout 15m -tags e2e ./test/e2e/...`

Expected: PASS.

- [ ] **Step 3: Run the authoritative coverage gate**

Run: `make coverage`

Expected: PASS with every package at or above 90.0%.

- [ ] **Step 4: Commit verification-only fixes, if any**

```bash
git add internal/testutil test/testenv test/integration test/e2e scripts Makefile .github/workflows/ci.yml README.md
git commit -m "test: finalize deterministic coverage harness"
```
