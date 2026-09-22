# Reliability and Coverage Remediation Plan Series

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan series. Complete each linked plan task-by-task and preserve its RED/GREEN evidence.

**Goal:** Sequence the four implementation plans so shared test infrastructure lands first, schema versions remain ordered, overlapping files have one clear integration order, and final verification measures the complete repository.

**Spec:** `docs/superpowers/specs/2026-09-22-reliability-and-coverage-remediation-design.md`

## Execution order

1. `2026-09-22-test-harness-and-coverage.md`
   - Establishes safe concurrent helpers, re-entrant fixtures, cleanup, and the per-package coverage gate.
   - Its coverage command is run as a baseline here and as the final authority after every production plan.
2. `2026-09-22-messaging-reliability.md`
   - Owns migration `000005`, SQS timing/identity, and outbox fencing/finalization.
   - Its configuration edits land before later configuration hardening.
3. `2026-09-22-lifecycle-and-edge-interfaces.md`
   - Builds on the messaging configuration, adjusts bootstrap/CLI ownership, and fixes JWKS/HTTP boundaries.
4. `2026-09-22-persistence-and-domain-integrity.md`
   - Owns migration `000006`, wide reconciliation, inbox state, and persistent/domain invariants.
5. Complete the final verification and independent review below.

## Shared-file ownership

- `internal/config/config.go`, `internal/config/config_test.go`: messaging changes first; lifecycle reconciles all unit/range/startup rules without removing messaging validation.
- `internal/bootstrap/bootstrap.go`: messaging wires relay finalization first; lifecycle then establishes final construction and runtime ownership.
- `internal/app/ports.go`: messaging changes outbox contracts first; persistence then changes reconciliation/inbox contracts without reverting outbox fencing.
- `README.md`, `ARCHITECTURE.md`, `.env.example`: each plan documents its own guarantee; the final review removes duplication and checks the configuration table against code.
- `test/integration/*`: the harness plan establishes resource naming and assertion rules; production plans must use those helpers rather than reintroduce ad hoc fixtures.

## Final verification

- [ ] **Step 1: Run formatting and static analysis**

```bash
gofmt -w cmd internal test
go vet ./...
go vet -tags integration,e2e ./...
golangci-lint run ./...
```

Expected: all commands exit 0; no complexity suppression is introduced.

- [ ] **Step 2: Run deterministic tests**

```bash
go test -count=1 ./...
go test -race -count=1 ./...
go test -race -count=2 -tags integration ./test/integration/...
go test -count=1 -timeout 15m -tags e2e ./test/e2e/...
```

Expected: all commands exit 0, the repeated integration run is re-entrant, and E2E leaves no owned temporary process or directory.

- [ ] **Step 3: Run per-package coverage and vulnerability analysis**

```bash
make coverage
govulncheck ./...
```

Expected: every package containing non-test Go statements appears in the combined report at 90.0% or higher, and no reachable vulnerability is reported.

- [ ] **Step 4: Audit documentation against executable behavior**

Compare every README environment variable/default with `internal/config`, every lifecycle statement with bootstrap tests, every messaging guarantee with integration tests, and every migration description with the SQL. Remove repeated implementation comments and retain only exported API contracts and non-obvious invariant rationale.

- [ ] **Step 5: Request independent final code review**

Use the `requesting-code-review` skill. The reviewer must inspect the complete diff against the approved spec, with special attention to temporal guarantees, context ownership, database constraint compatibility, false-green tests, package coverage, and public contract preservation.

- [ ] **Step 6: Commit only review-driven corrections, if any**

```bash
git add cmd internal test scripts Makefile .github README.md ARCHITECTURE.md docs .env.example
git commit -m "fix: address final remediation review"
```

- [ ] **Step 7: Record the complete audit disposition**

Create `docs/AUDIT.md` with one row per consolidated finding and the columns
`ID`, `severity`, `evidence`, `disposition`, `implementation/test`, and
`remaining decision`. Mark each item as fixed, rejected with evidence, or
deferred with a concrete compatibility/authorization reason. Include the
baseline commands and distinguish local, Docker-backed, AWS-unverified, and
production-unverified evidence. Do not describe statement coverage as proof of
temporal or distributed correctness.

- [ ] **Step 8: Commit the audit record**

```bash
git add docs/AUDIT.md
git commit -m "docs: record full code audit disposition"
```
