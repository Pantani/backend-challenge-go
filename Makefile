SHELL := /bin/bash
GO ?= go
COVERAGE_DIR := $(CURDIR)/coverage

.PHONY: all build test test-race vet lint test-integration test-e2e coverage coverage-inventory up down clean logs migrate-up migrate-down provision-queues

all: vet lint test-race

build:
	go build -o bin/wallet ./cmd/wallet

## Unit tests (no Docker needed).
test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...
	go vet -tags integration,e2e ./...

## gofmt/goimports + cyclomatic (<= 6) and cognitive (<= 8) complexity, incl. tests.
lint:
	golangci-lint run ./...

## Integration tests: real PostgreSQL, LocalStack and Keycloak via testcontainers.
test-integration:
	go test -race -count=1 -tags integration ./test/integration/...

## End-to-end: the compiled binary as 3 independent processes + crash/restart.
test-e2e:
	go test -count=1 -timeout 15m -tags e2e ./test/e2e/...

## Combined coverage of unit + integration + e2e (binary built with -cover).
coverage:
	rm -rf $(COVERAGE_DIR) && mkdir -p $(COVERAGE_DIR)/unit $(COVERAGE_DIR)/integration $(COVERAGE_DIR)/e2e
	scripts/test-check-go-coverage.sh
	$(MAKE) --no-print-directory coverage-inventory
	go test -race -covermode=atomic -count=1 -tags integration -coverpkg=./cmd/...,./internal/...,./test/testenv ./cmd/... ./internal/... ./test/testenv -args -test.gocoverdir=$(COVERAGE_DIR)/unit
	go test -race -covermode=atomic -count=1 -tags integration -coverpkg=./cmd/...,./internal/...,./test/testenv ./test/integration/... -args -test.gocoverdir=$(COVERAGE_DIR)/integration
	E2E_GOCOVERDIR=$(COVERAGE_DIR)/e2e go test -count=1 -timeout 15m -tags e2e ./test/e2e/...
	go tool covdata textfmt -i=$(COVERAGE_DIR)/unit,$(COVERAGE_DIR)/integration,$(COVERAGE_DIR)/e2e -o $(COVERAGE_DIR)/coverage.out
	go tool covdata percent -i=$(COVERAGE_DIR)/unit,$(COVERAGE_DIR)/integration,$(COVERAGE_DIR)/e2e > $(COVERAGE_DIR)/packages.txt
	scripts/check-go-coverage.sh $(COVERAGE_DIR)/packages.txt 90 $(COVERAGE_DIR)/packages.expected
	go tool cover -func=$(COVERAGE_DIR)/coverage.out | tail -1

coverage-inventory:
	mkdir -p $(COVERAGE_DIR)
	rm -f $(COVERAGE_DIR)/packages.expected $(COVERAGE_DIR)/packages.raw $(COVERAGE_DIR)/packages.filtered
	LC_ALL=C $(GO) list -tags integration,e2e -f '{{if .GoFiles}}{{.ImportPath}}{{end}}' ./cmd/... ./internal/... ./test/testenv > $(COVERAGE_DIR)/packages.raw
	LC_ALL=C sed '/^$$/d' $(COVERAGE_DIR)/packages.raw > $(COVERAGE_DIR)/packages.filtered
	LC_ALL=C sort -u $(COVERAGE_DIR)/packages.filtered > $(COVERAGE_DIR)/packages.expected
	rm -f $(COVERAGE_DIR)/packages.raw $(COVERAGE_DIR)/packages.filtered
	test -s $(COVERAGE_DIR)/packages.expected || { printf 'coverage package inventory is empty; go list found no non-test Go packages\n' >&2; exit 1; }

up:
	docker compose up --build -d

## Stop the stack, keeping the database and queue volumes.
down:
	docker compose down

## Stop the stack and delete its volumes.
clean:
	docker compose down -v

logs:
	docker compose logs -f app-1 app-2 app-3

migrate-up:
	docker compose run --rm migrate migrate up

migrate-down:
	docker compose run --rm migrate migrate down 1

provision-queues:
	docker compose run --rm provision-queues provision-queues
