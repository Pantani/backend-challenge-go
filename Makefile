SHELL := /bin/bash
COVERAGE_DIR := $(CURDIR)/coverage

.PHONY: all build test test-race vet lint test-integration test-e2e coverage up down logs migrate-up migrate-down provision-queues

all: lint test-race

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
	go test -race -covermode=atomic -count=1 -coverpkg=./internal/... ./internal/... -args -test.gocoverdir=$(COVERAGE_DIR)/unit
	go test -race -covermode=atomic -count=1 -tags integration -coverpkg=./internal/... ./test/integration/... -args -test.gocoverdir=$(COVERAGE_DIR)/integration
	E2E_GOCOVERDIR=$(COVERAGE_DIR)/e2e go test -count=1 -timeout 15m -tags e2e ./test/e2e/...
	go tool covdata textfmt -i=$(COVERAGE_DIR)/unit,$(COVERAGE_DIR)/integration,$(COVERAGE_DIR)/e2e -o $(COVERAGE_DIR)/coverage.out
	go tool cover -func=$(COVERAGE_DIR)/coverage.out | tail -1

up:
	docker compose up --build -d

down:
	docker compose down -v

logs:
	docker compose logs -f app-1 app-2 app-3

migrate-up:
	docker compose run --rm migrate migrate up

migrate-down:
	docker compose run --rm migrate migrate down 1

provision-queues:
	docker compose run --rm provision-queues provision-queues
