#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
checker="$script_dir/check-go-coverage.sh"
fixtures="$script_dir/testdata"
test_tmp=$(mktemp -d "${TMPDIR:-/tmp}/check-go-coverage.XXXXXX")
trap 'rm -rf "$test_tmp"' EXIT

expect_success() {
	name=$1
	report=$2

	if ! "$checker" "$report" 90 "$fixtures/packages.txt" >"$test_tmp/stdout" 2>"$test_tmp/stderr"; then
		printf 'FAIL: %s should succeed\n' "$name" >&2
		cat "$test_tmp/stderr" >&2
		exit 1
	fi
}

expect_failure() {
	name=$1
	report=$2
	want=$3

	if "$checker" "$report" 90 "$fixtures/packages.txt" >"$test_tmp/stdout" 2>"$test_tmp/stderr"; then
		printf 'FAIL: %s should fail\n' "$name" >&2
		exit 1
	fi
	if ! grep -F "$want" "$test_tmp/stderr" >/dev/null; then
		printf 'FAIL: %s did not report %s\n' "$name" "$want" >&2
		cat "$test_tmp/stderr" >&2
		exit 1
	fi
}

printf '%s\n' \
	'example.com/wallet/cmd/wallet coverage: 90.0% of statements extra' \
	>"$test_tmp/malformed.txt"
printf '%s\n' \
	'example.com/wallet/cmd/wallet coverage: unknown% of statements' \
	>"$test_tmp/invalid-percent.txt"
printf '%s\n' \
	'example.com/wallet/cmd/wallet coverage: 90.0% of statements' \
	>"$test_tmp/missing.txt"
printf '%s\n' \
	'example.com/wallet/cmd/wallet coverage: 90.0% of statements' \
	'example.com/wallet/cmd/wallet coverage: 95.0% of statements' \
	'example.com/wallet/internal/service coverage: 100.0% of statements' \
	>"$test_tmp/duplicate.txt"
: >"$test_tmp/empty.txt"

expect_success "passing report" "$fixtures/coverage-pass.txt"
expect_failure "low coverage" "$fixtures/coverage-low.txt" "coverage 89.9% is below 90.0%"
expect_failure "malformed row" "$test_tmp/malformed.txt" "malformed coverage row"
expect_failure "invalid percentage" "$test_tmp/invalid-percent.txt" "invalid coverage percentage"
expect_failure "missing package" "$test_tmp/missing.txt" "missing coverage: example.com/wallet/internal/service"
expect_failure "duplicate package" "$test_tmp/duplicate.txt" "duplicate coverage: example.com/wallet/cmd/wallet"
expect_failure "empty report" "$test_tmp/empty.txt" "coverage report contains no package rows"

printf 'PASS: coverage checker fixtures\n'
