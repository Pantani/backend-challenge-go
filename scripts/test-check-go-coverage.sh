#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
checker="$script_dir/check-go-coverage.sh"
fixtures="$script_dir/testdata"
test_tmp=$(mktemp -d "${TMPDIR:-/tmp}/check-go-coverage.XXXXXX")
trap 'rm -rf "$test_tmp"' EXIT

expect_success() {
	name=$1
	report=$2
	expected=$3

	if ! "$checker" "$report" 90 "$expected" >"$test_tmp/stdout" 2>"$test_tmp/stderr"; then
		printf 'FAIL: %s should succeed\n' "$name" >&2
		cat "$test_tmp/stderr" >&2
		exit 1
	fi
}

expect_failure() {
	name=$1
	report=$2
	expected=$3
	want=$4

	if "$checker" "$report" 90 "$expected" >"$test_tmp/stdout" 2>"$test_tmp/stderr"; then
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
: >"$test_tmp/empty-expected.txt"

expect_success "passing report" "$fixtures/coverage-pass.txt" "$fixtures/packages.txt"
expect_failure "low coverage" "$fixtures/coverage-low.txt" "$fixtures/packages.txt" "coverage 89.9% is below 90.0%"
expect_failure "malformed row" "$test_tmp/malformed.txt" "$fixtures/packages.txt" "malformed coverage row"
expect_failure "invalid percentage" "$test_tmp/invalid-percent.txt" "$fixtures/packages.txt" "invalid coverage percentage"
expect_failure "missing package" "$test_tmp/missing.txt" "$fixtures/packages.txt" "missing coverage: example.com/wallet/internal/service"
expect_failure "duplicate package" "$test_tmp/duplicate.txt" "$fixtures/packages.txt" "duplicate coverage: example.com/wallet/cmd/wallet"
expect_failure "empty report" "$test_tmp/empty.txt" "$fixtures/packages.txt" "coverage report contains no package rows"
expect_failure "empty expected inventory" "$fixtures/coverage-pass.txt" "$test_tmp/empty-expected.txt" "expected package inventory is empty"

comma_locale=""
for candidate in pt_BR.UTF-8 pt_BR; do
	if decimal=$(LC_ALL=$candidate locale decimal_point 2>/dev/null) && [ "$decimal" = "," ]; then
		comma_locale=$candidate
		break
	fi
done
if [ -n "$comma_locale" ]; then
	if LC_ALL=$comma_locale "$checker" "$fixtures/coverage-low.txt" 90 "$fixtures/packages.txt" >"$test_tmp/stdout" 2>"$test_tmp/stderr"; then
		printf 'FAIL: low coverage should fail under %s\n' "$comma_locale" >&2
		exit 1
	fi
	if ! grep -F "coverage 89.9% is below 90.0%" "$test_tmp/stderr" >/dev/null; then
		printf 'FAIL: decimal parsing changed under %s\n' "$comma_locale" >&2
		cat "$test_tmp/stderr" >&2
		exit 1
	fi
fi

printf '%s\n' '#!/bin/sh' 'printf "forced go list failure\n" >&2' 'exit 42' >"$test_tmp/fail-go"
chmod +x "$test_tmp/fail-go"
if make --no-print-directory -s -C "$repo_root" coverage-inventory \
	GO="$test_tmp/fail-go" COVERAGE_DIR="$test_tmp/inventory" >"$test_tmp/stdout" 2>"$test_tmp/stderr"; then
	printf 'FAIL: coverage inventory should propagate go list failure\n' >&2
	exit 1
fi
if ! grep -F "forced go list failure" "$test_tmp/stderr" >/dev/null; then
	printf 'FAIL: coverage inventory hid the go list diagnostic\n' >&2
	cat "$test_tmp/stderr" >&2
	exit 1
fi
if [ -s "$test_tmp/inventory/packages.expected" ]; then
	printf 'FAIL: coverage inventory retained packages after go list failure\n' >&2
	exit 1
fi

printf 'PASS: coverage checker fixtures\n'
