#!/bin/sh
set -eu
export LC_ALL=C

if [ "$#" -ne 3 ]; then
	printf 'usage: %s REPORT MINIMUM EXPECTED_PACKAGES\n' "$0" >&2
	exit 2
fi

report=$1
minimum=$2
expected=$3

if [ ! -r "$expected" ]; then
	printf 'expected package inventory is not readable: %s\n' "$expected" >&2
	exit 1
fi
if [ ! -s "$expected" ]; then
	printf 'expected package inventory is empty: %s\n' "$expected" >&2
	exit 1
fi

actual=$(mktemp "${TMPDIR:-/tmp}/go-coverage-packages.XXXXXX")
trap 'rm -f "$actual"' EXIT

awk -v minimum="$minimum" '
BEGIN {
  if (minimum !~ /^[0-9]+([.][0-9]+)?$/ || minimum + 0 > 100) {
    printf "invalid minimum coverage: %s\n", minimum > "/dev/stderr"
    exit 1
  }
}
NF {
  if (NF != 5 || $2 != "coverage:" || $4 != "of" || $5 != "statements") {
    printf "malformed coverage row: %s\n", $0 > "/dev/stderr"
    failed=1
    next
  }

  found=1
  package=$1
  percent=$3
  if (percent !~ /^[0-9]+([.][0-9]+)?%$/) {
    printf "invalid coverage percentage: %s\n", $3 > "/dev/stderr"
    failed=1
    next
  }

  sub(/%$/, "", percent)
  if (percent + 0 > 100) {
    printf "invalid coverage percentage: %s\n", $3 > "/dev/stderr"
    failed=1
    next
  }
  if (seen[package]++) {
    printf "duplicate coverage: %s\n", package > "/dev/stderr"
    failed=1
    next
  }

  print package
  if ((percent + 0) < minimum) {
    printf "%s coverage %.1f%% is below %.1f%%\n", package, percent, minimum > "/dev/stderr"
    failed=1
  }
}
END {
  if (!found) {
    print "coverage report contains no package rows" > "/dev/stderr"
    failed=1
  }
  if (failed) exit 1
}
' "$report" >"$actual"
sort -u -o "$actual" "$actual"

missing=$(comm -23 "$expected" "$actual")
test -z "$missing" || {
	printf 'missing coverage: %s\n' "$missing" >&2
	exit 1
}
