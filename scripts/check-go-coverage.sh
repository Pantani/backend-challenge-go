#!/bin/sh
set -eu
export LC_ALL=C

if [ "$#" -ne 3 ]; then
	printf 'usage: %s PROFILE MINIMUM EXPECTED_PACKAGES\n' "$0" >&2
	exit 2
fi

profile=$1
minimum=$2
expected=$3

if [ ! -r "$profile" ]; then
	printf 'coverage profile is not readable: %s\n' "$profile" >&2
	exit 1
fi
if [ ! -s "$profile" ]; then
	printf 'coverage profile is empty: %s\n' "$profile" >&2
	exit 1
fi
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
  minimumPartCount=split(minimum, minimumParts, ".")
  minimumScale=1
  if (minimumPartCount == 2) {
    for (i=1; i <= length(minimumParts[2]); i++) minimumScale *= 10
  }
  minimumNumerator=(minimumParts[1] * minimumScale) + minimumParts[2]
}
NR == 1 {
  if ($0 !~ /^mode: (set|count|atomic)$/) {
    printf "invalid coverage mode: %s\n", $0 > "/dev/stderr"
    failed=1
  }
  next
}
{
  line=$0
  if (!match(line, / [0-9]+ [0-9]+$/)) {
    printf "malformed coverage row: %s\n", $0 > "/dev/stderr"
    failed=1
    next
  }

  location=substr(line, 1, RSTART - 1)
  counters=substr(line, RSTART + 1)
  split(counters, fields, " ")
  statements=fields[1] + 0
  count=fields[2] + 0
  if (location !~ /:[0-9]+[.][0-9]+,[0-9]+[.][0-9]+$/) {
    printf "malformed coverage row: %s\n", $0 > "/dev/stderr"
    failed=1
    next
  }

  file=location
  sub(/:[0-9]+[.][0-9]+,[0-9]+[.][0-9]+$/, "", file)
  package=file
  if (!sub(/\/[^\/]+$/, "", package)) {
    printf "coverage row has no package path: %s\n", $0 > "/dev/stderr"
    failed=1
    next
  }
  if (seenBlock[location]++) {
    printf "duplicate coverage block: %s\n", location > "/dev/stderr"
    failed=1
    next
  }

  found=1
  totals[package] += statements
  if (count > 0) covered[package] += statements
}
END {
  if (failed) exit 1
  if (!found) {
    print "coverage profile contains no package rows" > "/dev/stderr"
    exit 1
  }
  for (package in totals) {
    total=totals[package]
    if (total <= 0) {
      printf "%s coverage profile contains no statements\n", package > "/dev/stderr"
      failed=1
      continue
    }
    print package
    if ((covered[package] * 100 * minimumScale) < (total * minimumNumerator)) {
      printf "%s coverage %.1f%% (%d/%d statements) is below %.1f%%\n", package, 100 * covered[package] / total, covered[package], total, minimum > "/dev/stderr"
      failed=1
    }
  }
  if (failed) exit 1
}
' "$profile" >"$actual"
sort -u -o "$actual" "$actual"

missing=$(comm -23 "$expected" "$actual")
test -z "$missing" || {
	printf 'missing coverage: %s\n' "$missing" >&2
	exit 1
}
