#!/usr/bin/env bash
# Free-disk guard run by `make up` and `make lite-up`. It only READS free space (df); it never deletes anything.
#   deploy/preflight.sh full|lite
# Refuses (exit 1) when free space is below the threshold: 12 GB for full, 5 GB for lite.
# Env: PREFLIGHT_MIN_FREE_GB=<whole number>  override the threshold (announced loudly, never silent). Must match
#                                            ^(0|[1-9][0-9]{0,4})$ (no leading zeros, at most 5 digits), else REFUSE.
#                                            0 is an explicit, announced override that DISABLES the guard.
#      PREFLIGHT_PATH=<dir>                  where Docker keeps its disk image (default: your home folder, which is on
#                                            the same volume as Docker Desktop's disk image unless you moved it)
# Fail closed: every ambiguous or erroring case refuses with a non-zero exit. Untrusted values (the override and the
# df output) are validated as plain decimal digits BEFORE any shell arithmetic, and arithmetic forces base 10 (10#).
# Comparisons are checked explicitly; nothing relies on an arithmetic error being treated as "false".
set -u

refuse() {  # code message...
  code="$1"; shift
  echo "preflight: $*" >&2
  exit "$code"
}

is_plain_digits() {  # value maxlen : non-empty, digits only (ASCII), at most maxlen characters
  case "$1" in
    ''|*[!0-9]*) return 1 ;;
  esac
  [ "${#1}" -le "$2" ]
}

mode="${1:-}"
case "$mode" in
  full) need=12 ;;
  lite) need=5 ;;
  *) refuse 2 "usage: preflight.sh full|lite (got '${mode}')" ;;
esac

if [ "${PREFLIGHT_MIN_FREE_GB+set}" = "set" ]; then
  ov="$PREFLIGHT_MIN_FREE_GB"
  # LC_ALL=C so [!0-9] cannot match non-ASCII digits; no leading zeros (bash would read 08 as invalid octal).
  if ! is_plain_digits "$ov" 5 || { [ "${#ov}" -gt 1 ] && [ "${ov#0}" != "$ov" ]; }; then
    refuse 2 "PREFLIGHT_MIN_FREE_GB must be a whole number of GB, 0 to 99999, no leading zeros (got '${ov}')."
  fi
  need=$((10#$ov))
  if [ "$need" -eq 0 ]; then
    echo "preflight: WARNING free-disk guard DISABLED by PREFLIGHT_MIN_FREE_GB=0. Nothing was checked." >&2
    exit 0
  fi
  echo "preflight: WARNING threshold overridden by PREFLIGHT_MIN_FREE_GB=${need} GB (default is different)." >&2
fi

path="${PREFLIGHT_PATH:-${HOME:-/}}"
# -P gives one POSIX-format line per filesystem with 1024-byte blocks on both macOS and Linux; column 4 is Available.
if ! out=$(LC_ALL=C df -Pk "$path" 2>&1); then
  refuse 1 "could not read free space: 'df -Pk ${path}' failed. Refusing to continue."
fi
data_lines=$(printf '%s\n' "$out" | awk 'NR>1 && NF>0 {n++} END {print n+0}')
if [ "$data_lines" != "1" ]; then
  echo "preflight: unexpected output from df for ${path} (${data_lines} data lines, expected 1). Refusing to continue." >&2
  refuse 1 "check by hand with: df -h /System/Volumes/Data"
fi
avail_kb=$(printf '%s\n' "$out" | awk 'NR>1 && NF>=4 {v=$4} END {print v}')
# Digits only, at most 15 of them (about a petabyte of 1 KB blocks); anything else is refused, never computed with.
if ! is_plain_digits "$avail_kb" 15; then
  echo "preflight: could not parse the output of df for ${path}. Refusing to continue." >&2
  refuse 1 "check by hand with: df -h /System/Volumes/Data"
fi
avail_kb=$((10#$avail_kb))

free_tenths=$(( (avail_kb * 10 + 524288) / 1048576 ))  # display only, rounded; the check below is exact
free_fmt="$(( free_tenths / 10 )).$(( free_tenths % 10 ))"
need_kb=$(( need * 1048576 ))
if [ "$avail_kb" -ge "$need_kb" ]; then
  echo "preflight: OK (${free_fmt} GB free, ${need} GB needed for ${mode})."
  exit 0
fi
echo "preflight: REFUSING to start the ${mode} stack: ${free_fmt} GB free, ${need} GB needed (checked ${path})." >&2
echo "preflight: nothing was changed or deleted. Options:" >&2
echo "  1. Check the real number first: df -h /System/Volumes/Data" >&2
if [ "$mode" = "full" ]; then
  echo "  2. Use the small-disk stack instead: make lite-up" >&2
fi
echo "  3. See 'Small disk' in docs/runbooks/local-stack.md (safe cleanup, moving Docker's disk image to an external drive)." >&2
echo "  4. Print the size plan: make disk-plan" >&2
exit 1
