#!/usr/bin/env bash
# Free-disk guard run by `make up` and `make lite-up`. It only READS free space (df); it never deletes anything.
#   deploy/preflight.sh full|lite
# Refuses (exit 1) when free space is below the threshold: 12 GB for full, 5 GB for lite.
# Env: PREFLIGHT_MIN_FREE_GB=<whole number>  override the threshold (announced loudly, never silent)
#      PREFLIGHT_PATH=<dir>                  where Docker keeps its disk image (default: your home folder, which is on
#                                            the same volume as Docker Desktop's disk image unless you moved it)
set -u

mode="${1:-}"
case "$mode" in
  full) need=12 ;;
  lite) need=5 ;;
  *) echo "preflight: usage: preflight.sh full|lite (got '${mode}')" >&2; exit 2 ;;
esac

if [ "${PREFLIGHT_MIN_FREE_GB+set}" = "set" ]; then
  case "$PREFLIGHT_MIN_FREE_GB" in
    ''|*[!0-9]*)
      echo "preflight: PREFLIGHT_MIN_FREE_GB must be a whole number of GB (got '${PREFLIGHT_MIN_FREE_GB}')." >&2
      exit 2 ;;
  esac
  need="$PREFLIGHT_MIN_FREE_GB"
  echo "preflight: WARNING threshold overridden by PREFLIGHT_MIN_FREE_GB=${need} GB (default is different)." >&2
fi

path="${PREFLIGHT_PATH:-${HOME:-/}}"
# -P gives one POSIX-format line per filesystem with 1024-byte blocks on both macOS and Linux; column 4 is Available.
if ! out=$(df -Pk "$path" 2>&1); then
  echo "preflight: could not read free space: 'df -Pk ${path}' failed. Refusing to continue." >&2
  exit 1
fi
avail_kb=$(printf '%s\n' "$out" | awk 'NR>1 && NF>=4 {v=$4} END {print v}')
case "$avail_kb" in
  ''|*[!0-9]*)
    echo "preflight: could not parse the output of df for ${path}. Refusing to continue." >&2
    echo "preflight: check by hand with: df -h /System/Volumes/Data" >&2
    exit 1 ;;
esac

free_tenths=$(( (avail_kb * 10 + 524288) / 1048576 ))  # display only, rounded; the check below is exact
free_fmt="$(( free_tenths / 10 )).$(( free_tenths % 10 ))"
if [ "$avail_kb" -lt $(( need * 1048576 )) ]; then
  echo "preflight: REFUSING to start the ${mode} stack: ${free_fmt} GB free, ${need} GB needed (checked ${path})." >&2
  echo "preflight: nothing was changed or deleted. Options:" >&2
  echo "  1. Check the real number first: df -h /System/Volumes/Data" >&2
  if [ "$mode" = "full" ]; then
    echo "  2. Use the small-disk stack instead: make lite-up" >&2
  fi
  echo "  3. See 'Small disk' in docs/runbooks/local-stack.md (safe cleanup, moving Docker's disk to an external drive)." >&2
  echo "  4. Print the size plan: make disk-plan" >&2
  exit 1
fi
echo "preflight: OK (${free_fmt} GB free, ${need} GB needed for ${mode})."
