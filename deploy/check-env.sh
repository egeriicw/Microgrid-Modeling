#!/bin/sh
# Refuse MICROGRID_* names in deploy/.env that Docker Compose would silently drop (FR-ENG-3).
#
#   ./deploy/check-env.sh deploy/.env
#
# Compose passes only the MICROGRID_* names listed in deploy/docker-compose.yml into the containers, so a typo such
# as MICROGRID_TEMPERATURE_UNIT (missing S) would never reach the app, and the app could not complain about it. This
# check stops first instead. It prints variable NAMES only, never values. Read-only; it changes nothing.
# Run by `make up`, `make lite-up`, `make config-check` and the smoke script, before any docker command.
set -eu

# MICROGRID_DATA_DIR is the host data folder (compose mounts it); the rest are passed through to the containers.
# Keep in step with the pass-through list in deploy/docker-compose.yml (tests/test_deploy_data_settings.py checks).
KNOWN="MICROGRID_DATA_DIR MICROGRID_RESSTOCK_CHARS_FILE MICROGRID_COMSTOCK_CHARS_FILE MICROGRID_TEMPERATURE_SOURCE MICROGRID_TEMPERATURE_COLUMN MICROGRID_TEMPERATURE_UNITS MICROGRID_TEMPERATURE_WEATHER_FILE MICROGRID_TEMPERATURE_TIME_COLUMN MICROGRID_MAX_REQUEST_BYTES MICROGRID_PERMISSION_OVERRIDES"

if [ "$#" -ne 1 ]; then
  echo "usage: check-env.sh <env file>" >&2
  exit 2
fi
file=$1
if [ ! -f "$file" ]; then
  echo "check-env: $file not found (copy deploy/.env.example to deploy/.env first)." >&2
  exit 2
fi

# Names of uncommented assignments starting with microgrid_ in any letter case (optionally after `export `).
names=$(sed -n 's/^[[:space:]]*\(export[[:space:]][[:space:]]*\)\{0,1\}\([Mm][Ii][Cc][Rr][Oo][Gg][Rr][Ii][Dd]_[A-Za-z0-9_]*\)[[:space:]]*=.*/\2/p' "$file")
bad=""
for name in $names; do
  case " $KNOWN " in
    *" $name "*) ;;
    *) bad="$bad $name" ;;
  esac
done
if [ -n "$bad" ]; then
  {
    echo "check-env: deploy/.env ($file) sets a MICROGRID_ name the app would never see (a typo, or a setting Compose"
    echo "fixes itself), so it would be silently ignored:"
    for name in $bad; do echo "  $name"; done
    echo "Settings you can use there:"
    for name in $KNOWN; do echo "  $name"; done
    echo "Fix the name or comment the line out (put '# ' in front). See deploy/.env.example."
  } >&2
  exit 1
fi
