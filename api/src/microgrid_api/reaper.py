"""One-shot stuck-run reaper: ``python -m microgrid_api.reaper`` (cron or a compose job; Celery beat runs the same
code on a schedule). Prints the number of runs marked failed to stderr; exit code 0 on success."""

from __future__ import annotations

import sys
from collections.abc import Sequence

from microgrid_api import worker
from microgrid_api.config import ConfigurationError, Settings


def main(argv: Sequence[str] | None = None) -> int:
    if argv:
        print("reaper: takes no arguments; configure via MICROGRID_* variables", file=sys.stderr)
        return 2
    try:
        settings = worker._settings_override or Settings()
        reaped = worker.reap_once(settings, worker._session_factory())
    except ConfigurationError as exc:
        print(f"reaper: {exc}", file=sys.stderr)
        return 2
    print(f"reaper: reaped {len(reaped)} stuck run(s)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
