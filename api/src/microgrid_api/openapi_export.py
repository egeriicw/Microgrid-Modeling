"""Export the OpenAPI schema to api/openapi.json (the frontend generates its typed client from it).

``python -m microgrid_api.openapi_export`` writes the file; ``--check`` exits 1 if it is stale (a test too).
The schema does not depend on the environment: a throwaway in-memory configuration is used.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

from microgrid_api.config import Settings
from microgrid_api.main import create_app

OPENAPI_PATH = Path(__file__).resolve().parents[2] / "openapi.json"


def render() -> str:
    app = create_app(Settings(environment="test", database_url="sqlite://", api_keys=[], _env_file=None))
    try:
        return json.dumps(app.openapi(), indent=2, sort_keys=True) + "\n"
    finally:
        app.state.engine.dispose()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail if the file is stale instead of writing it")
    parser.add_argument("--path", type=Path, default=OPENAPI_PATH)
    args = parser.parse_args(argv)
    text = render()
    if args.check:
        if not args.path.exists() or args.path.read_text() != text:
            print(f"{args.path} is stale; run: python -m microgrid_api.openapi_export", file=sys.stderr)
            return 1
        return 0
    args.path.write_text(text)
    print(f"wrote {args.path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
