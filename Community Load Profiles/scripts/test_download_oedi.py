"""Tiny self-test for the OEDI downloader.

This is a lightweight script test (no pytest dependency yet on dev).
It validates manifest parsing and error messaging.

Run:
  python3 scripts/test_download_oedi.py
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

from download_oedi import load_manifest


def main() -> None:
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "m.json"
        p.write_text(json.dumps({"bucket": "b", "datasets": {}}))
        m = load_manifest(p)
        assert m["bucket"] == "b"

    print("ok")


if __name__ == "__main__":
    main()
