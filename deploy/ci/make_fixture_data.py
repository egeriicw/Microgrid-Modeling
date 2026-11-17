"""Write the synthetic fixture dataset for the Compose smoke run (CI overlay mounts it read-only at /data).

    python deploy/ci/make_fixture_data.py tests/fixtures/data

Uses the test generator ``tests/fixtures/synthetic_data.py`` (made-up numbers in the real on-disk layout; no NREL
data). The API builds the run's ``data`` section on the server from MICROGRID_DATA_DIR and never sets the
characteristics file names, so the engine looks for its DataConfig defaults (``DC_upgrade0.xlsx`` and
``DC_upgrade0_agg.xlsx``) under ``background/``. The generator names them ``chars_resstock.xlsx`` and
``chars_comstock.xlsx``; this script adds copies under the default names. Generated files are git-ignored
(``*.xlsx``, ``*.parquet``).

Safety: refuses the owner's ``data/`` folder and any non-empty folder it did not create (a marker file records that
it did). It never deletes anything; a rerun overwrites only the files it writes.
"""

from __future__ import annotations

import importlib.util
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GENERATOR = ROOT / "tests" / "fixtures" / "synthetic_data.py"
MARKER = ".synthetic-fixture"
ALLOWED = {"README.md", MARKER, "background", "input"}
# The engine's DataConfig defaults (src/microgrid/community_load/synthetic/config.py); checked by a test.
DEFAULT_CHARS = {"chars_resstock.xlsx": "DC_upgrade0.xlsx", "chars_comstock.xlsx": "DC_upgrade0_agg.xlsx"}


def _fail(message: str) -> None:
    print(f"make_fixture_data: {message}", file=sys.stderr)
    raise SystemExit(2)


def _check_target(target: Path) -> None:
    resolved = target.resolve()
    owner_data = (ROOT / "data").resolve()
    if resolved == owner_data or resolved.is_relative_to(owner_data):
        _fail("refusing to write into the owner's data/ folder; pick another directory")
    if not resolved.exists():
        return
    if not resolved.is_dir():
        _fail(f"{target} exists and is not a directory")
    entries = {p.name for p in resolved.iterdir()}
    generated = entries & {"background", "input"}
    if not entries <= ALLOWED or (generated and MARKER not in entries):
        _fail(f"{target} is not empty and was not created by this script; refusing to write into it")


def _load_generator():
    spec = importlib.util.spec_from_file_location("_synthetic_data_for_compose", GENERATOR)
    if spec is None or spec.loader is None:
        _fail(f"cannot load the generator at {GENERATOR}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # dataclasses resolve annotations through sys.modules
    spec.loader.exec_module(module)
    return module


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 1:
        _fail("usage: make_fixture_data.py <target directory>")
    target = Path(args[0])
    _check_target(target)
    target.mkdir(parents=True, exist_ok=True)
    (target / MARKER).write_text("synthetic fixture data written by deploy/ci/make_fixture_data.py\n")
    dataset = _load_generator().build_synthetic_dataset(target)
    for generated, default_name in DEFAULT_CHARS.items():
        shutil.copyfile(dataset.background_dir / generated, dataset.background_dir / default_name)
    n_files = sum(1 for p in target.rglob("*") if p.is_file())
    print(f"make_fixture_data: wrote {n_files} files under {target}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
