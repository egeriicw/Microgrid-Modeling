"""Data-root manifest: relative path, size and sha256 per file (FR-DATA-1).

Safety rules, enforced in code and tests:
  * never downloads anything and never deletes or rewrites a data file; the only file ever written is the manifest,
    via a uniquely named temporary file created exclusively (never following a symlink) and renamed into place;
  * REFUSED (exit code 2): a symlinked FILE that resolves outside the data root, a symlink loop, a broken symlink,
    an unreadable file, and a manifest target outside the root;
  * SKIPPED (never followed, listed in build/verify/report output): symlinked DIRECTORIES, even if they point inside
    the root, so their contents are not in the manifest;
  * EXCLUDED from the walk (never hashed, never removed): ``manifest.json`` and leftover temporary files
    (``manifest.json.tmp``, ``.manifest.json.*.tmp``) directly under the root;
  * resumable and idempotent: a previous manifest is reused for files whose size and mtime are unchanged, and the
    manifest bytes carry no timestamps, so rebuilding an unchanged tree reproduces the same file.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath

from microgrid.datasets.catalog import CatalogError, resolve_data_root

MANIFEST_NAME = "manifest.json"
MANIFEST_VERSION = 1
DEFAULT_BUDGET_GB = 30.0  # owner's disk limit

Progress = Callable[[int, int, str], None]


@dataclass(frozen=True)
class FileEntry:
    size: int
    sha256: str
    mtime_ns: int = 0  # only used to skip re-hashing on resume; verify always re-hashes


@dataclass(frozen=True)
class Manifest:
    files: dict[str, FileEntry] = field(default_factory=dict)
    skipped_dirs: tuple[str, ...] = ()  # symlinked directories seen while walking; not followed, not serialized


@dataclass
class VerifyReport:
    missing: list[str] = field(default_factory=list)
    modified: list[str] = field(default_factory=list)
    extra: list[str] = field(default_factory=list)  # present on disk but not in the manifest (informational)
    skipped_dirs: list[str] = field(default_factory=list)  # symlinked directories, not followed

    @property
    def ok(self) -> bool:
        return not (self.missing or self.modified or self.extra)


def sha256_file(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        while block := fh.read(chunk):
            h.update(block)
    return h.hexdigest()


def _check_root(root: Path) -> Path:
    if not Path(root).is_dir():
        raise CatalogError(f"data root {root} does not exist or is not a directory")
    return Path(root).resolve()


def _inside(root: Path, path: Path, *, must_exist: bool = True) -> bool:
    try:
        path.resolve(strict=must_exist).relative_to(root.resolve())
    except ValueError:
        return False
    except (OSError, RuntimeError) as exc:  # broken symlink, symlink loop, permission problem
        raise CatalogError(f"cannot resolve {path} (broken or looping symlink?): {exc}") from exc
    return True


def _safe_rel(rel: str) -> str:
    p = PurePosixPath(rel)
    if p.is_absolute() or ".." in p.parts:
        raise CatalogError(f"manifest path {rel!r} is outside the data root")
    return rel


def _is_manifest_artifact(rel: str) -> bool:
    """The manifest itself and temporary files left by an interrupted write (root level only)."""
    if "/" in rel:
        return False
    return rel in (MANIFEST_NAME, MANIFEST_NAME + ".tmp") or (
        rel.startswith("." + MANIFEST_NAME + ".") and rel.endswith(".tmp")
    )


def _walk_all(root: Path) -> tuple[list[str], list[str]]:
    """Return (files, symlinked directories), both sorted and relative to ``root``. Directories are never followed."""
    found: list[str] = []
    skipped: list[str] = []
    for dirpath, dirs, names in os.walk(root, followlinks=False):
        for d in dirs:
            if (Path(dirpath) / d).is_symlink():
                skipped.append((Path(dirpath) / d).relative_to(root).as_posix())
        for name in names:
            full = Path(dirpath) / name
            rel = full.relative_to(root).as_posix()
            if _is_manifest_artifact(rel):
                continue
            if not _inside(root, full):
                raise CatalogError(f"{full} resolves outside the data root {root}; refusing")
            found.append(rel)
    return sorted(found), sorted(skipped)


def _walk(root: Path) -> list[str]:
    return _walk_all(root)[0]


def build_manifest(root: Path, *, previous: Manifest | None = None, progress: Progress | None = None) -> Manifest:
    """Walk ``root`` and hash every file (reusing ``previous`` hashes for files with unchanged size and mtime)."""
    root = _check_root(root)
    rels, skipped = _walk_all(root)
    files: dict[str, FileEntry] = {}
    for i, rel in enumerate(rels, start=1):
        st = (root / rel).stat()
        old = previous.files.get(rel) if previous else None
        if old and old.size == st.st_size and old.mtime_ns == st.st_mtime_ns:
            files[rel] = old
        else:
            files[rel] = FileEntry(st.st_size, sha256_file(root / rel), st.st_mtime_ns)
        if progress:
            progress(i, len(rels), rel)
    return Manifest(files, tuple(skipped))


def write_manifest(manifest: Manifest, path: Path, *, root: Path | None = None) -> Path:
    """Write the manifest atomically. If ``root`` is given, ``path`` must be inside it."""
    path = Path(path)
    if root is not None and not _inside(Path(root), path, must_exist=False):
        raise CatalogError(f"manifest target {path} is outside the data root {root}; refusing")
    doc = {
        "version": MANIFEST_VERSION,
        "files": [
            {"path": rel, "size": e.size, "sha256": e.sha256, "mtime_ns": e.mtime_ns}
            for rel, e in sorted(manifest.files.items())
        ],
    }
    payload = (json.dumps(doc, indent=1) + "\n").encode()
    # mkstemp creates a brand-new file (O_CREAT|O_EXCL, so an existing file or symlink of any name is never opened).
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix="." + path.name + ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        os.chmod(tmp_name, 0o644)
        os.replace(tmp_name, path)  # replaces the name itself; never writes through a symlink at ``path``
    except BaseException:
        Path(tmp_name).unlink(missing_ok=True)  # only our own just-created temp file
        raise
    return path


def read_manifest(path: Path) -> Manifest:
    try:
        doc = json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise CatalogError(f"cannot read manifest {path}: {exc}") from exc
    if not isinstance(doc, dict) or doc.get("version") != MANIFEST_VERSION or not isinstance(doc.get("files"), list):
        raise CatalogError(f"manifest {path} has an unsupported format")
    files: dict[str, FileEntry] = {}
    for i, item in enumerate(doc["files"]):
        try:
            rel = _safe_rel(str(item["path"]))
            files[rel] = FileEntry(int(item["size"]), str(item["sha256"]), int(item.get("mtime_ns", 0)))
        except (KeyError, TypeError, ValueError, AttributeError) as exc:
            raise CatalogError(
                f"manifest {path}: entry {i} is malformed (needs path, size, sha256): {type(exc).__name__} {exc}"
            ) from exc
    return Manifest(files)


def verify_manifest(root: Path, manifest: Manifest, *, progress: Progress | None = None) -> VerifyReport:
    """Re-hash every listed file; report missing, modified and unlisted files. Changes nothing on disk."""
    root = _check_root(root)
    report = VerifyReport()
    items = sorted(manifest.files.items())
    for i, (rel, entry) in enumerate(items, start=1):
        full = root / _safe_rel(rel)
        if not (full.is_file() or full.is_symlink()):
            report.missing.append(rel)
        elif not full.is_file():
            report.missing.append(rel)  # broken symlink or loop: the file is not there
        elif not _inside(root, full):
            raise CatalogError(f"{full} resolves outside the data root {root}; refusing")
        elif full.stat().st_size != entry.size or sha256_file(full) != entry.sha256:
            report.modified.append(rel)
        if progress:
            progress(i, len(items), rel)
    walked, report.skipped_dirs = _walk_all(root)
    report.extra = [r for r in walked if r not in manifest.files]
    return report


def disk_report(root: Path, *, budget_gb: float = DEFAULT_BUDGET_GB) -> dict[str, object]:
    """Bytes used under ``root`` (total and per top-level folder), free space on the volume, and budget use."""
    root = _check_root(root)
    by_top: dict[str, int] = {}
    total = 0
    walked, skipped = _walk_all(root)
    for rel in walked + ([MANIFEST_NAME] if (root / MANIFEST_NAME).is_file() else []):
        size = (root / rel).stat().st_size
        total += size
        top = rel.split("/", 1)[0] if "/" in rel else "."
        by_top[top] = by_top.get(top, 0) + size
    budget_bytes = budget_gb * 1024**3
    return {
        "root": str(root),
        "total_bytes": total,
        "by_top_level": dict(sorted(by_top.items())),
        "skipped_symlinked_dirs": skipped,
        "free_bytes": shutil.disk_usage(root).free,
        "budget_gb": budget_gb,
        "budget_used_fraction": total / budget_bytes if budget_bytes else 0.0,
    }


def _human(n: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def format_disk_report(rep: dict[str, object]) -> str:
    by_top = rep["by_top_level"]
    assert isinstance(by_top, dict)
    lines = [f"Disk use under {rep['root']}"]
    lines += [f"  {name:<20}{_human(size):>12}" for name, size in by_top.items()]
    skipped = rep.get("skipped_symlinked_dirs") or []
    assert isinstance(skipped, list)
    for name in skipped:
        lines.append(f"  skipped symlinked directory (not followed, not counted): {name}")
    lines.append(f"  {'TOTAL':<20}{_human(float(str(rep['total_bytes']))):>12}")
    pct = float(str(rep["budget_used_fraction"])) * 100
    lines.append(
        f"  budget {rep['budget_gb']} GB: {pct:.2f}% used; volume free {_human(float(str(rep['free_bytes'])))}"
    )
    return "\n".join(lines)


def _print_progress(i: int, total: int, rel: str) -> None:
    if i == total or i % 25 == 0 or total <= 25:
        print(f"[{i}/{total}] {rel}", flush=True)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Build, verify or report on a data-root manifest. Never downloads or deletes."
    )
    ap.add_argument("command", choices=["build", "verify", "report"])
    ap.add_argument("--data-root", default=None, help="data root (else env MICROGRID_DATA_DIR)")
    ap.add_argument("--budget-gb", type=float, default=DEFAULT_BUDGET_GB)
    args = ap.parse_args(argv)
    try:
        root = resolve_data_root(args.data_root)
        mpath = root / MANIFEST_NAME
        if args.command == "build":
            previous = read_manifest(mpath) if mpath.exists() else None
            m = build_manifest(root, previous=previous, progress=_print_progress)
            write_manifest(m, mpath, root=root)
            print(f"wrote {mpath} ({len(m.files)} files)")
            for d in m.skipped_dirs:
                print(f"skipped symlinked directory (not followed): {d}")
            rc = 0
        elif args.command == "verify":
            if not mpath.exists():
                raise CatalogError(f"no manifest at {mpath}; run 'build' first")
            rep = verify_manifest(root, read_manifest(mpath), progress=_print_progress)
            for label, names in (("missing", rep.missing), ("modified", rep.modified), ("not in manifest", rep.extra)):
                for n in names:
                    print(f"{label}: {n}")
            print("OK" if rep.ok else "MISMATCH")
            rc = 0 if rep.ok else 1
        else:
            rc = 0
        print(format_disk_report(disk_report(root, budget_gb=args.budget_gb)))
        return rc
    except CatalogError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except (OSError, RuntimeError) as exc:  # unreadable file, permission problem, symlink loop: never a traceback
        where = f" ({getattr(exc, 'filename', None)})" if getattr(exc, "filename", None) else ""
        print(f"error: filesystem problem{where}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
