"""Containment and identity checks by FILESYSTEM IDENTITY, not by spelling (FR-DATA-1).

Why: comparing path strings (``Path.resolve().relative_to(root)``) does not know that on macOS (APFS/HFS+, case
insensitive by default) ``/Users/me/DATA/x.md`` and ``/Users/me/data/x.md`` are the same place, nor that the volume also
folds Unicode normalisation forms (NFC/NFD). A report written through such a different spelling would land INSIDE the
data folder.

Rule used here: resolve the path (symlinks and ``..`` removed; a not-yet-existing tail is kept), then walk up its
ancestors. The path is inside the root if any ancestor that EXISTS has the same ``(st_dev, st_ino)`` as the root. A file
that does not exist yet is therefore judged by its deepest existing ancestor. The operating system itself decides
whether two spellings are one folder, so this follows the host's real rules with no case table of our own.

Guaranteed: case, NFC/NFD, ``..``, symlinks (also dangling ones) and relative spellings, on any local volume that gives
files stable, distinct inode numbers (APFS, HFS+, ext4, etc.).

NOT guaranteed: (1) volumes whose inode numbers are 0 or unstable (some network shares: SMB/NFS/FUSE). An ancestor
reporting inode 0 is not trusted as an identity; for it the check falls back to comparing the resolved path strings,
which is exactly the old, case-sensitive behaviour. (2) A different spelling of a folder that does not exist yet below
the deepest existing ancestor (a new folder cannot be the data folder). (3) A race in which someone moves folders while
the tool runs. When in doubt, put reports in your home folder.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from pathlib import Path

StatFn = Callable[[str | os.PathLike[str]], os.stat_result]

_Identity = tuple[int, int]


def _identity(path: str | os.PathLike[str], stat: StatFn) -> _Identity | None:
    """(device, inode) of an existing path, or None if it does not exist or has no trustworthy inode."""
    try:
        st = stat(path)
    except (FileNotFoundError, NotADirectoryError):
        return None
    if st.st_ino == 0:
        return None
    return (st.st_dev, st.st_ino)


def is_inside(root: Path, path: Path, *, stat: StatFn = os.stat) -> bool:
    """True if ``path`` (existing or not) is the folder ``root`` or anywhere below it, by filesystem identity."""
    root_real = Path(os.path.realpath(root))
    root_id = _identity(root_real, stat)
    cur = Path(os.path.realpath(path))
    while True:
        cur_id = _identity(cur, stat)
        if root_id is not None and cur_id is not None:
            if cur_id == root_id:
                return True
        elif cur == root_real:  # no trustworthy identity: fall back to the resolved spelling
            return True
        if cur.parent == cur:
            return False
        cur = cur.parent


def same_file(a: Path, b: Path, *, stat: StatFn = os.stat) -> bool:
    """True if ``a`` and ``b`` are the same existing file (or folder), through any spelling or link."""
    ia, ib = _identity(os.path.realpath(a), stat), _identity(os.path.realpath(b), stat)
    if ia is not None and ib is not None:
        return ia == ib
    return os.path.realpath(a) == os.path.realpath(b)
