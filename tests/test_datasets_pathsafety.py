"""Containment by filesystem identity: case, Unicode and symlink spellings of the same folder are the same folder."""

from __future__ import annotations

import os
import unicodedata
from pathlib import Path

import pytest

from microgrid.datasets.pathsafety import is_inside, same_file


class FakeFs:
    """Simulates a case- and Unicode-normalising volume (macOS APFS default): names are folded before lookup."""

    def __init__(self, real_paths: list[Path]) -> None:
        self.inodes: dict[str, int] = {self.fold(p): i + 1 for i, p in enumerate(real_paths)}

    @staticmethod
    def fold(p: str | os.PathLike[str]) -> str:
        return unicodedata.normalize("NFC", os.fspath(p)).casefold()

    def stat(self, p: str | os.PathLike[str]) -> os.stat_result:
        ino = self.inodes.get(self.fold(p))
        if ino is None:
            raise FileNotFoundError(p)
        return os.stat_result((0o40755, ino, 7, 1, 0, 0, 0, 0, 0, 0))


def test_real_filesystem_basic(tmp_path):
    root = tmp_path / "data"
    (root / "sub").mkdir(parents=True)
    assert is_inside(root, root / "sub" / "new.md")
    assert is_inside(root, root)
    assert is_inside(root, root / "sub" / ".." / "x.md")
    assert not is_inside(root, tmp_path / "other.md")
    assert not is_inside(root, tmp_path / "data2" / "x.md")  # prefix of the name is not containment
    assert not is_inside(root, tmp_path / "x" / ".." / "y.md")


def test_real_filesystem_symlinks(tmp_path):
    root = tmp_path / "data"
    root.mkdir()
    (tmp_path / "ln").symlink_to(root, target_is_directory=True)
    assert is_inside(root, tmp_path / "ln" / "r.md")
    assert is_inside(tmp_path / "ln", root / "r.md")  # root given through a link
    (tmp_path / "dangling.md").symlink_to(root / "planted.md")
    assert is_inside(root, tmp_path / "dangling.md")


@pytest.mark.parametrize(
    "spelling",
    ["DATA/Report.MD", "Data/sub/deep/Report.MD", "dAtA/Report.MD"],
)
def test_case_different_spelling_is_inside_under_simulated_case_insensitive_volume(tmp_path, spelling):
    root = tmp_path / "data"
    fs = FakeFs([tmp_path, root, root / "sub"])
    out = tmp_path / spelling
    assert is_inside(root, out, stat=fs.stat)
    assert not is_inside(root, tmp_path / "other" / "Report.MD", stat=fs.stat)


def test_unicode_normalisation_difference_is_inside_under_simulation(tmp_path):
    nfc = unicodedata.normalize("NFC", "café")
    nfd = unicodedata.normalize("NFD", "café")
    assert nfc != nfd
    root = tmp_path / nfc
    fs = FakeFs([tmp_path, root])
    assert is_inside(root, tmp_path / nfd / "r.md", stat=fs.stat)
    assert not is_inside(root, tmp_path / (nfd + "2") / "r.md", stat=fs.stat)


def test_deepest_existing_ancestor_decides_for_missing_output(tmp_path):
    root = tmp_path / "data"
    fs = FakeFs([tmp_path, root])
    # neither 'Missing' nor 'new.md' exists; the deepest existing ancestor 'DATA' is the root
    assert is_inside(root, tmp_path / "DATA" / "Missing" / "new.md", stat=fs.stat)


def test_zero_inode_is_not_trusted_as_identity(tmp_path):
    """Some network volumes report st_ino 0; two unrelated folders must not collapse into one identity."""
    root = tmp_path / "a"
    root.mkdir()
    other = tmp_path / "b"
    other.mkdir()

    def stat0(p):
        st = os.stat(p)
        return os.stat_result((st.st_mode, 0, st.st_dev, 1, 0, 0, 0, 0, 0, 0))

    assert not is_inside(root, other / "x.md", stat=stat0)


def test_same_file(tmp_path):
    a = tmp_path / "a.yaml"
    a.write_text("x")
    (tmp_path / "l.yaml").symlink_to(a)
    assert same_file(a, tmp_path / "l.yaml")
    assert not same_file(a, tmp_path / "nope.yaml")
    fs = FakeFs([tmp_path, a])
    assert same_file(a, tmp_path / "A.YAML", stat=fs.stat)
