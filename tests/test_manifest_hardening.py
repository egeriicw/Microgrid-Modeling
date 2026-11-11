"""Hardening tests for the data-root manifest (post-QA 1.6): temp-file safety, clean errors, symlinked directories."""

from __future__ import annotations

import json
import os

import pytest

from microgrid.datasets import manifest as mf


def test_stale_temp_files_are_not_hashed_and_not_removed(tmp_path):
    (tmp_path / "a.txt").write_text("data")
    (tmp_path / "manifest.json.tmp").write_text("stale")
    (tmp_path / ".manifest.json.abc123.tmp").write_text("stale2")
    assert mf.main(["build", "--data-root", str(tmp_path)]) == 0
    doc = json.loads((tmp_path / "manifest.json").read_text())
    assert [f["path"] for f in doc["files"]] == ["a.txt"]
    assert (tmp_path / "manifest.json.tmp").read_text() == "stale"
    assert (tmp_path / ".manifest.json.abc123.tmp").read_text() == "stale2"
    assert mf.main(["verify", "--data-root", str(tmp_path)]) == 0


def test_dangling_tmp_symlink_target_is_not_created(tmp_path):
    (tmp_path / "manifest.json.tmp").symlink_to(tmp_path / "created_by_follow.txt")
    assert mf.main(["build", "--data-root", str(tmp_path)]) == 0
    assert not (tmp_path / "created_by_follow.txt").exists()


def test_manifest_target_symlink_is_replaced_not_followed(tmp_path):
    victim = tmp_path / "a.txt"
    victim.write_text("precious")
    (tmp_path / "manifest.json").symlink_to(victim)
    mf.write_manifest(mf.Manifest(), tmp_path / "manifest.json", root=tmp_path)
    assert victim.read_text() == "precious"


def test_error_messages_name_the_path(tmp_path, capsys):
    (tmp_path / "broken").symlink_to(tmp_path / "nope")
    assert mf.main(["build", "--data-root", str(tmp_path)]) == 2
    assert "broken" in capsys.readouterr().err


def test_malformed_entries_name_the_path(tmp_path, capsys):
    (tmp_path / "manifest.json").write_text(json.dumps({"version": 1, "files": [{"path": "a.txt", "size": "x"}]}))
    assert mf.main(["verify", "--data-root", str(tmp_path)]) == 2
    assert "manifest.json" in capsys.readouterr().err
    (tmp_path / "manifest.json").write_text(json.dumps({"version": 1, "files": ["notamapping"]}))
    assert mf.main(["verify", "--data-root", str(tmp_path)]) == 2


@pytest.mark.skipif(os.geteuid() == 0, reason="root ignores file permissions")
def test_unreadable_file_is_a_clean_error(tmp_path, capsys):
    f = tmp_path / "secret.bin"
    f.write_bytes(b"x")
    f.chmod(0)
    try:
        assert mf.main(["build", "--data-root", str(tmp_path)]) == 2
        assert "secret.bin" in capsys.readouterr().err
    finally:
        f.chmod(0o600)


def test_symlinked_directories_are_skipped_reported_and_never_followed(tmp_path, capsys):
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "o.bin").write_bytes(b"o")
    root = tmp_path / "root"
    root.mkdir()
    (root / "a.txt").write_text("a")
    (root / "linked").symlink_to(outside, target_is_directory=True)
    m = mf.build_manifest(root)
    assert sorted(m.files) == ["a.txt"]
    assert m.skipped_dirs == ("linked",)
    assert mf.main(["build", "--data-root", str(root)]) == 0
    out = capsys.readouterr().out
    assert "skipped" in out and "linked" in out
    assert mf.main(["report", "--data-root", str(root)]) == 0
    out = capsys.readouterr().out
    assert "skipped" in out and "linked" in out
    assert mf.main(["verify", "--data-root", str(root)]) == 0
    assert "linked" in capsys.readouterr().out
