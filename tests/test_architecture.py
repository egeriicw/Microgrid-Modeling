"""Layering contract: dependencies point downward only.

    study
    economics -> dispatch -> der          (each also uses geo + core)
    community_load  (categorize, sources -> synthetic; uses geo + core)
    geo -> core                           (geography is the shared reference layer)

`geo` and `community_load` are parallel siblings: load profiling reads geography,
never the reverse. The SNLG engine (`synthetic`) knows nothing about geography.
Planner layers exchange load via core.LoadResult, so they never import community_load.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

SRC = Path(__file__).parent.parent / "src" / "microgrid"

_ALL = ["core", "geo", "community_load", "der", "dispatch", "economics", "study"]


def _ban(*allowed: str) -> list[str]:
    return [f"microgrid.{m}" for m in _ALL if m not in allowed]


# package prefix -> prefixes it must NOT import (longest prefix wins)
FORBIDDEN = {
    "microgrid.core": _ban("core"),
    "microgrid.geo": _ban("core", "geo"),
    "microgrid.community_load": _ban("core", "geo", "community_load"),
    "microgrid.community_load.synthetic": _ban("community_load") + [
        "microgrid.community_load.sources", "microgrid.community_load.categorize"],
    "microgrid.der": _ban("core", "geo", "der"),
    "microgrid.dispatch": _ban("core", "geo", "der", "dispatch"),
    "microgrid.economics": _ban("core", "geo", "der", "dispatch", "economics"),
}


def _module_name(path: Path) -> str:
    rel = path.relative_to(SRC.parent).with_suffix("")
    parts = list(rel.parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _imports(path: Path) -> set[str]:
    found: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            found.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module and node.level == 0:
            found.add(node.module)
    return found


def _under(name: str, prefix: str) -> bool:
    return name == prefix or name.startswith(prefix + ".")


@pytest.mark.parametrize("path", sorted(SRC.rglob("*.py")), ids=lambda p: _module_name(p))
def test_imports_respect_layers(path):
    mod = _module_name(path)
    banned = next((b for pkg, b in sorted(FORBIDDEN.items(), key=lambda kv: -len(kv[0]))
                   if _under(mod, pkg)), [])
    bad = sorted(i for i in _imports(path) if any(_under(i, b) for b in banned))
    assert not bad, f"{mod} must not import {bad}"
