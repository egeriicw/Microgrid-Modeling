"""Layering contract: dependencies point downward only.

    study
    economics -> dispatch -> der          (each also uses geo + core)
    community_load  (categorize, sources -> synthetic; uses geo + core)
    community_load.weather  (weather normalization interface, FR-ENG-13: pure numerics, imports only core)
    geo -> core                           (geography is the shared reference layer)
    datasets  (dataset identity, catalog, manifest; stdlib + pandas + PyYAML only, imports no other microgrid package)

`geo` and `community_load` are parallel siblings: load profiling reads geography,
never the reverse. The SNLG engine (`synthetic`) knows nothing about geography.
Planner layers exchange load via core.LoadResult, so they never import community_load.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

SRC = Path(__file__).parent.parent / "src" / "microgrid"

_ALL = ["core", "geo", "community_load", "der", "dispatch", "economics", "study", "datasets"]


def _ban(*allowed: str) -> list[str]:
    return [f"microgrid.{m}" for m in _ALL if m not in allowed]


# package prefix -> prefixes it must NOT import (longest prefix wins)
FORBIDDEN = {
    "microgrid.core": _ban("core"),
    "microgrid.geo": _ban("core", "geo"),
    "microgrid.datasets": _ban("datasets"),
    "microgrid.community_load": _ban("core", "geo", "community_load", "datasets"),
    "microgrid.community_load.synthetic": _ban("community_load")
    + [
        "microgrid.community_load.sources",
        "microgrid.community_load.categorize",
        "microgrid.community_load.dataset_resolution",
        "microgrid.community_load.permissions",
        "microgrid.community_load.exposed_attributes",
    ],
    # Pure numerics so the engine, measured-data sources and calibration can all use it later without cycles.
    "microgrid.community_load.weather": _ban("core"),
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


def _package_parts(path: Path) -> list[str]:
    """The dotted package a file belongs to (src/ files as ``microgrid...``, others relative to the repo root)."""
    for root in (SRC.parent, SRC.parent.parent):
        if path.resolve().is_relative_to(root.resolve()):
            parts = list(path.resolve().relative_to(root.resolve()).with_suffix("").parts)
            return parts[:-1]  # a module's package is its directory; for __init__ that is the package itself
    return []


def _imports(path: Path) -> set[str]:
    found: set[str] = set()
    package = _package_parts(path)
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            found.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0:
                base = node.module or ""
            else:
                # `from .x import y` / `from .. import y`: resolve against the module's own package
                anchor = package[: len(package) - (node.level - 1)] if node.level - 1 <= len(package) else []
                base = ".".join(anchor + ([node.module] if node.module else []))
            if not base:
                continue
            found.add(base)
            # `from pkg import submodule` imports pkg.submodule too
            found.update(f"{base}.{a.name}" for a in node.names)
    return found


def _under(name: str, prefix: str) -> bool:
    return name == prefix or name.startswith(prefix + ".")


def test_relative_imports_resolve_against_the_module_package():
    assert _package_parts(SRC / "community_load" / "weather.py") == ["microgrid", "community_load"]
    assert _package_parts(SRC / "community_load" / "__init__.py") == ["microgrid", "community_load"]


@pytest.mark.parametrize("path", sorted(SRC.rglob("*.py")), ids=lambda p: _module_name(p))
def test_imports_respect_layers(path):
    mod = _module_name(path)
    banned = next((b for pkg, b in sorted(FORBIDDEN.items(), key=lambda kv: -len(kv[0])) if _under(mod, pkg)), [])
    bad = sorted(i for i in _imports(path) if any(_under(i, b) for b in banned))
    assert not bad, f"{mod} must not import {bad}"
