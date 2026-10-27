"""Layering contract: dependencies point downward only.

Planner (der, dispatch, economics, study) -> community_load -> core.
Inside community_load: sources -> geo -> core, and synthetic is the engine that
sources adapts; geo and synthetic never import each other or anything above them.
"""
from __future__ import annotations

import ast
from pathlib import Path

import pytest

SRC = Path(__file__).parent.parent / "src" / "microgrid"

# package prefix -> prefixes it must NOT import
FORBIDDEN = {
    "microgrid.core": ["microgrid.community_load", "microgrid.der", "microgrid.dispatch",
                       "microgrid.economics", "microgrid.study"],
    "microgrid.community_load.synthetic": ["microgrid.community_load.geo",
                                            "microgrid.community_load.sources", "microgrid.der",
                                            "microgrid.dispatch", "microgrid.economics",
                                            "microgrid.study", "microgrid.core"],
    "microgrid.community_load.geo": ["microgrid.community_load.synthetic",
                                     "microgrid.community_load.sources", "microgrid.der",
                                     "microgrid.dispatch", "microgrid.economics",
                                     "microgrid.study"],
    "microgrid.community_load.sources": ["microgrid.der", "microgrid.dispatch",
                                         "microgrid.economics", "microgrid.study"],
    "microgrid.der": ["microgrid.study", "microgrid.dispatch", "microgrid.economics"],
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
