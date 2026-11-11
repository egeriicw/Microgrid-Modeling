"""YAML loading that refuses duplicate mapping keys.

PyYAML silently keeps the last of two equal keys, which can hide a typo or a bad merge in a config. Every YAML
file the load engine reads (scenario configs, the building-type registry, the exposed-attributes list) goes
through ``load_yaml_strict`` so a repeated key is an error naming the key and its line.
"""

from __future__ import annotations

from typing import Any

import yaml


class _StrictLoader(yaml.SafeLoader):
    pass


def _construct_mapping(loader: _StrictLoader, node: yaml.MappingNode, deep: bool = False) -> dict[Any, Any]:
    seen: dict[Any, int] = {}
    for key_node, _ in node.value:
        key = loader.construct_object(key_node, deep=True)
        try:
            hash(key)
        except TypeError:
            continue  # the default constructor reports unhashable keys
        if key in seen:
            raise ValueError(
                f"duplicate key {key!r} at line {key_node.start_mark.line + 1} "
                f"(first used at line {seen[key]}); remove one of them"
            )
        seen[key] = key_node.start_mark.line + 1
    return yaml.SafeLoader.construct_mapping(loader, node, deep=deep)


_StrictLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_mapping)


def load_yaml_strict(text: str) -> Any:
    """``yaml.safe_load`` that raises ValueError on a duplicate mapping key."""
    return yaml.load(text, Loader=_StrictLoader)  # noqa: S506  (SafeLoader subclass)
