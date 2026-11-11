"""Dataset ids to paths (FR-DATA-8, engine side).

A scenario config may name its source datasets by id (``data.dataset_resstock`` / ``data.dataset_comstock``,
for example ``resstock:2025_release_1:amy2018:baseline:DC``). The engine only carries those strings;
``resolve_dataset_paths`` looks them up in the dataset catalog and fills the path fields the engine already
uses, plus the load basis (gross demand or net of on-site generation) that is recorded with the run.

Layout assumed (the project's data folder): ``<data_root>/background/`` holds the characteristics files and
each catalog entry's ``relative_path`` ends in ``upgrade=<partition_id>/state=<state>`` under a
``timeseries_individual_buildings`` directory. Characteristics file *names* stay as configured in
``data.resstock_chars_file`` / ``data.comstock_chars_file``; they must exist under ``background/``.
"""

from __future__ import annotations

import copy
from pathlib import Path

from microgrid.community_load.synthetic.config import ScenarioConfig
from microgrid.datasets import Catalog, CatalogEntry, DatasetId

_PRODUCTS = ("resstock", "comstock")


def parse_dataset_id(text: str) -> DatasetId:
    """``product:release:weather:scenario:state`` (or the ``/``-separated ``str(DatasetId)`` form)."""
    sep = ":" if ":" in text else "/"
    parts = [p.strip() for p in text.split(sep)]
    if len(parts) != 5 or not all(parts):
        raise ValueError(f"dataset id must be 'product:release:weather:scenario:state', got {text!r}")
    return DatasetId(*parts)


def _entry_for(product: str, text: str, catalog: Catalog, require_verified: bool) -> CatalogEntry:
    ds_id = parse_dataset_id(text)
    if ds_id.product != product:
        raise ValueError(f"data.dataset_{product} names a {ds_id.product} dataset: {text!r}")
    entry = catalog.get(ds_id)  # CatalogError (a ValueError) naming the id and the known ones
    if require_verified and not entry.verified:
        raise ValueError(
            f"dataset {ds_id} is not verified in the catalog (release/layout not yet confirmed against the real "
            "files); verify it or resolve without require_verified"
        )
    return entry


def resolve_dataset_paths(
    cfg: ScenarioConfig,
    catalog: Catalog,
    data_root: Path | str,
    *,
    require_verified: bool = False,
    allow_mixed_load_basis: bool = False,
) -> ScenarioConfig:
    """A copy of ``cfg`` whose data paths and load basis come from the catalog entries it names by id.

    Fails loudly (ValueError / CatalogError) for: no ids at all, unknown ids, a product in the wrong field,
    unverified entries when ``require_verified``, a partition path that does not end in
    ``upgrade=<partition>/state=<state>``, resstock and comstock needing different partitions or states
    (the config has one of each), gross/net mixing unless ``allow_mixed_load_basis``, and any missing
    directory or characteristics file. A product without an id keeps its configured paths.
    """
    root = Path(data_root)
    ids = {p: getattr(cfg.data, f"dataset_{p}") for p in _PRODUCTS}
    named = {p: v for p, v in ids.items() if v is not None}
    if not named:
        raise ValueError("no dataset ids to resolve: set data.dataset_resstock and/or data.dataset_comstock")

    entries = {p: _entry_for(p, v, catalog, require_verified) for p, v in named.items()}
    dirs: dict[str, Path] = {}
    for p, e in entries.items():
        part = e.resolve(root)
        if part.parent.name != f"upgrade={e.partition_id}" or part.name != f"state={e.state}":
            raise ValueError(
                f"dataset {e.dataset_id}: relative_path {e.relative_path!r} must end in "
                f"upgrade=<partition>/state=<state> (upgrade={e.partition_id}/state={e.state})"
            )
        dirs[p] = part.parent.parent

    if len({e.partition_id for e in entries.values()}) > 1:
        raise ValueError(
            "resstock and comstock datasets use different partitions "
            f"({', '.join(f'{p}: upgrade={e.partition_id}' for p, e in entries.items())}); "
            "one scenario config has a single upgrade value"
        )
    if len({e.state for e in entries.values()}) > 1:
        raise ValueError("resstock and comstock datasets are for different states")
    bases = {p: e.load_basis for p, e in entries.items()}
    if len(set(bases.values())) > 1 and not allow_mixed_load_basis:
        raise ValueError(
            f"load basis differs between datasets ({bases}); mixing gross and net loads silently would be wrong. "
            "Pass allow_mixed_load_basis=True to do it on purpose"
        )

    background = root / "background"
    if not background.is_dir():
        raise ValueError(f"background directory {background} does not exist")
    for p, e in entries.items():
        part = e.resolve(root)
        if not part.is_dir():
            raise ValueError(f"dataset {e.dataset_id}: directory {part} does not exist")
        chars = background / getattr(cfg.data, f"{p}_chars_file")
        if not chars.is_file():
            raise ValueError(f"dataset {e.dataset_id}: characteristics file {chars} does not exist")

    out = copy.deepcopy(cfg)
    first = next(iter(entries.values()))
    out.data.upgrade = first.partition_id
    out.data.state = first.state
    out.data.background_dir = str(background)
    for p, e in entries.items():
        setattr(out.data, f"{p}_timeseries_dir", str(dirs[p]))
        setattr(out.data, f"{p}_load_basis", e.load_basis)
    return out
