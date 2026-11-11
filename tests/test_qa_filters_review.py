"""Independent QA review of Sprint 1 stories 1.6b (FR-ENG-11) and 1.11b (FR-DATA-8, engine side).

Tests marked ``xfail(strict=True)`` document real gaps found in review; they flip to failures the day the gap
is fixed, which is the signal to remove the marker. Nothing here changes production behavior.
"""

from __future__ import annotations

import copy
import dataclasses
import json
import os

import numpy as np
import pandas as pd
import pytest

from microgrid.community_load.dataset_resolution import resolve_dataset_paths
from microgrid.community_load.exposed_attributes import (
    ExposedAttributes,
    check_against_data,
    default_exposed_attributes,
)
from microgrid.community_load.permissions import (
    FLAGS,
    ROLES,
    FilterPermissionError,
    can,
    validate_filter_edit,
    validate_filters_allowed,
)
from microgrid.community_load.synthetic import run
from microgrid.community_load.synthetic.attribute_filters import AttributeFilter as F
from microgrid.community_load.synthetic.attribute_filters import apply_attribute_filters
from microgrid.community_load.synthetic.config import config_from_dict
from microgrid.community_load.synthetic.pool_preview import preview_pool
from tests.test_dataset_resolution import COM, RES, _catalog, _cfg, _entry
from tests.test_run_facade import build_tiny_config

# ---------------------------------------------------------------------------------------------------------
# (1) No numbers change: empty / [] / keep-everything filters give the identical full result
# ---------------------------------------------------------------------------------------------------------


def _ser(o: object) -> object:
    if dataclasses.is_dataclass(o) and not isinstance(o, type):
        return {f.name: _ser(getattr(o, f.name)) for f in dataclasses.fields(o)}
    if isinstance(o, pd.DataFrame | pd.Series):
        return o.to_json(orient="split", double_precision=15, date_format="iso")
    if isinstance(o, np.ndarray):
        return o.tolist()
    if isinstance(o, dict):
        return {str(k): _ser(v) for k, v in o.items()}
    if isinstance(o, list | tuple):
        return [_ser(v) for v in o]
    if isinstance(o, np.floating | np.integer):
        return o.item()
    return o if isinstance(o, str | int | float | bool | type(None)) else repr(o)


def _full_result(cfg: dict) -> str:
    r = run.run_scenario(cfg)
    d = _ser(r)
    assert isinstance(d, dict)
    d.pop("pool_summary")  # new record fields, not numbers
    d.pop("datasets")
    d.pop("config_snapshot")  # carries the new attributes key; seeds/metrics/ensemble are compared in full
    return json.dumps(d, sort_keys=True)


def _numbers_cfg(ds, attrs) -> dict:
    cfg = build_tiny_config(ds)
    cfg["ensemble"]["max_runs"] = 6
    cfg["perturbation"] = {"enabled": True}
    if attrs is not None:
        for c in cfg["archetype_filters"]["resstock"].values():
            c["attributes"] = copy.deepcopy(attrs)
    return cfg


def test_keep_everything_filters_leave_every_number_identical(synthetic_dataset):
    base = _full_result(_numbers_cfg(synthetic_dataset, None))
    assert _full_result(_numbers_cfg(synthetic_dataset, [])) == base
    keep_all = [
        {"column": "bldg_id", "op": "not_null"},
        {"column": "in.geometry_stories", "op": "range", "min": -1e9, "max": 1e9},
    ]
    assert _full_result(_numbers_cfg(synthetic_dataset, keep_all)) == base


def test_a_narrowing_filter_does_shrink_the_pool(synthetic_dataset):
    """Guards the guard: filters are really applied on this path (the identity test above is not vacuous)."""
    from microgrid.community_load.synthetic import archetype

    cfg = config_from_dict(
        _numbers_cfg(synthetic_dataset, [{"column": "in.geometry_stories", "op": "range", "min": 99}])
    )
    df = archetype.load_resstock_characteristics(synthetic_dataset.resstock_chars)
    assert archetype._filter_resstock_category(df, cfg.archetype_filters.resstock["mf_small"]) == []


# ---------------------------------------------------------------------------------------------------------
# (2) Filter correctness against pandas edge cases
# ---------------------------------------------------------------------------------------------------------


@pytest.fixture
def edge() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "num": [1, 2, 3, np.nan, 5.0],
            "txt": ["A", "a", " A", "", None],
            "b_str": ["True", "false", " TRUE ", None, "False"],
            "b_obj": [True, False, None, True, False],
            "cat": pd.Categorical(["x", "y", "x", None, "y"]),
            "nint": pd.array([1, None, 3, 4, 5], dtype="Int64"),
            "dot name": [1, 2, 3, 4, 5],
        }
    )


def _m(df: pd.DataFrame, *fs: F) -> list[bool]:
    return [bool(v) for v in apply_attribute_filters(df, list(fs))]


def test_nan_and_none_never_match_equals_range_or_boolean_filters(edge):
    assert _m(edge, F("num", "equals", value=2)) == [False, True, False, False, False]
    assert _m(edge, F("num", "range", min=0, max=10))[3] is False
    assert _m(edge, F("num", "range", min=0))[3] is False
    assert _m(edge, F("num", "range", max=10))[3] is False
    assert _m(edge, F("txt", "equals", value=""))[4] is False
    assert _m(edge, F("b_str", "is_true")) == [True, False, True, False, False]
    assert _m(edge, F("b_str", "is_false")) == [False, True, False, False, True]
    assert _m(edge, F("b_obj", "is_true"))[2] is False
    assert _m(edge, F("b_obj", "is_false"))[2] is False
    assert _m(edge, F("nint", "range", min=3)) == [False, False, True, True, True]
    assert _m(edge, F("cat", "in", values=("x",))) == [True, False, True, False, False]


def test_range_bounds_are_exact_at_equality(edge):
    assert _m(edge, F("num", "range", min=2, max=3)) == [False, True, True, False, False]
    assert _m(edge, F("num", "range", min=2, max=3, min_inclusive=False)) == [False, False, True, False, False]
    assert _m(edge, F("num", "range", min=2, max=3, max_inclusive=False)) == [False, True, False, False, False]
    assert _m(edge, F("num", "range", min=2, max=2)) == [False, True, False, False, False]
    assert _m(edge, F("num", "range", min=2, max=2, min_inclusive=False)) == [False] * 5


def test_values_are_case_and_whitespace_sensitive(edge):
    assert _m(edge, F("txt", "equals", value="A")) == [True, False, False, False, False]
    assert _m(edge, F("txt", "in", values=("a",))) == [False, True, False, False, False]


def test_text_numbers_and_unknown_boolean_spellings_fail_loudly_not_silently(edge):
    text_nums = pd.DataFrame({"t": ["1", "2", None], "yn": ["yes", "no", None], "z": [1, 0, 1]})
    with pytest.raises(ValueError, match="not numeric"):
        apply_attribute_filters(text_nums, [F("t", "range", min=1)])
    with pytest.raises(ValueError, match="text"):
        apply_attribute_filters(text_nums, [F("t", "equals", value=1)])
    with pytest.raises(ValueError, match="not boolean"):
        apply_attribute_filters(text_nums, [F("yn", "is_true")])
    with pytest.raises(ValueError, match="numeric"):
        apply_attribute_filters(text_nums, [F("z", "is_true")])


def test_column_with_dots_and_spaces_and_missing_column(edge):
    assert _m(edge, F("dot name", "range", min=2, max=3)) == [False, True, True, False, False]
    with pytest.raises(ValueError, match="similar columns"):
        apply_attribute_filters(edge, [F(" num", "not_null")])


def test_and_combination_is_a_real_and_with_a_non_default_index(edge):
    df = edge.copy()
    df.index = [10, 11, 12, 13, 14]
    got = apply_attribute_filters(df, [F("num", "range", min=2), F("b_str", "is_true")])
    assert got.tolist() == [False, False, True, False, False]
    assert got.index.tolist() == [10, 11, 12, 13, 14]


def test_filter_only_in_commercial_table_fails_on_residential_naming_column(synthetic_dataset):
    from microgrid.community_load.synthetic import archetype

    res = archetype.load_resstock_characteristics(synthetic_dataset.resstock_chars)
    com = archetype.load_comstock_characteristics(synthetic_dataset.comstock_chars)
    only_com = sorted(set(com.columns) - set(res.columns))
    assert only_com, "fixture should have a commercial-only column"
    col = only_com[0]
    cfg = archetype.ResidentialFilterConfig(
        building_type_heights=["Multi-Family with 2 - 4 Units"], attributes=[F(col, "not_null")]
    )
    with pytest.raises(ValueError, match=col.replace(".", r"\.")):
        archetype._filter_resstock_category(res, cfg)


def test_and_with_require_data_complete_and_building_type(synthetic_dataset):
    from microgrid.community_load.synthetic import archetype

    res = archetype.load_resstock_characteristics(synthetic_dataset.resstock_chars)
    types = ["Multi-Family with 2 - 4 Units"]
    base = archetype._filter_resstock_category(res, archetype.ResidentialFilterConfig(building_type_heights=types))
    keep = archetype.ResidentialFilterConfig(building_type_heights=types, attributes=[F("bldg_id", "not_null")])
    assert archetype._filter_resstock_category(res, keep) == base
    none = archetype.ResidentialFilterConfig(building_type_heights=types, attributes=[F("bldg_id", "is_null")])
    assert archetype._filter_resstock_category(res, none) == []
    # attributes can never widen past require_data_complete=True
    loose = archetype.ResidentialFilterConfig(
        building_type_heights=types, require_data_complete=False, attributes=[F("bldg_id", "not_null")]
    )
    assert set(base) <= set(archetype._filter_resstock_category(res, loose))


def test_zero_match_filter_fails_the_run_clearly_and_the_warning_comes_first(synthetic_dataset):
    cfg = build_tiny_config(synthetic_dataset)
    cfg["archetype_filters"]["resstock"]["mf_small"]["attributes"] = [
        {"column": "in.geometry_stories", "op": "range", "min": 999}
    ]
    events: list[run.ProgressEvent] = []
    with pytest.raises(ValueError, match="mf_small.*no buildings"):
        run.run_scenario(cfg, progress=events.append)
    assert events == [] or "no buildings match" in events[0].message


@pytest.mark.parametrize("matched, min_pool, warns", [(3, 3, False), (3, 4, True), (3, 2, False), (3, 0, False)])
def test_pool_warning_threshold_is_strictly_fewer_than(synthetic_dataset, matched, min_pool, warns):
    from microgrid.community_load.synthetic import archetype

    df = archetype.load_resstock_characteristics(synthetic_dataset.resstock_chars)
    base = archetype.ResidentialFilterConfig(building_type_heights=["Multi-Family with 5+ Units, 4-7 Stories"])
    n = len(archetype._filter_resstock_category(df, base))
    assert n == 3, "fixture changed: adjust this test"
    got = preview_pool(df, base, min_pool=min_pool)
    assert got.matched == matched
    assert bool(got.warnings) is warns


# --- silent or cryptic behaviours found in review (strict xfail: fix flips them) --------------------------


def test_in_nan_is_rejected_so_it_cannot_match_missing_values():
    for bad in (float("nan"), float("inf")):
        with pytest.raises(ValueError, match="'num'.*finite"):
            F("num", "in", values=(1, bad))
        with pytest.raises(ValueError, match="'num'.*finite"):
            F("num", "equals", value=bad)


def test_non_finite_bounds_are_rejected_at_construction():
    for bad in (float("nan"), float("inf"), float("-inf")):
        for key in ("min", "max"):
            with pytest.raises(ValueError, match=f"'num'.*`{key}`.*finite"):
                F("num", "range", **{key: bad})


def test_is_true_on_nullable_boolean_with_missing_values(edge):
    df = pd.DataFrame({"nb": pd.array([True, None, False], dtype="boolean")})
    assert _m(df, F("nb", "is_true")) == [True, False, False]
    assert _m(df, F("nb", "is_false")) == [False, False, True]  # NA matches neither, like NaN elsewhere


def test_duplicate_column_names_are_a_clear_error():
    dup = pd.DataFrame([[1, 2], [3, 4]], columns=["x", "x"])
    with pytest.raises(ValueError, match="duplicate columns named 'x'"):
        apply_attribute_filters(dup, [F("x", "not_null")])


# ---------------------------------------------------------------------------------------------------------
# (3) Permissions
# ---------------------------------------------------------------------------------------------------------

A = [{"column": "c", "op": "equals", "value": "x"}]


def _comp(attrs, cat="a", **extra) -> dict:
    return {
        "scenario": {"name": "x"},
        "composition": {
            "total_buildings": 4,
            "categories": {cat: {"min_fraction": 0, "max_fraction": 1, "source": "resstock"}},
        },
        "archetype_filters": {"resstock": {cat: {"building_type_heights": ["x"], "attributes": attrs, **extra}}},
    }


def test_full_role_flag_override_matrix():
    override_options = [
        None,
        {},
        {"client": {"filters.use": True}},
        {"client": {"filters.use": False}},
        {"admin": {"filters.use": False}},
        {"demo": {"filters.use": False}},
    ]
    for ov in override_options:
        for role in ROLES:
            expected = FLAGS["filters.use"][role]
            if ov and role in ov and "filters.use" in ov[role]:
                expected = ov[role]["filters.use"]
            assert can(role, "filters.use", ov) is expected
    for role in ROLES:
        # demo can never gain the flag, from any role's point of view
        with pytest.raises(ValueError, match="demo"):
            can(role, "filters.use", {"demo": {"filters.use": True}})
        with pytest.raises(ValueError):
            validate_filter_edit(role, new=_comp(A), overrides={"demo": {"filters.use": True}})


@pytest.mark.parametrize("role", [None, "", "Admin", " admin", "admin ", "ADMIN", "root", 1])
def test_malformed_roles_are_errors_not_defaults(role):
    with pytest.raises(ValueError, match="unknown role"):
        can(role, "filters.use")  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="unknown role"):
        validate_filter_edit(role, new=_comp(None))  # type: ignore[arg-type]


@pytest.mark.parametrize("flag", ["Filters.use", "filters", "", "filters.use ", None])
def test_malformed_flags_are_errors(flag):
    with pytest.raises(ValueError, match="unknown permission flag"):
        can("admin", flag)  # type: ignore[arg-type]


@pytest.mark.parametrize("value", [1, 0, "true", "yes", None])
def test_override_values_must_be_real_booleans(value):
    with pytest.raises(ValueError, match="true or false"):
        can("client", "filters.use", {"client": {"filters.use": value}})


def test_edit_not_run_rule(synthetic_dataset):
    saved = _numbers_cfg(synthetic_dataset, [{"column": "in.geometry_stories", "op": "range", "min": 1}])
    run.run_scenario({**saved, "ensemble": {"mode": "fixed", "max_runs": 2}})  # the facade takes no role at all
    for role in ("client", "demo"):
        validate_filter_edit(role, new=saved, saved=saved)  # unchanged re-save is allowed
        validate_filter_edit(role, new=copy.deepcopy(saved), saved=saved)
    # but running is not even a permission question: validate_filters_allowed is for new submissions only
    with pytest.raises(FilterPermissionError):
        validate_filters_allowed("client", saved)


@pytest.mark.parametrize(
    "new",
    [
        _comp([{"column": "c", "op": "equals", "value": "y"}]),  # change value
        _comp(None),  # remove
        _comp(A + [{"column": "d", "op": "not_null"}]),  # add one
        _comp([{"column": "d", "op": "equals", "value": "x"}]),  # change column
        _comp([{"column": "c", "op": "in", "values": ["x"]}]),  # equivalent form (denied: conservative)
        _comp(A, cat="b"),  # same filter moved to another category
    ],
    ids=["value", "remove", "add", "column", "in-vs-equals", "moved-category"],
)
def test_client_cannot_alter_filters_of_a_saved_composition(new):
    with pytest.raises(FilterPermissionError):
        validate_filter_edit("client", new=new, saved=_comp(A))
    with pytest.raises(FilterPermissionError):
        validate_filter_edit("demo", new=new, saved=_comp(A))
    validate_filter_edit("admin", new=new, saved=_comp(A))


def test_unchanged_is_decided_by_value_not_by_key_order_or_defaults():
    reordered = _comp([{"op": "equals", "value": "x", "column": "c"}])
    explicit_default = _comp([{"column": "c", "op": "equals", "value": "x", "min_inclusive": True}])
    validate_filter_edit("client", new=reordered, saved=_comp(A))
    validate_filter_edit("client", new=explicit_default, saved=_comp(A))
    r1 = _comp([{"column": "c", "op": "range", "min": 1, "max": 2}])
    r2 = _comp([{"column": "c", "op": "range", "min": 1.0, "max": 2.0}])
    validate_filter_edit("client", new=r2, saved=r1)
    noise = _comp([{"column": "c", "op": "range", "min": 1.0000000001, "max": 2}])
    with pytest.raises(FilterPermissionError):
        validate_filter_edit("client", new=noise, saved=r1)


def test_brand_new_composition_and_empty_saved_are_both_treated_as_no_filters():
    validate_filter_edit("client", new=_comp(None))
    validate_filter_edit("client", new=_comp([]), saved=_comp(None))
    with pytest.raises(FilterPermissionError):
        validate_filter_edit("client", new=_comp(A), saved=None)
    with pytest.raises(FilterPermissionError):
        validate_filter_edit("client", new=_comp(A), saved={})


def test_unchanged_resave_with_nan_bound_is_allowed():
    # NaN bounds can no longer be built normally; force one in to prove the comparison itself is NaN-safe
    f = F("c", "range", min=1.0)
    object.__setattr__(f, "min", float("nan"))
    g = F("c", "range", min=1.0)
    object.__setattr__(g, "min", float("nan"))
    validate_filter_edit("client", new=[f], saved=[g])
    with pytest.raises(FilterPermissionError):
        validate_filter_edit("client", new=[f], saved=[F("c", "range", min=2.0)])


def test_a_nan_bound_cannot_be_submitted_in_a_composition():
    with pytest.raises(ValueError, match="finite"):
        validate_filter_edit("client", new=_comp([{"column": "c", "op": "range", "min": float("nan")}]))


# ---------------------------------------------------------------------------------------------------------
# (4) Dataset resolution
# ---------------------------------------------------------------------------------------------------------


@pytest.fixture
def cat():
    return _catalog(_entry("resstock", "2025_release_1"), _entry("comstock", "2025_release_3"))


def test_resolution_never_mutates_its_input_and_is_idempotent(synthetic_dataset, cat):
    c = _cfg(synthetic_dataset, dataset_resstock=RES, dataset_comstock=COM)
    before = dataclasses.asdict(c)
    once = resolve_dataset_paths(c, cat, synthetic_dataset.root)
    assert dataclasses.asdict(c) == before
    twice = resolve_dataset_paths(once, cat, synthetic_dataset.root)
    assert dataclasses.asdict(once) == dataclasses.asdict(twice)
    once.data.resstock_chars_file = "changed"
    assert c.data.resstock_chars_file != "changed"  # deep copy, nothing shared


@pytest.mark.parametrize(
    "ident",
    [
        "resstock:2025_release_1:amy2018:baseline:dc",  # state case
        "resstock:2025_release_1:amy2018:../x:DC",
        "resstock:2025_release_1:amy2018:baseline:DC:",
        "resstock:2025_release_1:AMY2018:baseline:DC",
    ],
)
def test_ids_must_match_the_catalog_exactly(synthetic_dataset, cat, ident):
    with pytest.raises(ValueError):
        resolve_dataset_paths(_cfg(synthetic_dataset, dataset_resstock=ident), cat, synthetic_dataset.root)


@pytest.mark.parametrize(
    "path", ["a/Upgrade=0/State=DC", "a/upgrade=00/state=DC", "a/upgrade=0/state=dc", "somewhere/else"]
)
def test_odd_partition_paths_are_refused(synthetic_dataset, path):
    odd = _catalog(_entry("resstock", "2025_release_1", path=path))
    with pytest.raises(ValueError, match="upgrade=<partition>/state=<state>"):
        resolve_dataset_paths(_cfg(synthetic_dataset, dataset_resstock=RES), odd, synthetic_dataset.root)


@pytest.mark.parametrize("path", ["/abs/upgrade=0/state=DC", "a/../upgrade=0/state=DC", "../upgrade=0/state=DC"])
def test_traversal_and_absolute_catalog_paths_are_rejected_at_catalog_load(path):
    with pytest.raises(ValueError, match="relative"):
        _catalog(_entry("resstock", "2025_release_1", path=path))


def test_unverified_check_and_gross_net_check_are_each_load_bearing(synthetic_dataset):
    unverified = _catalog(_entry("resstock", "2025_release_1", verified=False), _entry("comstock", "2025_release_3"))
    cfg = _cfg(synthetic_dataset, dataset_resstock=RES, dataset_comstock=COM)
    with pytest.raises(ValueError, match="not verified"):
        resolve_dataset_paths(cfg, unverified, synthetic_dataset.root, require_verified=True)
    resolve_dataset_paths(cfg, unverified, synthetic_dataset.root)  # default: allowed (see report, owner question)
    net = _catalog(_entry("resstock", "2025_release_1"), _entry("comstock", "2025_release_3", basis="net"))
    with pytest.raises(ValueError, match="gross.*net"):
        resolve_dataset_paths(cfg, net, synthetic_dataset.root)
    only_net_both = _catalog(
        _entry("resstock", "2025_release_1", basis="net"), _entry("comstock", "2025_release_3", basis="net")
    )
    assert resolve_dataset_paths(cfg, only_net_both, synthetic_dataset.root).data.resstock_load_basis == "net"


def test_product_without_an_id_keeps_its_configured_paths(synthetic_dataset, cat):
    cfg = _cfg(synthetic_dataset, dataset_resstock=RES)
    cfg.data.comstock_timeseries_dir = "/configured/comstock"
    out = resolve_dataset_paths(cfg, cat, synthetic_dataset.root)
    assert out.data.comstock_timeseries_dir == "/configured/comstock"
    assert out.data.comstock_load_basis is None and out.data.dataset_comstock is None


def _one_product_root(synthetic_dataset, tmp_path, partition=5):
    root = tmp_path
    (root / "background").mkdir()
    (root / f"input/resstock/timeseries_individual_buildings/upgrade={partition}/state=DC").mkdir(parents=True)
    (root / "background" / synthetic_dataset.resstock_chars.name).write_bytes(b"x")
    return root


def test_partial_resolution_refuses_a_partition_that_differs_from_the_configured_upgrade(synthetic_dataset, tmp_path):
    root = _one_product_root(synthetic_dataset, tmp_path)
    catalog = _catalog(_entry("resstock", "2025_release_1", key="u5", partition=5))
    cfg = _cfg(synthetic_dataset, dataset_resstock="resstock:2025_release_1:amy2018:u5:DC")
    assert cfg.data.upgrade == "0"
    with pytest.raises(ValueError, match=r"data\.upgrade") as exc:
        resolve_dataset_paths(cfg, catalog, root)
    assert str(root) not in str(exc.value)  # names fields, not paths
    assert cfg.data.upgrade == "0"


def test_partial_resolution_refuses_a_state_that_differs_from_the_configured_state(synthetic_dataset, tmp_path):
    root = tmp_path
    (root / "background").mkdir()
    (root / "input/resstock/timeseries_individual_buildings/upgrade=0/state=MD").mkdir(parents=True)
    (root / "background" / synthetic_dataset.resstock_chars.name).write_bytes(b"x")
    catalog = _catalog(
        _entry(
            "resstock",
            "2025_release_1",
            state="MD",
            path="input/resstock/timeseries_individual_buildings/upgrade=0/state=MD",
        )
    )
    cfg = _cfg(synthetic_dataset, dataset_resstock="resstock:2025_release_1:amy2018:baseline:MD")
    with pytest.raises(ValueError, match=r"data\.state"):
        resolve_dataset_paths(cfg, catalog, root)


def test_partial_resolution_with_a_matching_partition_still_works(synthetic_dataset, cat):
    out = resolve_dataset_paths(_cfg(synthetic_dataset, dataset_resstock=RES), cat, synthetic_dataset.root)
    assert (out.data.upgrade, out.data.state) == ("0", "DC")


def test_symlink_out_of_the_data_root_is_refused(synthetic_dataset, tmp_path):
    root, outside = tmp_path / "root", tmp_path / "outside"
    (outside / "upgrade=0/state=DC").mkdir(parents=True)
    (root / "background").mkdir(parents=True)
    (root / "input/resstock").mkdir(parents=True)
    os.symlink(outside, root / "input/resstock/timeseries_individual_buildings")
    (root / "background" / synthetic_dataset.resstock_chars.name).write_bytes(b"x")
    catalog = _catalog(_entry("resstock", "2025_release_1"))
    with pytest.raises(ValueError, match="outside"):
        resolve_dataset_paths(_cfg(synthetic_dataset, dataset_resstock=RES), catalog, root)
    # the explicit opt-out documents the choice
    out = resolve_dataset_paths(
        _cfg(synthetic_dataset, dataset_resstock=RES), catalog, root, allow_symlinks_outside_root=True
    )
    assert out.data.resstock_timeseries_dir.startswith(str(root))


def test_symlinked_characteristics_file_outside_the_root_is_refused(synthetic_dataset, tmp_path):
    root, outside = tmp_path / "root", tmp_path / "outside"
    (root / "input/resstock/timeseries_individual_buildings/upgrade=0/state=DC").mkdir(parents=True)
    (root / "background").mkdir()
    outside.mkdir()
    (outside / "c.xlsx").write_bytes(b"x")
    os.symlink(outside / "c.xlsx", root / "background" / synthetic_dataset.resstock_chars.name)
    catalog = _catalog(_entry("resstock", "2025_release_1"))
    with pytest.raises(ValueError, match="outside"):
        resolve_dataset_paths(_cfg(synthetic_dataset, dataset_resstock=RES), catalog, root)


def test_synthetic_layer_boundary_is_strict_for_datasets_and_for_the_new_community_load_modules():
    from tests.test_architecture import FORBIDDEN

    banned = FORBIDDEN["microgrid.community_load.synthetic"]
    assert "microgrid.datasets" in banned  # the engine may not import the catalog layer
    assert "microgrid.core" in banned and "microgrid.geo" in banned


@pytest.mark.parametrize("mod", ["dataset_resolution", "permissions", "exposed_attributes"])
def test_engine_may_not_import_the_modules_that_sit_above_it(mod):
    from tests.test_architecture import FORBIDDEN, _under

    banned = FORBIDDEN["microgrid.community_load.synthetic"]
    assert any(_under(f"microgrid.community_load.{mod}", b) for b in banned)


# ---------------------------------------------------------------------------------------------------------
# (5) Exposed attributes
# ---------------------------------------------------------------------------------------------------------

SRC = "resstock"


def test_to_filters_cannot_name_a_column_the_admin_did_not_expose():
    d = default_exposed_attributes()
    with pytest.raises(ValueError, match="unknown key"):
        d.to_filters([{"attribute": "stories", "op": "equals", "value": 2, "column": "in.secret"}], source=SRC)
    with pytest.raises(ValueError, match="unknown exposed attribute"):
        d.to_filters([{"attribute": "in.secret", "op": "not_null"}], source=SRC)
    # the column in the result is always the admin-configured one
    (f,) = d.to_filters([{"attribute": "stories", "op": "equals", "value": 2}], source=SRC)
    assert f.column == d.get("stories").columns[SRC]


def test_to_filters_respects_source_inactive_and_building_type_scope():
    d = default_exposed_attributes()
    with pytest.raises(ValueError, match="does not apply"):
        d.to_filters([{"attribute": "dwelling_units", "op": "equals", "value": 3}], source="comstock")
    with pytest.raises(ValueError, match="does not apply"):
        d.to_filters(
            [{"attribute": "dwelling_units", "op": "equals", "value": 3}], source=SRC, building_type="sf_detached"
        )
    data = {
        "version": 1,
        "attributes": [
            {
                "key": "a",
                "label": "A",
                "kind": "numeric",
                "columns": {"resstock": "c"},
                "operators": ["range"],
                "active": False,
            }
        ],
    }
    with pytest.raises(ValueError, match="not active"):
        ExposedAttributes.from_dict(data).to_filters([{"attribute": "a", "op": "range", "min": 1}], source=SRC)


@pytest.mark.parametrize(
    "mut, msg",
    [
        ({"foo": 1}, "unknown field"),
        ({"kind": "categorical", "operators": ["range"]}, "not valid for categorical"),
        ({"operators": ["is_true"]}, "not valid for numeric"),
        ({"choices": ["a"]}, "only apply to categorical"),
        ({"min": 5, "max": 1}, "exceeds"),
        ({"columns": {"nonsense": "c"}}, "unknown source"),
        ({"operators": []}, "non-empty"),
        ({"active": "no"}, "true or false"),
    ],
)
def test_loader_is_strict(mut, msg):
    entry = {"key": "a", "label": "A", "kind": "numeric", "columns": {"resstock": "c"}, "operators": ["range"], **mut}
    with pytest.raises(ValueError, match=msg):
        ExposedAttributes.from_dict({"version": 1, "attributes": [entry]})


def test_loader_rejects_duplicates_bad_versions_and_unknown_top_level_keys():
    entry = {"key": "a", "label": "A", "kind": "numeric", "columns": {"resstock": "c"}, "operators": ["range"]}
    with pytest.raises(ValueError, match="duplicate key"):
        ExposedAttributes.from_dict({"version": 1, "attributes": [entry, entry]})
    for bad in ({"version": "1", "attributes": []}, {"version": 2, "attributes": []}, {"attributes": []}):
        with pytest.raises(ValueError, match="version"):
            ExposedAttributes.from_dict(bad)
    with pytest.raises(ValueError, match="unknown top-level"):
        ExposedAttributes.from_dict({"version": 1, "attributes": [], "extra": 1})


def test_check_against_data_names_every_missing_column():
    df = pd.DataFrame({"bldg_id": [1]})
    problems = check_against_data(default_exposed_attributes(), df, SRC)
    assert len(problems) == len(default_exposed_attributes().active_for(SRC))
    assert all("not in the data" in p for p in problems)


@pytest.mark.xfail(
    strict=True, reason="QA-F9: check_against_data does not compare declared choices with the data values"
)
def test_check_against_data_flags_choices_that_do_not_exist_in_the_data():
    d = ExposedAttributes.from_dict(
        {
            "version": 1,
            "attributes": [
                {
                    "key": "v",
                    "label": "V",
                    "kind": "categorical",
                    "columns": {"resstock": "in.v"},
                    "operators": ["equals"],
                    "choices": ["1950s", "ghost"],
                }
            ],
        }
    )
    problems = check_against_data(d, pd.DataFrame({"in.v": ["1950s", "1960s"]}), SRC)
    assert any("ghost" in p for p in problems)


@pytest.mark.xfail(
    strict=True, reason="QA-F10: to_filters accepts NaN bounds and wrong-typed numeric values (fail only later)"
)
def test_to_filters_rejects_nan_and_text_for_numeric_attributes():
    d = default_exposed_attributes()
    with pytest.raises(ValueError):
        d.to_filters([{"attribute": "stories", "op": "range", "min": float("nan")}], source=SRC)
    with pytest.raises(ValueError):
        d.to_filters([{"attribute": "stories", "op": "equals", "value": "3"}], source=SRC)


# ---------------------------------------------------------------------------------------------------------
# (6) Snapshot replay
# ---------------------------------------------------------------------------------------------------------


def test_config_snapshot_replays_through_config_from_dict(synthetic_dataset):
    raw = build_tiny_config(synthetic_dataset)
    raw["archetype_filters"]["resstock"]["mf_small"]["attributes"] = [
        {"column": "in.geometry_stories", "op": "range", "min": 1}
    ]
    raw["ensemble"]["max_runs"] = 2
    result = run.run_scenario(raw)
    replayed = config_from_dict(result.config_snapshot)
    assert replayed.archetype_filters.resstock["mf_small"].attributes == [F("in.geometry_stories", "range", min=1)]
    assert replayed.rng.composition_seed == result.seeds["composition_seed"]


def _rich_raw(ds) -> dict:
    raw = build_tiny_config(ds)
    raw["scenario"] = {"name": "rich", "description": "a description"}
    raw["data"] = {
        **raw["data"],
        "dataset_resstock": "resstock:2025_release_1:amy2018:baseline:DC",
        "dataset_comstock": "comstock:2025_release_3:amy2018:baseline:DC",
        "resstock_load_basis": "gross",
        "comstock_load_basis": "net",
    }
    res = raw["archetype_filters"]["resstock"]
    res["mf_small"].update(
        hvac_types=["a"],
        hvac_types_exclude=["b"],
        max_sqft=2500.5,
        require_no_garage=True,
        require_data_complete=False,
        attributes=[
            {"column": "c1", "op": "equals", "value": "x"},
            {"column": "c2", "op": "in", "values": [1, 2.5]},
            {"column": "c3", "op": "range", "min": 1, "max": 9, "min_inclusive": False},
            {"column": "c4", "op": "is_true"},
            {"column": "c5", "op": "not_null"},
        ],
    )
    raw["archetype_filters"]["comstock"] = {"office": {"building_types": ["Office"], "include_public": True}}
    raw["archetype_filters"]["min_pool_size"] = 3
    raw["composition"]["categories"]["office"] = {"fixed_count": 1, "source": "comstock"}
    raw["composition"]["dirichlet_concentration"] = 2.5
    raw["perturbation"] = {
        "enabled": True,
        "max_temporal_shift_hours": 1,
        "energy_tolerance": 0.1,
        "end_use_factors": {"heating": {"sigma": 0.2}},
    }
    raw["feasibility"] = {
        "hard": {"peak_kw": {"min": 1, "max": 5000}, "load_factor": {"max": 0.9}},
        "soft": {
            "weights": {"peak_kw": 2.0},
            "bounds": {"peak_kw": {"min": 10}, "load_factor": {"min": 0.1, "max": 0.8}},
        },
    }
    raw["convergence"] = {"lookback_window": 4, "min_stable_window": 3, "epsilon": 0.02, "metrics": ["peak_kw"]}
    raw["validation"] = {"benchmarks": {"peak_kw": [1.0, 2.0]}, "min_cv_peak": 0.05}
    raw["output"] = {"base_dir": "x", "per_run_csvs": False, "percentiles": [0.25, 0.75], "validation_report": False}
    return raw


def test_config_to_dict_round_trips_a_rich_config(synthetic_dataset):
    from microgrid.community_load.synthetic.config import config_to_dict

    cfg = config_from_dict(_rich_raw(synthetic_dataset))
    assert config_from_dict(config_to_dict(cfg)) == cfg
    # None-valued filter fields are dropped, and the shape is what the loader documents
    d = config_to_dict(cfg)
    assert d["scenario"] == {"name": "rich", "description": "a description"}
    assert d["archetype_filters"]["resstock"]["mf_small"]["attributes"][0] == {
        "column": "c1",
        "op": "equals",
        "value": "x",
    }
    assert set(d["feasibility"]) == {"hard", "soft"}
    assert d["feasibility"]["hard"]["peak_kw"] == {"min": 1.0, "max": 5000.0}


def test_config_to_dict_round_trips_the_default_config():
    from microgrid.community_load.synthetic.config import ScenarioConfig, config_to_dict

    cfg = ScenarioConfig()
    assert config_from_dict(config_to_dict(cfg)) == cfg


def test_snapshot_replays_to_identical_metrics(synthetic_dataset):
    raw = build_tiny_config(synthetic_dataset)
    raw["rng"] = {}  # fresh seeds: the snapshot must carry the ones actually used
    raw["archetype_filters"]["resstock"]["mf_small"]["attributes"] = [
        {"column": "in.geometry_stories", "op": "range", "min": 1}
    ]
    first = run.run_scenario(raw)
    assert first.config_snapshot["rng"] == first.seeds
    again = run.run_scenario(config_from_dict(first.config_snapshot))
    assert again.seeds == first.seeds
    assert json.dumps(again.metrics, sort_keys=True) == json.dumps(first.metrics, sort_keys=True)


# ---------------------------------------------------------------------------------------------------------
# Mutation survivors of the author's suite (each of these passed with the production line disabled)
# ---------------------------------------------------------------------------------------------------------


def test_commercial_categories_honor_attribute_filters(synthetic_dataset):
    from microgrid.community_load.synthetic import archetype
    from microgrid.community_load.synthetic.config import CommercialFilterConfig

    com = archetype.load_comstock_characteristics(synthetic_dataset.comstock_chars)
    btypes = sorted(com["in.comstock_building_type"].dropna().unique())
    base = archetype._filter_comstock_category(com, CommercialFilterConfig(building_types=btypes))
    assert base
    keep_none = CommercialFilterConfig(building_types=btypes, attributes=[F("in.number_stories", "range", min=999)])
    assert archetype._filter_comstock_category(com, keep_none) == []
    some = CommercialFilterConfig(building_types=btypes, attributes=[F("in.number_stories", "range", max=2)])
    got = archetype._filter_comstock_category(com, some)
    assert set(got) <= set(base)
    assert got == com[com["bldg_id"].isin(base) & (com["in.number_stories"] <= 2)]["bldg_id"].tolist()
    assert preview_pool(com, keep_none).matched == 0


def test_resstock_and_comstock_in_different_states_is_refused(synthetic_dataset, tmp_path):
    root = tmp_path
    (root / "background").mkdir()
    (root / "input/resstock/timeseries_individual_buildings/upgrade=0/state=DC").mkdir(parents=True)
    (root / "input/comstock/timeseries_individual_buildings/upgrade=0/state=MD").mkdir(parents=True)
    for name in (synthetic_dataset.resstock_chars.name, synthetic_dataset.comstock_chars.name):
        (root / "background" / name).write_bytes(b"x")
    catalog = _catalog(
        _entry("resstock", "2025_release_1"),
        _entry(
            "comstock",
            "2025_release_3",
            state="MD",
            path="input/comstock/timeseries_individual_buildings/upgrade=0/state=MD",
        ),
    )
    cfg = _cfg(synthetic_dataset, dataset_resstock=RES, dataset_comstock="comstock:2025_release_3:amy2018:baseline:MD")
    with pytest.raises(ValueError, match="different states"):
        resolve_dataset_paths(cfg, catalog, root)


def test_product_in_the_wrong_field_is_refused_even_when_config_validation_is_bypassed(synthetic_dataset, cat):
    cfg = _cfg(synthetic_dataset, dataset_resstock=RES)
    cfg.data.dataset_resstock = COM  # a hand-built ScenarioConfig skips config_from_dict's prefix check
    with pytest.raises(ValueError, match="names a comstock dataset"):
        resolve_dataset_paths(cfg, cat, synthetic_dataset.root)


def test_each_missing_piece_is_reported_by_its_own_message(synthetic_dataset, cat, tmp_path):
    cfg = _cfg(synthetic_dataset, dataset_resstock=RES)
    # background present with the characteristics file, partition directory absent
    (tmp_path / "background").mkdir()
    (tmp_path / "background" / synthetic_dataset.resstock_chars.name).write_bytes(b"x")
    with pytest.raises(ValueError, match="directory .* does not exist") as exc:
        resolve_dataset_paths(cfg, cat, tmp_path)
    assert "background" not in str(exc.value).split("directory", 1)[0]
    # partition present, background directory absent
    other = tmp_path / "other"
    (other / "input/resstock/timeseries_individual_buildings/upgrade=0/state=DC").mkdir(parents=True)
    with pytest.raises(ValueError, match="background directory .* does not exist"):
        resolve_dataset_paths(cfg, cat, other)


# --- dataset id forms: parse_dataset_id and config validation accept exactly the same strings ---------------


@pytest.mark.parametrize(
    "text, ok",
    [
        ("resstock:2025_release_1:amy2018:baseline:DC", True),
        ("resstock/2025_release_1/amy2018/baseline/DC", True),
        (" resstock:2025_release_1:amy2018:baseline:DC", False),
        ("resstock:2025_release_1:amy2018:baseline:DC\n", False),
        ("resstock:2025_release_1: amy2018:baseline:DC", False),
        ("resstock:2025_release_1:amy2018:baseline:DC ", False),
        ("resstock:2025_release_1:amy2018:baseline", False),
        ("resstock:2025_release_1:amy2018::DC", False),
        ("resstock/2025_release_1/amy2018/baseline/DC\n", False),
    ],
)
def test_parse_and_config_validation_agree_on_dataset_id_forms(synthetic_dataset, text, ok):
    from microgrid.community_load.dataset_resolution import parse_dataset_id

    raw = build_tiny_config(synthetic_dataset)
    raw["data"] = {**raw.get("data", {}), "dataset_resstock": text}
    if ok:
        assert parse_dataset_id(text).product == "resstock"
        assert config_from_dict(raw).data.dataset_resstock == text
    else:
        with pytest.raises(ValueError):
            parse_dataset_id(text)
        with pytest.raises(ValueError, match="dataset_resstock"):
            config_from_dict(raw)
