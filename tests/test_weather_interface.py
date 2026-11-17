"""FR-ENG-13 (c): the ``weather`` module is an interface only. This test pins its shape so a later implementation
cannot drift from the documented contract without a visible test change."""

from __future__ import annotations

import dataclasses
import inspect

import pandas as pd
import pytest

from microgrid.community_load import weather


def _params(fn) -> list[tuple[str, str]]:
    return [(p.name, p.kind.name) for p in inspect.signature(fn).parameters.values()]


def test_public_names():
    assert set(weather.__all__) == {
        "ChangePointModel",
        "FitQuality",
        "WeatherFit",
        "QualityReport",
        "fit",
        "predict",
        "quality_report",
        "KNOWN_LIMITS",
    }


def test_function_signatures():
    assert _params(weather.fit) == [
        ("load_kw", "POSITIONAL_OR_KEYWORD"),
        ("temperature_c", "POSITIONAL_OR_KEYWORD"),
        ("model", "KEYWORD_ONLY"),
        ("extra_weather", "KEYWORD_ONLY"),
        ("time_terms", "KEYWORD_ONLY"),
    ]
    assert _params(weather.predict) == [
        ("fitted", "POSITIONAL_OR_KEYWORD"),
        ("temperature_c", "POSITIONAL_OR_KEYWORD"),
        ("extra_weather", "KEYWORD_ONLY"),
    ]
    assert _params(weather.quality_report) == [
        ("fitted", "POSITIONAL_OR_KEYWORD"),
        ("thresholds", "KEYWORD_ONLY"),
    ]


def test_dataclass_fields():
    assert [f.name for f in dataclasses.fields(weather.FitQuality)] == [
        "r_squared",
        "cv_rmse",
        "n_observations",
    ]
    assert [f.name for f in dataclasses.fields(weather.WeatherFit)] == [
        "model",
        "coefficients",
        "change_points_c",
        "quality",
        "resolution",
        "fit_period",
        "variables",
    ]
    assert [f.name for f in dataclasses.fields(weather.QualityReport)] == [
        "quality",
        "thresholds",
        "passed",
        "notes",
    ]
    assert {m.value for m in weather.ChangePointModel} == {"2P", "3P_heating", "3P_cooling", "4P", "5P"}


def test_known_limits_are_stated():
    text = " ".join(weather.KNOWN_LIMITS).lower()
    assert "one weather year" in text and "extremes" in text
    assert "simulation" in text
    assert weather.__doc__ is not None and "methodology" in weather.__doc__.lower()


def test_every_function_is_not_implemented_with_a_clear_message():
    s = pd.Series([1.0, 2.0], index=pd.date_range("2018-01-01", periods=2, freq="h"))
    fake = weather.WeatherFit(
        model=weather.ChangePointModel.TWO_P,
        coefficients={},
        change_points_c=(),
        quality=weather.FitQuality(r_squared=0.0, cv_rmse=0.0, n_observations=0),
        resolution="hourly",
        fit_period=("2018-01-01", "2018-01-01"),
        variables=("temperature_c",),
    )
    calls = [
        lambda: weather.fit(s, s),
        lambda: weather.predict(fake, s),
        lambda: weather.quality_report(fake),
    ]
    for call in calls:
        with pytest.raises(NotImplementedError, match=r"FR-ENG-13.*owner"):
            call()
