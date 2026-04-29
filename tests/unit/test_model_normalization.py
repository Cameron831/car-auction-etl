import pytest

from app.model_normalization import normalize_model


@pytest.mark.parametrize(
    ("make", "model_raw", "expected"),
    [
        ("BMW", "M3", "M3"),
        ("Porsche", "911", "911"),
        ("Porsche", "Boxster", "Boxster"),
        ("Alfa Romeo", "Spider", "Spider"),
        ("Honda", "S2000", "S2000"),
        ("Ferrari", "360", "360"),
    ],
)
def test_normalize_model_maps_configured_models(make, model_raw, expected):
    assert normalize_model(make, model_raw) == expected


def test_normalize_model_matches_model_case_insensitively():
    assert normalize_model("BMW", "e46 m3 coupe") == "M3"


def test_normalize_model_matches_make_case_insensitively_after_whitespace_normalization():
    assert normalize_model("  alfa   romeo  ", "Duetto Spider") == "Spider"


@pytest.mark.parametrize(
    ("make", "model_raw"),
    [
        ("BMW", "M30"),
        ("BMW", "XM3"),
        ("Ferrari", "3600"),
        ("Porsche", "9110"),
    ],
)
def test_normalize_model_requires_token_boundaries(make, model_raw):
    assert normalize_model(make, model_raw) is None


def test_normalize_model_returns_none_for_non_match():
    assert normalize_model("Toyota", "Supra") is None


@pytest.mark.parametrize("model_raw", [None, "", "   "])
def test_normalize_model_returns_none_for_null_or_blank_model_raw(model_raw):
    assert normalize_model("BMW", model_raw) is None
