import re


MODEL_MAPPINGS = (
    ("BMW", "M3", "M3"),
    ("Porsche", "911", "911"),
    ("Porsche", "Boxster", "Boxster"),
    ("Alfa Romeo", "Spider", "Spider"),
    ("Honda", "S2000", "S2000"),
    ("Ferrari", "360", "360"),
)


def normalize_model(make, model_raw):
    if model_raw is None:
        return None

    normalized_make = _normalize_text(make).lower()
    normalized_model = _normalize_text(model_raw)
    if not normalized_model:
        return None

    for mapping_make, model_pattern, normalized_value in MODEL_MAPPINGS:
        if normalized_make == _normalize_text(mapping_make).lower() and _contains_token(
            normalized_model,
            model_pattern,
        ):
            return normalized_value

    return None


def _normalize_text(value):
    if value is None:
        return ""
    return " ".join(str(value).strip().split())


def _contains_token(value, token):
    pattern = rf"(?<![A-Za-z0-9]){re.escape(token)}(?![A-Za-z0-9])"
    return re.search(pattern, value, re.IGNORECASE) is not None
