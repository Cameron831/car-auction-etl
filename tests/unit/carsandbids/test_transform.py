import json
import logging
from pathlib import Path

import pytest

from app.sources.carsandbids import transform


FIXTURE_PATH = Path(__file__).resolve().parents[2] / "fixtures" / "carsandbids_listing.json"


def test_build_raw_listing_lookup_params_maps_listing_to_schema_columns():
    params = transform.build_raw_listing_lookup_params("test-id")

    assert params == {
        "source_site": "carsandbids",
        "source_listing_id": "test-id",
    }


def test_load_listing_json_retrieves_raw_json_from_postgres(mocker):
    calls = {}
    expected_payload = {"listing": {"make": "Porsche"}}

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params):
            calls["sql"] = sql
            calls["params"] = params

        def fetchone(self):
            return (expected_payload,)

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

    mocker.patch.dict(
        "os.environ",
        {"DATABASE_URL": "postgresql://user:pass@localhost/db"},
    )
    mocker.patch.object(transform.psycopg, "connect", return_value=FakeConnection())

    payload = transform.load_listing_json("test-id")

    assert payload == expected_payload
    assert "FROM raw_listing_json" in calls["sql"]
    assert calls["params"] == {
        "source_site": "carsandbids",
        "source_listing_id": "test-id",
    }


def test_load_listing_json_missing_record_raises_clear_error(mocker):
    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params):
            return None

        def fetchone(self):
            return None

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

    mocker.patch.dict(
        "os.environ",
        {"DATABASE_URL": "postgresql://user:pass@localhost/db"},
    )
    mocker.patch.object(transform.psycopg, "connect", return_value=FakeConnection())

    with pytest.raises(LookupError, match="Raw JSON record not found for listing ID: missing-id"):
        transform.load_listing_json("missing-id")


def test_load_listing_json_requires_database_url(mocker):
    mocker.patch.dict("os.environ", {}, clear=True)

    with pytest.raises(RuntimeError, match="DATABASE_URL must be set"):
        transform.load_listing_json("test-id")


def test_load_pending_raw_listing_json_selects_unprocessed_rows_in_stable_order(mocker):
    calls = {}
    expected_rows = [
        {"source_listing_id": "first"},
        {"source_listing_id": "second"},
    ]

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params):
            calls["sql"] = sql
            calls["params"] = params

        def fetchall(self):
            return expected_rows

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self, row_factory=None):
            calls["row_factory"] = row_factory
            return FakeCursor()

    mocker.patch.dict(
        "os.environ",
        {"DATABASE_URL": "postgresql://user:pass@localhost/db"},
    )
    mocker.patch.object(transform.psycopg, "connect", return_value=FakeConnection())

    rows = transform.load_pending_raw_listing_json()

    assert rows == expected_rows
    assert "FROM raw_listing_json" in calls["sql"]
    assert "processed = FALSE" in calls["sql"]
    assert "ORDER BY created_at ASC, id ASC" in calls["sql"]
    assert "LIMIT %(limit)s" not in calls["sql"]
    assert calls["params"] == {"source_site": "carsandbids"}
    assert calls["row_factory"] is transform.dict_row


def test_load_pending_raw_listing_json_applies_optional_limit(mocker):
    calls = {}

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params):
            calls["sql"] = sql
            calls["params"] = params

        def fetchall(self):
            return [{"source_listing_id": "first"}]

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self, row_factory=None):
            return FakeCursor()

    mocker.patch.dict(
        "os.environ",
        {"DATABASE_URL": "postgresql://user:pass@localhost/db"},
    )
    mocker.patch.object(transform.psycopg, "connect", return_value=FakeConnection())

    rows = transform.load_pending_raw_listing_json(limit=1)

    assert rows == [{"source_listing_id": "first"}]
    assert "LIMIT %(limit)s" in calls["sql"]
    assert calls["params"] == {"source_site": "carsandbids", "limit": 1}


def test_load_pending_raw_listing_json_returns_empty_for_non_positive_limit(mocker):
    connect = mocker.patch.object(transform.psycopg, "connect")
    mocker.patch.dict(
        "os.environ",
        {"DATABASE_URL": "postgresql://user:pass@localhost/db"},
    )

    assert transform.load_pending_raw_listing_json(limit=0) == []
    connect.assert_not_called()


def test_load_pending_raw_listing_json_requires_database_url(mocker):
    mocker.patch.dict("os.environ", {}, clear=True)

    with pytest.raises(RuntimeError, match="DATABASE_URL must be set"):
        transform.load_pending_raw_listing_json()


def test_transform_listing_json_maps_fixture_to_normalized_listing(mocker):
    payload = _fixture_payload()
    mocker.patch.object(transform, "load_listing_json", return_value=payload)

    transformed = transform.transform_listing_json("3pnjnnx6")

    assert transformed["source_site"] == "carsandbids"
    assert transformed["listing_id"] == "3pnjnnx6"
    assert transformed["url"] == "https://carsandbids.com/auctions/3pnjnnx6"
    assert transformed["make"] == "Porsche"
    assert transformed["model_raw"] == "991 911"
    assert transformed["model_normalized"] == "911"
    assert transformed["year"] == 2013
    assert transformed["mileage"] == 56700
    assert transformed["vin"] == "WP0AA2A95DS107582"
    assert transformed["sale_price"] == 78000
    assert transformed["sold"] is True
    assert transformed["auction_end_date"] == "2026-04-20"
    assert transformed["transmission"] == "manual"
    assert transformed["listing_details_raw"]["engine"] == "3.4L Flat-6"
    assert transformed["listing_details_raw"]["transmission_details"] == "7-Speed"
    assert "sections" in transformed["listing_details_raw"]
    assert "title" not in transformed["listing_details_raw"]
    assert "sub_title" not in transformed["listing_details_raw"]
    assert "location" not in transformed["listing_details_raw"]
    assert "drive_train" not in transformed["listing_details_raw"]
    assert "seller_type_details" not in transformed["listing_details_raw"]
    assert "seller" not in transformed["listing_details_raw"]


def test_transform_listing_json_logs_without_raw_json(mocker, caplog):
    payload = _fixture_payload()
    payload["listing"]["make"] = "SENSITIVE_RAW_JSON"
    mocker.patch.object(transform, "load_listing_json", return_value=payload)

    caplog.set_level(logging.INFO)
    transformed = transform.transform_listing_json("3pnjnnx6")

    assert transformed["make"] == "SENSITIVE_RAW_JSON"
    assert "Transforming Cars and Bids listing JSON for listing_id=3pnjnnx6" in caplog.text
    assert "Transformed Cars and Bids listing JSON for listing_id=3pnjnnx6" in caplog.text
    assert "SENSITIVE_RAW_JSON" not in caplog.text


@pytest.mark.parametrize(
    ("status", "sold"),
    [
        ("sold", True),
        ("sold_after", True),
        ("reserve_not_met", False),
        ("canceled", False),
    ],
)
def test_extract_sold_status_maps_approved_statuses(status, sold):
    payload = {"status": status}

    assert transform.extract_sold_status(payload) is sold


def test_extract_sold_status_rejects_unknown_status():
    with pytest.raises(ValueError, match="Could not parse sold status: active"):
        transform.extract_sold_status({"status": "active"})


def test_extract_sale_price_uses_current_bid_fallback():
    payload = {
        "stats": {
            "sale_amount": None,
            "current_bid": {"amount": 12345},
        }
    }

    assert transform.extract_sale_price(payload) == 12345


def test_extract_sale_price_requires_sale_amount_or_current_bid():
    with pytest.raises(ValueError, match="Could not parse sale price"):
        transform.extract_sale_price({"stats": {"sale_amount": None}})


def test_transform_listing_json_passes_null_mileage_through(mocker):
    payload = _fixture_payload()
    payload["listing"]["mileage"] = None
    mocker.patch.object(transform, "load_listing_json", return_value=payload)

    transformed = transform.transform_listing_json("3pnjnnx6")

    assert transformed["mileage"] is None


@pytest.mark.parametrize(
    ("raw_transmission", "expected"),
    [
        (1, "automatic"),
        (2, "manual"),
    ],
)
def test_normalize_transmission_maps_source_specific_values(raw_transmission, expected):
    assert transform.normalize_transmission(raw_transmission) == expected


def test_normalize_transmission_rejects_unknown_values():
    with pytest.raises(ValueError, match="Could not normalize transmission: 3"):
        transform.normalize_transmission(3)


def test_extract_auction_end_date_rejects_unparseable_date():
    with pytest.raises(ValueError, match="Could not parse auction end date"):
        transform.extract_auction_end_date({"stats": {"auction_end": "not-a-date"}})


def _fixture_payload():
    return json.loads(FIXTURE_PATH.read_text())
