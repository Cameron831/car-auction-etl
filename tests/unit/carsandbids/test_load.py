import logging

import pytest

from app.sources.carsandbids import load


def test_build_listing_params_maps_transformed_listing_to_schema_columns():
    params = load.build_listing_params(_transformed_listing())

    assert params["source_site"] == "carsandbids"
    assert params["source_listing_id"] == "test-listing"
    assert params["url"] == "https://carsandbids.com/auctions/test-listing"
    assert params["make"] == "Porsche"
    assert params["model_raw"] == "991 911"
    assert params["model_normalized"] == "911"
    assert params["year"] == 2013
    assert params["mileage"] == 56700
    assert params["vin"] == "WP0AA2A95DS107582"
    assert params["sale_price"] == 78000
    assert params["sold"] is True
    assert params["auction_end_date"] == "2026-04-20"
    assert params["transmission"] == "manual"
    assert params["listing_details_raw"].obj == {
        "title": "2013 Porsche 911 Carrera Coupe",
    }


def test_build_listing_params_allows_null_optional_fields():
    transformed_listing = _transformed_listing()
    transformed_listing["model_raw"] = None
    transformed_listing["model_normalized"] = None
    transformed_listing["mileage"] = None
    transformed_listing["vin"] = None
    transformed_listing["transmission"] = None

    params = load.build_listing_params(transformed_listing)

    assert params["model_raw"] is None
    assert params["model_normalized"] is None
    assert params["mileage"] is None
    assert params["vin"] is None
    assert params["transmission"] is None


def test_load_listing_executes_upsert_and_marks_raw_json_processed(mocker, caplog):
    calls = {"executions": []}

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params):
            calls["executions"].append((sql, params))

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

    def fake_connect(database_url):
        calls["database_url"] = database_url
        return FakeConnection()

    mocker.patch.dict(
        "os.environ",
        {"DATABASE_URL": "postgresql://user:pass@localhost/db"},
    )
    mocker.patch.object(load.psycopg, "connect", side_effect=fake_connect)

    caplog.set_level(logging.INFO)
    load.load_listing(_transformed_listing())

    listing_sql, listing_params = calls["executions"][0]
    processed_sql, processed_params = calls["executions"][1]

    assert calls["database_url"] == "postgresql://user:pass@localhost/db"
    assert "ON CONFLICT (source_site, source_listing_id) DO UPDATE" in listing_sql
    assert "updated_at = NOW()" in listing_sql
    assert "model_raw" in listing_sql
    assert "model_normalized" in listing_sql
    assert "model = EXCLUDED.model" not in listing_sql
    assert listing_params["source_listing_id"] == "test-listing"
    assert listing_params["listing_details_raw"].obj == {
        "title": "2013 Porsche 911 Carrera Coupe",
    }
    assert "UPDATE raw_listing_json" in processed_sql
    assert "SET processed = TRUE" in processed_sql
    assert "source_site = %(source_site)s" in processed_sql
    assert "source_listing_id = %(source_listing_id)s" in processed_sql
    assert processed_params is listing_params
    assert "Upserted Cars and Bids listing for listing_id=test-listing" in caplog.text
    assert "Marked Cars and Bids raw listing processed for listing_id=test-listing" in caplog.text
    assert "postgresql://user:pass@localhost/db" not in caplog.text


def test_load_listing_requires_database_url(mocker):
    mocker.patch.dict("os.environ", {}, clear=True)

    with pytest.raises(RuntimeError, match="DATABASE_URL must be set"):
        load.load_listing(_transformed_listing())


def _transformed_listing():
    return {
        "source_site": "carsandbids",
        "listing_id": "test-listing",
        "url": "https://carsandbids.com/auctions/test-listing",
        "make": "Porsche",
        "model_raw": "991 911",
        "model_normalized": "911",
        "year": 2013,
        "mileage": 56700,
        "vin": "WP0AA2A95DS107582",
        "sale_price": 78000,
        "sold": True,
        "auction_end_date": "2026-04-20",
        "transmission": "manual",
        "listing_details_raw": {
            "title": "2013 Porsche 911 Carrera Coupe",
        },
    }
