import pytest

from app.sources.bat import load


def test_build_listing_params_maps_transformed_listing_to_schema_columns():
    params = load.build_listing_params(_transformed_listing())

    assert params["source_site"] == "bringatrailer"
    assert params["source_listing_id"] == "test-listing"
    assert params["url"] == "https://bringatrailer.com/listing/test-listing/"
    assert params["make"] == "BMW"
    assert params["model"] == "M3"
    assert params["year"] == 2004
    assert params["mileage"] == 50250
    assert params["vin"] == "WBSBL93414PN57203"
    assert params["sale_price"] == 19750
    assert params["sold"] is True
    assert params["auction_end_date"] == "2026-03-30"
    assert params["transmission"] == "manual"
    assert params["listing_details_raw"].obj == [
        "Chassis: WBSBL93414PN57203",
        "50,250 Miles",
        "6-Speed Manual Transmission",
    ]


def test_load_listing_executes_upsert_with_expected_conflict_target(mocker):
    calls = {}

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params):
            calls["sql"] = sql
            calls["params"] = params

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

    load.load_listing(_transformed_listing())

    assert calls["database_url"] == "postgresql://user:pass@localhost/db"
    assert "ON CONFLICT (source_site, source_listing_id) DO UPDATE" in calls["sql"]
    assert "updated_at = NOW()" in calls["sql"]
    assert calls["params"]["source_listing_id"] == "test-listing"
    assert calls["params"]["listing_details_raw"].obj == [
        "Chassis: WBSBL93414PN57203",
        "50,250 Miles",
        "6-Speed Manual Transmission",
    ]


def test_load_listing_requires_database_url(mocker):
    mocker.patch.dict("os.environ", {}, clear=True)

    with pytest.raises(RuntimeError, match="DATABASE_URL must be set"):
        load.load_listing(_transformed_listing())


def _transformed_listing():
    return {
        "source_site": "bringatrailer",
        "listing_id": "test-listing",
        "url": "https://bringatrailer.com/listing/test-listing/",
        "make": "BMW",
        "model": "M3",
        "year": 2004,
        "mileage": 50250,
        "vin": "WBSBL93414PN57203",
        "sale_price": 19750,
        "sold": True,
        "auction_end_date": "2026-03-30",
        "transmission": "manual",
        "listing_details_raw": [
            "Chassis: WBSBL93414PN57203",
            "50,250 Miles",
            "6-Speed Manual Transmission",
        ],
    }
