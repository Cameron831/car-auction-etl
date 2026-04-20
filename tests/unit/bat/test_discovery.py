import logging

import pytest

from app.sources.bat import discovery


def test_build_discovered_listing_params_maps_candidate_to_schema_columns():
    params = discovery.build_discovered_listing_params(_candidate())

    assert params == {
        "source_site": "bringatrailer",
        "source_listing_id": "test-listing",
        "url": "https://bringatrailer.com/listing/test-listing/",
        "title": "2004 BMW M3 Coupe",
        "auction_end_date": "2026-03-30",
        "source_location": "USA",
    }


def test_build_discovered_listing_params_allows_missing_visible_metadata():
    params = discovery.build_discovered_listing_params(
        {
            "listing_id": "test-listing",
            "url": "https://bringatrailer.com/listing/test-listing/",
        }
    )

    assert params == {
        "source_site": "bringatrailer",
        "source_listing_id": "test-listing",
        "url": "https://bringatrailer.com/listing/test-listing/",
        "title": None,
        "auction_end_date": None,
        "source_location": None,
    }


def test_save_discovered_listing_executes_upsert_for_visible_metadata(mocker, caplog):
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
    mocker.patch.object(discovery.psycopg, "connect", side_effect=fake_connect)

    caplog.set_level(logging.INFO)
    discovery.save_discovered_listing(_candidate())

    sql, params = calls["executions"][0]

    assert calls["database_url"] == "postgresql://user:pass@localhost/db"
    assert "INSERT INTO discovered_listings" in sql
    assert "ON CONFLICT (source_site, source_listing_id) DO UPDATE" in sql
    assert "url = EXCLUDED.url" in sql
    assert "title = EXCLUDED.title" in sql
    assert "auction_end_date = EXCLUDED.auction_end_date" in sql
    assert "source_location = EXCLUDED.source_location" in sql
    assert "last_seen_at = NOW()" in sql
    assert "eligible" not in sql
    assert "eligibility_reason" not in sql
    assert "ingested_at" not in sql
    assert "discovered_at" not in sql
    assert params == {
        "source_site": "bringatrailer",
        "source_listing_id": "test-listing",
        "url": "https://bringatrailer.com/listing/test-listing/",
        "title": "2004 BMW M3 Coupe",
        "auction_end_date": "2026-03-30",
        "source_location": "USA",
    }
    assert "Upserted BAT discovered listing for listing_id=test-listing" in caplog.text
    assert "postgresql://user:pass@localhost/db" not in caplog.text


def test_save_discovered_listing_requires_database_url(mocker):
    mocker.patch.dict("os.environ", {}, clear=True)

    with pytest.raises(RuntimeError, match="DATABASE_URL must be set"):
        discovery.save_discovered_listing(_candidate())


def _candidate():
    return {
        "listing_id": "test-listing",
        "url": "https://bringatrailer.com/listing/test-listing/",
        "title": "2004 BMW M3 Coupe",
        "auction_end_date": "2026-03-30",
        "source_location": "USA",
    }
