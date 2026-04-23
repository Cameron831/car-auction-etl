import logging
from datetime import date, datetime, timezone

import pytest

from app.sources.bat import discovery


def test_fetch_completed_auctions_page_uses_endpoint_params_and_timeout(mocker, caplog):
    response = mocker.Mock()
    response.json.return_value = {"items": [{"url": "https://bringatrailer.com/listing/test/"}]}
    response.raise_for_status = mocker.Mock()
    mock_get = mocker.patch("app.sources.bat.discovery.requests.get", return_value=response)

    caplog.set_level(logging.INFO)
    payload = discovery.fetch_completed_auctions_page(2)

    assert payload == {"items": [{"url": "https://bringatrailer.com/listing/test/"}]}
    mock_get.assert_called_once_with(
        discovery.LISTINGS_FILTER_URL,
        params={
            "page": 2,
            "per_page": 60,
            "get_items": 1,
            "get_stats": 0,
            "sort": "td",
        },
        timeout=10,
    )
    response.raise_for_status.assert_called_once_with()
    response.json.assert_called_once_with()
    assert "Fetching BAT completed auctions page=2" in caplog.text
    assert "Fetched BAT completed auctions page=2 items=1" in caplog.text


def test_normalize_completed_auction_candidate_maps_endpoint_item():
    candidate = discovery.normalize_completed_auction_candidate(
        {
            "url": "https://bringatrailer.com/listing/test-listing/?utm_source=feed#comments",
            "title": " 2004 BMW M3 Coupe ",
            "timestamp_end": _timestamp("2026-04-20"),
            "country_code": "USA",
        }
    )

    assert candidate == {
        "source_site": "bringatrailer",
        "listing_id": "test-listing",
        "source_listing_id": "test-listing",
        "url": "https://bringatrailer.com/listing/test-listing/",
        "title": "2004 BMW M3 Coupe",
        "auction_end_date": "2026-04-20",
        "source_location": "USA",
    }


def test_normalize_completed_auction_candidate_allows_missing_optional_metadata():
    candidate = discovery.normalize_completed_auction_candidate(
        {"url": "/listing/test-listing/"}
    )

    assert candidate == {
        "source_site": "bringatrailer",
        "listing_id": "test-listing",
        "source_listing_id": "test-listing",
        "url": "https://bringatrailer.com/listing/test-listing/",
    }


def test_evaluate_discovery_eligibility_accepts_in_scope_us_car_listing_id():
    assert discovery.evaluate_discovery_eligibility("2004-bmw-m3-coupe", "US") == (True, None)


def test_evaluate_discovery_eligibility_rejects_missing_or_unparseable_year():
    assert discovery.evaluate_discovery_eligibility("bmw-m3-coupe", "US") == (
        False,
        "listing ID year missing",
    )


def test_evaluate_discovery_eligibility_rejects_pre_1946_listing_id():
    assert discovery.evaluate_discovery_eligibility("1941-ford-super-deluxe-coupe", "US") == (
        False,
        "year before 1946",
    )


def test_evaluate_discovery_eligibility_rejects_non_us_listing():
    assert discovery.evaluate_discovery_eligibility("2004-bmw-m3-coupe", "CA") == (
        False,
        "listing outside US",
    )


@pytest.mark.parametrize(
    "listing_id",
    [
        "2004-harley-davidson-motorcycle",
        "1967-porsche-911-literature-collection",
        "1989-polaris-atv",
        "1967-ford-f-250-fire-truck",
    ],
)
def test_evaluate_discovery_eligibility_keeps_valid_year_and_location_listing_ids_in_scope(listing_id):
    assert discovery.evaluate_discovery_eligibility(listing_id, "US") == (True, None)


def test_evaluate_discovery_eligibility_uses_listing_id_when_title_has_no_year():
    assert discovery.evaluate_discovery_eligibility("2010-am-general-hmmwv-military-5", "US") == (
        True,
        None,
    )


def test_discover_completed_auctions_returns_summary_counts_across_pages(mocker):
    fetch_completed_auctions_page = mocker.patch(
        "app.sources.bat.discovery.fetch_completed_auctions_page",
        side_effect=[
            {
                "items": [
                    _item("new-car", "2026-04-20", "New Car"),
                    _item("existing-car", "2026-04-20", "Existing Car"),
                ]
            },
            {"items": [_item("broken-car", "2026-04-20", "Broken Car")]},
            {"items": []},
        ],
    )
    save_discovered_listing = mocker.patch(
        "app.sources.bat.discovery.save_discovered_listing",
        side_effect=[True, False, RuntimeError("boom")],
    )

    summary = discovery.discover_completed_auctions(
        scrape_date=date(2026, 4, 20),
    )

    assert summary == discovery.DiscoverySummary(
        candidates_inspected=3,
        newly_discovered=1,
        already_discovered_or_updated=1,
        failed=1,
    )
    assert [call.args[0] for call in fetch_completed_auctions_page.call_args_list] == [1, 2, 3]
    assert save_discovered_listing.call_count == 3


def test_discover_completed_auctions_stops_when_candidate_is_older_than_scrape_date(mocker):
    fetch_completed_auctions_page = mocker.patch(
        "app.sources.bat.discovery.fetch_completed_auctions_page",
        side_effect=[
            {
                "items": [
                    _item("newest-car", "2026-04-20"),
                    _item("same-day-car", "2026-04-20"),
                    _item("older-car", "2026-04-19"),
                ]
            },
            {"items": [_item("should-not-fetch", "2026-04-19")]},
        ],
    )
    save_discovered_listing = mocker.patch(
        "app.sources.bat.discovery.save_discovered_listing",
        return_value=True,
    )

    summary = discovery.discover_completed_auctions(
        scrape_date="2026-04-20",
    )

    assert summary == discovery.DiscoverySummary(
        candidates_inspected=3,
        newly_discovered=2,
        already_discovered_or_updated=0,
        failed=0,
    )
    assert [call.args[0]["listing_id"] for call in save_discovered_listing.call_args_list] == [
        "newest-car",
        "same-day-car",
    ]
    assert [call.args[0] for call in fetch_completed_auctions_page.call_args_list] == [1]


def test_discover_completed_auctions_marks_missing_auction_end_date_as_failed(mocker):
    mocker.patch(
        "app.sources.bat.discovery.fetch_completed_auctions_page",
        side_effect=[
            {"items": [{"url": "https://bringatrailer.com/listing/missing-date/"}]},
            {"items": [_item("in-scope", "2026-04-20")]},
            {"items": []},
        ],
    )
    save_discovered_listing = mocker.patch(
        "app.sources.bat.discovery.save_discovered_listing",
        return_value=True,
    )

    summary = discovery.discover_completed_auctions(
        scrape_date=date(2026, 4, 20),
    )

    assert summary == discovery.DiscoverySummary(
        candidates_inspected=2,
        newly_discovered=1,
        already_discovered_or_updated=0,
        failed=1,
    )
    save_discovered_listing.assert_called_once_with(
        {
            "source_site": "bringatrailer",
            "listing_id": "in-scope",
            "source_listing_id": "in-scope",
            "url": "https://bringatrailer.com/listing/in-scope/",
            "title": "In Scope",
            "auction_end_date": "2026-04-20",
            "source_location": "USA",
        }
    )


def test_discover_completed_auctions_stops_at_max_candidates_without_extra_page_fetch(mocker):
    fetch_completed_auctions_page = mocker.patch(
        "app.sources.bat.discovery.fetch_completed_auctions_page",
        side_effect=[
            {
                "items": [
                    _item("first-car", "2026-04-20"),
                    _item("second-car", "2026-04-20"),
                    _item("third-car", "2026-04-20"),
                ]
            },
            {"items": [_item("should-not-fetch", "2026-04-20")]},
        ],
    )
    save_discovered_listing = mocker.patch(
        "app.sources.bat.discovery.save_discovered_listing",
        return_value=True,
    )

    summary = discovery.discover_completed_auctions(
        scrape_date="2026-04-20",
        max_candidates=2,
    )

    assert summary == discovery.DiscoverySummary(
        candidates_inspected=2,
        newly_discovered=2,
        already_discovered_or_updated=0,
        failed=0,
    )
    assert [call.args[0]["listing_id"] for call in save_discovered_listing.call_args_list] == [
        "first-car",
        "second-car",
    ]
    assert [call.args[0] for call in fetch_completed_auctions_page.call_args_list] == [1]


def test_discover_completed_auctions_stops_when_endpoint_returns_no_items(mocker):
    fetch_completed_auctions_page = mocker.patch(
        "app.sources.bat.discovery.fetch_completed_auctions_page",
        side_effect=[
            {"items": [_item("first-car", "2026-04-20")]},
            {"items": []},
        ],
    )
    save_discovered_listing = mocker.patch(
        "app.sources.bat.discovery.save_discovered_listing",
        return_value=True,
    )

    summary = discovery.discover_completed_auctions(
        scrape_date="2026-04-20",
    )

    assert summary == discovery.DiscoverySummary(
        candidates_inspected=1,
        newly_discovered=1,
        already_discovered_or_updated=0,
        failed=0,
    )
    assert [call.args[0] for call in fetch_completed_auctions_page.call_args_list] == [1, 2]
    save_discovered_listing.assert_called_once()


def test_discover_completed_auctions_counts_normalization_failures_and_continues(mocker):
    mocker.patch(
        "app.sources.bat.discovery.fetch_completed_auctions_page",
        side_effect=[
            {
                "items": [
                    {"url": "https://example.com/not-bat/"},
                    _item("valid-car", "2026-04-20"),
                ]
            },
            {"items": []},
        ],
    )
    save_discovered_listing = mocker.patch(
        "app.sources.bat.discovery.save_discovered_listing",
        return_value=True,
    )

    summary = discovery.discover_completed_auctions(scrape_date="2026-04-20")

    assert summary == discovery.DiscoverySummary(
        candidates_inspected=1,
        newly_discovered=1,
        already_discovered_or_updated=0,
        failed=1,
    )
    save_discovered_listing.assert_called_once()


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

        def fetchone(self):
            return (True,)

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


def test_load_pending_discovered_listings_selects_pending_bat_rows_in_stable_order(mocker):
    calls = {"executions": [], "cursor_kwargs": []}
    expected_rows = [
        {
            "id": 1,
            "source_site": "bringatrailer",
            "source_listing_id": "first-listing",
            "url": "https://bringatrailer.com/listing/first-listing/",
            "title": "First Listing",
            "auction_end_date": date(2026, 3, 30),
            "source_location": "USA",
            "eligible": None,
            "eligibility_reason": None,
            "discovered_at": datetime(2026, 4, 20, 8, 0, tzinfo=timezone.utc),
            "last_seen_at": datetime(2026, 4, 20, 8, 0, tzinfo=timezone.utc),
            "ingested_at": None,
        }
    ]

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params):
            calls["executions"].append((sql, params))

        def fetchall(self):
            return expected_rows

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self, **kwargs):
            calls["cursor_kwargs"].append(kwargs)
            return FakeCursor()

    mocker.patch.dict(
        "os.environ",
        {"DATABASE_URL": "postgresql://user:pass@localhost/db"},
    )
    mocker.patch.object(discovery.psycopg, "connect", return_value=FakeConnection())

    rows = discovery.load_pending_discovered_listings()

    sql, params = calls["executions"][0]
    assert rows == expected_rows
    assert calls["cursor_kwargs"] == [{"row_factory": discovery.dict_row}]
    assert "FROM discovered_listings" in sql
    assert "WHERE source_site = %(source_site)s" in sql
    assert "AND ingested_at IS NULL" in sql
    assert "ORDER BY discovered_at ASC, id ASC" in sql
    assert "LIMIT %(limit)s" not in sql
    assert params == {"source_site": "bringatrailer"}


def test_load_pending_discovered_listings_applies_optional_limit(mocker):
    calls = {"executions": []}

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params):
            calls["executions"].append((sql, params))

        def fetchall(self):
            return []

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self, **kwargs):
            return FakeCursor()

    mocker.patch.dict(
        "os.environ",
        {"DATABASE_URL": "postgresql://user:pass@localhost/db"},
    )
    mocker.patch.object(discovery.psycopg, "connect", return_value=FakeConnection())

    assert discovery.load_pending_discovered_listings(limit=2) == []

    sql, params = calls["executions"][0]
    assert "LIMIT %(limit)s" in sql
    assert params == {"source_site": "bringatrailer", "limit": 2}


def test_load_pending_discovered_listings_returns_empty_without_query_for_non_positive_limit(mocker):
    connect = mocker.patch.object(discovery.psycopg, "connect")
    mocker.patch.dict(
        "os.environ",
        {"DATABASE_URL": "postgresql://user:pass@localhost/db"},
    )

    assert discovery.load_pending_discovered_listings(limit=0) == []
    connect.assert_not_called()


def test_mark_discovered_listing_handled_ineligible_updates_state(mocker):
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

    mocker.patch.dict(
        "os.environ",
        {"DATABASE_URL": "postgresql://user:pass@localhost/db"},
    )
    mocker.patch.object(discovery.psycopg, "connect", return_value=FakeConnection())

    discovery.mark_discovered_listing_handled_ineligible(
        "test-listing",
        "auction did not meet eligibility rules",
    )

    sql, params = calls["executions"][0]
    assert "UPDATE discovered_listings" in sql
    assert "SET ingested_at = NOW()" in sql
    assert "eligible = FALSE" in sql
    assert "eligibility_reason = %(eligibility_reason)s" in sql
    assert params == {
        "source_site": "bringatrailer",
        "source_listing_id": "test-listing",
        "eligibility_reason": "auction did not meet eligibility rules",
    }


def test_mark_discovered_listing_handled_eligible_updates_state(mocker):
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

    mocker.patch.dict(
        "os.environ",
        {"DATABASE_URL": "postgresql://user:pass@localhost/db"},
    )
    mocker.patch.object(discovery.psycopg, "connect", return_value=FakeConnection())

    discovery.mark_discovered_listing_handled_eligible("test-listing")

    sql, params = calls["executions"][0]
    assert "UPDATE discovered_listings" in sql
    assert "SET ingested_at = NOW()" in sql
    assert "eligible = TRUE" in sql
    assert "eligibility_reason = NULL" in sql
    assert params == {
        "source_site": "bringatrailer",
        "source_listing_id": "test-listing",
    }


def _candidate():
    return {
        "listing_id": "test-listing",
        "url": "https://bringatrailer.com/listing/test-listing/",
        "title": "2004 BMW M3 Coupe",
        "auction_end_date": "2026-03-30",
        "source_location": "USA",
    }


def _item(listing_id, auction_end_date, title=None, country_code="USA"):
    return {
        "url": f"https://bringatrailer.com/listing/{listing_id}/",
        "title": title or listing_id.replace("-", " ").title(),
        "timestamp_end": _timestamp(auction_end_date),
        "country_code": country_code,
        "pages_total": 999,
    }


def _timestamp(iso_date):
    return int(
        datetime.fromisoformat(f"{iso_date}T12:00:00+00:00")
        .astimezone(timezone.utc)
        .timestamp()
    )
