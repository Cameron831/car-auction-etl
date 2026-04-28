import logging
import json
from datetime import date

import pytest

from app.sources.carsandbids import discovery


def test_normalize_completed_auction_candidate_maps_endpoint_auction():
    candidate = discovery.normalize_completed_auction_candidate(
        {
            "id": "3gNQk4RZ",
            "title": " 2004 BMW M3 Coupe ",
            "auction_end": "2026-04-20T18:30:00Z",
        }
    )

    assert candidate == {
        "source_site": "carsandbids",
        "listing_id": "3gNQk4RZ",
        "source_listing_id": "3gNQk4RZ",
        "url": "https://carsandbids.com/auctions/3gNQk4RZ",
        "title": "2004 BMW M3 Coupe",
        "auction_end_date": "2026-04-20",
    }


def test_normalize_completed_auction_candidate_parses_offset_auction_end():
    candidate = discovery.normalize_completed_auction_candidate(
        {
            "id": "3gNQk4RZ",
            "auction_end": "2026-04-20T18:30:00+00:00",
        }
    )

    assert candidate["auction_end_date"] == "2026-04-20"


def test_normalize_completed_auction_candidate_requires_id():
    with pytest.raises(ValueError, match="auction id is required"):
        discovery.normalize_completed_auction_candidate({"title": "Missing ID"})

    with pytest.raises(ValueError, match="auction id is required"):
        discovery.normalize_completed_auction_candidate({"id": "   "})


def test_normalize_completed_auction_candidate_requires_object():
    with pytest.raises(ValueError, match="auction must be an object"):
        discovery.normalize_completed_auction_candidate("not-an-object")


def test_extract_auctions_requires_list():
    assert discovery._extract_auctions({"auctions": []}) == []

    with pytest.raises(ValueError, match="auctions must be a list"):
        discovery._extract_auctions({"auctions": {"id": "not-a-list"}})


def test_capture_initial_completed_auctions_page_captures_signed_response(
    mocker, caplog
):
    payload = {"auctions": [_auction("first-car", "2026-04-20")]}
    response = FakeResponse(
        "https://carsandbids.com/v2/autos/auctions?limit=50&status=closed"
        "&timestamp=opaque-ts&signature=opaque-sig",
        payload=payload,
    )
    page = FakePage([response])
    playwright = FakePlaywright(page)
    mocker.patch.object(
        discovery, "sync_playwright", return_value=FakePlaywrightContext(playwright)
    )

    caplog.set_level(logging.INFO)
    captured_payload, timestamp, signature = (
        discovery.capture_initial_completed_auctions_page()
    )

    assert captured_payload == payload
    assert timestamp == "opaque-ts"
    assert signature == "opaque-sig"
    assert page.goto_calls == [
        ("https://carsandbids.com/past-auctions/", "domcontentloaded", 60000)
    ]
    assert playwright.chromium.launch_calls == [{"headless": True}]
    assert "Capturing initial Cars and Bids completed auctions page" in caplog.text
    assert "Captured initial Cars and Bids completed auctions page" in caplog.text


def test_capture_initial_completed_auctions_page_waits_for_late_matching_response(
    mocker,
):
    matching_response = FakeResponse(
        "https://carsandbids.com/v2/autos/auctions?status=closed"
        "&timestamp=late-ts&signature=late-sig",
        payload={"auctions": []},
    )
    page = FakePage([], wait_response=matching_response)
    mocker.patch.object(
        discovery,
        "sync_playwright",
        return_value=FakePlaywrightContext(FakePlaywright(page)),
    )

    payload, timestamp, signature = discovery.capture_initial_completed_auctions_page()

    assert payload == {"auctions": []}
    assert timestamp == "late-ts"
    assert signature == "late-sig"
    assert page.wait_for_event_calls == [("response", 15000)]


def test_capture_initial_completed_auctions_page_raises_when_response_is_absent(
    mocker,
):
    page = FakePage(
        [
            FakeResponse(
                "https://carsandbids.com/v2/autos/auctions?status=active"
                "&timestamp=ts&signature=sig"
            )
        ]
    )
    mocker.patch.object(
        discovery,
        "sync_playwright",
        return_value=FakePlaywrightContext(FakePlaywright(page)),
    )

    with pytest.raises(RuntimeError, match="API response not found"):
        discovery.capture_initial_completed_auctions_page()


def test_capture_initial_completed_auctions_page_raises_when_response_is_not_ok(
    mocker,
):
    response = FakeResponse(
        "https://carsandbids.com/v2/autos/auctions?status=closed"
        "&timestamp=ts&signature=sig",
        ok=False,
        status=500,
    )
    mocker.patch.object(
        discovery,
        "sync_playwright",
        return_value=FakePlaywrightContext(FakePlaywright(FakePage([response]))),
    )

    with pytest.raises(RuntimeError, match="API response failed status=500"):
        discovery.capture_initial_completed_auctions_page()


def test_capture_initial_completed_auctions_page_raises_for_missing_signature(
    mocker,
):
    response = FakeResponse(
        "https://carsandbids.com/v2/autos/auctions?status=closed&timestamp=ts",
        payload={"auctions": []},
    )
    mocker.patch.object(
        discovery,
        "sync_playwright",
        return_value=FakePlaywrightContext(FakePlaywright(FakePage([response]))),
    )

    with pytest.raises(RuntimeError, match="missing signed request parameters"):
        discovery.capture_initial_completed_auctions_page()


def test_capture_initial_completed_auctions_page_raises_for_invalid_json(mocker):
    response = FakeResponse(
        "https://carsandbids.com/v2/autos/auctions?status=closed"
        "&timestamp=ts&signature=sig",
        json_error=ValueError("invalid"),
    )
    mocker.patch.object(
        discovery,
        "sync_playwright",
        return_value=FakePlaywrightContext(FakePlaywright(FakePage([response]))),
    )

    with pytest.raises(RuntimeError, match="Invalid Cars and Bids"):
        discovery.capture_initial_completed_auctions_page()


def test_fetch_completed_auctions_page_uses_signed_endpoint_params(mocker, caplog):
    page = FakePage(
        [],
        evaluate_responses=[
            {"ok": True, "status": 200, "text": _json({"auctions": [_auction("next-car", "2026-04-20")]})}
        ],
    )

    caplog.set_level(logging.INFO)
    payload = discovery.fetch_completed_auctions_page(
        page,
        50,
        "opaque-ts",
        "opaque-sig",
    )

    assert payload == {"auctions": [_auction("next-car", "2026-04-20")]}
    assert page.evaluate_calls == [
        (
            {
                "url": "https://carsandbids.com/v2/autos/auctions",
                "params": {
                    "limit": 50,
                    "status": "closed",
                    "offset": 50,
                    "timestamp": "opaque-ts",
                    "signature": "opaque-sig",
                },
            }
        )
    ]
    assert "Fetching Cars and Bids completed auctions offset=50" in caplog.text
    assert "Fetched Cars and Bids completed auctions offset=50 auctions=1" in caplog.text


def test_fetch_completed_auctions_page_raises_when_response_is_not_ok():
    page = FakePage(
        [],
        evaluate_responses=[
            {"ok": False, "status": 403, "text": '{"message":"blocked"}'}
        ],
    )

    with pytest.raises(RuntimeError, match='status=403 body=\\{"message":"blocked"\\}'):
        discovery.fetch_completed_auctions_page(page, 50, "ts", "sig")


def test_fetch_completed_auctions_page_validates_auctions_collection():
    page = FakePage(
        [],
        evaluate_responses=[
            {"ok": True, "status": 200, "text": _json({"auctions": {"id": "not-a-list"}})}
        ],
    )

    with pytest.raises(ValueError, match="auctions must be a list"):
        discovery.fetch_completed_auctions_page(page, 50, "ts", "sig")


def test_discover_completed_auctions_processes_initial_page_then_followups(mocker):
    page = FakePage(
        [
            FakeResponse(
                "https://carsandbids.com/v2/autos/auctions?limit=50&status=closed"
                "&timestamp=ts&signature=sig",
                payload={
                    "auctions": [
                        _auction("new-car", "2026-04-20"),
                        _auction("existing-car", "2026-04-20"),
                    ]
                },
            )
        ],
        evaluate_responses=[
            {
                "ok": True,
                "status": 200,
                "text": _json({"auctions": [_auction("broken-save", "2026-04-20")]}),
            },
            {"ok": True, "status": 200, "text": _json({"auctions": []})},
        ],
    )
    mocker.patch.object(
        discovery,
        "sync_playwright",
        return_value=FakePlaywrightContext(FakePlaywright(page)),
    )
    save_listing = mocker.patch(
        "app.sources.carsandbids.discovery.save_discovered_listing",
        side_effect=[True, False, RuntimeError("boom")],
    )

    summary = discovery.discover_completed_auctions(scrape_date=date(2026, 4, 20))

    assert summary == discovery.DiscoverySummary(
        candidates_inspected=3,
        newly_discovered=1,
        already_discovered_or_updated=1,
        failed=1,
    )
    assert [call["params"]["offset"] for call in page.evaluate_call_args] == [50, 100]
    assert [call.args[0]["listing_id"] for call in save_listing.call_args_list] == [
        "new-car",
        "existing-car",
        "broken-save",
    ]


def test_discover_completed_auctions_uses_page_fetch_for_followups(mocker):
    page = FakePage(
        [
            FakeResponse(
                "https://carsandbids.com/v2/autos/auctions?status=closed"
                "&timestamp=ts&signature=sig",
                payload={"auctions": [_auction("first-car", "2026-04-20")]},
            )
        ],
        evaluate_responses=[{"ok": True, "status": 200, "text": _json({"auctions": []})}],
    )
    mocker.patch.object(
        discovery,
        "sync_playwright",
        return_value=FakePlaywrightContext(FakePlaywright(page)),
    )
    mocker.patch(
        "app.sources.carsandbids.discovery.save_discovered_listing",
        return_value=True,
    )

    discovery.discover_completed_auctions(scrape_date="2026-04-20")

    assert page.evaluate_call_args == [
        {
            "url": "https://carsandbids.com/v2/autos/auctions",
            "params": {
                "limit": 50,
                "status": "closed",
                "offset": 50,
                "timestamp": "ts",
                "signature": "sig",
            },
        }
    ]


def test_discover_completed_auctions_stops_at_cutoff_date(mocker):
    page = FakePage(
        [
            FakeResponse(
                "https://carsandbids.com/v2/autos/auctions?status=closed"
                "&timestamp=ts&signature=sig",
                payload={
                    "auctions": [
                        _auction("newest-car", "2026-04-20"),
                        _auction("same-day-car", "2026-04-20"),
                        _auction("older-car", "2026-04-19"),
                    ]
                },
            )
        ]
    )
    mocker.patch.object(
        discovery,
        "sync_playwright",
        return_value=FakePlaywrightContext(FakePlaywright(page)),
    )
    save_listing = mocker.patch(
        "app.sources.carsandbids.discovery.save_discovered_listing",
        return_value=True,
    )

    summary = discovery.discover_completed_auctions(scrape_date="2026-04-20")

    assert summary == discovery.DiscoverySummary(
        candidates_inspected=3,
        newly_discovered=2,
        already_discovered_or_updated=0,
        failed=0,
    )
    assert [call.args[0]["listing_id"] for call in save_listing.call_args_list] == [
        "newest-car",
        "same-day-car",
    ]
    assert page.evaluate_call_args == []


def test_discover_completed_auctions_stops_at_max_candidates_without_followup(mocker):
    page = FakePage(
        [
            FakeResponse(
                "https://carsandbids.com/v2/autos/auctions?status=closed"
                "&timestamp=ts&signature=sig",
                payload={
                    "auctions": [
                        _auction("first-car", "2026-04-20"),
                        _auction("second-car", "2026-04-20"),
                        _auction("third-car", "2026-04-20"),
                    ]
                },
            )
        ]
    )
    mocker.patch.object(
        discovery,
        "sync_playwright",
        return_value=FakePlaywrightContext(FakePlaywright(page)),
    )
    save_listing = mocker.patch(
        "app.sources.carsandbids.discovery.save_discovered_listing",
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
    assert [call.args[0]["listing_id"] for call in save_listing.call_args_list] == [
        "first-car",
        "second-car",
    ]
    assert page.evaluate_call_args == []


def test_discover_completed_auctions_stops_when_no_auctions_returned(mocker):
    page = FakePage(
        [
            FakeResponse(
                "https://carsandbids.com/v2/autos/auctions?status=closed"
                "&timestamp=ts&signature=sig",
                payload={"auctions": [_auction("first-car", "2026-04-20")]},
            )
        ],
        evaluate_responses=[{"ok": True, "status": 200, "text": _json({"auctions": []})}],
    )
    mocker.patch.object(
        discovery,
        "sync_playwright",
        return_value=FakePlaywrightContext(FakePlaywright(page)),
    )
    save_listing = mocker.patch(
        "app.sources.carsandbids.discovery.save_discovered_listing",
        return_value=True,
    )

    summary = discovery.discover_completed_auctions(scrape_date="2026-04-20")

    assert summary == discovery.DiscoverySummary(
        candidates_inspected=1,
        newly_discovered=1,
        already_discovered_or_updated=0,
        failed=0,
    )
    assert [call["params"]["offset"] for call in page.evaluate_call_args] == [50]
    save_listing.assert_called_once()


def test_discover_completed_auctions_counts_malformed_auctions_and_continues(
    mocker,
):
    page = FakePage(
        [
            FakeResponse(
                "https://carsandbids.com/v2/autos/auctions?status=closed"
                "&timestamp=ts&signature=sig",
                payload={
                    "auctions": [
                        {"title": "Missing ID"},
                        "not-an-object",
                        _auction("valid-car", "2026-04-20"),
                    ]
                },
            )
        ],
        evaluate_responses=[{"ok": True, "status": 200, "text": _json({"auctions": []})}],
    )
    mocker.patch.object(
        discovery,
        "sync_playwright",
        return_value=FakePlaywrightContext(FakePlaywright(page)),
    )
    save_listing = mocker.patch(
        "app.sources.carsandbids.discovery.save_discovered_listing",
        return_value=True,
    )

    summary = discovery.discover_completed_auctions(scrape_date="2026-04-20")

    assert summary == discovery.DiscoverySummary(
        candidates_inspected=1,
        newly_discovered=1,
        already_discovered_or_updated=0,
        failed=2,
    )
    save_listing.assert_called_once()


def test_discover_completed_auctions_marks_missing_auction_end_date_as_failed(
    mocker,
):
    page = FakePage(
        [
            FakeResponse(
                "https://carsandbids.com/v2/autos/auctions?status=closed"
                "&timestamp=ts&signature=sig",
                payload={
                    "auctions": [
                        {"id": "missing-date"},
                        _auction("valid-car", "2026-04-20"),
                    ]
                },
            )
        ],
        evaluate_responses=[{"ok": True, "status": 200, "text": _json({"auctions": []})}],
    )
    mocker.patch.object(
        discovery,
        "sync_playwright",
        return_value=FakePlaywrightContext(FakePlaywright(page)),
    )
    save_listing = mocker.patch(
        "app.sources.carsandbids.discovery.save_discovered_listing",
        return_value=True,
    )

    summary = discovery.discover_completed_auctions(scrape_date="2026-04-20")

    assert summary == discovery.DiscoverySummary(
        candidates_inspected=2,
        newly_discovered=1,
        already_discovered_or_updated=0,
        failed=1,
    )
    save_listing.assert_called_once()


def test_discover_completed_auctions_returns_empty_summary_for_non_positive_limit(
    mocker,
):
    sync_playwright = mocker.patch.object(discovery, "sync_playwright")

    assert discovery.discover_completed_auctions(
        scrape_date="2026-04-20",
        max_candidates=0,
    ) == discovery.DiscoverySummary()
    sync_playwright.assert_not_called()


def test_build_discovered_listing_params_maps_candidate_to_schema_columns():
    params = discovery.build_discovered_listing_params(
        {
            "listing_id": "3gNQk4RZ",
            "url": "https://carsandbids.com/auctions/3gNQk4RZ",
            "title": "2004 BMW M3 Coupe",
            "auction_end_date": "2026-04-20",
        }
    )

    assert params == {
        "source_site": "carsandbids",
        "source_listing_id": "3gNQk4RZ",
        "url": "https://carsandbids.com/auctions/3gNQk4RZ",
        "title": "2004 BMW M3 Coupe",
        "auction_end_date": "2026-04-20",
    }


def test_save_discovered_listing_executes_source_generic_upsert(mocker, caplog):
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

    mocker.patch.dict(
        "os.environ",
        {"DATABASE_URL": "postgresql://user:pass@localhost/db"},
    )
    mocker.patch.object(discovery.psycopg, "connect", return_value=FakeConnection())

    caplog.set_level(logging.INFO)
    inserted = discovery.save_discovered_listing(
        {
            "listing_id": "3gNQk4RZ",
            "url": "https://carsandbids.com/auctions/3gNQk4RZ",
            "title": "2004 BMW M3 Coupe",
            "auction_end_date": "2026-04-20",
        }
    )

    sql, params = calls["executions"][0]
    assert inserted is True
    assert "INSERT INTO discovered_listings" in sql
    assert "ON CONFLICT (source_site, source_listing_id) DO UPDATE" in sql
    assert "url = EXCLUDED.url" in sql
    assert "title = EXCLUDED.title" in sql
    assert "auction_end_date = EXCLUDED.auction_end_date" in sql
    assert "source_location" not in sql
    assert "last_seen_at = NOW()" in sql
    assert "eligible" not in sql
    assert "eligibility_reason" not in sql
    assert "ingested_at" not in sql
    assert params == {
        "source_site": "carsandbids",
        "source_listing_id": "3gNQk4RZ",
        "url": "https://carsandbids.com/auctions/3gNQk4RZ",
        "title": "2004 BMW M3 Coupe",
        "auction_end_date": "2026-04-20",
    }
    assert "Upserted Cars and Bids discovered listing for listing_id=3gNQk4RZ" in caplog.text
    assert "postgresql://user:pass@localhost/db" not in caplog.text


def test_save_discovered_listing_requires_database_url(mocker):
    mocker.patch.dict("os.environ", {}, clear=True)

    with pytest.raises(RuntimeError, match="DATABASE_URL must be set"):
        discovery.save_discovered_listing(
            {
                "listing_id": "3gNQk4RZ",
                "url": "https://carsandbids.com/auctions/3gNQk4RZ",
            }
        )


def test_load_pending_discovered_listings_executes_pending_query(mocker):
    calls = {"executions": [], "row_factory": None}
    rows = [
        {
            "source_site": "carsandbids",
            "source_listing_id": "3gNQk4RZ",
            "url": "https://carsandbids.com/auctions/3gNQk4RZ",
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
            return rows

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
    connect = mocker.patch.object(
        discovery.psycopg,
        "connect",
        return_value=FakeConnection(),
    )

    assert discovery.load_pending_discovered_listings(limit=5) == rows

    connect.assert_called_once_with("postgresql://user:pass@localhost/db")
    sql, params = calls["executions"][0]
    assert "FROM discovered_listings" in sql
    assert "WHERE source_site = %(source_site)s" in sql
    assert "AND eligible IS NULL" in sql
    assert "ORDER BY discovered_at ASC, id ASC" in sql
    assert "LIMIT %(limit)s" in sql
    assert params == {"source_site": "carsandbids", "limit": 5}
    assert calls["row_factory"] is discovery.dict_row


def test_load_pending_discovered_listings_omits_limit_when_not_requested(mocker):
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

        def cursor(self, row_factory=None):
            return FakeCursor()

    mocker.patch.dict(
        "os.environ",
        {"DATABASE_URL": "postgresql://user:pass@localhost/db"},
    )
    mocker.patch.object(discovery.psycopg, "connect", return_value=FakeConnection())

    assert discovery.load_pending_discovered_listings() == []

    sql, params = calls["executions"][0]
    assert "LIMIT %(limit)s" not in sql
    assert params == {"source_site": "carsandbids"}


def test_load_pending_discovered_listings_returns_empty_for_non_positive_limit(mocker):
    mocker.patch.dict("os.environ", {}, clear=True)
    connect = mocker.patch.object(discovery.psycopg, "connect")

    assert discovery.load_pending_discovered_listings(limit=0) == []
    assert discovery.load_pending_discovered_listings(limit=-1) == []
    connect.assert_not_called()


def test_load_pending_discovered_listings_requires_database_url(mocker):
    mocker.patch.dict("os.environ", {}, clear=True)

    with pytest.raises(RuntimeError, match="DATABASE_URL must be set"):
        discovery.load_pending_discovered_listings()


def test_mark_discovered_listing_handled_marks_eligible_and_clears_reason(mocker):
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

    discovery.mark_discovered_listing_handled("3gNQk4RZ", True, "ignored")

    sql, params = calls["executions"][0]
    assert "UPDATE discovered_listings" in sql
    assert "SET eligible = %(eligible)s" in sql
    assert "eligibility_reason = %(eligibility_reason)s" in sql
    assert "WHERE source_site = %(source_site)s" in sql
    assert "AND source_listing_id = %(source_listing_id)s" in sql
    assert params == {
        "source_site": "carsandbids",
        "source_listing_id": "3gNQk4RZ",
        "eligible": True,
        "eligibility_reason": None,
    }


def test_mark_discovered_listing_handled_marks_rejected_with_reason(mocker):
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

    discovery.mark_discovered_listing_handled(
        "3gNQk4RZ",
        False,
        "year before 1946",
    )

    _, params = calls["executions"][0]
    assert params == {
        "source_site": "carsandbids",
        "source_listing_id": "3gNQk4RZ",
        "eligible": False,
        "eligibility_reason": "year before 1946",
    }


def test_mark_discovered_listing_handled_requires_rejection_reason():
    with pytest.raises(ValueError, match="reason is required"):
        discovery.mark_discovered_listing_handled("3gNQk4RZ", False, None)

    with pytest.raises(ValueError, match="reason is required"):
        discovery.mark_discovered_listing_handled("3gNQk4RZ", False, "   ")


def test_mark_discovered_listing_handled_requires_database_url(mocker):
    mocker.patch.dict("os.environ", {}, clear=True)

    with pytest.raises(RuntimeError, match="DATABASE_URL must be set"):
        discovery.mark_discovered_listing_handled("3gNQk4RZ", True, None)


class FakeResponse:
    def __init__(
        self,
        url="https://carsandbids.com/v2/autos/auctions?status=closed",
        payload=None,
        ok=True,
        status=200,
        json_error=None,
    ):
        self.url = url
        self.payload = payload or {"auctions": []}
        self.ok = ok
        self.status = status
        self.json_error = json_error

    def json(self):
        if self.json_error is not None:
            raise self.json_error
        return self.payload


class FakePage:
    def __init__(self, responses, wait_response=None, evaluate_responses=None):
        self.responses = responses
        self.wait_response = wait_response
        self.evaluate_responses = evaluate_responses or []
        self.response_handler = None
        self.goto_calls = []
        self.wait_for_event_calls = []
        self.evaluate_calls = []
        self.evaluate_call_args = []

    def on(self, event_name, handler):
        assert event_name == "response"
        self.response_handler = handler

    def goto(self, url, wait_until, timeout):
        self.goto_calls.append((url, wait_until, timeout))
        for response in self.responses:
            self.response_handler(response)

    def wait_for_event(self, event_name, predicate, timeout):
        assert event_name == "response"
        self.wait_for_event_calls.append((event_name, timeout))
        candidates = self.responses[:]
        if self.wait_response is not None:
            candidates.append(self.wait_response)
        for response in candidates:
            if predicate(response):
                return response
        raise TimeoutError("response not found")

    def evaluate(self, script, arg):
        assert "fetch" in script
        self.evaluate_calls.append(arg)
        self.evaluate_call_args.append(arg)
        if not self.evaluate_responses:
            raise AssertionError("No fake evaluate response configured")
        return self.evaluate_responses.pop(0)


class FakeBrowser:
    def __init__(self, page):
        self.page = page
        self.closed = False

    def new_page(self):
        return self.page

    def close(self):
        self.closed = True


class FakeChromium:
    def __init__(self, page):
        self.browser = FakeBrowser(page)
        self.launch_calls = []

    def launch(self, headless):
        self.launch_calls.append({"headless": headless})
        return self.browser


class FakePlaywright:
    def __init__(self, page):
        self.chromium = FakeChromium(page)


class FakePlaywrightContext:
    def __init__(self, playwright):
        self.playwright = playwright

    def __enter__(self):
        return self.playwright

    def __exit__(self, exc_type, exc, tb):
        return False


def _auction(listing_id, auction_end_date, title=None, location="Dallas, TX"):
    return {
        "id": listing_id,
        "title": title or listing_id.replace("-", " ").title(),
        "auction_end": f"{auction_end_date}T12:00:00+00:00",
        "location": location,
    }


def _json(value):
    return json.dumps(value)
