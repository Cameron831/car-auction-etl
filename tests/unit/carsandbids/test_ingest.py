import json
import logging
from pathlib import Path

import pytest
from psycopg.types.json import Jsonb

from app.sources.carsandbids import ingest
from app.sources.carsandbids.browser import CARSANDBIDS_CHROME_USER_AGENT
from app.sources.carsandbids.ingest import (
    build_listing_url,
    evaluate_listing_eligibility,
    fetch_listing_json,
    save_listing_json,
)


def _eligible_payload(status="sold", listing=None):
    payload = {
        "status": status,
        "listing": {
            "year": 2013,
            "make": "Porsche",
            "model": "911",
            "is_not_car": False,
        },
    }
    if listing:
        payload["listing"].update(listing)
    return payload


def test_build_listing_url_returns_public_auction_url():
    assert (
        build_listing_url("test-auction")
        == "https://carsandbids.com/auctions/test-auction"
    )


def test_evaluate_listing_eligibility_accepts_fixture_payload():
    payload = json.loads(
        Path("tests/fixtures/carsandbids_listing.json").read_text(encoding="utf-8")
    )

    assert evaluate_listing_eligibility(payload) == (True, None)


@pytest.mark.parametrize(
    ("payload", "expected_reason"),
    [
        (
            _eligible_payload(listing={"is_not_car": True}),
            "listing marked not car",
        ),
        (_eligible_payload(listing={"model": "Kart"}), "excluded model: Kart"),
        (
            _eligible_payload(listing={"make": "Military Vehicle"}),
            "excluded make: Military Vehicle",
        ),
        (_eligible_payload(status="canceled"), "listing canceled"),
        (_eligible_payload(listing={"year": None}), "listing year missing"),
        (_eligible_payload(listing={"year": "unknown"}), "listing year missing"),
        (_eligible_payload(listing={"year": 1945}), "year before 1946"),
    ],
)
def test_evaluate_listing_eligibility_rejects_ineligible_payloads(
    payload, expected_reason
):
    assert evaluate_listing_eligibility(payload) == (False, expected_reason)


@pytest.mark.parametrize(
    ("field_name", "field_value", "expected_reason"),
    [
        ("model", "  GOLF   CART  ", "excluded model:   GOLF   CART  "),
        ("make", "  OTHER  ", "excluded make:   OTHER  "),
        ("status", "  CANCELED  ", "listing canceled"),
    ],
)
def test_evaluate_listing_eligibility_normalizes_exact_excluded_values(
    field_name, field_value, expected_reason
):
    if field_name == "status":
        payload = _eligible_payload(status=field_value)
    else:
        payload = _eligible_payload(listing={field_name: field_value})

    assert evaluate_listing_eligibility(payload) == (False, expected_reason)


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("model", "customized"),
        ("model", "golf cart trailer"),
        ("make", "other manufacturer"),
        ("make", "military vehicle parts"),
    ],
)
def test_evaluate_listing_eligibility_allows_partial_excluded_values(
    field_name, field_value
):
    payload = _eligible_payload(listing={field_name: field_value})

    assert evaluate_listing_eligibility(payload) == (True, None)


def test_evaluate_listing_eligibility_applies_rejection_order():
    payload = _eligible_payload(
        status="canceled",
        listing={
            "is_not_car": True,
            "model": "Kart",
            "make": "Other",
            "year": 1945,
        },
    )

    assert evaluate_listing_eligibility(payload) == (False, "listing marked not car")


@pytest.mark.parametrize(
    ("payload", "expected_reason"),
    [
        (
            _eligible_payload(listing={"model": "Replica", "make": "Other"}),
            "excluded model: Replica",
        ),
        (
            _eligible_payload(
                status="canceled",
                listing={"make": "Other", "year": 1945},
            ),
            "excluded make: Other",
        ),
        (
            _eligible_payload(status="canceled", listing={"year": "unknown"}),
            "listing canceled",
        ),
    ],
)
def test_evaluate_listing_eligibility_applies_adjacent_rejection_order(
    payload, expected_reason
):
    assert evaluate_listing_eligibility(payload) == (False, expected_reason)


def test_evaluate_listing_eligibility_allows_truthy_non_boolean_is_not_car():
    payload = _eligible_payload(listing={"is_not_car": "true"})

    assert evaluate_listing_eligibility(payload) == (True, None)


def test_fetch_listing_json_captures_matching_response(mocker, caplog):
    payload = {"id": "test-auction", "title": "Test Listing"}
    response = FakeResponse(
        "https://carsandbids.com/v2/autos/auctions/test-auction?include=seller",
        payload=payload,
    )
    page = FakePage([response])
    playwright = FakePlaywright(page)
    mocker.patch.object(
        ingest, "sync_playwright", return_value=FakePlaywrightContext(playwright)
    )

    caplog.set_level(logging.INFO)
    result = fetch_listing_json("test-auction")

    assert result == payload
    assert page.goto_calls == [
        ("https://carsandbids.com/auctions/test-auction", "domcontentloaded", 60000)
    ]
    assert playwright.chromium.launch_calls == [{"headless": True}]
    assert playwright.chromium.browser.new_context_calls == [
        {"user_agent": CARSANDBIDS_CHROME_USER_AGENT}
    ]
    assert playwright.chromium.browser.context.new_page_calls == 1
    assert (
        "Fetching Cars and Bids listing JSON for listing_id=test-auction"
        in caplog.text
    )
    assert "Fetched Cars and Bids listing JSON for listing_id=test-auction" in caplog.text
    assert "Test Listing" not in caplog.text


def test_fetch_listing_json_raises_when_matching_response_is_absent(mocker):
    response = FakeResponse("https://carsandbids.com/v2/autos/auctions/other-auction")
    playwright = FakePlaywright(FakePage([response]))
    mocker.patch.object(
        ingest, "sync_playwright", return_value=FakePlaywrightContext(playwright)
    )

    with pytest.raises(RuntimeError, match="API response not found"):
        fetch_listing_json("test-auction")


def test_fetch_listing_json_raises_when_response_is_not_ok(mocker):
    response = FakeResponse(
        "https://carsandbids.com/v2/autos/auctions/test-auction",
        ok=False,
        status=500,
    )
    playwright = FakePlaywright(FakePage([response]))
    mocker.patch.object(
        ingest, "sync_playwright", return_value=FakePlaywrightContext(playwright)
    )

    with pytest.raises(RuntimeError, match="API response failed.*status=500"):
        fetch_listing_json("test-auction")


def test_fetch_listing_json_raises_when_json_is_invalid(mocker):
    response = FakeResponse(
        "https://carsandbids.com/v2/autos/auctions/test-auction",
        json_error=ValueError("invalid json"),
    )
    playwright = FakePlaywright(FakePage([response]))
    mocker.patch.object(
        ingest, "sync_playwright", return_value=FakePlaywrightContext(playwright)
    )

    with pytest.raises(RuntimeError, match="Invalid Cars and Bids API JSON"):
        fetch_listing_json("test-auction")


def test_build_raw_listing_json_params_maps_listing_to_schema_columns():
    payload = {"id": "test-auction"}
    params = ingest.build_raw_listing_json_params(
        "test-auction",
        payload,
        "https://example.test/auctions/test-auction",
    )

    assert params["source_site"] == "carsandbids"
    assert params["source_listing_id"] == "test-auction"
    assert params["url"] == "https://example.test/auctions/test-auction"
    assert isinstance(params["raw_json"], Jsonb)
    assert params["raw_json"].obj == payload


def test_build_raw_listing_json_params_defaults_public_listing_url():
    params = ingest.build_raw_listing_json_params("test-auction", {"id": "test-auction"})

    assert params["url"] == "https://carsandbids.com/auctions/test-auction"


def test_save_listing_json_executes_upsert_with_expected_conflict_target(
    mocker, caplog
):
    calls = {"execute": []}
    payload = {"id": "test-auction", "secret": "payload-content"}

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params):
            calls["execute"].append((sql, params))

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
    mocker.patch.object(ingest.psycopg, "connect", side_effect=fake_connect)

    caplog.set_level(logging.INFO)
    save_listing_json(
        "test-auction",
        payload,
        "https://example.test/auctions/test-auction",
    )

    assert calls["database_url"] == "postgresql://user:pass@localhost/db"
    assert len(calls["execute"]) == 2
    upsert_sql, upsert_params = calls["execute"][0]
    marker_sql, marker_params = calls["execute"][1]
    assert "ON CONFLICT (source_site, source_listing_id) DO UPDATE" in upsert_sql
    assert "url = EXCLUDED.url" in upsert_sql
    assert "raw_json = EXCLUDED.raw_json" in upsert_sql
    assert "processed = FALSE" in upsert_sql
    assert "UPDATE discovered_listings" in marker_sql
    assert "SET ingested_at = NOW()" in marker_sql
    assert "source_site = %(source_site)s" in marker_sql
    assert "source_listing_id = %(source_listing_id)s" in marker_sql
    assert "eligible" not in marker_sql
    assert "eligibility_reason" not in marker_sql
    assert upsert_params["source_site"] == "carsandbids"
    assert upsert_params["source_listing_id"] == "test-auction"
    assert upsert_params["url"] == "https://example.test/auctions/test-auction"
    assert "eligible" not in upsert_params
    assert "eligibility_reason" not in upsert_params
    assert isinstance(upsert_params["raw_json"], Jsonb)
    assert upsert_params["raw_json"].obj == payload
    assert marker_params == upsert_params
    assert (
        "Saved Cars and Bids raw listing JSON for listing_id=test-auction"
        in caplog.text
    )
    assert "payload-content" not in caplog.text
    assert "postgresql://user:pass@localhost/db" not in caplog.text


def test_save_listing_json_requires_database_url(mocker):
    mocker.patch.dict("os.environ", {}, clear=True)

    with pytest.raises(RuntimeError, match="DATABASE_URL must be set"):
        save_listing_json("test-auction", {"id": "test-auction"})


class FakeResponse:
    def __init__(self, url, payload=None, ok=True, status=200, json_error=None):
        self.url = url
        self.payload = payload or {"id": "test-auction"}
        self.ok = ok
        self.status = status
        self.json_error = json_error

    def json(self):
        if self.json_error is not None:
            raise self.json_error
        return self.payload


class FakePage:
    def __init__(self, responses):
        self.responses = responses
        self.response_handler = None
        self.goto_calls = []

    def on(self, event_name, handler):
        assert event_name == "response"
        self.response_handler = handler

    def goto(self, url, wait_until, timeout):
        self.goto_calls.append((url, wait_until, timeout))
        for response in self.responses:
            self.response_handler(response)

    def wait_for_event(self, event_name, predicate, timeout):
        assert event_name == "response"
        assert timeout == 15000
        for response in self.responses:
            if predicate(response):
                return response
        raise TimeoutError("response not found")


class FakeBrowser:
    def __init__(self, page):
        self.page = page
        self.context = FakeBrowserContext(page)
        self.new_context_calls = []
        self.closed = False

    def new_context(self, user_agent):
        self.new_context_calls.append({"user_agent": user_agent})
        return self.context

    def close(self):
        self.closed = True


class FakeBrowserContext:
    def __init__(self, page):
        self.page = page
        self.new_page_calls = 0

    def new_page(self):
        self.new_page_calls += 1
        return self.page


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
