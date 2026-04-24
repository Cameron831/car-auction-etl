import logging
import os

import psycopg
from playwright.sync_api import sync_playwright
from psycopg.types.json import Jsonb


SOURCE_SITE = "carsandbids"
LISTING_URL_BASE = "https://carsandbids.com/auctions"
API_AUCTION_URL_BASE = "https://carsandbids.com/v2/autos/auctions"
PAGE_LOAD_TIMEOUT_MS = 60_000
API_RESPONSE_TIMEOUT_MS = 15_000
logger = logging.getLogger(__name__)

UPSERT_RAW_LISTING_JSON_SQL = """
INSERT INTO raw_listing_json (
    source_site,
    source_listing_id,
    url,
    raw_json,
    processed
) VALUES (
    %(source_site)s,
    %(source_listing_id)s,
    %(url)s,
    %(raw_json)s,
    FALSE
)
ON CONFLICT (source_site, source_listing_id) DO UPDATE SET
    url = EXCLUDED.url,
    raw_json = EXCLUDED.raw_json,
    processed = FALSE
"""


def build_listing_url(listing_id):
    return f"{LISTING_URL_BASE}/{listing_id}"


def fetch_listing_json(listing_id):
    listing_url = build_listing_url(listing_id)
    api_url_prefix = f"{API_AUCTION_URL_BASE}/{listing_id}"
    matched_response = None

    def is_matching_response(response):
        return response.url.startswith(api_url_prefix)

    def capture_matching_response(response):
        nonlocal matched_response
        if matched_response is None and is_matching_response(response):
            matched_response = response

    logger.info("Fetching Cars and Bids listing JSON for listing_id=%s", listing_id)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.on("response", capture_matching_response)
            page.goto(
                listing_url,
                wait_until="domcontentloaded",
                timeout=PAGE_LOAD_TIMEOUT_MS,
            )
            if matched_response is None:
                try:
                    matched_response = page.wait_for_event(
                        "response",
                        predicate=is_matching_response,
                        timeout=API_RESPONSE_TIMEOUT_MS,
                    )
                except Exception as exc:
                    raise RuntimeError(
                        "Cars and Bids API response not found "
                        f"for listing_id={listing_id}"
                    ) from exc

            if matched_response is None:
                raise RuntimeError(
                    f"Cars and Bids API response not found for listing_id={listing_id}"
                )
            if not matched_response.ok:
                raise RuntimeError(
                    "Cars and Bids API response failed "
                    f"for listing_id={listing_id} status={matched_response.status}"
                )
            try:
                payload = matched_response.json()
            except Exception as exc:
                raise RuntimeError(
                    f"Invalid Cars and Bids API JSON for listing_id={listing_id}"
                ) from exc
        finally:
            browser.close()

    logger.info("Fetched Cars and Bids listing JSON for listing_id=%s", listing_id)
    return payload


def build_raw_listing_json_params(listing_id, payload, url=None):
    return {
        "source_site": SOURCE_SITE,
        "source_listing_id": listing_id,
        "url": url or build_listing_url(listing_id),
        "raw_json": Jsonb(payload),
    }


def save_listing_json(listing_id, payload, url=None):
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL must be set")

    params = build_raw_listing_json_params(listing_id, payload, url)

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(UPSERT_RAW_LISTING_JSON_SQL, params)
            logger.info(
                "Saved Cars and Bids raw listing JSON for listing_id=%s", listing_id
            )
