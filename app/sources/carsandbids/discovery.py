import logging
import os
from dataclasses import dataclass
from datetime import date, datetime
from urllib.parse import parse_qs, urlparse

import psycopg
from playwright.sync_api import sync_playwright


SOURCE_SITE = "carsandbids"
PAST_AUCTIONS_URL = "https://carsandbids.com/past-auctions/"
LISTING_URL_BASE = "https://carsandbids.com/auctions"
API_AUCTIONS_URL = "https://carsandbids.com/v2/autos/auctions"
DISCOVERY_PAGE_SIZE = 50
DISCOVERY_TIMEOUT_SECONDS = 10
PAGE_LOAD_TIMEOUT_MS = 60_000
API_RESPONSE_TIMEOUT_MS = 15_000
logger = logging.getLogger(__name__)


UPSERT_DISCOVERED_LISTING_SQL = """
INSERT INTO discovered_listings (
    source_site,
    source_listing_id,
    url,
    title,
    auction_end_date
) VALUES (
    %(source_site)s,
    %(source_listing_id)s,
    %(url)s,
    %(title)s,
    %(auction_end_date)s
)
ON CONFLICT (source_site, source_listing_id) DO UPDATE SET
    url = EXCLUDED.url,
    title = EXCLUDED.title,
    auction_end_date = EXCLUDED.auction_end_date,
    last_seen_at = NOW()
RETURNING xmax = 0 AS inserted
"""


@dataclass
class DiscoverySummary:
    candidates_inspected: int = 0
    newly_discovered: int = 0
    already_discovered_or_updated: int = 0
    failed: int = 0


def capture_initial_completed_auctions_page(headless=True):
    logger.info("Capturing initial Cars and Bids completed auctions page")
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        try:
            page = browser.new_page()
            payload, timestamp, signature = _capture_initial_completed_auctions_page(
                page
            )
        finally:
            browser.close()

    logger.info("Captured initial Cars and Bids completed auctions page")
    return payload, timestamp, signature


def _capture_initial_completed_auctions_page(page):
    matched_response = None

    def is_matching_response(response):
        return _is_matching_completed_auctions_response(response.url)

    def capture_matching_response(response):
        nonlocal matched_response
        if matched_response is None and is_matching_response(response):
            matched_response = response

    page.on("response", capture_matching_response)
    page.goto(
        PAST_AUCTIONS_URL,
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
                "Cars and Bids completed auctions API response not found"
            ) from exc

    if matched_response is None:
        raise RuntimeError("Cars and Bids completed auctions API response not found")
    if not matched_response.ok:
        raise RuntimeError(
            "Cars and Bids completed auctions API response failed "
            f"status={matched_response.status}"
        )
    try:
        payload = matched_response.json()
    except Exception as exc:
        raise RuntimeError("Invalid Cars and Bids completed auctions API JSON") from exc

    timestamp, signature = _extract_signed_request_params(matched_response.url)
    _extract_auctions(payload)
    return payload, timestamp, signature


def fetch_completed_auctions_page(page, offset, timestamp, signature):
    params = {
        "limit": DISCOVERY_PAGE_SIZE,
        "status": "closed",
        "offset": offset,
        "timestamp": timestamp,
        "signature": signature,
    }
    logger.info("Fetching Cars and Bids completed auctions offset=%s", offset)
    response = page.evaluate(
        """async ({url, params}) => {
            const requestUrl = new URL(url);
            for (const [key, value] of Object.entries(params)) {
                requestUrl.searchParams.set(key, String(value));
            }
            const response = await fetch(requestUrl.toString(), {
                credentials: "include",
                headers: {"accept": "application/json"},
            });
            const text = await response.text();
            return {
                ok: response.ok,
                status: response.status,
                text,
            };
        }""",
        {"url": API_AUCTIONS_URL, "params": params},
    )
    if not response["ok"]:
        raise RuntimeError(
            "Cars and Bids completed auctions API response failed "
            f"offset={offset} status={response['status']} body={response['text']}"
        )
    payload = _parse_json_response_text(response["text"], offset)
    auctions = _extract_auctions(payload)
    logger.info(
        "Fetched Cars and Bids completed auctions offset=%s auctions=%s",
        offset,
        len(auctions),
    )
    return payload


def _is_matching_completed_auctions_response(url):
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    return (
        url.startswith(API_AUCTIONS_URL)
        and params.get("status") == ["closed"]
    )


def discover_completed_auctions(scrape_date, max_candidates=None, headless=True):
    if max_candidates is not None and max_candidates <= 0:
        return DiscoverySummary()

    normalized_scrape_date = _normalize_scrape_date(scrape_date)
    summary = DiscoverySummary()

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        try:
            page = browser.new_page()
            payload, timestamp, signature = _capture_initial_completed_auctions_page(
                page
            )
            offset = DISCOVERY_PAGE_SIZE

            while True:
                auctions = _extract_auctions(payload)
                if not auctions:
                    logger.info(
                        "Stopping Cars and Bids discovery because no auctions were returned"
                    )
                    break

                stop_discovery = False
                for auction in auctions:
                    if (
                        max_candidates is not None
                        and summary.candidates_inspected >= max_candidates
                    ):
                        stop_discovery = True
                        break

                    candidate = _build_candidate_from_auction(auction, summary)
                    if candidate is None:
                        continue

                    summary.candidates_inspected += 1
                    listing_id = candidate["listing_id"]
                    auction_end_date = candidate.get("auction_end_date")

                    if not auction_end_date:
                        summary.failed += 1
                        logger.error(
                            "Failed Cars and Bids discovery candidate for listing_id=%s "
                            "because auction_end_date is missing",
                            listing_id,
                        )
                        continue

                    if date.fromisoformat(auction_end_date) < normalized_scrape_date:
                        logger.info(
                            "Stopping Cars and Bids discovery at listing_id=%s "
                            "because auction_end_date=%s is older than scrape_date=%s",
                            listing_id,
                            auction_end_date,
                            normalized_scrape_date.isoformat(),
                        )
                        stop_discovery = True
                        break

                    try:
                        if save_discovered_listing(candidate):
                            summary.newly_discovered += 1
                        else:
                            summary.already_discovered_or_updated += 1
                    except Exception:
                        summary.failed += 1
                        logger.error(
                            "Failed Cars and Bids discovery candidate for listing_id=%s",
                            listing_id,
                        )

                if stop_discovery:
                    break

                if (
                    max_candidates is not None
                    and summary.candidates_inspected >= max_candidates
                ):
                    break

                payload = fetch_completed_auctions_page(
                    page,
                    offset,
                    timestamp,
                    signature,
                )
                offset += DISCOVERY_PAGE_SIZE
        finally:
            browser.close()

    return summary


def normalize_completed_auction_candidate(auction):
    if not isinstance(auction, dict):
        raise ValueError("Cars and Bids discovery auction must be an object")

    listing_id = auction.get("id")
    if listing_id in (None, ""):
        raise ValueError("Cars and Bids discovery auction id is required")

    listing_id = str(listing_id).strip()
    if not listing_id:
        raise ValueError("Cars and Bids discovery auction id is required")

    candidate = {
        "source_site": SOURCE_SITE,
        "listing_id": listing_id,
        "source_listing_id": listing_id,
        "url": f"{LISTING_URL_BASE}/{listing_id}",
    }

    title = auction.get("title")
    if title:
        candidate["title"] = str(title).strip()

    auction_end_date = _parse_auction_end_date(auction.get("auction_end"))
    if auction_end_date:
        candidate["auction_end_date"] = auction_end_date

    return candidate


def build_discovered_listing_params(candidate):
    return {
        "source_site": SOURCE_SITE,
        "source_listing_id": candidate["listing_id"],
        "url": candidate["url"],
        "title": candidate.get("title"),
        "auction_end_date": candidate.get("auction_end_date"),
    }


def save_discovered_listing(candidate):
    database_url = _get_database_url()
    params = build_discovered_listing_params(candidate)

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(UPSERT_DISCOVERED_LISTING_SQL, params)
            inserted, = cur.fetchone()
            logger.info(
                "Upserted Cars and Bids discovered listing for listing_id=%s",
                params["source_listing_id"],
            )
            return inserted


def _get_database_url():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL must be set")
    return database_url


def _normalize_scrape_date(value):
    if isinstance(value, date):
        return value

    if isinstance(value, str):
        return date.fromisoformat(value)

    raise TypeError("scrape_date must be a date or ISO date string")


def _extract_signed_request_params(url):
    params = parse_qs(urlparse(url).query)
    timestamp = _single_query_value(params, "timestamp")
    signature = _single_query_value(params, "signature")
    if not timestamp or not signature:
        raise RuntimeError(
            "Cars and Bids completed auctions API response missing signed "
            "request parameters"
        )
    return timestamp, signature


def _single_query_value(params, key):
    values = params.get(key)
    if not values:
        return None
    return values[0]


def _extract_auctions(payload):
    auctions = payload.get("auctions")
    if not isinstance(auctions, list):
        raise ValueError("Cars and Bids discovery payload auctions must be a list")
    return auctions


def _parse_json_response_text(text, offset):
    try:
        import json

        return json.loads(text)
    except Exception as exc:
        raise RuntimeError(
            "Invalid Cars and Bids completed auctions API JSON "
            f"offset={offset}"
        ) from exc


def _build_candidate_from_auction(auction, summary):
    try:
        return normalize_completed_auction_candidate(auction)
    except Exception:
        summary.failed += 1
        auction_id = auction.get("id") if isinstance(auction, dict) else None
        logger.error(
            "Failed Cars and Bids discovery auction normalization for id=%s",
            auction_id,
        )
        return None


def _parse_auction_end_date(value):
    if value in (None, ""):
        return None

    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    return datetime.fromisoformat(text).date().isoformat()

