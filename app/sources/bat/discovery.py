import logging
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timezone
from urllib.parse import urljoin, urlparse

import psycopg
import requests


SOURCE_SITE = "bringatrailer"
BASE_URL = "https://bringatrailer.com"
LISTINGS_FILTER_URL = f"{BASE_URL}/wp-json/bringatrailer/1.0/data/listings-filter"
DISCOVERY_PER_PAGE = 60
DISCOVERY_TIMEOUT_SECONDS = 10
logger = logging.getLogger(__name__)


UPSERT_DISCOVERED_LISTING_SQL = """
INSERT INTO discovered_listings (
    source_site,
    source_listing_id,
    url,
    title,
    auction_end_date,
    source_location
) VALUES (
    %(source_site)s,
    %(source_listing_id)s,
    %(url)s,
    %(title)s,
    %(auction_end_date)s,
    %(source_location)s
)
ON CONFLICT (source_site, source_listing_id) DO UPDATE SET
    url = EXCLUDED.url,
    title = EXCLUDED.title,
    auction_end_date = EXCLUDED.auction_end_date,
    source_location = EXCLUDED.source_location,
    last_seen_at = NOW()
RETURNING xmax = 0 AS inserted
"""


@dataclass
class DiscoverySummary:
    candidates_inspected: int = 0
    newly_discovered: int = 0
    already_discovered_or_updated: int = 0
    failed: int = 0


def fetch_completed_auctions_page(page):
    if page <= 0:
        raise ValueError("page must be positive")
    if not 10 <= DISCOVERY_PER_PAGE <= 60:
        raise ValueError("DISCOVERY_PER_PAGE must be between 10 and 60")

    params = {
        "page": page,
        "per_page": DISCOVERY_PER_PAGE,
        "get_items": 1,
        "get_stats": 0,
        "sort": "td",
    }
    logger.info("Fetching BAT completed auctions page=%s", page)
    response = requests.get(
        LISTINGS_FILTER_URL,
        params=params,
        timeout=DISCOVERY_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    payload = response.json()
    items = payload.get("items")
    if items is None:
        items = []
    if not isinstance(items, list):
        raise ValueError("BAT discovery payload items must be a list")
    logger.info("Fetched BAT completed auctions page=%s items=%s", page, len(items))
    return payload


def normalize_completed_auction_candidate(item):
    normalized = _normalize_listing_url(item.get("url"))
    if normalized is None:
        raise ValueError("BAT discovery item url must be a Bring a Trailer listing URL")

    listing_id, url = normalized
    candidate = {
        "source_site": SOURCE_SITE,
        "listing_id": listing_id,
        "source_listing_id": listing_id,
        "url": url,
    }

    title = item.get("title")
    if title:
        candidate["title"] = str(title).strip()

    auction_end_date = _parse_auction_end_date(item.get("timestamp_end"))
    if auction_end_date:
        candidate["auction_end_date"] = auction_end_date

    source_location = item.get("country_code")
    if source_location:
        candidate["source_location"] = str(source_location).strip()

    return candidate


def discover_completed_auctions(scrape_date, max_candidates=None):
    if max_candidates is not None and max_candidates <= 0:
        return DiscoverySummary()

    normalized_scrape_date = _normalize_scrape_date(scrape_date)
    summary = DiscoverySummary()
    page = 1

    while True:
        payload = fetch_completed_auctions_page(page)
        items = payload.get("items") or []
        if not items:
            logger.info("Stopping BAT discovery at page=%s because no items were returned", page)
            break

        stop_discovery = False
        for item in items:
            if max_candidates is not None and summary.candidates_inspected >= max_candidates:
                stop_discovery = True
                break

            candidate = _build_candidate_from_item(item, summary)
            if candidate is None:
                continue

            summary.candidates_inspected += 1
            listing_id = candidate["listing_id"]
            auction_end_date = candidate.get("auction_end_date")

            if not auction_end_date:
                summary.failed += 1
                logger.error(
                    "Failed BAT discovery candidate for listing_id=%s because auction_end_date is missing",
                    listing_id,
                )
                continue

            if date.fromisoformat(auction_end_date) < normalized_scrape_date:
                logger.info(
                    "Stopping BAT discovery at listing_id=%s because auction_end_date=%s is older than scrape_date=%s",
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
                    "Failed BAT discovery candidate for listing_id=%s",
                    listing_id,
                )

        if stop_discovery:
            break

        page += 1

    return summary


def save_discovered_listing(candidate):
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL must be set")

    params = build_discovered_listing_params(candidate)

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(UPSERT_DISCOVERED_LISTING_SQL, params)
            inserted, = cur.fetchone()
            logger.info(
                "Upserted BAT discovered listing for listing_id=%s",
                params["source_listing_id"],
            )
            return inserted


def build_discovered_listing_params(candidate):
    return {
        "source_site": SOURCE_SITE,
        "source_listing_id": candidate["listing_id"],
        "url": candidate["url"],
        "title": candidate.get("title"),
        "auction_end_date": candidate.get("auction_end_date"),
        "source_location": candidate.get("source_location"),
    }


def _normalize_scrape_date(value):
    if isinstance(value, date):
        return value

    if isinstance(value, str):
        return date.fromisoformat(value)

    raise TypeError("scrape_date must be a date or ISO date string")


def _build_candidate_from_item(item, summary):
    try:
        return normalize_completed_auction_candidate(item)
    except Exception:
        summary.failed += 1
        logger.error(
            "Failed BAT discovery item normalization for url=%s",
            item.get("url"),
        )
        return None


def _normalize_listing_url(url):
    if not url:
        return None

    parsed = urlparse(urljoin(BASE_URL, url))
    if parsed.netloc and parsed.netloc.lower() != "bringatrailer.com":
        return None

    match = re.fullmatch(r"/listing/([^/]+)/?", parsed.path)
    if match is None:
        return None

    listing_id = match.group(1)
    return listing_id, f"{BASE_URL}/listing/{listing_id}/"


def _parse_auction_end_date(timestamp_end):
    if timestamp_end in (None, ""):
        return None

    timestamp = int(timestamp_end)
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).date().isoformat()
