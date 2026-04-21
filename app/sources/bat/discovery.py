import logging
import os
import re
from dataclasses import dataclass
from datetime import date, datetime
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
import psycopg
import requests


SOURCE_SITE = "bringatrailer"
BASE_URL = "https://bringatrailer.com"
COMPLETED_AUCTIONS_SECTION_TITLE = "All Completed Auctions"
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


def parse_completed_auction_candidates(html, max_candidates=None):
    if max_candidates is not None and max_candidates <= 0:
        return []

    soup = BeautifulSoup(html, "html.parser")
    heading = _find_completed_auctions_heading(soup)
    if heading is None:
        return []

    candidates = []
    seen_listing_ids = set()

    for link in _iter_completed_auction_links(heading):
        normalized = _normalize_listing_href(link.get("href"))
        if normalized is None:
            continue

        listing_id, url = normalized
        if listing_id in seen_listing_ids:
            continue

        seen_listing_ids.add(listing_id)
        candidate = {
            "source_site": SOURCE_SITE,
            "listing_id": listing_id,
            "source_listing_id": listing_id,
            "url": url,
        }
        candidate.update(_extract_card_metadata(link))
        candidates.append(candidate)

        if max_candidates is not None and len(candidates) >= max_candidates:
            break

    return candidates


def fetch_completed_auctions_results(results_url):
    logger.info("Fetching BAT completed auctions results from url=%s", results_url)
    response = requests.get(results_url, timeout=10)
    response.raise_for_status()
    logger.info("Fetched BAT completed auctions results from url=%s", results_url)
    return response.text


def discover_completed_auctions(results_url, scrape_date, max_candidates=None):
    normalized_scrape_date = _normalize_scrape_date(scrape_date)
    html = fetch_completed_auctions_results(results_url)
    candidates = parse_completed_auction_candidates(html, max_candidates=max_candidates)
    summary = DiscoverySummary()

    for candidate in candidates:
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


def _find_completed_auctions_heading(soup):
    for tag in soup.find_all(True):
        if _normalized_text(tag) == COMPLETED_AUCTIONS_SECTION_TITLE:
            return tag
    return None


def _iter_completed_auction_links(heading):
    for element in heading.next_elements:
        if element is heading:
            continue
        if not getattr(element, "name", None):
            continue
        if (
            element.name == "a"
            and element.get("href")
            and _is_listing_card_link(element)
        ):
            yield element


def _is_listing_card_link(element):
    return "listing-card" in element.get("class", [])


def _normalize_listing_href(href):
    if not href:
        return None

    parsed = urlparse(urljoin(BASE_URL, href))
    if parsed.netloc and parsed.netloc.lower() != "bringatrailer.com":
        return None

    match = re.fullmatch(r"/listing/([^/]+)/?", parsed.path)
    if match is None:
        return None

    listing_id = match.group(1)
    return listing_id, f"{BASE_URL}/listing/{listing_id}/"


def _extract_card_metadata(card):
    metadata = {}

    title = _extract_title(card)
    if title:
        metadata["title"] = title

    auction_end_date = _extract_auction_end_date(card)
    if auction_end_date:
        metadata["auction_end_date"] = auction_end_date

    source_location = _extract_source_location(card)
    if source_location:
        metadata["source_location"] = source_location

    return metadata


def _extract_title(card):
    title = card.select_one(".content-main h3")
    if title is None:
        return None
    return _normalized_text(title) or None


def _extract_auction_end_date(card):
    result = card.select_one(".content-main .item-results")
    if result is None:
        return None
    return _parse_date(_normalized_text(result))


def _extract_source_location(card):
    location = card.select_one(".content-main .show-country-name")
    if location is None:
        return None
    return _normalized_text(location) or None


def _parse_date(value):
    if not value:
        return None

    text = str(value).strip()
    iso_match = re.search(r"\d{4}-\d{2}-\d{2}", text)
    if iso_match:
        return iso_match.group(0)

    numeric_match = re.search(r"\d{1,2}/\d{1,2}/\d{4}", text)
    if numeric_match is None:
        return None

    return datetime.strptime(numeric_match.group(0), "%m/%d/%Y").date().isoformat()


def _normalized_text(tag):
    return " ".join(tag.get_text(" ", strip=True).split())
