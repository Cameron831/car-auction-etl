import logging
import os
import re
from dataclasses import dataclass
from urllib.parse import urljoin, urlparse

import psycopg
import requests
from bs4 import BeautifulSoup

from app.sources.bat.ingest import SOURCE_SITE, fetch_listing_html, save_listing_html
from app.sources.bat.load import load_listing
from app.sources.bat.transform import transform_listing_html


RESULTS_URL = "https://bringatrailer.com/auctions/results/"
UNSUPPORTED_SKIP_REASON = "unsupported_listing_type"
AMBIGUOUS_SKIP_REASON = "ambiguous_listing_type"

UNSUPPORTED_TERMS = (
    "automobilia",
    "boat",
    "boats",
    "book",
    "brochure",
    "collectible",
    "collectibles",
    "engine",
    "memorabilia",
    "motorcycle",
    "motorcycles",
    "parts",
    "sign",
    "signage",
    "trailer",
    "trailers",
    "watch",
    "wheels",
)

UPSERT_DISCOVERED_LISTING_SQL = """
INSERT INTO discovered_listings (
    source_site,
    source_listing_id,
    url,
    eligible,
    skip_reason
) VALUES (
    %(source_site)s,
    %(source_listing_id)s,
    %(url)s,
    %(eligible)s,
    %(skip_reason)s
)
ON CONFLICT (source_site, source_listing_id) DO UPDATE SET
    url = EXCLUDED.url,
    eligible = EXCLUDED.eligible,
    skip_reason = EXCLUDED.skip_reason
"""

SELECT_EXISTING_DISCOVERED_IDS_SQL = """
SELECT source_listing_id
FROM discovered_listings
WHERE source_site = %(source_site)s
  AND source_listing_id = ANY(%(source_listing_ids)s)
"""

SELECT_PENDING_ELIGIBLE_DISCOVERED_SQL = """
SELECT source_listing_id, url
FROM discovered_listings
WHERE source_site = %(source_site)s
  AND eligible = TRUE
  AND ingested_at IS NULL
ORDER BY created_at ASC, id ASC
"""

MARK_DISCOVERED_LISTING_INGESTED_SQL = """
UPDATE discovered_listings
SET ingested_at = NOW()
WHERE source_site = %(source_site)s
  AND source_listing_id = %(source_listing_id)s
"""

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DiscoveredListing:
    source_listing_id: str
    url: str
    eligible: bool
    skip_reason: str | None = None


def build_results_page_url(page_number):
    if page_number < 1:
        raise ValueError("page_number must be >= 1")
    if page_number == 1:
        return RESULTS_URL
    return f"{RESULTS_URL}?page={page_number}"


def fetch_results_page(url):
    logger.info("Fetching BAT discovery results page url=%s", url)
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    logger.info("Fetched BAT discovery results page url=%s", url)
    return response.text


def parse_results_page(html, base_url=RESULTS_URL):
    soup = BeautifulSoup(html, "html.parser")
    listings = []
    seen_listing_ids = set()

    for link in soup.find_all("a", href=True):
        try:
            listing_id, url = normalize_listing_url(link["href"], base_url)
        except ValueError:
            continue

        if listing_id in seen_listing_ids:
            continue

        seen_listing_ids.add(listing_id)
        eligible, skip_reason = classify_listing(_candidate_text(link))
        listings.append(
            DiscoveredListing(
                source_listing_id=listing_id,
                url=url,
                eligible=eligible,
                skip_reason=skip_reason,
            )
        )

    return listings


def normalize_listing_url(href, base_url=RESULTS_URL):
    absolute_url = urljoin(base_url, href)
    parsed = urlparse(absolute_url)
    if parsed.netloc.lower() != "bringatrailer.com":
        raise ValueError("Listing URL must be on bringatrailer.com")

    match = re.match(r"^/listing/([^/?#]+)/?$", parsed.path)
    if not match:
        raise ValueError("URL is not a BAT listing URL")

    listing_id = match.group(1).strip().lower()
    if not listing_id:
        raise ValueError("Listing URL missing listing ID")

    return listing_id, f"https://bringatrailer.com/listing/{listing_id}/"


def classify_listing(text):
    normalized_text = re.sub(r"\s+", " ", text or "").strip().lower()
    if not normalized_text:
        return False, AMBIGUOUS_SKIP_REASON

    if any(re.search(rf"\b{re.escape(term)}\b", normalized_text) for term in UNSUPPORTED_TERMS):
        return False, UNSUPPORTED_SKIP_REASON

    if not re.search(r"\b(?:18|19|20)\d{2}\b", normalized_text):
        return False, AMBIGUOUS_SKIP_REASON

    return True, None


def build_discovered_listing_params(listing):
    return {
        "source_site": SOURCE_SITE,
        "source_listing_id": listing.source_listing_id,
        "url": listing.url,
        "eligible": listing.eligible,
        "skip_reason": listing.skip_reason,
    }


def save_discovered_listings(listings):
    if not listings:
        return []

    database_url = _database_url()
    listing_ids = [listing.source_listing_id for listing in listings]

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            existing_ids = select_existing_discovered_ids(cur, listing_ids)
            for listing in listings:
                cur.execute(UPSERT_DISCOVERED_LISTING_SQL, build_discovered_listing_params(listing))

    return [listing_id for listing_id in listing_ids if listing_id not in existing_ids]


def select_existing_discovered_ids(cur, listing_ids):
    cur.execute(
        SELECT_EXISTING_DISCOVERED_IDS_SQL,
        {
            "source_site": SOURCE_SITE,
            "source_listing_ids": listing_ids,
        },
    )
    return {row[0] for row in cur.fetchall()}


def load_pending_eligible_discovered_listings():
    database_url = _database_url()
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(SELECT_PENDING_ELIGIBLE_DISCOVERED_SQL, {"source_site": SOURCE_SITE})
            return [
                DiscoveredListing(
                    source_listing_id=row[0],
                    url=row[1],
                    eligible=True,
                    skip_reason=None,
                )
                for row in cur.fetchall()
            ]


def mark_discovered_listing_ingested(listing_id):
    database_url = _database_url()
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                MARK_DISCOVERED_LISTING_INGESTED_SQL,
                {
                    "source_site": SOURCE_SITE,
                    "source_listing_id": listing_id,
                },
            )


def discover_recent_listings(max_pages, fetch_page=fetch_results_page, save_listings=save_discovered_listings):
    if max_pages < 1:
        raise ValueError("max_pages must be >= 1")

    discovered_count = 0
    new_listing_ids = []

    for page_number in range(1, max_pages + 1):
        page_url = build_results_page_url(page_number)
        listings = parse_results_page(fetch_page(page_url), page_url)
        page_new_listing_ids = save_listings(listings)
        discovered_count += len(listings)
        new_listing_ids.extend(page_new_listing_ids)

        logger.info(
            "Processed BAT discovery page page_number=%s discovered=%s new=%s",
            page_number,
            len(listings),
            len(page_new_listing_ids),
        )
        if not page_new_listing_ids:
            break

    return {
        "discovered_count": discovered_count,
        "new_listing_ids": new_listing_ids,
    }


def ingest_pending_discovered_listings(load_pending=load_pending_eligible_discovered_listings):
    ingested_listing_ids = []

    for listing in load_pending():
        html = fetch_listing_html(listing.source_listing_id)
        save_listing_html(listing.source_listing_id, html, listing.url)
        mark_discovered_listing_ingested(listing.source_listing_id)
        transformed_listing = transform_listing_html(listing.source_listing_id)
        load_listing(transformed_listing)
        ingested_listing_ids.append(listing.source_listing_id)

    return ingested_listing_ids


def run_daily_discovery(max_pages):
    discovery_result = discover_recent_listings(max_pages)
    ingested_listing_ids = ingest_pending_discovered_listings()
    return {
        **discovery_result,
        "ingested_listing_ids": ingested_listing_ids,
    }


def _candidate_text(link):
    direct_text = _link_title_text(link)
    if direct_text:
        return direct_text

    for parent in link.parents:
        if parent.name in ("article", "li"):
            return _card_title_text(parent) or parent.get_text(" ", strip=True)
        if parent.name == "div" and _looks_like_listing_card(parent):
            return _card_title_text(parent) or parent.get_text(" ", strip=True)
    return link.get_text(" ", strip=True)


def _looks_like_listing_card(tag):
    class_text = " ".join(tag.get("class", [])).lower()
    return any(token in class_text for token in ("auction", "listing", "result", "post"))


def _link_title_text(link):
    text = link.get_text(" ", strip=True)
    if text:
        return text

    title = link.get("title")
    if title:
        return title.strip()

    image = link.find("img")
    if image and image.get("alt"):
        return image["alt"].strip()

    return ""


def _card_title_text(card):
    title_link = card.select_one("h1 a, h2 a, h3 a, h4 a")
    if title_link:
        return _link_title_text(title_link)

    title = card.select_one("h1, h2, h3, h4")
    if title:
        return title.get_text(" ", strip=True)

    image = card.find("img", alt=True)
    if image:
        return image["alt"].strip()

    return ""


def _database_url():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL must be set")
    return database_url
