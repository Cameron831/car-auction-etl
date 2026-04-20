import logging
import os

import psycopg


SOURCE_SITE = "bringatrailer"
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
"""


def save_discovered_listing(candidate):
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL must be set")

    params = build_discovered_listing_params(candidate)

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(UPSERT_DISCOVERED_LISTING_SQL, params)
            logger.info(
                "Upserted BAT discovered listing for listing_id=%s",
                params["source_listing_id"],
            )


def build_discovered_listing_params(candidate):
    return {
        "source_site": SOURCE_SITE,
        "source_listing_id": candidate["listing_id"],
        "url": candidate["url"],
        "title": candidate.get("title"),
        "auction_end_date": candidate.get("auction_end_date"),
        "source_location": candidate.get("source_location"),
    }
