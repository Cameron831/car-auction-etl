import logging
import os

import psycopg
import requests


SOURCE_SITE = "bringatrailer"
logger = logging.getLogger(__name__)

UPSERT_RAW_LISTING_HTML_SQL = """
INSERT INTO raw_listing_html (
    source_site,
    source_listing_id,
    url,
    raw_html,
    processed
) VALUES (
    %(source_site)s,
    %(source_listing_id)s,
    %(url)s,
    %(raw_html)s,
    FALSE
)
ON CONFLICT (source_site, source_listing_id) DO UPDATE SET
    url = EXCLUDED.url,
    raw_html = EXCLUDED.raw_html,
    processed = FALSE
"""


def fetch_listing_html(id):
    url = f"https://bringatrailer.com/listing/{id}"
    logger.info("Fetching BAT listing HTML for listing_id=%s", id)
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    logger.info("Fetched BAT listing HTML for listing_id=%s", id)
    return response.text


def save_listing_html(listing_id, html, url=None):
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL must be set")

    params = build_raw_listing_html_params(listing_id, html, url)

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(UPSERT_RAW_LISTING_HTML_SQL, params)
            logger.info("Saved BAT raw listing HTML for listing_id=%s", listing_id)


def build_raw_listing_html_params(listing_id, html, url=None):
    return {
        "source_site": SOURCE_SITE,
        "source_listing_id": listing_id,
        "url": url or f"https://bringatrailer.com/listing/{listing_id}/",
        "raw_html": html,
    }
