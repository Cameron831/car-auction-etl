import logging
import os
from datetime import datetime

import psycopg
from psycopg.rows import dict_row

from app.sources.carsandbids.ingest import build_listing_url


SOURCE_SITE = "carsandbids"
logger = logging.getLogger(__name__)

SELECT_RAW_LISTING_JSON_SQL = """
SELECT raw_json
FROM raw_listing_json
WHERE source_site = %(source_site)s
  AND source_listing_id = %(source_listing_id)s
"""

SELECT_PENDING_RAW_LISTING_JSON_SQL = """
SELECT
    id,
    source_site,
    source_listing_id,
    url,
    raw_json,
    created_at,
    processed
FROM raw_listing_json
WHERE source_site = %(source_site)s
  AND processed = FALSE
ORDER BY created_at ASC, id ASC
"""

SOLD_STATUS_VALUES = {"sold", "sold_after"}
UNSOLD_STATUS_VALUES = {"reserve_not_met", "canceled"}
TRANSMISSION_VALUES = {
    1: "automatic",
    2: "manual",
}


def build_raw_listing_lookup_params(listing_id):
    return {
        "source_site": SOURCE_SITE,
        "source_listing_id": listing_id,
    }


def load_listing_json(listing_id):
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL must be set")

    params = build_raw_listing_lookup_params(listing_id)

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(SELECT_RAW_LISTING_JSON_SQL, params)
            row = cur.fetchone()

    if row is None:
        raise LookupError(f"Raw JSON record not found for listing ID: {listing_id}")

    return row[0]


def load_pending_raw_listing_json(limit=None):
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL must be set")

    params = {"source_site": SOURCE_SITE}
    sql = SELECT_PENDING_RAW_LISTING_JSON_SQL

    if limit is not None:
        if limit <= 0:
            return []
        sql = f"{sql}\nLIMIT %(limit)s"
        params["limit"] = limit

    with psycopg.connect(database_url) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def transform_listing_json(listing_id):
    logger.info("Transforming Cars and Bids listing JSON for listing_id=%s", listing_id)
    payload = load_listing_json(listing_id)
    listing = payload["listing"]

    transformed_data = {
        "source_site": SOURCE_SITE,
        "listing_id": listing_id,
        "url": build_listing_url(listing_id),
        "make": listing["make"],
        "model": listing["model"],
        "year": listing["year"],
        "mileage": listing["mileage"],
        "vin": listing["vin"],
        "sale_price": extract_sale_price(payload),
        "sold": extract_sold_status(payload),
        "auction_end_date": extract_auction_end_date(payload),
        "transmission": normalize_transmission(listing["transmission"]),
        "listing_details_raw": extract_listing_details_raw(listing),
    }
    logger.info("Transformed Cars and Bids listing JSON for listing_id=%s", listing_id)
    return transformed_data


def extract_sale_price(payload):
    stats = payload["stats"]
    if stats.get("sale_amount") is not None:
        return stats["sale_amount"]
    try:
        return stats["current_bid"]["amount"]
    except KeyError as exc:
        raise ValueError("Could not parse sale price") from exc


def extract_sold_status(payload):
    status = payload["status"]
    if status in SOLD_STATUS_VALUES:
        return True
    if status in UNSOLD_STATUS_VALUES:
        return False
    raise ValueError(f"Could not parse sold status: {status}")


def extract_auction_end_date(payload):
    raw_date = payload["stats"]["auction_end"]
    try:
        return datetime.fromisoformat(raw_date.replace("Z", "+00:00")).date().isoformat()
    except ValueError as exc:
        raise ValueError("Could not parse auction end date") from exc


def normalize_transmission(raw_transmission):
    try:
        return TRANSMISSION_VALUES[raw_transmission]
    except KeyError as exc:
        raise ValueError(f"Could not normalize transmission: {raw_transmission}") from exc


def extract_listing_details_raw(listing):
    return {
        "engine": listing["engine"],
        "transmission_details": listing["transmission_details"],
        "exterior_color": listing["exterior_color"],
        "interior_color": listing["interior_color"],
        "title_status": listing["title_status"],
        "highlighted_points": listing["highlighted_points"],
        "sections": listing["sections"],
    }
