import logging
import os

import psycopg
from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)


INSERT_LISTING_SQL = """
INSERT INTO listings (
    source_site,
    source_listing_id,
    url,
    make,
    model_raw,
    model_normalized,
    year,
    mileage,
    tmu,
    vin,
    sale_price,
    sold,
    auction_end_date,
    transmission,
    listing_details_raw
) VALUES (
    %(source_site)s,
    %(source_listing_id)s,
    %(url)s,
    %(make)s,
    %(model_raw)s,
    %(model_normalized)s,
    %(year)s,
    %(mileage)s,
    %(tmu)s,
    %(vin)s,
    %(sale_price)s,
    %(sold)s,
    %(auction_end_date)s,
    %(transmission)s,
    %(listing_details_raw)s
)
ON CONFLICT (source_site, source_listing_id) DO UPDATE SET
    url = EXCLUDED.url,
    make = EXCLUDED.make,
    model_raw = EXCLUDED.model_raw,
    model_normalized = EXCLUDED.model_normalized,
    year = EXCLUDED.year,
    mileage = EXCLUDED.mileage,
    tmu = EXCLUDED.tmu,
    vin = EXCLUDED.vin,
    sale_price = EXCLUDED.sale_price,
    sold = EXCLUDED.sold,
    auction_end_date = EXCLUDED.auction_end_date,
    transmission = EXCLUDED.transmission,
    listing_details_raw = EXCLUDED.listing_details_raw,
    updated_at = NOW()
"""

MARK_RAW_LISTING_PROCESSED_SQL = """
UPDATE raw_listing_json
SET processed = TRUE
WHERE source_site = %(source_site)s
  AND source_listing_id = %(source_listing_id)s
"""


def load_listing(transformed_listing):
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL must be set")
    params = build_listing_params(transformed_listing)

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(INSERT_LISTING_SQL, params)
            logger.info(
                "Upserted Cars and Bids listing for listing_id=%s",
                params["source_listing_id"],
            )
            cur.execute(MARK_RAW_LISTING_PROCESSED_SQL, params)
            logger.info(
                "Marked Cars and Bids raw listing processed for listing_id=%s",
                params["source_listing_id"],
            )


def build_listing_params(transformed_listing):
    return {
        "source_site": transformed_listing["source_site"],
        "source_listing_id": transformed_listing["listing_id"],
        "url": transformed_listing["url"],
        "make": transformed_listing["make"],
        "model_raw": transformed_listing["model_raw"],
        "model_normalized": transformed_listing["model_normalized"],
        "year": transformed_listing["year"],
        "mileage": transformed_listing["mileage"],
        "tmu": transformed_listing["tmu"],
        "vin": transformed_listing["vin"],
        "sale_price": transformed_listing["sale_price"],
        "sold": transformed_listing["sold"],
        "auction_end_date": transformed_listing["auction_end_date"],
        "transmission": transformed_listing["transmission"],
        "listing_details_raw": Jsonb(transformed_listing["listing_details_raw"]),
    }
