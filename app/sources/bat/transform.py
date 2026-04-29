import json
import logging
import os
import re
from datetime import datetime
from bs4 import BeautifulSoup
import psycopg
from psycopg.rows import dict_row

from app.sources.bat.ingest import (
    extract_country,
    extract_group_value,
    parse_listing_id_year,
)
from app.model_normalization import normalize_model


SOURCE_SITE = "bringatrailer"
logger = logging.getLogger(__name__)

SELECT_RAW_LISTING_HTML_SQL = """
SELECT raw_html
FROM raw_listing_html
WHERE source_site = %(source_site)s
  AND source_listing_id = %(source_listing_id)s
"""

SELECT_PENDING_RAW_LISTING_HTML_SQL = """
SELECT
    id,
    source_site,
    source_listing_id,
    url,
    raw_html,
    created_at,
    processed
FROM raw_listing_html
WHERE source_site = %(source_site)s
  AND processed = FALSE
ORDER BY created_at ASC, id ASC
"""

TRANSMISSION_DETAIL_PATTERN = (
    r"\b(?:Transmission|Transaxle|Gearbox)\b"
    r"|"
    r"\b(?:column|floor|console|dash)-?shift(?:ed)?\b.*\b(?:\w+-)?speed\b.*\b(?:manual|automatic)\b"
)


def load_listing_html(listing_id):
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL must be set")

    params = build_raw_listing_lookup_params(listing_id)

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(SELECT_RAW_LISTING_HTML_SQL, params)
            row = cur.fetchone()

    if row is None:
        raise LookupError(f"Raw HTML record not found for listing ID: {listing_id}")

    return row[0]


def build_raw_listing_lookup_params(listing_id):
    return {
        "source_site": SOURCE_SITE,
        "source_listing_id": listing_id,
    }


def load_pending_raw_listing_html(limit=None):
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL must be set")

    params = {"source_site": SOURCE_SITE}
    sql = SELECT_PENDING_RAW_LISTING_HTML_SQL

    if limit is not None:
        if limit <= 0:
            return []
        sql = f"{sql}\nLIMIT %(limit)s"
        params["limit"] = limit

    with psycopg.connect(database_url) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def transform_listing_html(listing_id):
    logger.info("Transforming BAT listing HTML for listing_id=%s", listing_id)
    html = load_listing_html(listing_id)
    soup = BeautifulSoup(html, "html.parser")
    product_data = get_product_json_ld(soup)
    listing_details = get_listing_details(soup)

    # transformed entries
    year = parse_listing_id_year(listing_id)
    make = parse_make(soup)
    model_raw = parse_model(soup)
    model_normalized = normalize_model(make, model_raw)
    mileage = parse_mileage(find_detail_value(listing_details, r"\bmiles?\b|\btmu\b|\bunknown\b", "Mileage"))
    vin = extract_vin(find_detail_value(listing_details, r"^Chassis:", "VIN"))
    sale_price = extract_sale_price(soup, product_data)
    sold = extract_sold_status(soup)
    auction_end_date = extract_auction_end_date(soup)
    transmission = normalize_transmission(find_detail_value(listing_details, TRANSMISSION_DETAIL_PATTERN, "Transmission"))

    # transaformed data object
    transformed_data = {
        "source_site": SOURCE_SITE,
        "listing_id": listing_id,
        "url": f"https://bringatrailer.com/listing/{listing_id}/",
        "make": make,
        "model_raw": model_raw,
        "model_normalized": model_normalized,
        "year": year,
        "mileage": mileage,
        "vin": vin,
        "sale_price": sale_price,
        "sold": sold,
        "auction_end_date": auction_end_date,
        "transmission": transmission,
        "listing_details_raw": listing_details,
    }
    logger.info("Transformed BAT listing HTML for listing_id=%s", listing_id)
    return transformed_data


def get_product_json_ld(soup):
    for script_tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        script_content = script_tag.string
        # Some script tags may be empty or contain invalid JSON, so we need to handle those cases gracefully
        if not script_content:
            continue
        # Attempt to extract the JSON-LD content
        try:
            payload = json.loads(script_content)
        except json.JSONDecodeError:
            continue
        # The JSON-LD may be a single object or an array of objects, so we need to handle both cases
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict) and item.get("@type") == "Product":
                    return item
        elif isinstance(payload, dict) and payload.get("@type") == "Product":
            return payload
    raise ValueError("No valid Product JSON-LD found in listing HTML")

def extract_listing_title(soup, product_data):
    title = product_data.get("name")
    # if the title isn't in the JSON-LD, fall back to the meta tag
    if not title:
        meta_tag = soup.find("meta", attrs={"name": "parsely-title"})
        title = meta_tag.get("content") if meta_tag else None
    # if we still don't have a title, raise an error
    if not title:
        raise ValueError("Could not parse listing title")
    return _strip_listing_prefix(title)

def _strip_listing_prefix(title):
    match = re.search(r"(\d{4}\s+.+)", title)
    if not match:
        raise ValueError("Could not parse listing title")
    return match.group(1).strip()

def parse_model(soup):
    values = extract_group_value(soup, "Model")
    if values is None:
        return None
    return values[0]

def parse_make(soup):
    values = extract_group_value(soup, "Make")
    if values is None:
        raise ValueError("Could not find 'Make' group")
    return values[0]

def get_listing_details(soup):
    details_header = soup.find("strong", string=re.compile(r"Listing Details"))
    if not details_header:
        raise ValueError("Could not parse listing details")

    details_container = details_header.find_parent("div", class_="item")
    details_list = details_container.find("ul") if details_container else None
    if not details_list:
        raise ValueError("Could not parse listing details")

    values = [item.get_text(" ", strip=True) for item in details_list.find_all("li")]
    if not values:
        raise ValueError("Could not parse listing details")
    return values

# Find the corresponding value for a given field in the listing details
def find_detail_value(values, pattern, field_name):
    for value in values:
        if re.search(pattern, value, re.IGNORECASE):
            return value
    raise ValueError(f"Could not parse {field_name}")

def parse_mileage(raw_mileage):
    mileage = raw_mileage.strip().lower()

    if "tmu" in mileage or "unknown" in mileage:
        return None

    match = re.search(r"(\d{1,3}(?:,\d{3})*|\d+)(k)?\s+miles\b", mileage)
    if not match:
        raise ValueError(f"Could not parse mileage")

    mileage = int(match.group(1).replace(",", ""))
    if match.group(2):
        mileage *= 1000

    return mileage

def extract_vin(raw_vin):
    match = re.search(r"Chassis:\s*([A-HJ-NPR-Z0-9]+)", raw_vin, re.IGNORECASE)
    if not match:
        raise ValueError("Could not parse VIN")
    return match.group(1).upper()

def extract_sale_price(soup, product_data):
    offers = product_data.get("offers", {})
    price = offers.get("price") if isinstance(offers, dict) else None
    if price:
        return int(float(price))

    bid_label = soup.find(string=re.compile(r"Winning Bid"))
    if not bid_label:
        raise ValueError("Could not parse sale price")

    bid_row = bid_label.find_parent("tr")
    bid_text = bid_row.get_text(" ", strip=True) if bid_row else ""
    match = re.search(r"\$([\d,]+)", bid_text)
    if not match:
        raise ValueError("Could not parse sale price")
    return int(match.group(1).replace(",", ""))

def extract_sold_status(soup):
    available_info = soup.select_one(".listing-available-info")
    if not available_info:
        raise ValueError("Could not parse sold status")
    text = available_info.get_text(" ", strip=True)
    if "Bid to" in text or "Withdrawn on" in text:
        return False
    elif "Sold for" in text:
        return True
    raise ValueError("Could not parse sold status")

def extract_auction_end_date(soup):
    date_tag = soup.select_one(
        "span.date.date-localize, span.info-value.noborder-tiny.date-localize"
    )

    if not date_tag:
        raise ValueError("Could not find sale date")

    timestamp = date_tag.get("data-timestamp")
    if not timestamp:
        raise ValueError("Sale date missing data-timestamp")

    return datetime.fromtimestamp(int(timestamp)).date().isoformat()

def normalize_transmission(raw_transmission):
    value = raw_transmission.lower()
    if "manual" in value:
        return "manual"
    if value:
        return "automatic"
    raise ValueError("Could not normalize transmission")
