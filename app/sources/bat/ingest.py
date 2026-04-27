import logging
import os
import re

import psycopg
import requests


SOURCE_SITE = "bringatrailer"
logger = logging.getLogger(__name__)
BAT_MIN_YEAR = 1946
BAT_ALLOWED_COUNTRY = "USA"
EXCLUDED_CATEGORY_VALUES = {
    "aircraft",
    "all-terrain vehicles",
    "boats",
    "charity & non-profit",
    "go-karts",
    "hot rods",
    "military vehicles",
    "minibikes & scooters",
    "motorcycles",
    "parts",
    "projects",
    "race cars",
    "rvs & campers",
    "service vehicles",
    "side-by-sides",
    "tractors",
    "trains",
    "wheels",
}

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


def evaluate_listing_eligibility(soup, listing_id):
    try:
        year = parse_listing_id_year(listing_id)
    except ValueError:
        return False, "listing ID year missing"

    if year < BAT_MIN_YEAR:
        return False, "year before 1946"

    if extract_country(soup) != BAT_ALLOWED_COUNTRY:
        return False, "listing outside US"

    categories = extract_group_value(soup, "Category")
    if categories is None:
        return True, None

    for category in categories:
        if _normalize_category_value(category) in EXCLUDED_CATEGORY_VALUES:
            return False, f"excluded category: {category}"

    return True, None


def parse_listing_id_year(listing_id):
    match = re.match(r"^(\d{4})(?:-|$)", listing_id or "")
    if not match:
        raise ValueError("Could not parse year from listing ID")
    return int(match.group(1))


def extract_country(soup):
    country_tag = soup.select_one("span.show-country-name")
    if country_tag is None:
        return None

    country = country_tag.get_text(" ", strip=True)
    if not country:
        return None
    return country


def extract_group_value(soup, label):
    values = []

    for label_tag in soup.select("strong.group-title-label"):
        if label_tag.get_text(strip=True) != label:
            continue

        group_tag = (
            label_tag.find_parent("button", class_="group-title")
            or label_tag.find_parent("a", class_="group-link")
        )
        if not group_tag:
            continue

        full_text = group_tag.get_text(" ", strip=True)
        label_text = label_tag.get_text(" ", strip=True)

        value = full_text.removeprefix(label_text).strip()
        if not value:
            continue

        values.append(value)

    if not values:
        return None

    return values


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


def _normalize_category_value(value):
    return " ".join(value.lower().split())
