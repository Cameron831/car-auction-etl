import json
import re
from pathlib import Path
from bs4 import BeautifulSoup

RAW_HTML_DIR = Path(__file__).resolve().parents[3] / "data" / "raw" / "bat"
TRANSFORMED_HTML_DIR = Path(__file__).resolve().parents[3] / "data" / "transformed" / "bat"

SOURCE_SITE = "bringatrailer"

def load_listing_html(listing_id):
    file_path = RAW_HTML_DIR / f"{listing_id}.html"
    if not file_path.exists():
        raise FileNotFoundError(f"Raw HTML file not found for listing ID: {listing_id}")
    return file_path.read_text(encoding="utf-8")

def transform_listing_html(listing_id):
    html = load_listing_html(listing_id)
    soup = BeautifulSoup(html, "html.parser")
    product_data = get_product_json_ld(soup)
    listing_title = extract_listing_title(soup, product_data)
    listing_details = get_listing_details(soup)

    # transformed entries
    year = parse_year(listing_title)
    make = parse_make(soup)
    model = parse_model(soup)
    mileage = parse_mileage(find_detail_value(listing_details, r"\bmiles?\b|\btmu\b|\bunknown\b", "Mileage"))

    # Placeholder for transformation logic - to be implemented
    transformed_data = {
        "make": make,
        "model": model,
        "year": year,
        "mileage": mileage,
    }
    return transformed_data

def store_transformed_data(listing_id, data):
    file_path = TRANSFORMED_HTML_DIR / f"{listing_id}.json"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(json.dumps(data, default=str, indent=2), encoding="utf-8")
    return file_path

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

def parse_year(title):
    match = re.match(r"(\d{4})", title)
    if not match:
        raise ValueError("Could not parse year from listing title")
    return int(match.group(1))

def parse_model(soup):
   return extract_group_value(soup, "Model")

def parse_make(soup):
    return extract_group_value(soup, "Make")

def extract_group_value(soup: BeautifulSoup, label: str) -> str:
    for button in soup.select("button.group-title"):
        label_tag = button.select_one("strong.group-title-label")
        if not label_tag:
            continue

        if label_tag.get_text(strip=True) != label:
            continue

        # Get the button text, then remove the label text from the front
        full_text = button.get_text(" ", strip=True)
        label_text = label_tag.get_text(" ", strip=True)

        value = full_text.removeprefix(label_text).strip()
        if not value:
            raise ValueError(f"Found '{label}' group but it had no value")

        return value

    raise ValueError(f"Could not find '{label}' group")

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