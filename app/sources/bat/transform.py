import json
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
    # Placeholder for transformation logic - to be implemented
    transformed_data = {}
    return transformed_data

def store_transformed_data(listing_id, data):
    file_path = TRANSFORMED_HTML_DIR / f"{listing_id}.json"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(json.dumps(data, default=str, indent=2), encoding="utf-8")
    return file_path