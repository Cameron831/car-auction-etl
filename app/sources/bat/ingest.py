from pathlib import Path

import requests


RAW_HTML_DIR = Path(__file__).resolve().parents[3] / "data" / "raw" / "bat"


def fetch_listing_html(id):
    url = f"https://bringatrailer.com/listing/{id}"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.text


def save_listing_html(listing_id, html):
    RAW_HTML_DIR.mkdir(parents=True, exist_ok=True)
    file_path = RAW_HTML_DIR / f"{listing_id}.html"
    file_path.write_text(html, encoding="utf-8", newline="")
    return file_path
