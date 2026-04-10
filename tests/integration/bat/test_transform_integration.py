import json
import shutil
from pathlib import Path

import pytest

from app.sources.bat.transform import store_transformed_data, transform_listing_html


LISTING_ID = "2004-bmw-m3-coupe-232"
FIXTURE_PATH = Path(__file__).resolve().parents[3] / "data" / "raw" / "bat" / f"{LISTING_ID}.html"

EXPECTED_TRANSFORMED_DATA = {
    "source_site": "bringatrailer",
    "listing_id": LISTING_ID,
    "url": f"https://bringatrailer.com/listing/{LISTING_ID}/",
    "make": "BMW",
    "model": "BMW E46 M3",
    "year": 2004,
    "mileage": 178000,
    "vin": "WBSBL93414PN57203",
    "sale_price": 19750,
    "sold": True,
    "auction_end_date": "2026-03-30",
    "transmission": "manual",
    "listing_details_raw": [
        "Chassis: WBSBL93414PN57203",
        "178k Miles",
        "3.2-Liter S54 Inline-Six",
        "Six-Speed Manual Transmission",
        "Limited-Slip Differential",
        "Carbon Black Metallic Paint",
        "Cinnamon Nappa Leather Upholstery",
        '18" Apex EC-7 Wheels',
        "Xenon Headlights",
        "Glass Sunroof",
        "Parking Sensors",
        "Heated Power-Adjustable Front Seats",
        "CD Stereo",
        "Harman Kardon Sound System",
        "Automatic Climate Control",
        "Window Sticker",
        "Spare Parts",
    ],
}


def test_transform_listing_html_and_store_transformed_data_with_real_fixture(tmp_path, mocker):
    raw_dir = tmp_path / "data" / "raw" / "bat"
    transformed_dir = tmp_path / "data" / "transformed" / "bat"
    raw_dir.mkdir(parents=True)
    shutil.copyfile(FIXTURE_PATH, raw_dir / f"{LISTING_ID}.html")

    mocker.patch("app.sources.bat.transform.RAW_HTML_DIR", raw_dir)
    mocker.patch("app.sources.bat.transform.TRANSFORMED_HTML_DIR", transformed_dir)

    transformed_data = transform_listing_html(LISTING_ID)
    assert transformed_data == EXPECTED_TRANSFORMED_DATA

    saved_path = store_transformed_data(LISTING_ID, transformed_data)

    assert saved_path == transformed_dir / f"{LISTING_ID}.json"
    assert json.loads(saved_path.read_text(encoding="utf-8")) == EXPECTED_TRANSFORMED_DATA


def test_store_transformed_data_overwrites_existing_transformed_fixture(tmp_path, mocker):
    transformed_dir = tmp_path / "data" / "transformed" / "bat"
    existing_path = transformed_dir / f"{LISTING_ID}.json"
    existing_path.parent.mkdir(parents=True)
    existing_path.write_text(json.dumps({"listing_id": LISTING_ID, "stale": True}), encoding="utf-8")

    mocker.patch("app.sources.bat.transform.TRANSFORMED_HTML_DIR", transformed_dir)

    saved_path = store_transformed_data(LISTING_ID, EXPECTED_TRANSFORMED_DATA)

    assert saved_path == existing_path
    assert json.loads(saved_path.read_text(encoding="utf-8")) == EXPECTED_TRANSFORMED_DATA


def test_transform_listing_html_missing_raw_fixture_does_not_write_output(tmp_path, mocker):
    raw_dir = tmp_path / "data" / "raw" / "bat"
    transformed_dir = tmp_path / "data" / "transformed" / "bat"
    raw_dir.mkdir(parents=True)

    mocker.patch("app.sources.bat.transform.RAW_HTML_DIR", raw_dir)
    mocker.patch("app.sources.bat.transform.TRANSFORMED_HTML_DIR", transformed_dir)

    with pytest.raises(FileNotFoundError, match="Raw HTML file not found for listing ID: missing-listing"):
        transform_listing_html("missing-listing")

    assert not (transformed_dir / "missing-listing.json").exists()
