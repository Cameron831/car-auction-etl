import subprocess
import time
import uuid
from pathlib import Path

import psycopg
import pytest

from app.sources.bat.transform import transform_listing_html


LISTING_ID = "2004-bmw-m3-coupe-232"
REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_PATH = REPO_ROOT / "app" / "db" / "schema.sql"
RAW_LISTING_HTML = """
<html>
    <head>
        <script type="application/ld+json">
        {
            "@context": "http://schema.org",
            "@type": "Product",
            "name": "BMW E46 M3",
            "offers": {
                "@type": "Offer",
                "priceCurrency": "USD",
                "price": 19750
            }
        }
        </script>
    </head>
    <body>
        <button class="group-title">
            <strong class="group-title-label">Make</strong>
            BMW
        </button>
        <button class="group-title">
            <strong class="group-title-label">Model</strong>
            BMW E46 M3
        </button>
        <div class="item">
            <strong>Listing Details</strong>
            <ul>
                <li>Chassis: WBSBL93414PN57203</li>
                <li>178k Miles</li>
                <li>3.2-Liter S54 Inline-Six</li>
                <li>Six-Speed Manual Transmission</li>
                <li>Limited-Slip Differential</li>
                <li>Carbon Black Metallic Paint</li>
                <li>Cinnamon Nappa Leather Upholstery</li>
                <li>18" Apex EC-7 Wheels</li>
                <li>Xenon Headlights</li>
                <li>Glass Sunroof</li>
                <li>Parking Sensors</li>
                <li>Heated Power-Adjustable Front Seats</li>
                <li>CD Stereo</li>
                <li>Harman Kardon Sound System</li>
                <li>Automatic Climate Control</li>
                <li>Window Sticker</li>
                <li>Spare Parts</li>
            </ul>
        </div>
        <div class="listing-available">
            <div class="listing-available-info">
                <span class="info-value noborder-tiny">
                    Sold for <strong>USD $19,750</strong>
                </span>
            </div>
            <span class="date date-localize" data-timestamp="1774898451"></span>
        </div>
    </body>
</html>
"""

EXPECTED_TRANSFORMED_DATA = {
    "source_site": "bringatrailer",
    "listing_id": LISTING_ID,
    "url": f"https://bringatrailer.com/listing/{LISTING_ID}/",
    "make": "BMW",
    "model_raw": "BMW E46 M3",
    "model_normalized": "M3",
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


def test_transform_listing_html_reads_raw_html_from_postgres_container(
    monkeypatch,
):
    if not _docker_daemon_available():
        pytest.skip("Docker daemon is not available")

    container_name = f"auction-transform-test-{uuid.uuid4().hex}"
    schema_mount = f"{SCHEMA_PATH}:/docker-entrypoint-initdb.d/001-schema.sql:ro"

    try:
        _run(
            [
                "docker",
                "run",
                "--rm",
                "-d",
                "--name",
                container_name,
                "-e",
                "POSTGRES_DB=auction_etl",
                "-e",
                "POSTGRES_USER=auction_user",
                "-e",
                "POSTGRES_PASSWORD=localdevpassword",
                "-p",
                "127.0.0.1::5432",
                "-v",
                schema_mount,
                "postgres:17",
            ]
        )
        _wait_for_postgres(container_name)

        port = _host_port(container_name)
        database_url = (
            f"postgresql://auction_user:localdevpassword@127.0.0.1:{port}/auction_etl"
        )
        _wait_for_database_url(database_url)
        monkeypatch.setenv("DATABASE_URL", database_url)
        _insert_raw_html(database_url, LISTING_ID, RAW_LISTING_HTML)

        transformed_data = transform_listing_html(LISTING_ID)
        assert transformed_data == EXPECTED_TRANSFORMED_DATA
    finally:
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, text=True)


def test_transform_listing_html_missing_raw_record_raises_clear_error(
    monkeypatch,
):
    if not _docker_daemon_available():
        pytest.skip("Docker daemon is not available")

    container_name = f"auction-transform-missing-test-{uuid.uuid4().hex}"
    schema_mount = f"{SCHEMA_PATH}:/docker-entrypoint-initdb.d/001-schema.sql:ro"

    try:
        _run(
            [
                "docker",
                "run",
                "--rm",
                "-d",
                "--name",
                container_name,
                "-e",
                "POSTGRES_DB=auction_etl",
                "-e",
                "POSTGRES_USER=auction_user",
                "-e",
                "POSTGRES_PASSWORD=localdevpassword",
                "-p",
                "127.0.0.1::5432",
                "-v",
                schema_mount,
                "postgres:17",
            ]
        )
        _wait_for_postgres(container_name)

        port = _host_port(container_name)
        database_url = (
            f"postgresql://auction_user:localdevpassword@127.0.0.1:{port}/auction_etl"
        )
        _wait_for_database_url(database_url)
        monkeypatch.setenv("DATABASE_URL", database_url)

        with pytest.raises(LookupError, match="Raw HTML record not found for listing ID: missing-listing"):
            transform_listing_html("missing-listing")
    finally:
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, text=True)


def _insert_raw_html(database_url, listing_id, raw_html):
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO raw_listing_html (
                    source_site,
                    source_listing_id,
                    url,
                    raw_html
                ) VALUES (
                    %s,
                    %s,
                    %s,
                    %s
                )
                """,
                (
                    "bringatrailer",
                    listing_id,
                    f"https://bringatrailer.com/listing/{listing_id}/",
                    raw_html,
                ),
            )


def _docker_daemon_available():
    try:
        result = subprocess.run(["docker", "info"], capture_output=True, text=True)
    except FileNotFoundError:
        return False
    return result.returncode == 0


def _wait_for_postgres(container_name):
    deadline = time.monotonic() + 30
    last_error = ""
    while time.monotonic() < deadline:
        result = subprocess.run(
            [
                "docker",
                "exec",
                container_name,
                "psql",
                "-U",
                "auction_user",
                "-d",
                "auction_etl",
                "-t",
                "-A",
                "-c",
                "SELECT 1;",
            ],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            return
        last_error = result.stderr or result.stdout
        time.sleep(1)

    pytest.fail(f"Postgres did not become ready: {last_error}")


def _host_port(container_name):
    result = _run(["docker", "port", container_name, "5432/tcp"])
    return result.stdout.rsplit(":", maxsplit=1)[-1].strip()


def _wait_for_database_url(database_url):
    deadline = time.monotonic() + 30
    last_error = ""
    while time.monotonic() < deadline:
        try:
            with psycopg.connect(database_url) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
            return
        except psycopg.OperationalError as exc:
            last_error = str(exc)
            time.sleep(1)

    pytest.fail(f"Postgres host connection did not become ready: {last_error}")


def _run(command):
    return subprocess.run(command, capture_output=True, text=True, check=True)
