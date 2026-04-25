import json
import subprocess
import time
import uuid
from pathlib import Path

import psycopg
import pytest
from psycopg.types.json import Jsonb

from app.sources.carsandbids.load import load_listing
from app.sources.carsandbids.transform import transform_listing_json


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_PATH = REPO_ROOT / "app" / "db" / "schema.sql"
FIXTURE_PATH = REPO_ROOT / "tests" / "fixtures" / "carsandbids_listing.json"


def test_transform_and_load_listing_json_upserts_into_postgres_container(monkeypatch):
    if not _docker_daemon_available():
        pytest.skip("Docker daemon is not available")

    container_name = f"auction-cnb-loader-test-{uuid.uuid4().hex}"
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

        payload = json.loads(FIXTURE_PATH.read_text())
        _insert_raw_listing_json(database_url, "3pnjnnx6", payload)

        listing = transform_listing_json("3pnjnnx6")
        load_listing(listing)

        payload["stats"]["sale_amount"] = 80000
        _insert_raw_listing_json(database_url, "3pnjnnx6", payload)
        updated_listing = transform_listing_json("3pnjnnx6")
        load_listing(updated_listing)

        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*), MAX(sale_price)
                    FROM listings
                    WHERE source_site = %s AND source_listing_id = %s
                    """,
                    ("carsandbids", "3pnjnnx6"),
                )
                row_count, sale_price = cur.fetchone()
                cur.execute(
                    """
                    SELECT make, model, year, mileage, vin, sold, auction_end_date, transmission, listing_details_raw
                    FROM listings
                    WHERE source_site = %s AND source_listing_id = %s
                    """,
                    ("carsandbids", "3pnjnnx6"),
                )
                listing_row = cur.fetchone()
                cur.execute(
                    """
                    SELECT processed
                    FROM raw_listing_json
                    WHERE source_site = %s AND source_listing_id = %s
                    """,
                    ("carsandbids", "3pnjnnx6"),
                )
                (raw_processed,) = cur.fetchone()

        assert row_count == 1
        assert sale_price == 80000
        assert listing_row[0:6] == (
            "Porsche",
            "991 911",
            2013,
            56700,
            "WP0AA2A95DS107582",
            True,
        )
        assert listing_row[6].isoformat() == "2026-04-20"
        assert listing_row[7] == "manual"
        assert listing_row[8]["engine"] == "3.4L Flat-6"
        assert "title" not in listing_row[8]
        assert "location" not in listing_row[8]
        assert raw_processed is True
    finally:
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, text=True)


def _insert_raw_listing_json(database_url, listing_id, payload):
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO raw_listing_json (
                    source_site,
                    source_listing_id,
                    url,
                    raw_json,
                    processed
                ) VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    FALSE
                )
                ON CONFLICT (source_site, source_listing_id) DO UPDATE SET
                    raw_json = EXCLUDED.raw_json,
                    processed = FALSE
                """,
                (
                    "carsandbids",
                    listing_id,
                    f"https://carsandbids.com/auctions/{listing_id}",
                    Jsonb(payload),
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
