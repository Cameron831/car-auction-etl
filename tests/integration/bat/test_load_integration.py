import subprocess
import time
import uuid
from pathlib import Path

import psycopg
import pytest

from app.sources.bat.load import load_listing


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_PATH = REPO_ROOT / "sql" / "schema.sql"


def test_load_listing_upserts_into_postgres_container(monkeypatch):
    if not _docker_daemon_available():
        pytest.skip("Docker daemon is not available")

    container_name = f"auction-loader-test-{uuid.uuid4().hex}"
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
        monkeypatch.setenv("DATABASE_URL", database_url)

        listing = _transformed_listing(sale_price=19750, details=["Original detail"])
        load_listing(listing)

        updated_listing = _transformed_listing(
            sale_price=20500,
            details=["Updated detail", "6-Speed Manual Transmission"],
        )
        load_listing(updated_listing)

        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*), MAX(sale_price)
                    FROM listings
                    WHERE source_site = %s AND source_listing_id = %s
                    """,
                    ("bringatrailer", "test-listing"),
                )
                row_count, sale_price = cur.fetchone()
                cur.execute(
                    """
                    SELECT listing_details_raw
                    FROM listings
                    WHERE source_site = %s AND source_listing_id = %s
                    """,
                    ("bringatrailer", "test-listing"),
                )
                (listing_details_raw,) = cur.fetchone()

        assert row_count == 1
        assert sale_price == 20500
        assert listing_details_raw == ["Updated detail", "6-Speed Manual Transmission"]
    finally:
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, text=True)


def _transformed_listing(sale_price, details):
    return {
        "source_site": "bringatrailer",
        "listing_id": "test-listing",
        "url": "https://bringatrailer.com/listing/test-listing/",
        "make": "BMW",
        "model": "M3",
        "year": 2004,
        "mileage": 50250,
        "vin": "WBSBL93414PN57203",
        "sale_price": sale_price,
        "sold": True,
        "auction_end_date": "2026-03-30",
        "transmission": "manual",
        "listing_details_raw": details,
    }


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


def _run(command):
    return subprocess.run(command, capture_output=True, text=True, check=True)
