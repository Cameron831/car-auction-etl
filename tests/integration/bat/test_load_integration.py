import subprocess
import time
import uuid
from pathlib import Path

import psycopg
import pytest

from app.sources.bat import discovery
from app.sources.bat.load import load_listing

"""
note:
This test module was generated through my agent-assisted workflow and only
lightly reviewed. Treat it as provisional until it is manually
validated and refined.
"""

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_PATH = REPO_ROOT / "app" / "db" / "schema.sql"


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
        _wait_for_database_url(database_url)
        monkeypatch.setenv("DATABASE_URL", database_url)

        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO raw_listing_html (
                        source_site,
                        source_listing_id,
                        url,
                        raw_html,
                        processed
                    ) VALUES (
                        %s,
                        %s,
                        %s,
                        %s,
                        FALSE
                    )
                    """,
                    (
                        "bringatrailer",
                        "test-listing",
                        "https://bringatrailer.com/listing/test-listing/",
                        "<html>Raw</html>",
                    ),
                )

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
                    SELECT model_raw, model_normalized, listing_details_raw
                    FROM listings
                    WHERE source_site = %s AND source_listing_id = %s
                    """,
                    ("bringatrailer", "test-listing"),
                )
                model_raw, model_normalized, listing_details_raw = cur.fetchone()
                cur.execute(
                    """
                    SELECT processed
                    FROM raw_listing_html
                    WHERE source_site = %s AND source_listing_id = %s
                    """,
                    ("bringatrailer", "test-listing"),
                )
                (raw_processed,) = cur.fetchone()

        assert row_count == 1
        assert sale_price == 20500
        assert model_raw == "BMW E46 M3"
        assert model_normalized == "M3"
        assert listing_details_raw == ["Updated detail", "6-Speed Manual Transmission"]
        assert raw_processed is True
    finally:
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, text=True)


def test_discovery_helpers_select_pending_rows_and_persist_handled_state(monkeypatch):
    if not _docker_daemon_available():
        pytest.skip("Docker daemon is not available")

    container_name = f"auction-discovery-test-{uuid.uuid4().hex}"
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
        _insert_discovered_listing_rows(database_url)

        pending_rows = discovery.load_pending_discovered_listings()
        pending_ids = [row["source_listing_id"] for row in pending_rows]

        limited_rows = discovery.load_pending_discovered_listings(limit=1)
        limited_ids = [row["source_listing_id"] for row in limited_rows]

        discovery.mark_discovered_listing_handled(
            "first-pending",
            False,
            "sale_price missing",
        )
        discovery.mark_discovered_listing_handled("second-pending", True, None)

        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT source_listing_id, eligible, eligibility_reason, ingested_at IS NULL
                    FROM discovered_listings
                    WHERE source_site = 'bringatrailer'
                      AND source_listing_id IN ('first-pending', 'second-pending')
                    ORDER BY source_listing_id ASC
                    """
                )
                state_rows = cur.fetchall()

        assert pending_ids == ["first-pending", "second-pending"]
        assert limited_ids == ["first-pending"]
        assert state_rows == [
            ("first-pending", False, "sale_price missing", True),
            ("second-pending", True, None, True),
        ]
    finally:
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, text=True)


def _transformed_listing(sale_price, details):
    return {
        "source_site": "bringatrailer",
        "listing_id": "test-listing",
        "url": "https://bringatrailer.com/listing/test-listing/",
        "make": "BMW",
        "model_raw": "BMW E46 M3",
        "model_normalized": "M3",
        "year": 2004,
        "mileage": 50250,
        "vin": "WBSBL93414PN57203",
        "sale_price": sale_price,
        "sold": True,
        "auction_end_date": "2026-03-30",
        "transmission": "manual",
        "listing_details_raw": details,
    }


def _insert_discovered_listing_rows(database_url):
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO discovered_listings (
                    id,
                    source_site,
                    source_listing_id,
                    url,
                    title,
                    auction_end_date,
                    eligible,
                    discovered_at,
                    last_seen_at,
                    ingested_at
                ) VALUES
                    (
                        10,
                        'bringatrailer',
                        'first-pending',
                        'https://bringatrailer.com/listing/first-pending/',
                        'First Pending',
                        DATE '2026-03-30',
                        NULL,
                        TIMESTAMPTZ '2026-04-20 08:00:00+00',
                        TIMESTAMPTZ '2026-04-20 08:00:00+00',
                        NULL
                    ),
                    (
                        20,
                        'bringatrailer',
                        'second-pending',
                        'https://bringatrailer.com/listing/second-pending/',
                        'Second Pending',
                        DATE '2026-03-31',
                        NULL,
                        TIMESTAMPTZ '2026-04-20 08:00:00+00',
                        TIMESTAMPTZ '2026-04-20 08:00:00+00',
                        NULL
                    ),
                    (
                        30,
                        'bringatrailer',
                        'already-ingested',
                        'https://bringatrailer.com/listing/already-ingested/',
                        'Already Ingested',
                        DATE '2026-03-29',
                        True,
                        TIMESTAMPTZ '2026-04-19 08:00:00+00',
                        TIMESTAMPTZ '2026-04-19 08:00:00+00',
                        TIMESTAMPTZ '2026-04-21 08:00:00+00'
                    ),
                    (
                        40,
                        'carsandbids',
                        'other-source',
                        'https://carsandbids.com/auctions/other-source',
                        'Other Source',
                        DATE '2026-03-28',
                        True,
                        TIMESTAMPTZ '2026-04-18 08:00:00+00',
                        TIMESTAMPTZ '2026-04-18 08:00:00+00',
                        NULL
                    )
                """
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
