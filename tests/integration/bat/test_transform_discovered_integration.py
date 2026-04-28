import subprocess
import time
import uuid
from pathlib import Path

import psycopg
import pytest

from app.pipeline import bat


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_PATH = REPO_ROOT / "app" / "db" / "schema.sql"
VALID_RAW_HTML = """
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
                <li>Six-Speed Manual Transmission</li>
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


def test_transform_discovered_processes_successes_retains_failures_and_respects_batch_size(
    monkeypatch,
):
    if not _docker_daemon_available():
        pytest.skip("Docker daemon is not available")

    container_name = f"auction-transform-discovered-test-{uuid.uuid4().hex}"
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
        _insert_raw_listing_rows(database_url)

        first_summary = bat.transform_discovered_listings(batch_size=2)
        assert first_summary == bat.BatchTransformSummary(
            selected=2,
            transformed_and_loaded=1,
            transform_failed=1,
            load_failed=0,
        )

        assert _raw_processed_states(database_url) == [
            ("2004-first-success", True),
            ("2005-second-success", False),
            ("transform-fail", False),
        ]
        assert _listing_ids(database_url) == ["2004-first-success"]

        second_summary = bat.transform_discovered_listings()
        assert second_summary == bat.BatchTransformSummary(
            selected=2,
            transformed_and_loaded=1,
            transform_failed=1,
            load_failed=0,
        )

        assert _raw_processed_states(database_url) == [
            ("2004-first-success", True),
            ("2005-second-success", True),
            ("transform-fail", False),
        ]
        assert _listing_ids(database_url) == ["2004-first-success", "2005-second-success"]
    finally:
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, text=True)


def _insert_raw_listing_rows(database_url):
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO raw_listing_html (
                    id,
                    source_site,
                    source_listing_id,
                    url,
                    raw_html,
                    created_at,
                    processed
                ) VALUES
                    (
                        10,
                        'bringatrailer',
                        '2004-first-success',
                        'https://bringatrailer.com/listing/2004-first-success/',
                        %s,
                        TIMESTAMPTZ '2026-04-20 08:00:00+00',
                        FALSE
                    ),
                    (
                        20,
                        'bringatrailer',
                        'transform-fail',
                        'https://bringatrailer.com/listing/transform-fail/',
                        '<html><body>broken</body></html>',
                        TIMESTAMPTZ '2026-04-20 08:00:00+00',
                        FALSE
                    ),
                    (
                        30,
                        'bringatrailer',
                        '2005-second-success',
                        'https://bringatrailer.com/listing/2005-second-success/',
                        %s,
                        TIMESTAMPTZ '2026-04-20 09:00:00+00',
                        FALSE
                    )
                """,
                (VALID_RAW_HTML, VALID_RAW_HTML),
            )


def _raw_processed_states(database_url):
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT source_listing_id, processed
                FROM raw_listing_html
                WHERE source_site = 'bringatrailer'
                ORDER BY source_listing_id ASC
                """
            )
            return cur.fetchall()


def _listing_ids(database_url):
    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT source_listing_id
                FROM listings
                WHERE source_site = 'bringatrailer'
                ORDER BY source_listing_id ASC
                """
            )
            return [row[0] for row in cur.fetchall()]


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
