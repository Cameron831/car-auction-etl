import subprocess
import time
import uuid
from pathlib import Path

import psycopg
import pytest

from app.sources.bat.ingest import save_listing_html


REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_PATH = REPO_ROOT / "sql" / "schema.sql"


def test_save_listing_html_upserts_raw_html_in_postgres_container(monkeypatch):
    if not _docker_daemon_available():
        pytest.skip("Docker daemon is not available")

    container_name = f"auction-ingest-test-{uuid.uuid4().hex}"
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

        save_listing_html(
            "test-listing",
            "<html>First</html>",
            "https://example.test/first",
        )
        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE raw_listing_html
                    SET processed = TRUE
                    WHERE source_site = %s AND source_listing_id = %s
                    """,
                    ("bringatrailer", "test-listing"),
                )

        save_listing_html(
            "test-listing",
            "<html>Second</html>",
            "https://example.test/second",
        )

        with psycopg.connect(database_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*), MAX(url), MAX(raw_html), bool_or(processed)
                    FROM raw_listing_html
                    WHERE source_site = %s AND source_listing_id = %s
                    """,
                    ("bringatrailer", "test-listing"),
                )
                row_count, url, raw_html, processed = cur.fetchone()

        assert row_count == 1
        assert url == "https://example.test/second"
        assert raw_html == "<html>Second</html>"
        assert processed is False
    finally:
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, text=True)


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
