import subprocess
import time
import uuid
from pathlib import Path

import pytest

"""
note:
This test module was generated through my agent-assisted workflow and only
lightly reviewed. Treat it as provisional until it is manually
validated and refined.
"""

REPO_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = REPO_ROOT / "sql" / "schema.sql"


def test_schema_sql_applies_in_isolated_postgres_container():
    if not _docker_daemon_available():
        pytest.skip("Docker daemon is not available")

    container_name = f"auction-schema-test-{uuid.uuid4().hex}"
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
                "-v",
                schema_mount,
                "postgres:17",
            ]
        )
        _wait_for_postgres(container_name)

        column_rows = _psql(
            container_name,
            """
            SELECT column_name || ':' || data_type
            FROM information_schema.columns
            WHERE table_name = 'listings'
              AND column_name IN ('auction_end_date', 'listing_details_raw')
            ORDER BY column_name;
            """,
        )
        assert column_rows == ["auction_end_date:date", "listing_details_raw:jsonb"]

        unique_columns = _psql(
            container_name,
            """
            SELECT string_agg(a.attname, ',' ORDER BY array_position(c.conkey, a.attnum))
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
            WHERE t.relname = 'listings'
              AND c.contype = 'u'
            GROUP BY c.oid;
            """,
        )
        assert "source_site,source_listing_id" in unique_columns
    finally:
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, text=True)


def _docker_daemon_available():
    result = subprocess.run(["docker", "info"], capture_output=True, text=True)
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
            time.sleep(1)
            stable_result = subprocess.run(
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
            if stable_result.returncode == 0:
                return
            last_error = stable_result.stderr or stable_result.stdout
            continue
        last_error = result.stderr or result.stdout
        time.sleep(1)

    pytest.fail(f"Postgres did not become ready: {last_error}")


def _psql(container_name, query):
    result = _run(
        [
            "docker",
            "exec",
            "-i",
            container_name,
            "psql",
            "-U",
            "auction_user",
            "-d",
            "auction_etl",
            "-t",
            "-A",
            "-c",
            query,
        ]
    )
    return [line for line in result.stdout.splitlines() if line]


def _run(command):
    return subprocess.run(command, capture_output=True, text=True, check=True)
