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
SCHEMA_PATH = REPO_ROOT / "app" / "db" / "schema.sql"


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
            SELECT column_name || ':' || data_type || ':' || is_nullable
            FROM information_schema.columns
            WHERE table_name = 'listings'
              AND column_name IN ('auction_end_date', 'listing_details_raw', 'make', 'model')
            ORDER BY column_name;
            """,
        )
        assert column_rows == [
            "auction_end_date:date:NO",
            "listing_details_raw:jsonb:YES",
            "make:text:NO",
            "model:text:YES",
        ]

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

        raw_column_rows = _psql(
            container_name,
            """
            SELECT column_name || ':' || data_type || ':' || is_nullable
            FROM information_schema.columns
            WHERE table_name = 'raw_listing_html'
              AND column_name IN (
                  'id',
                  'source_site',
                  'source_listing_id',
                  'url',
                  'raw_html',
                  'created_at',
                  'processed'
              )
            ORDER BY column_name;
            """,
        )
        assert raw_column_rows == [
            "created_at:timestamp with time zone:NO",
            "id:bigint:NO",
            "processed:boolean:NO",
            "raw_html:text:NO",
            "source_listing_id:text:NO",
            "source_site:text:NO",
            "url:text:NO",
        ]

        raw_unique_columns = _psql(
            container_name,
            """
            SELECT string_agg(a.attname, ',' ORDER BY array_position(c.conkey, a.attnum))
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
            WHERE t.relname = 'raw_listing_html'
              AND c.contype = 'u'
            GROUP BY c.oid;
            """,
        )
        assert "source_site,source_listing_id" in raw_unique_columns

        raw_defaults = _psql(
            container_name,
            """
            WITH inserted AS (
                INSERT INTO raw_listing_html (
                    source_site,
                    source_listing_id,
                    url,
                    raw_html
                ) VALUES (
                    'bringatrailer',
                    'schema-test',
                    'https://example.test/schema-test',
                    '<html>schema</html>'
                )
                RETURNING processed, created_at
            )
            SELECT processed::text, created_at IS NOT NULL
            FROM inserted;
            """,
        )
        assert raw_defaults == ["false|t"]

        raw_json_column_rows = _psql(
            container_name,
            """
            SELECT column_name || ':' || data_type || ':' || is_nullable
            FROM information_schema.columns
            WHERE table_name = 'raw_listing_json'
              AND column_name IN (
                  'id',
                  'source_site',
                  'source_listing_id',
                  'url',
                  'raw_json',
                  'created_at',
                  'processed'
              )
            ORDER BY column_name;
            """,
        )
        assert raw_json_column_rows == [
            "created_at:timestamp with time zone:NO",
            "id:bigint:NO",
            "processed:boolean:NO",
            "raw_json:jsonb:NO",
            "source_listing_id:text:NO",
            "source_site:text:NO",
            "url:text:NO",
        ]

        raw_json_unique_columns = _psql(
            container_name,
            """
            SELECT string_agg(a.attname, ',' ORDER BY array_position(c.conkey, a.attnum))
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
            WHERE t.relname = 'raw_listing_json'
              AND c.contype = 'u'
            GROUP BY c.oid;
            """,
        )
        assert "source_site,source_listing_id" in raw_json_unique_columns

        raw_json_defaults = _psql(
            container_name,
            """
            WITH inserted AS (
                INSERT INTO raw_listing_json (
                    source_site,
                    source_listing_id,
                    url,
                    raw_json
                ) VALUES (
                    'bringatrailer',
                    'schema-json-test',
                    'https://example.test/schema-json-test',
                    '{"listing":"schema-json-test","sold":true}'::jsonb
                )
                RETURNING raw_json, processed, created_at
            )
            SELECT jsonb_typeof(raw_json), processed::text, created_at IS NOT NULL
            FROM inserted;
            """,
        )
        assert raw_json_defaults == ["object|false|t"]

        discovered_column_rows = _psql(
            container_name,
            """
            SELECT column_name || ':' || data_type || ':' || is_nullable
            FROM information_schema.columns
            WHERE table_name = 'discovered_listings'
              AND column_name IN (
                  'id',
                  'source_site',
                  'source_listing_id',
                  'url',
                  'title',
                  'auction_end_date',
                  'source_location',
                  'eligible',
                  'eligibility_reason',
                  'discovered_at',
                  'last_seen_at',
                  'ingested_at'
              )
            ORDER BY column_name;
            """,
        )
        assert discovered_column_rows == [
            "auction_end_date:date:YES",
            "discovered_at:timestamp with time zone:NO",
            "eligibility_reason:text:YES",
            "eligible:boolean:YES",
            "id:bigint:NO",
            "ingested_at:timestamp with time zone:YES",
            "last_seen_at:timestamp with time zone:NO",
            "source_listing_id:text:NO",
            "source_location:text:YES",
            "source_site:text:NO",
            "title:text:YES",
            "url:text:NO",
        ]

        discovered_unique_columns = _psql(
            container_name,
            """
            SELECT string_agg(a.attname, ',' ORDER BY array_position(c.conkey, a.attnum))
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(c.conkey)
            WHERE t.relname = 'discovered_listings'
              AND c.contype = 'u'
            GROUP BY c.oid;
            """,
        )
        assert "source_site,source_listing_id" in discovered_unique_columns

        discovered_defaults = _psql(
            container_name,
            """
            WITH inserted AS (
                INSERT INTO discovered_listings (
                    source_site,
                    source_listing_id,
                    url
                ) VALUES (
                    'bringatrailer',
                    'schema-discovery-test',
                    'https://bringatrailer.com/listing/schema-discovery-test/'
                )
                RETURNING
                    title,
                    auction_end_date,
                    source_location,
                    eligible,
                    eligibility_reason,
                    discovered_at,
                    last_seen_at,
                    ingested_at
            )
            SELECT
                title IS NULL,
                auction_end_date IS NULL,
                source_location IS NULL,
                eligible IS NULL,
                eligibility_reason IS NULL,
                discovered_at IS NOT NULL,
                last_seen_at IS NOT NULL,
                ingested_at IS NULL
            FROM inserted;
            """,
        )
        assert discovered_defaults == ["t|t|t|t|t|t|t|t"]

        discovered_upsert = _psql(
            container_name,
            """
            WITH original AS (
                SELECT discovered_at
                FROM discovered_listings
                WHERE source_site = 'bringatrailer'
                  AND source_listing_id = 'schema-discovery-test'
            ),
            upserted AS (
                INSERT INTO discovered_listings (
                    source_site,
                    source_listing_id,
                    url,
                    title,
                    auction_end_date,
                    source_location
                ) VALUES (
                    'bringatrailer',
                    'schema-discovery-test',
                    'https://bringatrailer.com/listing/schema-discovery-test-updated/',
                    'Updated title',
                    '2026-03-30',
                    'CAN'
                )
                ON CONFLICT (source_site, source_listing_id) DO UPDATE SET
                    url = EXCLUDED.url,
                    title = EXCLUDED.title,
                    auction_end_date = EXCLUDED.auction_end_date,
                    source_location = EXCLUDED.source_location,
                    last_seen_at = NOW()
                RETURNING
                    id,
                    url,
                    title,
                    auction_end_date,
                    source_location,
                    discovered_at,
                    last_seen_at,
                    eligible,
                    eligibility_reason,
                    ingested_at
            )
            SELECT
                (SELECT COUNT(*) FROM discovered_listings
                 WHERE source_site = 'bringatrailer'
                   AND source_listing_id = 'schema-discovery-test'),
                url,
                title,
                auction_end_date,
                source_location,
                upserted.discovered_at = original.discovered_at,
                upserted.last_seen_at >= original.discovered_at,
                eligible IS NULL,
                eligibility_reason IS NULL,
                ingested_at IS NULL
            FROM upserted
            CROSS JOIN original;
            """,
        )
        assert discovered_upsert == [
            (
                "1|https://bringatrailer.com/listing/schema-discovery-test-updated/"
                "|Updated title|2026-03-30|CAN|t|t|t|t|t"
            )
        ]
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
