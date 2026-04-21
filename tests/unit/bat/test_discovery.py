import logging
from datetime import date
from pathlib import Path

import pytest

from app.sources.bat import discovery

FIXTURES_DIR = Path(__file__).parents[2] / "fixtures"


def test_parse_completed_auction_candidates_returns_normalized_unique_candidates():
    candidates = discovery.parse_completed_auction_candidates(_results_html())

    assert candidates == [
        {
            "source_site": "bringatrailer",
            "listing_id": "newest-car",
            "source_listing_id": "newest-car",
            "url": "https://bringatrailer.com/listing/newest-car/",
            "title": "1995 Porsche 911 Carrera Coupe",
            "auction_end_date": "2026-04-19",
            "source_location": "USA",
        },
        {
            "source_site": "bringatrailer",
            "listing_id": "older-car",
            "source_listing_id": "older-car",
            "url": "https://bringatrailer.com/listing/older-car/",
            "title": "2004 BMW M3 Coupe",
            "auction_end_date": "2026-04-18",
            "source_location": "CAN",
        },
    ]


def test_parse_completed_auction_candidates_starts_at_completed_auctions_section():
    candidates = discovery.parse_completed_auction_candidates(
        """
        <main>
            <h2>This Week's Popular Listings</h2>
            <article>
                <a href="/listing/popular-listing/">Popular listing</a>
            </article>
            <h2>Selected Market Results</h2>
            <article>
                <a href="/listing/market-result/">Market result</a>
            </article>
            <h2>Recent Exceptional Results</h2>
            <a class="listing-card" href="/listing/exceptional-result/">
                <h3>Exceptional result</h3>
            </a>
            <h2>All Completed Auctions</h2>
            <a class="listing-card" href="/listing/target-listing/">
                <h3>Target listing</h3>
            </a>
        </main>
        """
    )

    assert [candidate["listing_id"] for candidate in candidates] == ["target-listing"]


def test_parse_completed_auction_candidates_ignores_non_listing_links():
    candidates = discovery.parse_completed_auction_candidates(
        """
        <main>
            <h2>All Completed Auctions</h2>
            <nav>
                <a href="/">Home</a>
                <a href="/auctions/">Auctions</a>
                <a href="/search/?q=porsche">Search</a>
                <a href="/parts/air-cooled-sign/">Parts</a>
                <a href="/model/porsche-911/">Model market</a>
                <a href="https://example.com/listing/not-bat/">External</a>
                <a href="/listing/not-a-card/">Not a card</a>
            </nav>
            <a class="listing-card" href="/listing/real-listing/">
                <h3>Real listing</h3>
            </a>
        </main>
        """
    )

    assert [candidate["listing_id"] for candidate in candidates] == ["real-listing"]


def test_parse_completed_auction_candidates_applies_max_after_normalization():
    candidates = discovery.parse_completed_auction_candidates(
        _results_html(), max_candidates=1
    )

    assert [candidate["listing_id"] for candidate in candidates] == ["newest-car"]


def test_parse_completed_auction_candidates_allows_missing_metadata():
    candidates = discovery.parse_completed_auction_candidates(
        """
        <main>
            <h2>All Completed Auctions</h2>
            <a class="listing-card" href="/listing/no-metadata/">View listing</a>
        </main>
        """
    )

    assert candidates == [
        {
            "source_site": "bringatrailer",
            "listing_id": "no-metadata",
            "source_listing_id": "no-metadata",
            "url": "https://bringatrailer.com/listing/no-metadata/",
        }
    ]


def test_parse_completed_auction_candidates_returns_empty_without_target_section():
    candidates = discovery.parse_completed_auction_candidates(
        """
        <main>
            <h2>This Week's Popular Listings</h2>
            <article>
                <a href="/listing/popular-listing/">Popular listing</a>
            </article>
        </main>
        """
    )

    assert candidates == []


def test_parse_completed_auction_candidates_ignores_json_initial_data_only():
    candidates = discovery.parse_completed_auction_candidates(
        """
        <main>
            <h2>All Completed Auctions</h2>
            <script id="bat-theme-auctions-completed-initial-data">
                var auctionsCompletedInitialData = {
                    "items": [
                        {
                            "title": "JSON-only listing",
                            "url": "https://bringatrailer.com/listing/json-only/"
                        }
                    ]
                };
            </script>
        </main>
        """
    )

    assert candidates == []


def test_parse_completed_auction_candidates_reads_rendered_card_fixture():
    card = (FIXTURES_DIR / "card.html").read_text(encoding="utf-8")

    candidates = discovery.parse_completed_auction_candidates(
        f"""
        <main>
            <h2>All Completed Auctions</h2>
            {card}
        </main>
        """
    )

    assert candidates == [
        {
            "source_site": "bringatrailer",
            "listing_id": "1958-gmc-pickup-7",
            "source_listing_id": "1958-gmc-pickup-7",
            "url": "https://bringatrailer.com/listing/1958-gmc-pickup-7/",
            "title": "1958 GMC 9310 Stepside Pickup 3-Speed",
            "auction_end_date": "2026-04-20",
            "source_location": "CAN",
        }
    ]


def test_fetch_completed_auctions_results_uses_url_and_timeout(mocker, caplog):
    response = mocker.Mock()
    response.text = "<html>Results</html>"
    response.raise_for_status = mocker.Mock()
    mock_get = mocker.patch("app.sources.bat.discovery.requests.get", return_value=response)

    caplog.set_level(logging.INFO)
    html = discovery.fetch_completed_auctions_results(
        "https://bringatrailer.com/auctions/results/"
    )

    assert html == "<html>Results</html>"
    mock_get.assert_called_once_with(
        "https://bringatrailer.com/auctions/results/",
        timeout=10,
    )
    response.raise_for_status.assert_called_once_with()
    assert "Fetching BAT completed auctions results from url=https://bringatrailer.com/auctions/results/" in caplog.text
    assert "Fetched BAT completed auctions results from url=https://bringatrailer.com/auctions/results/" in caplog.text


def test_discover_completed_auctions_returns_summary_counts(mocker):
    mocker.patch(
        "app.sources.bat.discovery.fetch_completed_auctions_results",
        return_value="<html>Results</html>",
    )
    mocker.patch(
        "app.sources.bat.discovery.parse_completed_auction_candidates",
        return_value=[
            {
                "listing_id": "new-car",
                "url": "https://bringatrailer.com/listing/new-car/",
                "auction_end_date": "2026-04-20",
            },
            {
                "listing_id": "existing-car",
                "url": "https://bringatrailer.com/listing/existing-car/",
                "auction_end_date": "2026-04-20",
            },
            {
                "listing_id": "broken-car",
                "url": "https://bringatrailer.com/listing/broken-car/",
                "auction_end_date": "2026-04-20",
            },
        ],
    )
    save_discovered_listing = mocker.patch(
        "app.sources.bat.discovery.save_discovered_listing",
        side_effect=[True, False, RuntimeError("boom")],
    )

    summary = discovery.discover_completed_auctions(
        results_url="https://bringatrailer.com/auctions/results/",
        scrape_date=date(2026, 4, 20),
        max_candidates=3,
    )

    assert summary == discovery.DiscoverySummary(
        candidates_inspected=3,
        newly_discovered=1,
        already_discovered_or_updated=1,
        failed=1,
    )
    assert save_discovered_listing.call_count == 3


def test_discover_completed_auctions_stops_when_candidate_is_older_than_scrape_date(mocker):
    mocker.patch(
        "app.sources.bat.discovery.fetch_completed_auctions_results",
        return_value="<html>Results</html>",
    )
    mocker.patch(
        "app.sources.bat.discovery.parse_completed_auction_candidates",
        return_value=[
            {
                "listing_id": "newest-car",
                "url": "https://bringatrailer.com/listing/newest-car/",
                "auction_end_date": "2026-04-20",
            },
            {
                "listing_id": "same-day-car",
                "url": "https://bringatrailer.com/listing/same-day-car/",
                "auction_end_date": "2026-04-20",
            },
            {
                "listing_id": "older-car",
                "url": "https://bringatrailer.com/listing/older-car/",
                "auction_end_date": "2026-04-19",
            },
            {
                "listing_id": "oldest-car",
                "url": "https://bringatrailer.com/listing/oldest-car/",
                "auction_end_date": "2026-04-18",
            },
        ],
    )
    save_discovered_listing = mocker.patch(
        "app.sources.bat.discovery.save_discovered_listing",
        return_value=True,
    )

    summary = discovery.discover_completed_auctions(
        results_url="https://bringatrailer.com/auctions/results/",
        scrape_date="2026-04-20",
    )

    assert summary == discovery.DiscoverySummary(
        candidates_inspected=3,
        newly_discovered=2,
        already_discovered_or_updated=0,
        failed=0,
    )
    assert [call.args[0]["listing_id"] for call in save_discovered_listing.call_args_list] == [
        "newest-car",
        "same-day-car",
    ]


def test_discover_completed_auctions_marks_missing_auction_end_date_as_failed(mocker):
    mocker.patch(
        "app.sources.bat.discovery.fetch_completed_auctions_results",
        return_value="<html>Results</html>",
    )
    mocker.patch(
        "app.sources.bat.discovery.parse_completed_auction_candidates",
        return_value=[
            {
                "listing_id": "missing-date",
                "url": "https://bringatrailer.com/listing/missing-date/",
            },
            {
                "listing_id": "in-scope",
                "url": "https://bringatrailer.com/listing/in-scope/",
                "auction_end_date": "2026-04-20",
            },
        ],
    )
    save_discovered_listing = mocker.patch(
        "app.sources.bat.discovery.save_discovered_listing",
        return_value=True,
    )
    raw_html_writer = mocker.patch("app.sources.bat.ingest.save_listing_html")

    summary = discovery.discover_completed_auctions(
        results_url="https://bringatrailer.com/auctions/results/",
        scrape_date=date(2026, 4, 20),
    )

    assert summary == discovery.DiscoverySummary(
        candidates_inspected=2,
        newly_discovered=1,
        already_discovered_or_updated=0,
        failed=1,
    )
    save_discovered_listing.assert_called_once_with(
        {
            "listing_id": "in-scope",
            "url": "https://bringatrailer.com/listing/in-scope/",
            "auction_end_date": "2026-04-20",
        }
    )
    raw_html_writer.assert_not_called()


def test_build_discovered_listing_params_maps_candidate_to_schema_columns():
    params = discovery.build_discovered_listing_params(_candidate())

    assert params == {
        "source_site": "bringatrailer",
        "source_listing_id": "test-listing",
        "url": "https://bringatrailer.com/listing/test-listing/",
        "title": "2004 BMW M3 Coupe",
        "auction_end_date": "2026-03-30",
        "source_location": "USA",
    }


def test_build_discovered_listing_params_allows_missing_visible_metadata():
    params = discovery.build_discovered_listing_params(
        {
            "listing_id": "test-listing",
            "url": "https://bringatrailer.com/listing/test-listing/",
        }
    )

    assert params == {
        "source_site": "bringatrailer",
        "source_listing_id": "test-listing",
        "url": "https://bringatrailer.com/listing/test-listing/",
        "title": None,
        "auction_end_date": None,
        "source_location": None,
    }


def test_save_discovered_listing_executes_upsert_for_visible_metadata(mocker, caplog):
    calls = {"executions": []}

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params):
            calls["executions"].append((sql, params))

        def fetchone(self):
            return (True,)

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

    def fake_connect(database_url):
        calls["database_url"] = database_url
        return FakeConnection()

    mocker.patch.dict(
        "os.environ",
        {"DATABASE_URL": "postgresql://user:pass@localhost/db"},
    )
    mocker.patch.object(discovery.psycopg, "connect", side_effect=fake_connect)

    caplog.set_level(logging.INFO)
    discovery.save_discovered_listing(_candidate())

    sql, params = calls["executions"][0]

    assert calls["database_url"] == "postgresql://user:pass@localhost/db"
    assert "INSERT INTO discovered_listings" in sql
    assert "ON CONFLICT (source_site, source_listing_id) DO UPDATE" in sql
    assert "url = EXCLUDED.url" in sql
    assert "title = EXCLUDED.title" in sql
    assert "auction_end_date = EXCLUDED.auction_end_date" in sql
    assert "source_location = EXCLUDED.source_location" in sql
    assert "last_seen_at = NOW()" in sql
    assert "eligible" not in sql
    assert "eligibility_reason" not in sql
    assert "ingested_at" not in sql
    assert "discovered_at" not in sql
    assert params == {
        "source_site": "bringatrailer",
        "source_listing_id": "test-listing",
        "url": "https://bringatrailer.com/listing/test-listing/",
        "title": "2004 BMW M3 Coupe",
        "auction_end_date": "2026-03-30",
        "source_location": "USA",
    }
    assert "Upserted BAT discovered listing for listing_id=test-listing" in caplog.text
    assert "postgresql://user:pass@localhost/db" not in caplog.text


def test_save_discovered_listing_requires_database_url(mocker):
    mocker.patch.dict("os.environ", {}, clear=True)

    with pytest.raises(RuntimeError, match="DATABASE_URL must be set"):
        discovery.save_discovered_listing(_candidate())


def _candidate():
    return {
        "listing_id": "test-listing",
        "url": "https://bringatrailer.com/listing/test-listing/",
        "title": "2004 BMW M3 Coupe",
        "auction_end_date": "2026-03-30",
        "source_location": "USA",
    }


def _results_html():
    return """
    <main>
        <section>
            <h2>All Completed Auctions</h2>
            <a class="listing-card" href="/listing/newest-car/?utm_source=feed">
                <div class="content-main">
                    <h3>1995 Porsche 911 Carrera Coupe</h3>
                    <span class="show-country-name">USA</span>
                    <div class="item-results">
                        Sold for USD $19,911 <span> on 04/19/2026 </span>
                    </div>
                </div>
            </a>
            <a class="listing-card" href="https://bringatrailer.com/listing/newest-car#comments">
                <div class="content-main">
                    <h3>Duplicate link</h3>
                </div>
            </a>
            <a class="listing-card" href="https://bringatrailer.com/listing/older-car/">
                <div class="content-main">
                    <h3>2004 BMW M3 Coupe</h3>
                    <span class="show-country-name">CAN</span>
                    <div class="item-results">
                        Bid to USD $30,000 <span> on 04/18/2026 </span>
                    </div>
                </div>
            </a>
        </section>
    </main>
    """
