import pytest

from app.sources.bat import discover


def test_parse_results_page_extracts_normalized_listing_ids_and_filters_unsupported():
    html = """
    <html>
      <body>
        <article class="auctions-item">
          <a href="/listing/2004-bmw-m3/?utm_source=test">2004 BMW M3 Coupe</a>
        </article>
        <article class="auctions-item">
          <a href="https://bringatrailer.com/listing/1972-honda-cb750/">1972 Honda CB750 Motorcycle</a>
        </article>
        <article class="auctions-item">
          <a href="/listing/2004-bmw-m3/">Duplicate BMW Link</a>
        </article>
      </body>
    </html>
    """

    listings = discover.parse_results_page(html)

    assert listings == [
        discover.DiscoveredListing(
            source_listing_id="2004-bmw-m3",
            url="https://bringatrailer.com/listing/2004-bmw-m3/",
            eligible=True,
            skip_reason=None,
        ),
        discover.DiscoveredListing(
            source_listing_id="1972-honda-cb750",
            url="https://bringatrailer.com/listing/1972-honda-cb750/",
            eligible=False,
            skip_reason="unsupported_listing_type",
        ),
    ]


def test_parse_results_page_classifies_from_title_not_description_text():
    html = """
    <html>
      <body>
        <div class="listing-card">
          <a class="image-overlay" href="/listing/2015-porsche-918-spyder/" title="2015 Porsche 918 Spyder"></a>
          <h3><a href="/listing/2015-porsche-918-spyder/">2015 Porsche 918 Spyder</a></h3>
          <div class="item-excerpt">
            This car has center-lock wheels and a hybrid engine.
          </div>
        </div>
      </body>
    </html>
    """

    listings = discover.parse_results_page(html)

    assert listings == [
        discover.DiscoveredListing(
            source_listing_id="2015-porsche-918-spyder",
            url="https://bringatrailer.com/listing/2015-porsche-918-spyder/",
            eligible=True,
            skip_reason=None,
        )
    ]


def test_normalize_listing_url_rejects_non_listing_urls():
    with pytest.raises(ValueError, match="URL is not a BAT listing URL"):
        discover.normalize_listing_url("https://bringatrailer.com/auctions/results/")


def test_classify_listing_skips_ambiguous_records():
    assert discover.classify_listing("No title metadata") == (
        False,
        "ambiguous_listing_type",
    )


def test_build_discovered_listing_params_maps_schema_columns():
    listing = discover.DiscoveredListing(
        source_listing_id="2004-bmw-m3",
        url="https://bringatrailer.com/listing/2004-bmw-m3/",
        eligible=True,
    )

    assert discover.build_discovered_listing_params(listing) == {
        "source_site": "bringatrailer",
        "source_listing_id": "2004-bmw-m3",
        "url": "https://bringatrailer.com/listing/2004-bmw-m3/",
        "eligible": True,
        "skip_reason": None,
    }


def test_save_discovered_listings_upserts_and_returns_only_new_ids(mocker):
    calls = {"executions": []}

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params):
            calls["executions"].append((sql, params))

        def fetchall(self):
            return [("existing-listing",)]

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
    mocker.patch.object(discover.psycopg, "connect", side_effect=fake_connect)

    new_ids = discover.save_discovered_listings(
        [
            discover.DiscoveredListing(
                source_listing_id="new-listing",
                url="https://bringatrailer.com/listing/new-listing/",
                eligible=True,
            ),
            discover.DiscoveredListing(
                source_listing_id="existing-listing",
                url="https://bringatrailer.com/listing/existing-listing/",
                eligible=True,
            ),
        ]
    )

    assert calls["database_url"] == "postgresql://user:pass@localhost/db"
    assert new_ids == ["new-listing"]
    select_sql, select_params = calls["executions"][0]
    first_upsert_sql, first_upsert_params = calls["executions"][1]
    assert "FROM discovered_listings" in select_sql
    assert select_params == {
        "source_site": "bringatrailer",
        "source_listing_ids": ["new-listing", "existing-listing"],
    }
    assert "ON CONFLICT (source_site, source_listing_id) DO UPDATE" in first_upsert_sql
    assert first_upsert_params["source_listing_id"] == "new-listing"


def test_mark_discovered_listing_ingested_updates_ingested_at(mocker):
    calls = {}

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params):
            calls["sql"] = sql
            calls["params"] = params

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

    mocker.patch.dict(
        "os.environ",
        {"DATABASE_URL": "postgresql://user:pass@localhost/db"},
    )
    mocker.patch.object(discover.psycopg, "connect", return_value=FakeConnection())

    discover.mark_discovered_listing_ingested("2004-bmw-m3")

    assert "UPDATE discovered_listings" in calls["sql"]
    assert "SET ingested_at = NOW()" in calls["sql"]
    assert calls["params"] == {
        "source_site": "bringatrailer",
        "source_listing_id": "2004-bmw-m3",
    }


def test_discover_recent_listings_stops_after_first_page_with_no_new_ids():
    fetched_urls = []

    def fake_fetch_page(url):
        fetched_urls.append(url)
        return f"""
        <article class="auction-card">
          <a href="/listing/page-{len(fetched_urls)}-listing/">2004 BMW M3</a>
        </article>
        """

    def fake_save_listings(listings):
        if listings[0].source_listing_id == "page-2-listing":
            return []
        return [listing.source_listing_id for listing in listings]

    result = discover.discover_recent_listings(
        max_pages=5,
        fetch_page=fake_fetch_page,
        save_listings=fake_save_listings,
    )

    assert fetched_urls == [
        "https://bringatrailer.com/auctions/results/",
        "https://bringatrailer.com/auctions/results/?page=2",
    ]
    assert result == {
        "discovered_count": 2,
        "new_listing_ids": ["page-1-listing"],
    }


def test_discover_recent_listings_honors_max_pages():
    fetched_urls = []

    def fake_fetch_page(url):
        fetched_urls.append(url)
        return f"""
        <article class="auction-card">
          <a href="/listing/page-{len(fetched_urls)}-listing/">2004 BMW M3</a>
        </article>
        """

    result = discover.discover_recent_listings(
        max_pages=2,
        fetch_page=fake_fetch_page,
        save_listings=lambda listings: [listing.source_listing_id for listing in listings],
    )

    assert fetched_urls == [
        "https://bringatrailer.com/auctions/results/",
        "https://bringatrailer.com/auctions/results/?page=2",
    ]
    assert result["new_listing_ids"] == ["page-1-listing", "page-2-listing"]


def test_ingest_pending_discovered_listings_fetches_only_loaded_pending_records(mocker):
    pending = [
        discover.DiscoveredListing(
            source_listing_id="2004-bmw-m3",
            url="https://bringatrailer.com/listing/2004-bmw-m3/",
            eligible=True,
        )
    ]
    fetch_listing_html = mocker.patch.object(discover, "fetch_listing_html", return_value="<html>listing</html>")
    save_listing_html = mocker.patch.object(discover, "save_listing_html")
    mark_ingested = mocker.patch.object(discover, "mark_discovered_listing_ingested")
    transform_listing_html = mocker.patch.object(
        discover,
        "transform_listing_html",
        return_value={"listing_id": "2004-bmw-m3"},
    )
    load_listing = mocker.patch.object(discover, "load_listing")

    ingested_ids = discover.ingest_pending_discovered_listings(load_pending=lambda: pending)

    assert ingested_ids == ["2004-bmw-m3"]
    fetch_listing_html.assert_called_once_with("2004-bmw-m3")
    save_listing_html.assert_called_once_with(
        "2004-bmw-m3",
        "<html>listing</html>",
        "https://bringatrailer.com/listing/2004-bmw-m3/",
    )
    mark_ingested.assert_called_once_with("2004-bmw-m3")
    transform_listing_html.assert_called_once_with("2004-bmw-m3")
    load_listing.assert_called_once_with({"listing_id": "2004-bmw-m3"})
