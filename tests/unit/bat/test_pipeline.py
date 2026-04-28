import logging
from datetime import date

from app.pipeline import bat


def test_ingest_listing_fetches_and_saves_listing_html(mocker):
    fetch_listing_html = mocker.patch(
        "app.pipeline.bat.fetch_listing_html",
        return_value="<html>Test</html>",
    )
    save_listing_html = mocker.patch("app.pipeline.bat.save_listing_html")

    bat.ingest_listing("test-id")

    fetch_listing_html.assert_called_once_with("test-id")
    save_listing_html.assert_called_once_with("test-id", "<html>Test</html>")


def test_transform_listing_transforms_and_loads_listing(mocker):
    transformed_listing = {"listing_id": "test-id"}
    transform_listing_html = mocker.patch(
        "app.pipeline.bat.transform_listing_html",
        return_value=transformed_listing,
    )
    load_listing = mocker.patch("app.pipeline.bat.load_listing")

    bat.transform_listing("test-id")

    transform_listing_html.assert_called_once_with("test-id")
    load_listing.assert_called_once_with(transformed_listing)


def test_run_listing_executes_ingest_transform_load_in_order(mocker):
    calls = []
    transformed_listing = {"listing_id": "test-id"}

    mocker.patch(
        "app.pipeline.bat.fetch_listing_html",
        side_effect=lambda listing_id: calls.append(("fetch", listing_id)) or "<html>Test</html>",
    )
    mocker.patch(
        "app.pipeline.bat.save_listing_html",
        side_effect=lambda listing_id, html: calls.append(("save", listing_id, html)),
    )
    mocker.patch(
        "app.pipeline.bat.transform_listing_html",
        side_effect=lambda listing_id: calls.append(("transform", listing_id)) or transformed_listing,
    )
    mocker.patch(
        "app.pipeline.bat.load_listing",
        side_effect=lambda listing: calls.append(("load", listing)),
    )

    bat.run_listing("test-id")

    assert calls == [
        ("fetch", "test-id"),
        ("save", "test-id", "<html>Test</html>"),
        ("transform", "test-id"),
        ("load", transformed_listing),
    ]


def test_discover_listings_delegates_to_discovery(mocker):
    discover_completed_auctions = mocker.patch("app.pipeline.bat.discover_completed_auctions")

    bat.discover_listings(scrape_date=date(2026, 4, 20), max_candidates=5)

    discover_completed_auctions.assert_called_once_with(
        scrape_date=date(2026, 4, 20),
        max_candidates=5,
    )


def test_ingest_discovered_listings_returns_zeroed_summary_for_empty_batch(mocker):
    mocker.patch(
        "app.pipeline.bat.load_pending_discovered_listings",
        return_value=[],
    )

    summary = bat.ingest_discovered_listings()

    assert summary == bat.BatchIngestSummary()


def test_transform_discovered_listings_returns_zeroed_summary_for_empty_batch(mocker):
    mocker.patch(
        "app.pipeline.bat.load_pending_raw_listing_html",
        return_value=[],
    )

    summary = bat.transform_discovered_listings()

    assert summary == bat.BatchTransformSummary()


def test_ingest_discovered_listings_marks_reject_without_saving_html(mocker, caplog):
    mocker.patch(
        "app.pipeline.bat.load_pending_discovered_listings",
        return_value=[
            {
                "source_listing_id": "rejected",
                "title": "1940 Ford Coupe",
                "url": "https://bringatrailer.com/listing/rejected/",
            }
        ],
    )
    mocker.patch(
        "app.pipeline.bat.fetch_listing_html",
        return_value="<html><body>listing</body></html>",
    )
    evaluate_listing_eligibility = mocker.patch(
        "app.pipeline.bat.evaluate_listing_eligibility",
        return_value=(False, "year before 1946"),
    )
    mark_handled = mocker.patch("app.pipeline.bat.mark_discovered_listing_handled")
    save_listing_html = mocker.patch("app.pipeline.bat.save_listing_html")

    caplog.set_level(logging.INFO)
    summary = bat.ingest_discovered_listings()

    evaluate_listing_eligibility.assert_called_once()
    _, listing_id = evaluate_listing_eligibility.call_args.args
    assert listing_id == "rejected"
    mark_handled.assert_called_once_with("rejected", False, "year before 1946")
    save_listing_html.assert_not_called()
    assert (
        "BAT ingest-discovered listing rejected for listing_id=rejected "
        "reason=year before 1946"
    ) in caplog.text
    assert summary == bat.BatchIngestSummary(
        selected=1,
        scrape_attempted=1,
        rejected=1,
    )


def test_ingest_discovered_listings_records_scrape_failure_without_marking_row(mocker):
    mocker.patch(
        "app.pipeline.bat.load_pending_discovered_listings",
        return_value=[
            {
                "source_listing_id": "scrape-fail",
                "title": "1967 Porsche 911S Coupe",
                "url": "https://bringatrailer.com/listing/scrape-fail/",
            }
        ],
    )
    mocker.patch(
        "app.pipeline.bat.fetch_listing_html",
        side_effect=RuntimeError("network failed"),
    )
    mark_handled = mocker.patch("app.pipeline.bat.mark_discovered_listing_handled")

    summary = bat.ingest_discovered_listings()

    mark_handled.assert_not_called()
    assert summary == bat.BatchIngestSummary(
        selected=1,
        scrape_attempted=1,
        scrape_failed=1,
    )


def test_ingest_discovered_listings_marks_category_reject_without_saving_html(mocker, caplog):
    mocker.patch(
        "app.pipeline.bat.load_pending_discovered_listings",
        return_value=[
            {
                "source_listing_id": "category-reject",
                "title": "1967 Porsche 911S Coupe",
                "url": "https://bringatrailer.com/listing/category-reject/",
            }
        ],
    )
    mocker.patch(
        "app.pipeline.bat.fetch_listing_html",
        return_value="<html><body>listing</body></html>",
    )
    evaluate_listing_eligibility = mocker.patch(
        "app.pipeline.bat.evaluate_listing_eligibility",
        return_value=(False, "excluded category: projects"),
    )
    mark_handled = mocker.patch("app.pipeline.bat.mark_discovered_listing_handled")
    save_listing_html = mocker.patch("app.pipeline.bat.save_listing_html")

    caplog.set_level(logging.INFO)
    summary = bat.ingest_discovered_listings()

    evaluate_listing_eligibility.assert_called_once()
    _, listing_id = evaluate_listing_eligibility.call_args.args
    assert listing_id == "category-reject"
    mark_handled.assert_called_once_with(
        "category-reject",
        False,
        "excluded category: projects",
    )
    save_listing_html.assert_not_called()
    assert (
        "BAT ingest-discovered listing rejected for listing_id=category-reject "
        "reason=excluded category: projects"
    ) in caplog.text
    assert summary == bat.BatchIngestSummary(
        selected=1,
        scrape_attempted=1,
        rejected=1,
    )


def test_ingest_discovered_listings_saves_html_and_marks_eligible_for_pass(mocker):
    mocker.patch(
        "app.pipeline.bat.load_pending_discovered_listings",
        return_value=[
            {
                "source_listing_id": "accepted",
                "title": "1967 Porsche 911S Coupe",
                "url": "https://bringatrailer.com/listing/accepted/",
            }
        ],
    )
    mocker.patch(
        "app.pipeline.bat.fetch_listing_html",
        return_value="<html><body>listing</body></html>",
    )
    mocker.patch(
        "app.pipeline.bat.evaluate_listing_eligibility",
        return_value=(True, None),
    )
    save_listing_html = mocker.patch("app.pipeline.bat.save_listing_html")
    mark_handled = mocker.patch("app.pipeline.bat.mark_discovered_listing_handled")
    calls = mocker.Mock()
    calls.attach_mock(save_listing_html, "save_listing_html")
    calls.attach_mock(mark_handled, "mark_handled")

    summary = bat.ingest_discovered_listings()

    save_listing_html.assert_called_once_with(
        "accepted",
        "<html><body>listing</body></html>",
        url="https://bringatrailer.com/listing/accepted/",
    )
    mark_handled.assert_called_once_with("accepted", True, None)
    assert calls.mock_calls == [
        mocker.call.mark_handled("accepted", True, None),
        mocker.call.save_listing_html(
            "accepted",
            "<html><body>listing</body></html>",
            url="https://bringatrailer.com/listing/accepted/",
        ),
    ]
    assert summary == bat.BatchIngestSummary(
        selected=1,
        scrape_attempted=1,
        raw_html_stored=1,
        accepted=1,
    )


def test_ingest_discovered_listings_uses_listing_id_when_discovered_title_missing(mocker):
    mocker.patch(
        "app.pipeline.bat.load_pending_discovered_listings",
        return_value=[
            {
                "source_listing_id": "1967-fallback-title",
                "title": None,
                "url": "https://bringatrailer.com/listing/1967-fallback-title/",
            }
        ],
    )
    mocker.patch(
        "app.pipeline.bat.fetch_listing_html",
        return_value="<html><body>listing</body></html>",
    )
    evaluate_listing_eligibility = mocker.patch(
        "app.pipeline.bat.evaluate_listing_eligibility",
        return_value=(True, None),
    )
    mocker.patch("app.pipeline.bat.save_listing_html")
    mocker.patch("app.pipeline.bat.mark_discovered_listing_handled")

    bat.ingest_discovered_listings()

    evaluate_listing_eligibility.assert_called_once()
    _, listing_id = evaluate_listing_eligibility.call_args.args
    assert listing_id == "1967-fallback-title"


def test_ingest_discovered_listings_handles_mixed_batch_outcomes(mocker):
    mocker.patch(
        "app.pipeline.bat.load_pending_discovered_listings",
        return_value=[
            {
                "source_listing_id": "year-reject",
                "title": "1940 Ford Coupe",
                "url": "https://bringatrailer.com/listing/year-reject/",
            },
            {
                "source_listing_id": "scrape-fail",
                "title": "1967 Porsche 911S Coupe",
                "url": "https://bringatrailer.com/listing/scrape-fail/",
            },
            {
                "source_listing_id": "category-reject",
                "title": "1969 Porsche 911E Coupe",
                "url": "https://bringatrailer.com/listing/category-reject/",
            },
            {
                "source_listing_id": "accepted",
                "title": "1970 Porsche 911T Coupe",
                "url": "https://bringatrailer.com/listing/accepted/",
            },
        ],
    )
    mocker.patch(
        "app.pipeline.bat.fetch_listing_html",
        side_effect=[
            "<html><body>year reject</body></html>",
            RuntimeError("network failed"),
            "<html><body>category reject</body></html>",
            "<html><body>accepted</body></html>",
        ],
    )
    mocker.patch(
        "app.pipeline.bat.evaluate_listing_eligibility",
        side_effect=[
            (False, "year before 1946"),
            (False, "excluded category: projects"),
            (True, None),
        ],
    )
    save_listing_html = mocker.patch("app.pipeline.bat.save_listing_html")
    mark_handled = mocker.patch("app.pipeline.bat.mark_discovered_listing_handled")
    calls = mocker.Mock()
    calls.attach_mock(save_listing_html, "save_listing_html")
    calls.attach_mock(mark_handled, "mark_handled")

    summary = bat.ingest_discovered_listings()

    assert summary == bat.BatchIngestSummary(
        selected=4,
        scrape_attempted=4,
        scrape_failed=1,
        rejected=2,
        raw_html_stored=1,
        accepted=1,
    )
    assert mark_handled.call_args_list == [
        mocker.call("year-reject", False, "year before 1946"),
        mocker.call("category-reject", False, "excluded category: projects"),
        mocker.call("accepted", True, None),
    ]
    save_listing_html.assert_called_once_with(
        "accepted",
        "<html><body>accepted</body></html>",
        url="https://bringatrailer.com/listing/accepted/",
    )
    assert calls.mock_calls[-2:] == [
        mocker.call.mark_handled("accepted", True, None),
        mocker.call.save_listing_html(
            "accepted",
            "<html><body>accepted</body></html>",
            url="https://bringatrailer.com/listing/accepted/",
        ),
    ]


def test_transform_discovered_listings_handles_mixed_batch_outcomes(mocker, caplog):
    mocker.patch(
        "app.pipeline.bat.load_pending_raw_listing_html",
        return_value=[
            {"source_listing_id": "transform-fail"},
            {"source_listing_id": "load-fail"},
            {"source_listing_id": "success"},
        ],
    )
    transformed_load_fail = {"listing_id": "load-fail"}
    transformed_success = {"listing_id": "success"}
    mocker.patch(
        "app.pipeline.bat.transform_listing_html",
        side_effect=[
            RuntimeError("missing raw html"),
            transformed_load_fail,
            transformed_success,
        ],
    )
    load_listing = mocker.patch(
        "app.pipeline.bat.load_listing",
        side_effect=[
            RuntimeError("constraint violation"),
            None,
        ],
    )

    caplog.set_level(logging.ERROR)
    summary = bat.transform_discovered_listings(batch_size=3)

    load_listing.assert_has_calls(
        [
            mocker.call(transformed_load_fail),
            mocker.call(transformed_success),
        ]
    )
    assert summary == bat.BatchTransformSummary(
        selected=3,
        transformed_and_loaded=1,
        transform_failed=1,
        load_failed=1,
    )
    assert (
        "BAT transform-discovered row failed for listing_id=transform-fail "
        "stage=transform error=missing raw html"
    ) in caplog.text
    assert (
        "BAT transform-discovered row failed for listing_id=load-fail "
        "stage=load error=constraint violation"
    ) in caplog.text
    assert "Traceback" not in caplog.text
