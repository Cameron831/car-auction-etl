import logging
from datetime import date

from app.pipeline import carsandbids


def _patch_batch_browser(mocker):
    fake_playwright = object()
    fake_browser = mocker.Mock()
    fake_context = mocker.Mock(name="carsandbids_context")
    playwright_manager = mocker.MagicMock()
    playwright_manager.__enter__.return_value = fake_playwright
    playwright_manager.__exit__.return_value = False
    sync_playwright = mocker.patch(
        "app.pipeline.carsandbids.sync_playwright",
        return_value=playwright_manager,
    )
    launch_context = mocker.patch(
        "app.pipeline.carsandbids.launch_carsandbids_browser_context",
        return_value=(fake_browser, fake_context),
    )
    return fake_playwright, fake_browser, fake_context, sync_playwright, launch_context


def test_ingest_listing_fetches_and_saves_listing_json(mocker):
    payload = {"listing": {"year": 2004}}
    fetch_listing_json = mocker.patch(
        "app.pipeline.carsandbids.fetch_listing_json",
        return_value=payload,
    )
    save_listing_json = mocker.patch("app.pipeline.carsandbids.save_listing_json")
    evaluate_listing_eligibility = mocker.patch(
        "app.pipeline.carsandbids.evaluate_listing_eligibility",
        return_value=(True, None),
    )
    mark_discovered_listing_handled = mocker.patch(
        "app.pipeline.carsandbids.mark_discovered_listing_handled"
    )

    summary = carsandbids.ingest_listing("test-id")

    fetch_listing_json.assert_called_once_with("test-id")
    evaluate_listing_eligibility.assert_called_once_with(payload)
    mark_discovered_listing_handled.assert_called_once_with("test-id", True, None)
    save_listing_json.assert_called_once_with("test-id", payload)
    assert summary == carsandbids.SingleIngestSummary(
        listing_id="test-id",
        accepted=True,
        raw_stored=True,
    )


def test_ingest_listing_marks_rejected_listing_without_saving_json(mocker):
    payload = {"listing": {"year": 1940}}
    mocker.patch(
        "app.pipeline.carsandbids.fetch_listing_json",
        return_value=payload,
    )
    mocker.patch(
        "app.pipeline.carsandbids.evaluate_listing_eligibility",
        return_value=(False, "year before 1946"),
    )
    mark_discovered_listing_handled = mocker.patch(
        "app.pipeline.carsandbids.mark_discovered_listing_handled"
    )
    save_listing_json = mocker.patch("app.pipeline.carsandbids.save_listing_json")

    summary = carsandbids.ingest_listing("test-id")

    mark_discovered_listing_handled.assert_called_once_with(
        "test-id",
        False,
        "year before 1946",
    )
    save_listing_json.assert_not_called()
    assert summary == carsandbids.SingleIngestSummary(
        listing_id="test-id",
        accepted=False,
        raw_stored=False,
        reason="year before 1946",
    )


def test_transform_listing_transforms_and_loads_listing(mocker):
    transformed_listing = {"listing_id": "test-id"}
    transform_listing_json = mocker.patch(
        "app.pipeline.carsandbids.transform_listing_json",
        return_value=transformed_listing,
    )
    load_listing = mocker.patch("app.pipeline.carsandbids.load_listing")

    summary = carsandbids.transform_listing("test-id")

    transform_listing_json.assert_called_once_with("test-id")
    load_listing.assert_called_once_with(transformed_listing)
    assert summary == carsandbids.SingleTransformSummary(
        listing_id="test-id",
        transformed=True,
        loaded=True,
    )


def test_run_listing_executes_ingest_transform_load_in_order(mocker):
    calls = []
    payload = {"listing": {"year": 2004}}
    transformed_listing = {"listing_id": "test-id"}

    mocker.patch(
        "app.pipeline.carsandbids.fetch_listing_json",
        side_effect=lambda listing_id: calls.append(("fetch", listing_id)) or payload,
    )
    mocker.patch(
        "app.pipeline.carsandbids.save_listing_json",
        side_effect=lambda listing_id, listing_json: calls.append(
            ("save", listing_id, listing_json)
        ),
    )
    mocker.patch(
        "app.pipeline.carsandbids.evaluate_listing_eligibility",
        side_effect=lambda listing_json: calls.append(("evaluate", listing_json))
        or (True, None),
    )
    mocker.patch(
        "app.pipeline.carsandbids.mark_discovered_listing_handled",
        side_effect=lambda listing_id, eligible, reason: calls.append(
            ("mark_handled", listing_id, eligible, reason)
        ),
    )
    mocker.patch(
        "app.pipeline.carsandbids.transform_listing_json",
        side_effect=lambda listing_id: calls.append(("transform", listing_id))
        or transformed_listing,
    )
    mocker.patch(
        "app.pipeline.carsandbids.load_listing",
        side_effect=lambda listing: calls.append(("load", listing)),
    )

    summary = carsandbids.run_listing("test-id")

    assert calls == [
        ("fetch", "test-id"),
        ("evaluate", payload),
        ("mark_handled", "test-id", True, None),
        ("save", "test-id", payload),
        ("transform", "test-id"),
        ("load", transformed_listing),
    ]
    assert summary == carsandbids.SingleRunSummary(
        listing_id="test-id",
        accepted=True,
        raw_stored=True,
        transformed=True,
        loaded=True,
    )


def test_run_listing_skips_transform_when_ingest_rejects_listing(mocker):
    mocker.patch(
        "app.pipeline.carsandbids.ingest_listing",
        return_value=carsandbids.SingleIngestSummary(
            listing_id="test-id",
            accepted=False,
            raw_stored=False,
            reason="year before 1946",
        ),
    )
    transform_listing = mocker.patch("app.pipeline.carsandbids.transform_listing")

    summary = carsandbids.run_listing("test-id")

    transform_listing.assert_not_called()
    assert summary == carsandbids.SingleRunSummary(
        listing_id="test-id",
        accepted=False,
        raw_stored=False,
        transformed=False,
        loaded=False,
        reason="year before 1946",
    )


def test_discover_listings_delegates_to_discovery(mocker):
    discover_completed_auctions = mocker.patch(
        "app.pipeline.carsandbids.discover_completed_auctions"
    )

    carsandbids.discover_listings(scrape_date=date(2026, 4, 20), max_candidates=5)

    discover_completed_auctions.assert_called_once_with(
        scrape_date=date(2026, 4, 20),
        max_candidates=5,
    )


def test_ingest_discovered_listings_returns_zeroed_summary_for_empty_batch(mocker):
    mocker.patch(
        "app.pipeline.carsandbids.load_pending_discovered_listings",
        return_value=[],
    )
    sync_playwright = mocker.patch("app.pipeline.carsandbids.sync_playwright")
    launch_context = mocker.patch(
        "app.pipeline.carsandbids.launch_carsandbids_browser_context"
    )

    summary = carsandbids.ingest_discovered_listings()

    assert summary == carsandbids.BatchIngestSummary()
    sync_playwright.assert_not_called()
    launch_context.assert_not_called()


def test_transform_discovered_listings_returns_zeroed_summary_for_empty_batch(mocker):
    mocker.patch(
        "app.pipeline.carsandbids.load_pending_raw_listing_json",
        return_value=[],
    )

    summary = carsandbids.transform_discovered_listings()

    assert summary == carsandbids.BatchTransformSummary()


def test_ingest_discovered_listings_marks_reject_without_saving_json(mocker, caplog):
    payload = {"listing": {"year": 1940}}
    fake_playwright, fake_browser, fake_context, _, launch_context = (
        _patch_batch_browser(mocker)
    )
    mocker.patch(
        "app.pipeline.carsandbids.load_pending_discovered_listings",
        return_value=[
            {
                "source_listing_id": "rejected",
                "title": "1940 Ford Coupe",
                "url": "https://carsandbids.com/auctions/rejected",
            }
        ],
    )
    fetch_listing_json_with_context = mocker.patch(
        "app.pipeline.carsandbids.fetch_listing_json_with_context",
        return_value=payload,
    )
    evaluate_listing_eligibility = mocker.patch(
        "app.pipeline.carsandbids.evaluate_listing_eligibility",
        return_value=(False, "year before 1946"),
    )
    mark_handled = mocker.patch(
        "app.pipeline.carsandbids.mark_discovered_listing_handled"
    )
    save_listing_json = mocker.patch("app.pipeline.carsandbids.save_listing_json")

    caplog.set_level(logging.INFO)
    summary = carsandbids.ingest_discovered_listings()

    launch_context.assert_called_once_with(fake_playwright, headless=True)
    fetch_listing_json_with_context.assert_called_once_with("rejected", fake_context)
    fake_browser.close.assert_called_once_with()
    evaluate_listing_eligibility.assert_called_once_with(payload)
    mark_handled.assert_called_once_with("rejected", False, "year before 1946")
    save_listing_json.assert_not_called()
    assert (
        "carsandbids ingest-discovered listing rejected for listing_id=rejected "
        "reason=year before 1946"
    ) in caplog.text
    assert summary == carsandbids.BatchIngestSummary(
        selected=1,
        scrape_attempted=1,
        rejected=1,
    )


def test_ingest_discovered_listings_records_scrape_failure_without_marking_row(mocker):
    _, fake_browser, fake_context, _, _ = _patch_batch_browser(mocker)
    mocker.patch(
        "app.pipeline.carsandbids.load_pending_discovered_listings",
        return_value=[
            {
                "source_listing_id": "scrape-fail",
                "title": "2004 BMW M3 Coupe",
                "url": "https://carsandbids.com/auctions/scrape-fail",
            }
        ],
    )
    fetch_listing_json_with_context = mocker.patch(
        "app.pipeline.carsandbids.fetch_listing_json_with_context",
        side_effect=RuntimeError("network failed"),
    )
    mark_handled = mocker.patch(
        "app.pipeline.carsandbids.mark_discovered_listing_handled"
    )

    summary = carsandbids.ingest_discovered_listings()

    fetch_listing_json_with_context.assert_called_once_with("scrape-fail", fake_context)
    fake_browser.close.assert_called_once_with()
    mark_handled.assert_not_called()
    assert summary == carsandbids.BatchIngestSummary(
        selected=1,
        scrape_attempted=1,
        scrape_failed=1,
    )


def test_ingest_discovered_listings_saves_json_and_marks_eligible_for_pass(mocker):
    payload = {"listing": {"year": 2004}}
    _, fake_browser, fake_context, _, _ = _patch_batch_browser(mocker)
    mocker.patch(
        "app.pipeline.carsandbids.load_pending_discovered_listings",
        return_value=[
            {
                "source_listing_id": "accepted",
                "title": "2004 BMW M3 Coupe",
                "url": "https://carsandbids.com/auctions/accepted",
            }
        ],
    )
    fetch_listing_json_with_context = mocker.patch(
        "app.pipeline.carsandbids.fetch_listing_json_with_context",
        return_value=payload,
    )
    mocker.patch(
        "app.pipeline.carsandbids.evaluate_listing_eligibility",
        return_value=(True, None),
    )
    save_listing_json = mocker.patch("app.pipeline.carsandbids.save_listing_json")
    mark_handled = mocker.patch(
        "app.pipeline.carsandbids.mark_discovered_listing_handled"
    )
    calls = mocker.Mock()
    calls.attach_mock(save_listing_json, "save_listing_json")
    calls.attach_mock(mark_handled, "mark_handled")

    summary = carsandbids.ingest_discovered_listings()

    fetch_listing_json_with_context.assert_called_once_with("accepted", fake_context)
    fake_browser.close.assert_called_once_with()
    save_listing_json.assert_called_once_with(
        "accepted",
        payload,
        url="https://carsandbids.com/auctions/accepted",
    )
    mark_handled.assert_called_once_with("accepted", True, None)
    assert calls.mock_calls == [
        mocker.call.mark_handled("accepted", True, None),
        mocker.call.save_listing_json(
            "accepted",
            payload,
            url="https://carsandbids.com/auctions/accepted",
        ),
    ]
    assert summary == carsandbids.BatchIngestSummary(
        selected=1,
        scrape_attempted=1,
        raw_json_stored=1,
        accepted=1,
    )


def test_ingest_discovered_listings_handles_mixed_batch_outcomes(mocker):
    accepted_payload = {"listing": {"year": 2004}}
    _, fake_browser, fake_context, _, launch_context = _patch_batch_browser(mocker)
    mocker.patch(
        "app.pipeline.carsandbids.load_pending_discovered_listings",
        return_value=[
            {
                "source_listing_id": "year-reject",
                "title": "1940 Ford Coupe",
                "url": "https://carsandbids.com/auctions/year-reject",
            },
            {
                "source_listing_id": "scrape-fail",
                "title": "2004 BMW M3 Coupe",
                "url": "https://carsandbids.com/auctions/scrape-fail",
            },
            {
                "source_listing_id": "model-reject",
                "title": "Custom Kart",
                "url": "https://carsandbids.com/auctions/model-reject",
            },
            {
                "source_listing_id": "accepted",
                "title": "2004 BMW M3 Coupe",
                "url": "https://carsandbids.com/auctions/accepted",
            },
        ],
    )
    fetch_listing_json_with_context = mocker.patch(
        "app.pipeline.carsandbids.fetch_listing_json_with_context",
        side_effect=[
            {"listing": {"year": 1940}},
            RuntimeError("network failed"),
            {"listing": {"model": "Kart"}},
            accepted_payload,
        ],
    )
    mocker.patch(
        "app.pipeline.carsandbids.evaluate_listing_eligibility",
        side_effect=[
            (False, "year before 1946"),
            (False, "excluded model: Kart"),
            (True, None),
        ],
    )
    save_listing_json = mocker.patch("app.pipeline.carsandbids.save_listing_json")
    mark_handled = mocker.patch(
        "app.pipeline.carsandbids.mark_discovered_listing_handled"
    )
    calls = mocker.Mock()
    calls.attach_mock(save_listing_json, "save_listing_json")
    calls.attach_mock(mark_handled, "mark_handled")

    summary = carsandbids.ingest_discovered_listings()

    launch_context.assert_called_once()
    assert fetch_listing_json_with_context.call_args_list == [
        mocker.call("year-reject", fake_context),
        mocker.call("scrape-fail", fake_context),
        mocker.call("model-reject", fake_context),
        mocker.call("accepted", fake_context),
    ]
    fake_browser.close.assert_called_once_with()
    assert summary == carsandbids.BatchIngestSummary(
        selected=4,
        scrape_attempted=4,
        scrape_failed=1,
        rejected=2,
        raw_json_stored=1,
        accepted=1,
    )
    assert mark_handled.call_args_list == [
        mocker.call("year-reject", False, "year before 1946"),
        mocker.call("model-reject", False, "excluded model: Kart"),
        mocker.call("accepted", True, None),
    ]
    save_listing_json.assert_called_once_with(
        "accepted",
        accepted_payload,
        url="https://carsandbids.com/auctions/accepted",
    )
    assert calls.mock_calls[-2:] == [
        mocker.call.mark_handled("accepted", True, None),
        mocker.call.save_listing_json(
            "accepted",
            accepted_payload,
            url="https://carsandbids.com/auctions/accepted",
        ),
    ]


def test_transform_discovered_listings_handles_mixed_batch_outcomes(mocker, caplog):
    mocker.patch(
        "app.pipeline.carsandbids.load_pending_raw_listing_json",
        return_value=[
            {"source_listing_id": "transform-fail"},
            {"source_listing_id": "load-fail"},
            {"source_listing_id": "success"},
        ],
    )
    transformed_load_fail = {"listing_id": "load-fail"}
    transformed_success = {"listing_id": "success"}
    mocker.patch(
        "app.pipeline.carsandbids.transform_listing_json",
        side_effect=[
            RuntimeError("missing raw json"),
            transformed_load_fail,
            transformed_success,
        ],
    )
    load_listing = mocker.patch(
        "app.pipeline.carsandbids.load_listing",
        side_effect=[
            RuntimeError("constraint violation"),
            None,
        ],
    )

    caplog.set_level(logging.ERROR)
    summary = carsandbids.transform_discovered_listings(batch_size=3)

    load_listing.assert_has_calls(
        [
            mocker.call(transformed_load_fail),
            mocker.call(transformed_success),
        ]
    )
    assert summary == carsandbids.BatchTransformSummary(
        selected=3,
        transformed_and_loaded=1,
        transform_failed=1,
        load_failed=1,
    )
    assert (
        "carsandbids transform-discovered row failed for listing_id=transform-fail "
        "stage=transform error=missing raw json"
    ) in caplog.text
    assert (
        "carsandbids transform-discovered row failed for listing_id=load-fail "
        "stage=load error=constraint violation"
    ) in caplog.text
    assert "Traceback" not in caplog.text
