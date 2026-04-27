import logging
from datetime import date

import pytest

from app.sources.bat import cli


def test_ingest_command_fetches_and_saves_listing_html(mocker, caplog):
    fetch_listing_html = mocker.patch(
        "app.sources.bat.cli.fetch_listing_html",
        return_value="<html>Test</html>",
    )
    save_listing_html = mocker.patch("app.sources.bat.cli.save_listing_html")

    caplog.set_level(logging.INFO)
    cli.main(["ingest", "--listing-id", "test-id"])

    fetch_listing_html.assert_called_once_with("test-id")
    save_listing_html.assert_called_once_with("test-id", "<html>Test</html>")
    assert "BAT ingest command started for listing_id=test-id" in caplog.text
    assert "BAT ingest command completed for listing_id=test-id" in caplog.text
    assert "<html>Test</html>" not in caplog.text


def test_transform_command_logs_failure_context_without_traceback_and_reraises(mocker, caplog):
    error = RuntimeError("transform failed")
    mocker.patch(
        "app.sources.bat.cli.transform_listing_html",
        side_effect=error,
    )

    caplog.set_level(logging.INFO)
    with pytest.raises(RuntimeError) as exc_info:
        cli.main(["transform", "--listing-id", "test-id"])

    assert exc_info.value is error
    assert "BAT transform command started for listing_id=test-id" in caplog.text
    assert "BAT transform command failed for listing_id=test-id" in caplog.text
    assert "Traceback" not in caplog.text
    assert "RuntimeError: transform failed" not in caplog.text


def test_run_command_executes_ingest_transform_load_in_order(mocker):
    calls = []
    transformed_listing = {"listing_id": "test-id"}

    mocker.patch(
        "app.sources.bat.cli.fetch_listing_html",
        side_effect=lambda listing_id: calls.append(("fetch", listing_id)) or "<html>Test</html>",
    )
    mocker.patch(
        "app.sources.bat.cli.save_listing_html",
        side_effect=lambda listing_id, html: calls.append(("save", listing_id, html)),
    )
    mocker.patch(
        "app.sources.bat.cli.transform_listing_html",
        side_effect=lambda listing_id: calls.append(("transform", listing_id)) or transformed_listing,
    )
    mocker.patch(
        "app.sources.bat.cli.load_listing",
        side_effect=lambda listing: calls.append(("load", listing)),
    )

    cli.main(["run", "--listing-id", "test-id"])

    assert calls == [
        ("fetch", "test-id"),
        ("save", "test-id", "<html>Test</html>"),
        ("transform", "test-id"),
        ("load", transformed_listing),
    ]


def test_discover_command_parses_without_listing_id():
    args = cli.build_parser().parse_args(["discover"])

    assert args.command == "discover"
    assert args.max_candidates is None
    assert isinstance(args.scrape_date, date)


def test_discover_command_dispatches_with_parsed_options(mocker, caplog, capsys):
    discover_completed_auctions = mocker.patch(
        "app.sources.bat.cli.discover_completed_auctions",
        return_value=mocker.Mock(
            candidates_inspected=2,
            newly_discovered=1,
            already_discovered_or_updated=1,
            failed=0,
        ),
    )

    caplog.set_level(logging.INFO)
    cli.main(
        [
            "discover",
            "--scrape-date",
            "2026-04-20",
            "--max-candidates",
            "5",
        ]
    )

    discover_completed_auctions.assert_called_once_with(
        scrape_date=date(2026, 4, 20),
        max_candidates=5,
    )
    assert (
        "BAT discover command started for scrape_date=2026-04-20 max_candidates=5"
    ) in caplog.text
    assert "BAT discover summary inspected=2 new=1 existing_or_updated=1 failed=0" in caplog.text
    assert "BAT discover command completed for scrape_date=2026-04-20" in caplog.text
    assert (
        "Discovery summary: inspected=2 new=1 existing_or_updated=1 failed=0"
        in capsys.readouterr().out
    )


def test_discover_command_logs_failure_context_without_traceback_and_reraises(mocker, caplog):
    error = RuntimeError("discover failed")
    mocker.patch(
        "app.sources.bat.cli.discover_completed_auctions",
        side_effect=error,
    )

    caplog.set_level(logging.INFO)
    with pytest.raises(RuntimeError) as exc_info:
        cli.main(["discover", "--scrape-date", "2026-04-20"])

    assert exc_info.value is error
    assert "BAT discover command started for scrape_date=2026-04-20 max_candidates=None" in caplog.text
    assert "BAT discover command failed for scrape_date=2026-04-20" in caplog.text
    assert "Traceback" not in caplog.text
    assert "RuntimeError: discover failed" not in caplog.text


def test_discover_command_rejects_results_url_option(capsys):
    with pytest.raises(SystemExit) as exc_info:
        cli.main(["discover", "--results-url", "https://bringatrailer.com/auctions/results/"])

    assert exc_info.value.code == 2
    assert "--results-url" in capsys.readouterr().err


def test_ingest_discovered_command_parses_without_batch_size():
    args = cli.build_parser().parse_args(["ingest-discovered"])

    assert args.command == "ingest-discovered"
    assert args.batch_size is None


def test_ingest_discovered_command_parses_with_batch_size():
    args = cli.build_parser().parse_args(["ingest-discovered", "--batch-size", "5"])

    assert args.command == "ingest-discovered"
    assert args.batch_size == 5


def test_transform_discovered_command_parses_without_batch_size():
    args = cli.build_parser().parse_args(["transform-discovered"])

    assert args.command == "transform-discovered"
    assert args.batch_size is None


def test_transform_discovered_command_parses_with_batch_size():
    args = cli.build_parser().parse_args(["transform-discovered", "--batch-size", "5"])

    assert args.command == "transform-discovered"
    assert args.batch_size == 5


def test_ingest_discovered_command_dispatches_with_parsed_options(mocker, caplog, capsys):
    ingest_discovered_listings = mocker.patch(
        "app.sources.bat.cli.ingest_discovered_listings",
        return_value=cli.BatchIngestSummary(
            selected=3,
            scrape_attempted=2,
            scrape_failed=1,
            rejected=1,
            raw_html_stored=1,
            accepted=1,
        ),
    )

    caplog.set_level(logging.INFO)
    cli.main(["ingest-discovered", "--batch-size", "5"])

    ingest_discovered_listings.assert_called_once_with(batch_size=5)
    assert "BAT ingest-discovered command started for batch_size=5" in caplog.text
    assert (
        "BAT ingest-discovered summary selected=3 scrape_attempted=2 "
        "scrape_failed=1 rejected=1 raw_html_stored=1 accepted=1"
    ) in caplog.text
    assert "BAT ingest-discovered command completed for batch_size=5" in caplog.text
    assert (
        "Ingest-discovered summary: selected=3 scrape_attempted=2 "
        "scrape_failed=1 rejected=1 raw_html_stored=1 accepted=1"
    ) in capsys.readouterr().out


def test_ingest_discovered_command_logs_failure_context_without_traceback_and_reraises(
    mocker, caplog
):
    error = RuntimeError("ingest-discovered failed")
    mocker.patch(
        "app.sources.bat.cli.ingest_discovered_listings",
        side_effect=error,
    )

    caplog.set_level(logging.INFO)
    with pytest.raises(RuntimeError) as exc_info:
        cli.main(["ingest-discovered", "--batch-size", "5"])

    assert exc_info.value is error
    assert "BAT ingest-discovered command started for batch_size=5" in caplog.text
    assert "BAT ingest-discovered command failed for batch_size=5" in caplog.text
    assert "Traceback" not in caplog.text
    assert "RuntimeError: ingest-discovered failed" not in caplog.text


def test_transform_discovered_command_dispatches_with_parsed_options(mocker, caplog, capsys):
    transform_discovered_listings = mocker.patch(
        "app.sources.bat.cli.transform_discovered_listings",
        return_value=cli.BatchTransformSummary(
            selected=3,
            transformed_and_loaded=1,
            transform_failed=1,
            load_failed=1,
        ),
    )

    caplog.set_level(logging.INFO)
    cli.main(["transform-discovered", "--batch-size", "5"])

    transform_discovered_listings.assert_called_once_with(batch_size=5)
    assert "BAT transform-discovered command started for batch_size=5" in caplog.text
    assert (
        "BAT transform-discovered summary selected=3 transformed_and_loaded=1 "
        "transform_failed=1 load_failed=1"
    ) in caplog.text
    assert "BAT transform-discovered command completed for batch_size=5" in caplog.text
    assert (
        "Transform-discovered summary: selected=3 transformed_and_loaded=1 "
        "transform_failed=1 load_failed=1"
    ) in capsys.readouterr().out


def test_transform_discovered_command_logs_failure_context_without_traceback_and_reraises(
    mocker, caplog
):
    error = RuntimeError("transform-discovered failed")
    mocker.patch(
        "app.sources.bat.cli.transform_discovered_listings",
        side_effect=error,
    )

    caplog.set_level(logging.INFO)
    with pytest.raises(RuntimeError) as exc_info:
        cli.main(["transform-discovered", "--batch-size", "5"])

    assert exc_info.value is error
    assert "BAT transform-discovered command started for batch_size=5" in caplog.text
    assert "BAT transform-discovered command failed for batch_size=5" in caplog.text
    assert "Traceback" not in caplog.text
    assert "RuntimeError: transform-discovered failed" not in caplog.text


def test_ingest_discovered_listings_returns_zeroed_summary_for_empty_batch(mocker):
    mocker.patch(
        "app.sources.bat.cli.load_pending_discovered_listings",
        return_value=[],
    )

    summary = cli.ingest_discovered_listings()

    assert summary == cli.BatchIngestSummary()


def test_transform_discovered_listings_returns_zeroed_summary_for_empty_batch(mocker):
    mocker.patch(
        "app.sources.bat.cli.load_pending_raw_listing_html",
        return_value=[],
    )

    summary = cli.transform_discovered_listings()

    assert summary == cli.BatchTransformSummary()


def test_ingest_discovered_listings_marks_reject_without_saving_html(mocker, caplog):
    mocker.patch(
        "app.sources.bat.cli.load_pending_discovered_listings",
        return_value=[
            {
                "source_listing_id": "rejected",
                "title": "1940 Ford Coupe",
                "url": "https://bringatrailer.com/listing/rejected/",
            }
        ],
    )
    mocker.patch(
        "app.sources.bat.cli.fetch_listing_html",
        return_value="<html><body>listing</body></html>",
    )
    evaluate_listing_eligibility = mocker.patch(
        "app.sources.bat.cli.evaluate_listing_eligibility",
        return_value=(False, "year before 1946"),
    )
    mark_ineligible = mocker.patch("app.sources.bat.cli.mark_discovered_listing_handled_ineligible")
    save_listing_html = mocker.patch("app.sources.bat.cli.save_listing_html")

    caplog.set_level(logging.INFO)
    summary = cli.ingest_discovered_listings()

    evaluate_listing_eligibility.assert_called_once()
    _, listing_id = evaluate_listing_eligibility.call_args.args
    assert listing_id == "rejected"
    mark_ineligible.assert_called_once_with("rejected", "year before 1946")
    save_listing_html.assert_not_called()
    assert (
        "BAT ingest-discovered listing rejected for listing_id=rejected "
        "reason=year before 1946"
    ) in caplog.text
    assert summary == cli.BatchIngestSummary(
        selected=1,
        scrape_attempted=1,
        rejected=1,
    )


def test_ingest_discovered_listings_records_scrape_failure_without_marking_row(mocker):
    mocker.patch(
        "app.sources.bat.cli.load_pending_discovered_listings",
        return_value=[
            {
                "source_listing_id": "scrape-fail",
                "title": "1967 Porsche 911S Coupe",
                "url": "https://bringatrailer.com/listing/scrape-fail/",
            }
        ],
    )
    mocker.patch(
        "app.sources.bat.cli.fetch_listing_html",
        side_effect=RuntimeError("network failed"),
    )
    mark_ineligible = mocker.patch("app.sources.bat.cli.mark_discovered_listing_handled_ineligible")
    mark_eligible = mocker.patch("app.sources.bat.cli.mark_discovered_listing_handled_eligible")

    summary = cli.ingest_discovered_listings()

    mark_ineligible.assert_not_called()
    mark_eligible.assert_not_called()
    assert summary == cli.BatchIngestSummary(
        selected=1,
        scrape_attempted=1,
        scrape_failed=1,
    )


def test_ingest_discovered_listings_marks_category_reject_without_saving_html(mocker, caplog):
    mocker.patch(
        "app.sources.bat.cli.load_pending_discovered_listings",
        return_value=[
            {
                "source_listing_id": "category-reject",
                "title": "1967 Porsche 911S Coupe",
                "url": "https://bringatrailer.com/listing/category-reject/",
            }
        ],
    )
    mocker.patch(
        "app.sources.bat.cli.fetch_listing_html",
        return_value="<html><body>listing</body></html>",
    )
    evaluate_listing_eligibility = mocker.patch(
        "app.sources.bat.cli.evaluate_listing_eligibility",
        return_value=(False, "excluded category: projects"),
    )
    mark_ineligible = mocker.patch("app.sources.bat.cli.mark_discovered_listing_handled_ineligible")
    save_listing_html = mocker.patch("app.sources.bat.cli.save_listing_html")

    caplog.set_level(logging.INFO)
    summary = cli.ingest_discovered_listings()

    evaluate_listing_eligibility.assert_called_once()
    _, listing_id = evaluate_listing_eligibility.call_args.args
    assert listing_id == "category-reject"
    mark_ineligible.assert_called_once_with("category-reject", "excluded category: projects")
    save_listing_html.assert_not_called()
    assert (
        "BAT ingest-discovered listing rejected for listing_id=category-reject "
        "reason=excluded category: projects"
    ) in caplog.text
    assert summary == cli.BatchIngestSummary(
        selected=1,
        scrape_attempted=1,
        rejected=1,
    )


def test_ingest_discovered_listings_saves_html_and_marks_eligible_for_pass(mocker):
    mocker.patch(
        "app.sources.bat.cli.load_pending_discovered_listings",
        return_value=[
            {
                "source_listing_id": "accepted",
                "title": "1967 Porsche 911S Coupe",
                "url": "https://bringatrailer.com/listing/accepted/",
            }
        ],
    )
    mocker.patch(
        "app.sources.bat.cli.fetch_listing_html",
        return_value="<html><body>listing</body></html>",
    )
    mocker.patch(
        "app.sources.bat.cli.evaluate_listing_eligibility",
        return_value=(True, None),
    )
    save_listing_html = mocker.patch("app.sources.bat.cli.save_listing_html")
    mark_eligible = mocker.patch("app.sources.bat.cli.mark_discovered_listing_handled_eligible")

    summary = cli.ingest_discovered_listings()

    save_listing_html.assert_called_once_with(
        "accepted",
        "<html><body>listing</body></html>",
        url="https://bringatrailer.com/listing/accepted/",
    )
    mark_eligible.assert_called_once_with("accepted")
    assert summary == cli.BatchIngestSummary(
        selected=1,
        scrape_attempted=1,
        raw_html_stored=1,
        accepted=1,
    )


def test_ingest_discovered_listings_uses_listing_id_when_discovered_title_missing(mocker):
    mocker.patch(
        "app.sources.bat.cli.load_pending_discovered_listings",
        return_value=[
            {
                "source_listing_id": "1967-fallback-title",
                "title": None,
                "url": "https://bringatrailer.com/listing/1967-fallback-title/",
            }
        ],
    )
    mocker.patch(
        "app.sources.bat.cli.fetch_listing_html",
        return_value="<html><body>listing</body></html>",
    )
    evaluate_listing_eligibility = mocker.patch(
        "app.sources.bat.cli.evaluate_listing_eligibility",
        return_value=(True, None),
    )
    mocker.patch("app.sources.bat.cli.save_listing_html")
    mocker.patch("app.sources.bat.cli.mark_discovered_listing_handled_eligible")

    cli.ingest_discovered_listings()

    evaluate_listing_eligibility.assert_called_once()
    _, listing_id = evaluate_listing_eligibility.call_args.args
    assert listing_id == "1967-fallback-title"


def test_ingest_discovered_listings_handles_mixed_batch_outcomes(mocker):
    mocker.patch(
        "app.sources.bat.cli.load_pending_discovered_listings",
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
        "app.sources.bat.cli.fetch_listing_html",
        side_effect=[
            "<html><body>year reject</body></html>",
            RuntimeError("network failed"),
            "<html><body>category reject</body></html>",
            "<html><body>accepted</body></html>",
        ],
    )
    mocker.patch(
        "app.sources.bat.cli.evaluate_listing_eligibility",
        side_effect=[
            (False, "year before 1946"),
            (False, "excluded category: projects"),
            (True, None),
        ],
    )
    save_listing_html = mocker.patch("app.sources.bat.cli.save_listing_html")
    mark_ineligible = mocker.patch("app.sources.bat.cli.mark_discovered_listing_handled_ineligible")
    mark_eligible = mocker.patch("app.sources.bat.cli.mark_discovered_listing_handled_eligible")

    summary = cli.ingest_discovered_listings()

    assert summary == cli.BatchIngestSummary(
        selected=4,
        scrape_attempted=4,
        scrape_failed=1,
        rejected=2,
        raw_html_stored=1,
        accepted=1,
    )
    assert mark_ineligible.call_args_list == [
        mocker.call("year-reject", "year before 1946"),
        mocker.call("category-reject", "excluded category: projects"),
    ]
    save_listing_html.assert_called_once_with(
        "accepted",
        "<html><body>accepted</body></html>",
        url="https://bringatrailer.com/listing/accepted/",
    )
    mark_eligible.assert_called_once_with("accepted")


def test_transform_discovered_listings_handles_mixed_batch_outcomes(mocker, caplog):
    mocker.patch(
        "app.sources.bat.cli.load_pending_raw_listing_html",
        return_value=[
            {"source_listing_id": "transform-fail"},
            {"source_listing_id": "load-fail"},
            {"source_listing_id": "success"},
        ],
    )
    transformed_load_fail = {"listing_id": "load-fail"}
    transformed_success = {"listing_id": "success"}
    mocker.patch(
        "app.sources.bat.cli.transform_listing_html",
        side_effect=[
            RuntimeError("missing raw html"),
            transformed_load_fail,
            transformed_success,
        ],
    )
    load_listing = mocker.patch(
        "app.sources.bat.cli.load_listing",
        side_effect=[
            RuntimeError("constraint violation"),
            None,
        ],
    )

    caplog.set_level(logging.ERROR)
    summary = cli.transform_discovered_listings(batch_size=3)

    load_listing.assert_has_calls(
        [
            mocker.call(transformed_load_fail),
            mocker.call(transformed_success),
        ]
    )
    assert summary == cli.BatchTransformSummary(
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


@pytest.mark.parametrize("command", ["ingest", "transform", "run"])
def test_commands_require_listing_id(command, capsys):
    with pytest.raises(SystemExit) as exc_info:
        cli.main([command])

    assert exc_info.value.code == 2
    assert "--listing-id" in capsys.readouterr().err
