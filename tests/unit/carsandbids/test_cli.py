import logging
from datetime import date

import pytest

from app.pipeline import carsandbids
from app.sources.carsandbids import cli


def test_ingest_command_fetches_and_saves_listing_json(mocker, caplog):
    ingest_listing = mocker.patch(
        "app.sources.carsandbids.cli.carsandbids_pipeline.ingest_listing"
    )

    caplog.set_level(logging.INFO)
    cli.main(["ingest", "--listing-id", "test-id"])

    ingest_listing.assert_called_once_with("test-id")
    assert "carsandbids ingest command started for listing_id=test-id" in caplog.text
    assert "carsandbids ingest command completed for listing_id=test-id" in caplog.text


def test_transform_command_logs_failure_context_without_traceback_and_reraises(
    mocker, caplog
):
    error = RuntimeError("transform failed")
    mocker.patch(
        "app.sources.carsandbids.cli.carsandbids_pipeline.transform_listing",
        side_effect=error,
    )

    caplog.set_level(logging.INFO)
    with pytest.raises(RuntimeError) as exc_info:
        cli.main(["transform", "--listing-id", "test-id"])

    assert exc_info.value is error
    assert "carsandbids transform command started for listing_id=test-id" in caplog.text
    assert "carsandbids transform command failed for listing_id=test-id" in caplog.text
    assert "Traceback" not in caplog.text
    assert "RuntimeError: transform failed" not in caplog.text


def test_run_command_executes_ingest_transform_load_in_order(mocker):
    run_listing = mocker.patch(
        "app.sources.carsandbids.cli.carsandbids_pipeline.run_listing"
    )

    cli.main(["run", "--listing-id", "test-id"])

    run_listing.assert_called_once_with("test-id")


def test_discover_command_parses_without_listing_id():
    args = cli.build_parser().parse_args(["discover"])

    assert args.command == "discover"
    assert args.max_candidates is None
    assert isinstance(args.scrape_date, date)


def test_discover_command_dispatches_with_parsed_options(mocker, caplog, capsys):
    discover_listings = mocker.patch(
        "app.sources.carsandbids.cli.carsandbids_pipeline.discover_listings",
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

    discover_listings.assert_called_once_with(
        scrape_date=date(2026, 4, 20),
        max_candidates=5,
    )
    assert (
        "carsandbids discover command started for scrape_date=2026-04-20 max_candidates=5"
    ) in caplog.text
    assert (
        "carsandbids discover summary inspected=2 new=1 existing_or_updated=1 failed=0"
    ) in caplog.text
    assert "carsandbids discover command completed for scrape_date=2026-04-20" in caplog.text
    assert (
        "Discovery summary: inspected=2 new=1 existing_or_updated=1 failed=0"
        in capsys.readouterr().out
    )


def test_discover_command_logs_failure_context_without_traceback_and_reraises(
    mocker, caplog
):
    error = RuntimeError("discover failed")
    mocker.patch(
        "app.sources.carsandbids.cli.carsandbids_pipeline.discover_listings",
        side_effect=error,
    )

    caplog.set_level(logging.INFO)
    with pytest.raises(RuntimeError) as exc_info:
        cli.main(["discover", "--scrape-date", "2026-04-20"])

    assert exc_info.value is error
    assert (
        "carsandbids discover command started for scrape_date=2026-04-20 max_candidates=None"
        in caplog.text
    )
    assert "carsandbids discover command failed for scrape_date=2026-04-20" in caplog.text
    assert "Traceback" not in caplog.text
    assert "RuntimeError: discover failed" not in caplog.text


def test_discover_command_rejects_results_url_option(capsys):
    with pytest.raises(SystemExit) as exc_info:
        cli.main(["discover", "--results-url", "https://carsandbids.com/past-auctions/"])

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


def test_ingest_discovered_command_dispatches_with_parsed_options(
    mocker, caplog, capsys
):
    ingest_discovered_listings = mocker.patch(
        "app.sources.carsandbids.cli.carsandbids_pipeline.ingest_discovered_listings",
        return_value=carsandbids.BatchIngestSummary(
            selected=3,
            scrape_attempted=2,
            scrape_failed=1,
            rejected=1,
            raw_json_stored=1,
            accepted=1,
        ),
    )

    caplog.set_level(logging.INFO)
    cli.main(["ingest-discovered", "--batch-size", "5"])

    ingest_discovered_listings.assert_called_once_with(batch_size=5)
    assert "carsandbids ingest-discovered command started for batch_size=5" in caplog.text
    assert (
        "carsandbids ingest-discovered summary selected=3 scrape_attempted=2 "
        "scrape_failed=1 rejected=1 raw_json_stored=1 accepted=1"
    ) in caplog.text
    assert "carsandbids ingest-discovered command completed for batch_size=5" in caplog.text
    assert (
        "Ingest-discovered summary: selected=3 scrape_attempted=2 "
        "scrape_failed=1 rejected=1 raw_json_stored=1 accepted=1"
    ) in capsys.readouterr().out


def test_ingest_discovered_command_logs_failure_context_without_traceback_and_reraises(
    mocker, caplog
):
    error = RuntimeError("ingest-discovered failed")
    mocker.patch(
        "app.sources.carsandbids.cli.carsandbids_pipeline.ingest_discovered_listings",
        side_effect=error,
    )

    caplog.set_level(logging.INFO)
    with pytest.raises(RuntimeError) as exc_info:
        cli.main(["ingest-discovered", "--batch-size", "5"])

    assert exc_info.value is error
    assert "carsandbids ingest-discovered command started for batch_size=5" in caplog.text
    assert "carsandbids ingest-discovered command failed for batch_size=5" in caplog.text
    assert "Traceback" not in caplog.text
    assert "RuntimeError: ingest-discovered failed" not in caplog.text


def test_transform_discovered_command_dispatches_with_parsed_options(
    mocker, caplog, capsys
):
    transform_discovered_listings = mocker.patch(
        "app.sources.carsandbids.cli.carsandbids_pipeline.transform_discovered_listings",
        return_value=carsandbids.BatchTransformSummary(
            selected=3,
            transformed_and_loaded=1,
            transform_failed=1,
            load_failed=1,
        ),
    )

    caplog.set_level(logging.INFO)
    cli.main(["transform-discovered", "--batch-size", "5"])

    transform_discovered_listings.assert_called_once_with(batch_size=5)
    assert (
        "carsandbids transform-discovered command started for batch_size=5"
        in caplog.text
    )
    assert (
        "carsandbids transform-discovered summary selected=3 transformed_and_loaded=1 "
        "transform_failed=1 load_failed=1"
    ) in caplog.text
    assert (
        "carsandbids transform-discovered command completed for batch_size=5"
        in caplog.text
    )
    assert (
        "Transform-discovered summary: selected=3 transformed_and_loaded=1 "
        "transform_failed=1 load_failed=1"
    ) in capsys.readouterr().out


def test_transform_discovered_command_logs_failure_context_without_traceback_and_reraises(
    mocker, caplog
):
    error = RuntimeError("transform-discovered failed")
    mocker.patch(
        "app.sources.carsandbids.cli.carsandbids_pipeline.transform_discovered_listings",
        side_effect=error,
    )

    caplog.set_level(logging.INFO)
    with pytest.raises(RuntimeError) as exc_info:
        cli.main(["transform-discovered", "--batch-size", "5"])

    assert exc_info.value is error
    assert (
        "carsandbids transform-discovered command started for batch_size=5"
        in caplog.text
    )
    assert (
        "carsandbids transform-discovered command failed for batch_size=5"
        in caplog.text
    )
    assert "Traceback" not in caplog.text
    assert "RuntimeError: transform-discovered failed" not in caplog.text


@pytest.mark.parametrize("command", ["ingest", "transform", "run"])
def test_commands_require_listing_id(command, capsys):
    with pytest.raises(SystemExit) as exc_info:
        cli.main([command])

    assert exc_info.value.code == 2
    assert "--listing-id" in capsys.readouterr().err
