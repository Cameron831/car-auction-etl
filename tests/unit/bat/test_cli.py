import logging
from datetime import date

import pytest

from app.pipeline import bat
from app.sources.bat import cli


def test_ingest_command_fetches_and_saves_listing_html(mocker, caplog, capsys):
    ingest_listing = mocker.patch(
        "app.sources.bat.cli.bat_pipeline.ingest_listing",
        return_value=bat.SingleIngestSummary(
            listing_id="test-id",
            accepted=True,
            raw_stored=True,
        ),
    )

    caplog.set_level(logging.INFO)
    cli.main(["ingest", "--listing-id", "test-id"])

    ingest_listing.assert_called_once_with("test-id")
    assert "BAT ingest command started for listing_id=test-id" in caplog.text
    assert "BAT ingest command completed for listing_id=test-id" in caplog.text
    assert (
        "Ingest summary: listing_id=test-id accepted=true raw_stored=true\n"
        == capsys.readouterr().out
    )


def test_ingest_command_prints_rejected_summary_without_reason(mocker, capsys):
    mocker.patch(
        "app.sources.bat.cli.bat_pipeline.ingest_listing",
        return_value=bat.SingleIngestSummary(
            listing_id="test-id",
            accepted=False,
            raw_stored=False,
            reason="year before 1946",
        ),
    )

    cli.main(["ingest", "--listing-id", "test-id"])

    out = capsys.readouterr().out
    assert out == "Ingest summary: listing_id=test-id accepted=false raw_stored=false\n"
    assert "reason" not in out
    assert "source" not in out


def test_transform_command_prints_summary(mocker, capsys):
    transform_listing = mocker.patch(
        "app.sources.bat.cli.bat_pipeline.transform_listing",
        return_value=bat.SingleTransformSummary(
            listing_id="test-id",
            transformed=True,
            loaded=True,
        ),
    )

    cli.main(["transform", "--listing-id", "test-id"])

    transform_listing.assert_called_once_with("test-id")
    assert (
        "Transform summary: listing_id=test-id transformed=true loaded=true\n"
        == capsys.readouterr().out
    )


def test_transform_command_logs_failure_context_without_traceback_and_reraises(
    mocker, caplog, capsys
):
    error = RuntimeError("transform failed")
    mocker.patch(
        "app.sources.bat.cli.bat_pipeline.transform_listing",
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
    assert "Transform summary:" not in capsys.readouterr().out


def test_run_command_executes_ingest_transform_load_in_order(mocker, capsys):
    run_listing = mocker.patch(
        "app.sources.bat.cli.bat_pipeline.run_listing",
        return_value=bat.SingleRunSummary(
            listing_id="test-id",
            accepted=True,
            raw_stored=True,
            transformed=True,
            loaded=True,
        ),
    )

    cli.main(["run", "--listing-id", "test-id"])

    run_listing.assert_called_once_with("test-id")
    assert (
        "Run summary: listing_id=test-id accepted=true raw_stored=true "
        "transformed=true loaded=true\n"
    ) == capsys.readouterr().out


def test_run_command_prints_rejected_summary_without_reason(mocker, capsys):
    mocker.patch(
        "app.sources.bat.cli.bat_pipeline.run_listing",
        return_value=bat.SingleRunSummary(
            listing_id="test-id",
            accepted=False,
            raw_stored=False,
            transformed=False,
            loaded=False,
            reason="year before 1946",
        ),
    )

    cli.main(["run", "--listing-id", "test-id"])

    out = capsys.readouterr().out
    assert (
        "Run summary: listing_id=test-id accepted=false raw_stored=false "
        "transformed=false loaded=false\n"
    ) == out
    assert "reason" not in out
    assert "source" not in out


def test_discover_command_parses_without_listing_id():
    args = cli.build_parser().parse_args(["discover"])

    assert args.command == "discover"
    assert args.max_candidates is None
    assert isinstance(args.scrape_date, date)


def test_discover_command_dispatches_with_parsed_options(mocker, caplog, capsys):
    discover_listings = mocker.patch(
        "app.sources.bat.cli.bat_pipeline.discover_listings",
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
        "app.sources.bat.cli.bat_pipeline.discover_listings",
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
        "app.sources.bat.cli.bat_pipeline.ingest_discovered_listings",
        return_value=bat.BatchIngestSummary(
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
        "app.sources.bat.cli.bat_pipeline.ingest_discovered_listings",
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
        "app.sources.bat.cli.bat_pipeline.transform_discovered_listings",
        return_value=bat.BatchTransformSummary(
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
        "app.sources.bat.cli.bat_pipeline.transform_discovered_listings",
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


@pytest.mark.parametrize("command", ["ingest", "transform", "run"])
def test_commands_require_listing_id(command, capsys):
    with pytest.raises(SystemExit) as exc_info:
        cli.main([command])

    assert exc_info.value.code == 2
    assert "--listing-id" in capsys.readouterr().err
