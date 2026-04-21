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


def test_transform_command_transforms_listing_html(mocker):
    transform_listing_html = mocker.patch(
        "app.sources.bat.cli.transform_listing_html",
        return_value={"listing_id": "test-id"},
    )

    cli.main(["transform", "--listing-id", "test-id"])

    transform_listing_html.assert_called_once_with("test-id")


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


def test_load_command_transforms_and_loads_listing(mocker):
    transformed_listing = {"listing_id": "test-id"}
    transform_listing_html = mocker.patch(
        "app.sources.bat.cli.transform_listing_html",
        return_value=transformed_listing,
    )
    load_listing = mocker.patch("app.sources.bat.cli.load_listing")

    cli.main(["load", "--listing-id", "test-id"])

    transform_listing_html.assert_called_once_with("test-id")
    load_listing.assert_called_once_with(transformed_listing)


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


@pytest.mark.parametrize("command", ["ingest", "transform", "load", "run"])
def test_commands_require_listing_id(command, capsys):
    with pytest.raises(SystemExit) as exc_info:
        cli.main([command])

    assert exc_info.value.code == 2
    assert "--listing-id" in capsys.readouterr().err
