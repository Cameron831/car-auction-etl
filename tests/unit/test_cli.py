import pytest

from app import cli


def test_bat_source_delegates_remaining_args_to_bat_cli(mocker):
    bat_main = mocker.patch("app.cli.bat_cli.main")

    cli.main(["bat", "ingest", "--listing-id", "test-id"])

    bat_main.assert_called_once_with(["ingest", "--listing-id", "test-id"])


def test_cab_source_delegates_remaining_args_to_carsandbids_cli(mocker):
    carsandbids_main = mocker.patch("app.cli.carsandbids_cli.main")

    cli.main(
        [
            "cab",
            "discover",
            "--scrape-date",
            "2026-04-20",
            "--max-candidates",
            "5",
        ]
    )

    carsandbids_main.assert_called_once_with(
        ["discover", "--scrape-date", "2026-04-20", "--max-candidates", "5"]
    )


def test_missing_source_prefix_exits_with_argparse_error_code(capsys):
    with pytest.raises(SystemExit) as exc_info:
        cli.main([])

    assert exc_info.value.code == 2
    assert "source" in capsys.readouterr().err


def test_unknown_source_prefix_exits_with_argparse_error_code(capsys):
    with pytest.raises(SystemExit) as exc_info:
        cli.main(["unknown", "ingest"])

    assert exc_info.value.code == 2
    assert "invalid choice" in capsys.readouterr().err


def test_top_level_help_prints_router_help(capsys):
    with pytest.raises(SystemExit) as exc_info:
        cli.main(["--help"])

    assert exc_info.value.code == 0
    assert "Run car auction ETL commands." in capsys.readouterr().out


def test_bat_help_delegates_to_bat_cli(mocker):
    bat_main = mocker.patch("app.cli.bat_cli.main")

    cli.main(["bat", "--help"])

    bat_main.assert_called_once_with(["--help"])


def test_cab_help_delegates_to_carsandbids_cli(mocker):
    carsandbids_main = mocker.patch("app.cli.carsandbids_cli.main")

    cli.main(["cab", "--help"])

    carsandbids_main.assert_called_once_with(["--help"])


def test_delegated_exceptions_are_not_swallowed(mocker):
    error = RuntimeError("delegated failure")
    mocker.patch("app.cli.bat_cli.main", side_effect=error)

    with pytest.raises(RuntimeError) as exc_info:
        cli.main(["bat", "ingest", "--listing-id", "test-id"])

    assert exc_info.value is error


def test_delegated_system_exit_is_not_swallowed(mocker):
    mocker.patch("app.cli.carsandbids_cli.main", side_effect=SystemExit(17))

    with pytest.raises(SystemExit) as exc_info:
        cli.main(["cab", "discover"])

    assert exc_info.value.code == 17
