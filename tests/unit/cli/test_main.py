from importlib import import_module

import pytest

cli_main = import_module("app.cli.main")


def test_build_parser_accepts_listing_id():
    parser = cli_main.build_parser()

    args = parser.parse_args(["2004-bmw-m3-coupe-232"])

    assert args.listing_id == "2004-bmw-m3-coupe-232"


def test_main_runs_bat_pipeline_in_order(mocker):
    fetch_listing_html = mocker.patch(
        "app.cli.main.ingest.fetch_listing_html",
        return_value="<html>raw</html>",
    )
    save_listing_html = mocker.patch("app.cli.main.ingest.save_listing_html")
    transform_listing_html = mocker.patch(
        "app.cli.main.transform.transform_listing_html",
        return_value={"listing_id": "2004-bmw-m3-coupe-232"},
    )
    load_listing = mocker.patch("app.cli.main.load.load_listing")

    exit_code = cli_main.main(["2004-bmw-m3-coupe-232"])

    assert exit_code == 0
    fetch_listing_html.assert_called_once_with("2004-bmw-m3-coupe-232")
    save_listing_html.assert_called_once_with(
        "2004-bmw-m3-coupe-232",
        "<html>raw</html>",
    )
    transform_listing_html.assert_called_once_with("2004-bmw-m3-coupe-232")
    load_listing.assert_called_once_with({"listing_id": "2004-bmw-m3-coupe-232"})


def test_main_requires_listing_id(capsys):
    with pytest.raises(SystemExit) as exc_info:
        cli_main.main([])

    assert exc_info.value.code == 2
    stderr = capsys.readouterr().err
    assert "the following arguments are required: listing_id" in stderr


def test_main_help_exits_cleanly(capsys):
    with pytest.raises(SystemExit) as exc_info:
        cli_main.main(["--help"])

    assert exc_info.value.code == 0
    stdout = capsys.readouterr().out
    assert "Run the BAT ETL pipeline" in stdout
