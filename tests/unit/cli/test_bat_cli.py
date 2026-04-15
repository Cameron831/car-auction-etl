import pytest

from app.cli import bat


def test_parser_accepts_required_listing_id():
    args = bat.build_parser().parse_args(["2004-bmw-m3-123"])

    assert args.listing_id == "2004-bmw-m3-123"


def test_help_prints_usage(capsys):
    with pytest.raises(SystemExit) as exc_info:
        bat.main(["--help"])

    captured = capsys.readouterr()

    assert exc_info.value.code == 0
    assert "usage: python -m app.cli" in captured.out
    assert "Run the BAT ETL pipeline" in captured.out
    assert "listing_id" in captured.out


def test_missing_listing_id_fails_with_clear_error(capsys):
    with pytest.raises(SystemExit) as exc_info:
        bat.main([])

    captured = capsys.readouterr()

    assert exc_info.value.code == 2
    assert "the following arguments are required: listing_id" in captured.err


def test_main_dispatches_pipeline_in_order(mocker):
    calls = []

    mocker.patch.object(
        bat.ingest,
        "fetch_listing_html",
        side_effect=lambda listing_id: calls.append(("fetch", listing_id)) or "<html></html>",
    )
    mocker.patch.object(
        bat.ingest,
        "save_listing_html",
        side_effect=lambda listing_id, html: calls.append(("save", listing_id, html)),
    )
    mocker.patch.object(
        bat.transform,
        "transform_listing_html",
        side_effect=lambda listing_id: calls.append(("transform", listing_id))
        or {"listing_id": listing_id},
    )
    mocker.patch.object(
        bat.load,
        "load_listing",
        side_effect=lambda listing: calls.append(("load", listing)),
    )

    exit_code = bat.main(["2004-bmw-m3-123"])

    assert exit_code == 0
    assert calls == [
        ("fetch", "2004-bmw-m3-123"),
        ("save", "2004-bmw-m3-123", "<html></html>"),
        ("transform", "2004-bmw-m3-123"),
        ("load", {"listing_id": "2004-bmw-m3-123"}),
    ]
