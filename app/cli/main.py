import argparse

from app.sources.bat import ingest, load, transform


def build_parser():
    parser = argparse.ArgumentParser(
        prog="python -m app.cli",
        description="Run the BAT ETL pipeline for a single Phase 1 listing.",
    )
    parser.add_argument(
        "listing_id",
        help="Required Bring a Trailer listing ID, such as 2004-bmw-m3-123.",
    )
    return parser


def run_pipeline(listing_id):
    html = ingest.fetch_listing_html(listing_id)
    ingest.save_listing_html(listing_id, html)
    transformed_listing = transform.transform_listing_html(listing_id)
    load.load_listing(transformed_listing)


def main(argv=None):
    args = build_parser().parse_args(argv)
    run_pipeline(args.listing_id)
    return 0
