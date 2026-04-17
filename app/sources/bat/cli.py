import argparse

from dotenv import load_dotenv

from app.sources.bat.ingest import fetch_listing_html, save_listing_html
from app.sources.bat.load import load_listing
from app.sources.bat.transform import transform_listing_html

load_dotenv()


def build_parser():
    parser = argparse.ArgumentParser(description="Run Bring a Trailer ETL commands.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for command in ("ingest", "transform", "load", "run"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument("--listing-id", required=True)

    return parser


def ingest_listing(listing_id):
    html = fetch_listing_html(listing_id)
    save_listing_html(listing_id, html)


def transform_listing(listing_id):
    return transform_listing_html(listing_id)


def load_transformed_listing(listing_id):
    transformed_listing = transform_listing_html(listing_id)
    load_listing(transformed_listing)


def run_listing(listing_id):
    ingest_listing(listing_id)
    transformed_listing = transform_listing(listing_id)
    load_listing(transformed_listing)


def main(argv=None):
    args = build_parser().parse_args(argv)

    if args.command == "ingest":
        ingest_listing(args.listing_id)
    elif args.command == "transform":
        transform_listing(args.listing_id)
    elif args.command == "load":
        load_transformed_listing(args.listing_id)
    elif args.command == "run":
        run_listing(args.listing_id)


if __name__ == "__main__":
    main()
