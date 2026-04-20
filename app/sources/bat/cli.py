import argparse
import logging

from dotenv import load_dotenv

from app.sources.bat.discover import run_daily_discovery
from app.sources.bat.ingest import fetch_listing_html, save_listing_html
from app.sources.bat.load import load_listing
from app.sources.bat.transform import transform_listing_html

load_dotenv()

logger = logging.getLogger(__name__)


def build_parser():
    parser = argparse.ArgumentParser(description="Run Bring a Trailer ETL commands.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for command in ("ingest", "transform", "load", "run"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument("--listing-id", required=True)

    discover_parser = subparsers.add_parser("discover")
    discover_parser.add_argument("--max-pages", type=int, default=5)

    return parser


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(name)s %(message)s",
    )


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


def discover_and_ingest(max_pages):
    return run_daily_discovery(max_pages=max_pages)


def main(argv=None):
    configure_logging()
    args = build_parser().parse_args(argv)

    listing_id = getattr(args, "listing_id", None)
    logger.info("BAT %s command started for listing_id=%s", args.command, listing_id)
    try:
        if args.command == "ingest":
            ingest_listing(args.listing_id)
        elif args.command == "transform":
            transform_listing(args.listing_id)
        elif args.command == "load":
            load_transformed_listing(args.listing_id)
        elif args.command == "run":
            run_listing(args.listing_id)
        elif args.command == "discover":
            discover_and_ingest(args.max_pages)
    except Exception:
        logger.error("BAT %s command failed for listing_id=%s", args.command, listing_id)
        raise
    logger.info("BAT %s command completed for listing_id=%s", args.command, listing_id)


if __name__ == "__main__":
    main()
