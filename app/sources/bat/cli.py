import argparse
import logging
from datetime import date

from dotenv import load_dotenv

from app.sources.bat.discovery import discover_completed_auctions
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
    discover_parser.add_argument(
        "--results-url",
        default="https://bringatrailer.com/auctions/results/",
    )
    discover_parser.add_argument(
        "--scrape-date",
        type=date.fromisoformat,
        default=date.today(),
    )
    discover_parser.add_argument("--max-candidates", type=int)

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


def discover_listings(results_url, scrape_date, max_candidates=None):
    return discover_completed_auctions(
        results_url=results_url,
        scrape_date=scrape_date,
        max_candidates=max_candidates,
    )


def main(argv=None):
    configure_logging()
    args = build_parser().parse_args(argv)

    try:
        if args.command == "discover":
            logger.info(
                "BAT discover command started for results_url=%s scrape_date=%s max_candidates=%s",
                args.results_url,
                args.scrape_date.isoformat(),
                args.max_candidates,
            )
            summary = discover_listings(
                results_url=args.results_url,
                scrape_date=args.scrape_date,
                max_candidates=args.max_candidates,
            )
            logger.info(
                "BAT discover summary inspected=%s new=%s existing_or_updated=%s failed=%s",
                summary.candidates_inspected,
                summary.newly_discovered,
                summary.already_discovered_or_updated,
                summary.failed,
            )
            print(
                "Discovery summary: "
                f"inspected={summary.candidates_inspected} "
                f"new={summary.newly_discovered} "
                f"existing_or_updated={summary.already_discovered_or_updated} "
                f"failed={summary.failed}"
            )
            logger.info(
                "BAT discover command completed for results_url=%s scrape_date=%s",
                args.results_url,
                args.scrape_date.isoformat(),
            )
            return

        logger.info("BAT %s command started for listing_id=%s", args.command, args.listing_id)
        if args.command == "ingest":
            ingest_listing(args.listing_id)
        elif args.command == "transform":
            transform_listing(args.listing_id)
        elif args.command == "load":
            load_transformed_listing(args.listing_id)
        elif args.command == "run":
            run_listing(args.listing_id)
    except Exception:
        if args.command == "discover":
            logger.error(
                "BAT discover command failed for results_url=%s scrape_date=%s",
                args.results_url,
                args.scrape_date.isoformat(),
            )
        else:
            logger.error("BAT %s command failed for listing_id=%s", args.command, args.listing_id)
        raise
    logger.info("BAT %s command completed for listing_id=%s", args.command, args.listing_id)


if __name__ == "__main__":
    main()
