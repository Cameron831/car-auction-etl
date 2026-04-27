import argparse
import logging
from dataclasses import dataclass
from datetime import date

from bs4 import BeautifulSoup
from dotenv import load_dotenv

from app.sources.bat.discovery import (
    discover_completed_auctions,
    load_pending_discovered_listings,
    mark_discovered_listing_handled_eligible,
    mark_discovered_listing_handled_ineligible,
)
from app.sources.bat.ingest import fetch_listing_html, save_listing_html
from app.sources.bat.load import load_listing
from app.sources.bat.transform import (
    evaluate_listing_eligibility,
    load_pending_raw_listing_html,
    transform_listing_html,
)

load_dotenv()

logger = logging.getLogger(__name__)


@dataclass
class BatchIngestSummary:
    selected: int = 0
    scrape_attempted: int = 0
    scrape_failed: int = 0
    rejected: int = 0
    raw_html_stored: int = 0
    accepted: int = 0


@dataclass
class BatchTransformSummary:
    selected: int = 0
    transformed_and_loaded: int = 0
    transform_failed: int = 0
    load_failed: int = 0


def build_parser():
    parser = argparse.ArgumentParser(description="Run Bring a Trailer ETL commands.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for command in ("ingest", "transform", "run"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument("--listing-id", required=True)

    discover_parser = subparsers.add_parser("discover")
    discover_parser.add_argument(
        "--scrape-date",
        type=date.fromisoformat,
        default=date.today(),
    )
    discover_parser.add_argument("--max-candidates", type=int)

    ingest_discovered_parser = subparsers.add_parser("ingest-discovered")
    ingest_discovered_parser.add_argument("--batch-size", type=int)

    transform_discovered_parser = subparsers.add_parser("transform-discovered")
    transform_discovered_parser.add_argument("--batch-size", type=int)

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
    transformed_listing = transform_listing_html(listing_id)
    load_listing(transformed_listing)


def run_listing(listing_id):
    ingest_listing(listing_id)
    transform_listing(listing_id)


def discover_listings(scrape_date, max_candidates=None):
    return discover_completed_auctions(
        scrape_date=scrape_date,
        max_candidates=max_candidates,
    )


def ingest_discovered_listings(batch_size=None):
    summary = BatchIngestSummary()
    pending_rows = load_pending_discovered_listings(limit=batch_size)

    for row in pending_rows:
        summary.selected += 1
        listing_id = row["source_listing_id"]

        summary.scrape_attempted += 1
        try:
            html = fetch_listing_html(listing_id)
        except Exception:
            summary.scrape_failed += 1
            logger.error("BAT ingest-discovered scrape failed for listing_id=%s", listing_id)
            continue

        soup = BeautifulSoup(html, "html.parser")
        eligible, reason = evaluate_listing_eligibility(soup, listing_id)
        if not eligible:
            logger.info(
                "BAT ingest-discovered listing rejected for listing_id=%s reason=%s",
                listing_id,
                reason,
            )
            mark_discovered_listing_handled_ineligible(listing_id, reason)
            summary.rejected += 1
            continue

        save_listing_html(listing_id, html, url=row["url"])
        summary.raw_html_stored += 1
        mark_discovered_listing_handled_eligible(listing_id)
        summary.accepted += 1

    return summary


def transform_discovered_listings(batch_size=None):
    summary = BatchTransformSummary()
    pending_rows = load_pending_raw_listing_html(limit=batch_size)

    for row in pending_rows:
        summary.selected += 1
        listing_id = row["source_listing_id"]

        try:
            transformed_listing = transform_listing_html(listing_id)
        except Exception as exc:
            summary.transform_failed += 1
            logger.error(
                "BAT transform-discovered row failed for listing_id=%s stage=transform error=%s",
                listing_id,
                exc,
            )
            continue

        try:
            load_listing(transformed_listing)
        except Exception as exc:
            summary.load_failed += 1
            logger.error(
                "BAT transform-discovered row failed for listing_id=%s stage=load error=%s",
                listing_id,
                exc,
            )
            continue

        summary.transformed_and_loaded += 1

    return summary


def main(argv=None):
    configure_logging()
    args = build_parser().parse_args(argv)

    try:
        if args.command == "discover":
            logger.info(
                "BAT discover command started for scrape_date=%s max_candidates=%s",
                args.scrape_date.isoformat(),
                args.max_candidates,
            )
            summary = discover_listings(
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
                "BAT discover command completed for scrape_date=%s",
                args.scrape_date.isoformat(),
            )
            return
        if args.command == "ingest-discovered":
            logger.info(
                "BAT ingest-discovered command started for batch_size=%s",
                args.batch_size,
            )
            summary = ingest_discovered_listings(batch_size=args.batch_size)
            logger.info(
                "BAT ingest-discovered summary selected=%s scrape_attempted=%s scrape_failed=%s rejected=%s raw_html_stored=%s accepted=%s",
                summary.selected,
                summary.scrape_attempted,
                summary.scrape_failed,
                summary.rejected,
                summary.raw_html_stored,
                summary.accepted,
            )
            print(
                "Ingest-discovered summary: "
                f"selected={summary.selected} "
                f"scrape_attempted={summary.scrape_attempted} "
                f"scrape_failed={summary.scrape_failed} "
                f"rejected={summary.rejected} "
                f"raw_html_stored={summary.raw_html_stored} "
                f"accepted={summary.accepted}"
            )
            logger.info("BAT ingest-discovered command completed for batch_size=%s", args.batch_size)
            return
        if args.command == "transform-discovered":
            logger.info(
                "BAT transform-discovered command started for batch_size=%s",
                args.batch_size,
            )
            summary = transform_discovered_listings(batch_size=args.batch_size)
            logger.info(
                "BAT transform-discovered summary selected=%s transformed_and_loaded=%s transform_failed=%s load_failed=%s",
                summary.selected,
                summary.transformed_and_loaded,
                summary.transform_failed,
                summary.load_failed,
            )
            print(
                "Transform-discovered summary: "
                f"selected={summary.selected} "
                f"transformed_and_loaded={summary.transformed_and_loaded} "
                f"transform_failed={summary.transform_failed} "
                f"load_failed={summary.load_failed}"
            )
            logger.info(
                "BAT transform-discovered command completed for batch_size=%s",
                args.batch_size,
            )
            return

        logger.info("BAT %s command started for listing_id=%s", args.command, args.listing_id)
        if args.command == "ingest":
            ingest_listing(args.listing_id)
        elif args.command == "transform":
            transform_listing(args.listing_id)
        elif args.command == "run":
            run_listing(args.listing_id)
    except Exception:
        if args.command == "discover":
            logger.error(
                "BAT discover command failed for scrape_date=%s",
                args.scrape_date.isoformat(),
            )
        elif args.command == "ingest-discovered":
            logger.error(
                "BAT ingest-discovered command failed for batch_size=%s",
                args.batch_size,
            )
        elif args.command == "transform-discovered":
            logger.error(
                "BAT transform-discovered command failed for batch_size=%s",
                args.batch_size,
            )
        else:
            logger.error("BAT %s command failed for listing_id=%s", args.command, args.listing_id)
        raise
    logger.info("BAT %s command completed for listing_id=%s", args.command, args.listing_id)


if __name__ == "__main__":
    main()
