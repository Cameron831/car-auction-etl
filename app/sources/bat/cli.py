import argparse
import logging
from dataclasses import dataclass
from datetime import date

from bs4 import BeautifulSoup
from dotenv import load_dotenv

from app.sources.bat.discovery import (
    discover_completed_auctions,
    evaluate_discovery_eligibility,
    load_pending_discovered_listings,
    mark_discovered_listing_handled_eligible,
    mark_discovered_listing_handled_ineligible,
)
from app.sources.bat.ingest import fetch_listing_html, save_listing_html
from app.sources.bat.load import load_listing
from app.sources.bat.transform import (
    evaluate_listing_eligibility,
    extract_listing_title,
    get_product_json_ld,
    transform_listing_html,
)

load_dotenv()

logger = logging.getLogger(__name__)


@dataclass
class BatchIngestSummary:
    selected: int = 0
    stage_1_rejected: int = 0
    scrape_attempted: int = 0
    scrape_failed: int = 0
    stage_2_rejected: int = 0
    raw_html_stored: int = 0
    accepted: int = 0


def build_parser():
    parser = argparse.ArgumentParser(description="Run Bring a Trailer ETL commands.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for command in ("ingest", "transform", "load", "run"):
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
        eligible, reason = evaluate_discovery_eligibility(
            row.get("title"),
            row.get("source_location"),
        )
        if not eligible:
            mark_discovered_listing_handled_ineligible(listing_id, reason)
            summary.stage_1_rejected += 1
            continue

        summary.scrape_attempted += 1
        try:
            html = fetch_listing_html(listing_id)
        except Exception:
            summary.scrape_failed += 1
            logger.error("BAT ingest-discovered scrape failed for listing_id=%s", listing_id)
            continue

        soup = BeautifulSoup(html, "html.parser")
        listing_title = row.get("title")
        if not listing_title:
            listing_title = extract_listing_title(soup, get_product_json_ld(soup))

        eligible, reason = evaluate_listing_eligibility(soup, listing_title)
        if not eligible:
            mark_discovered_listing_handled_ineligible(listing_id, reason)
            summary.stage_2_rejected += 1
            continue

        save_listing_html(listing_id, html, url=row["url"])
        summary.raw_html_stored += 1
        mark_discovered_listing_handled_eligible(listing_id)
        summary.accepted += 1

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
                "BAT ingest-discovered summary selected=%s stage_1_rejected=%s scrape_attempted=%s scrape_failed=%s stage_2_rejected=%s raw_html_stored=%s accepted=%s",
                summary.selected,
                summary.stage_1_rejected,
                summary.scrape_attempted,
                summary.scrape_failed,
                summary.stage_2_rejected,
                summary.raw_html_stored,
                summary.accepted,
            )
            print(
                "Ingest-discovered summary: "
                f"selected={summary.selected} "
                f"stage_1_rejected={summary.stage_1_rejected} "
                f"scrape_attempted={summary.scrape_attempted} "
                f"scrape_failed={summary.scrape_failed} "
                f"stage_2_rejected={summary.stage_2_rejected} "
                f"raw_html_stored={summary.raw_html_stored} "
                f"accepted={summary.accepted}"
            )
            logger.info("BAT ingest-discovered command completed for batch_size=%s", args.batch_size)
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
                "BAT discover command failed for scrape_date=%s",
                args.scrape_date.isoformat(),
            )
        elif args.command == "ingest-discovered":
            logger.error(
                "BAT ingest-discovered command failed for batch_size=%s",
                args.batch_size,
            )
        else:
            logger.error("BAT %s command failed for listing_id=%s", args.command, args.listing_id)
        raise
    logger.info("BAT %s command completed for listing_id=%s", args.command, args.listing_id)


if __name__ == "__main__":
    main()
