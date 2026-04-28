import argparse
import logging
from datetime import date

from dotenv import load_dotenv

from app.pipeline import carsandbids as carsandbids_pipeline

load_dotenv()

logger = logging.getLogger(__name__)


def build_parser():
    parser = argparse.ArgumentParser(description="Run Cars and Bids ETL commands.")
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


def main(argv=None):
    configure_logging()
    args = build_parser().parse_args(argv)

    try:
        if args.command == "discover":
            logger.info(
                "carsandbids discover command started for scrape_date=%s max_candidates=%s",
                args.scrape_date.isoformat(),
                args.max_candidates,
            )
            summary = carsandbids_pipeline.discover_listings(
                scrape_date=args.scrape_date,
                max_candidates=args.max_candidates,
            )
            logger.info(
                "carsandbids discover summary inspected=%s new=%s existing_or_updated=%s failed=%s",
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
                "carsandbids discover command completed for scrape_date=%s",
                args.scrape_date.isoformat(),
            )
            return
        if args.command == "ingest-discovered":
            logger.info(
                "carsandbids ingest-discovered command started for batch_size=%s",
                args.batch_size,
            )
            summary = carsandbids_pipeline.ingest_discovered_listings(
                batch_size=args.batch_size
            )
            logger.info(
                "carsandbids ingest-discovered summary selected=%s scrape_attempted=%s scrape_failed=%s rejected=%s raw_json_stored=%s accepted=%s",
                summary.selected,
                summary.scrape_attempted,
                summary.scrape_failed,
                summary.rejected,
                summary.raw_json_stored,
                summary.accepted,
            )
            print(
                "Ingest-discovered summary: "
                f"selected={summary.selected} "
                f"scrape_attempted={summary.scrape_attempted} "
                f"scrape_failed={summary.scrape_failed} "
                f"rejected={summary.rejected} "
                f"raw_json_stored={summary.raw_json_stored} "
                f"accepted={summary.accepted}"
            )
            logger.info(
                "carsandbids ingest-discovered command completed for batch_size=%s",
                args.batch_size,
            )
            return
        if args.command == "transform-discovered":
            logger.info(
                "carsandbids transform-discovered command started for batch_size=%s",
                args.batch_size,
            )
            summary = carsandbids_pipeline.transform_discovered_listings(
                batch_size=args.batch_size
            )
            logger.info(
                "carsandbids transform-discovered summary selected=%s transformed_and_loaded=%s transform_failed=%s load_failed=%s",
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
                "carsandbids transform-discovered command completed for batch_size=%s",
                args.batch_size,
            )
            return

        logger.info(
            "carsandbids %s command started for listing_id=%s",
            args.command,
            args.listing_id,
        )
        if args.command == "ingest":
            carsandbids_pipeline.ingest_listing(args.listing_id)
        elif args.command == "transform":
            carsandbids_pipeline.transform_listing(args.listing_id)
        elif args.command == "run":
            carsandbids_pipeline.run_listing(args.listing_id)
    except Exception:
        if args.command == "discover":
            logger.error(
                "carsandbids discover command failed for scrape_date=%s",
                args.scrape_date.isoformat(),
            )
        elif args.command == "ingest-discovered":
            logger.error(
                "carsandbids ingest-discovered command failed for batch_size=%s",
                args.batch_size,
            )
        elif args.command == "transform-discovered":
            logger.error(
                "carsandbids transform-discovered command failed for batch_size=%s",
                args.batch_size,
            )
        else:
            logger.error(
                "carsandbids %s command failed for listing_id=%s",
                args.command,
                args.listing_id,
            )
        raise
    logger.info(
        "carsandbids %s command completed for listing_id=%s",
        args.command,
        args.listing_id,
    )


if __name__ == "__main__":
    main()
