import logging
from dataclasses import dataclass

from playwright.sync_api import sync_playwright

from app.sources.carsandbids.browser import launch_carsandbids_browser_context
from app.sources.carsandbids.discovery import (
    discover_completed_auctions,
    load_pending_discovered_listings,
    mark_discovered_listing_handled,
)
from app.sources.carsandbids.ingest import (
    evaluate_listing_eligibility,
    fetch_listing_json,
    fetch_listing_json_with_context,
    save_listing_json,
)
from app.sources.carsandbids.load import load_listing
from app.sources.carsandbids.transform import (
    load_pending_raw_listing_json,
    transform_listing_json,
)

logger = logging.getLogger(__name__)


@dataclass
class BatchIngestSummary:
    selected: int = 0
    scrape_attempted: int = 0
    scrape_failed: int = 0
    rejected: int = 0
    raw_json_stored: int = 0
    accepted: int = 0


@dataclass
class BatchTransformSummary:
    selected: int = 0
    transformed_and_loaded: int = 0
    transform_failed: int = 0
    load_failed: int = 0


@dataclass
class SingleIngestSummary:
    listing_id: str
    accepted: bool
    raw_stored: bool
    reason: str | None = None


@dataclass
class SingleTransformSummary:
    listing_id: str
    transformed: bool
    loaded: bool


@dataclass
class SingleRunSummary:
    listing_id: str
    accepted: bool
    raw_stored: bool
    transformed: bool
    loaded: bool
    reason: str | None = None


def ingest_listing(listing_id):
    payload = fetch_listing_json(listing_id)
    eligible, reason = evaluate_listing_eligibility(payload)
    mark_discovered_listing_handled(listing_id, eligible, reason)

    if not eligible:
        return SingleIngestSummary(
            listing_id=listing_id,
            accepted=False,
            raw_stored=False,
            reason=reason,
        )

    save_listing_json(listing_id, payload)
    return SingleIngestSummary(
        listing_id=listing_id,
        accepted=True,
        raw_stored=True,
    )


def transform_listing(listing_id):
    transformed_listing = transform_listing_json(listing_id)
    load_listing(transformed_listing)
    return SingleTransformSummary(
        listing_id=listing_id,
        transformed=True,
        loaded=True,
    )


def run_listing(listing_id):
    ingest_summary = ingest_listing(listing_id)
    if not ingest_summary.accepted:
        return SingleRunSummary(
            listing_id=listing_id,
            accepted=False,
            raw_stored=False,
            transformed=False,
            loaded=False,
            reason=ingest_summary.reason,
        )

    transform_summary = transform_listing(listing_id)
    return SingleRunSummary(
        listing_id=listing_id,
        accepted=True,
        raw_stored=ingest_summary.raw_stored,
        transformed=transform_summary.transformed,
        loaded=transform_summary.loaded,
    )


def discover_listings(scrape_date, max_candidates=None):
    return discover_completed_auctions(
        scrape_date=scrape_date,
        max_candidates=max_candidates,
    )


def ingest_discovered_listings(batch_size=None):
    summary = BatchIngestSummary()
    pending_rows = load_pending_discovered_listings(limit=batch_size)

    if not pending_rows:
        return summary

    with sync_playwright() as playwright:
        browser, context = launch_carsandbids_browser_context(playwright, headless=True)
        try:
            for row in pending_rows:
                summary.selected += 1
                listing_id = row["source_listing_id"]

                summary.scrape_attempted += 1
                try:
                    payload = fetch_listing_json_with_context(listing_id, context)
                except Exception as exc:
                    summary.scrape_failed += 1
                    logger.error(
                        "carsandbids ingest-discovered scrape failed "
                        "for listing_id=%s error=%s",
                        listing_id,
                        exc,
                    )
                    continue

                eligible, reason = evaluate_listing_eligibility(payload)
                mark_discovered_listing_handled(listing_id, eligible, reason)
                if not eligible:
                    logger.info(
                        "carsandbids ingest-discovered listing rejected "
                        "for listing_id=%s reason=%s",
                        listing_id,
                        reason,
                    )
                    summary.rejected += 1
                    continue

                save_listing_json(listing_id, payload, url=row["url"])
                summary.raw_json_stored += 1
                summary.accepted += 1
        finally:
            browser.close()

    return summary


def transform_discovered_listings(batch_size=None):
    summary = BatchTransformSummary()
    pending_rows = load_pending_raw_listing_json(limit=batch_size)

    for row in pending_rows:
        summary.selected += 1
        listing_id = row["source_listing_id"]

        try:
            transformed_listing = transform_listing_json(listing_id)
        except Exception as exc:
            summary.transform_failed += 1
            logger.error(
                "carsandbids transform-discovered row failed for listing_id=%s "
                "stage=transform error=%s",
                listing_id,
                exc,
            )
            continue

        try:
            load_listing(transformed_listing)
        except Exception as exc:
            summary.load_failed += 1
            logger.error(
                "carsandbids transform-discovered row failed for listing_id=%s "
                "stage=load error=%s",
                listing_id,
                exc,
            )
            continue

        summary.transformed_and_loaded += 1

    return summary
