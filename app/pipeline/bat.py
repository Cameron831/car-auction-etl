import logging
from dataclasses import dataclass

from bs4 import BeautifulSoup

from app.sources.bat.discovery import (
    discover_completed_auctions,
    load_pending_discovered_listings,
    mark_discovered_listing_handled,
)
from app.sources.bat.ingest import (
    evaluate_listing_eligibility,
    fetch_listing_html,
    save_listing_html,
)
from app.sources.bat.load import load_listing
from app.sources.bat.transform import (
    load_pending_raw_listing_html,
    transform_listing_html,
)

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
    html = fetch_listing_html(listing_id)
    soup = BeautifulSoup(html, "html.parser")
    eligible, reason = evaluate_listing_eligibility(soup, listing_id)
    mark_discovered_listing_handled(listing_id, eligible, reason)

    if not eligible:
        return SingleIngestSummary(
            listing_id=listing_id,
            accepted=False,
            raw_stored=False,
            reason=reason,
        )

    save_listing_html(listing_id, html)
    return SingleIngestSummary(
        listing_id=listing_id,
        accepted=True,
        raw_stored=True,
    )


def transform_listing(listing_id):
    transformed_listing = transform_listing_html(listing_id)
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
        mark_discovered_listing_handled(listing_id, eligible, reason)
        if not eligible:
            logger.info(
                "BAT ingest-discovered listing rejected for listing_id=%s reason=%s",
                listing_id,
                reason,
            )
            summary.rejected += 1
            continue

        save_listing_html(listing_id, html, url=row["url"])
        summary.raw_html_stored += 1
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
