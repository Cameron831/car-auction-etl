import argparse
import json
from urllib.parse import parse_qs, urlparse

from playwright.sync_api import TimeoutError, sync_playwright

from app.sources.carsandbids import discovery


def main():
    parser = argparse.ArgumentParser(
        description="Capture a live Cars and Bids completed-auctions API response."
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run Chromium headless. Default is headed for manual Cloudflare/browser checks.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=180,
        help="Seconds to wait for the signed closed-auctions API response.",
    )
    parser.add_argument(
        "--print-auction",
        action="store_true",
        help="Print the first captured auction object.",
    )
    args = parser.parse_args()

    payload, response_url = capture_live_response(
        headless=args.headless,
        timeout_ms=args.timeout_seconds * 1000,
    )
    timestamp, signature = discovery._extract_signed_request_params(response_url)
    auctions = discovery._extract_auctions(payload)

    print(
        json.dumps(
            {
                "response_url": response_url,
                "count": payload.get("count"),
                "total": payload.get("total"),
                "auctions": len(auctions),
                "timestamp": timestamp,
                "signature_length": len(signature),
            },
            indent=2,
        )
    )
    if args.print_auction:
        print(json.dumps(auctions[0] if auctions else None, indent=2))


def capture_live_response(headless=False, timeout_ms=180_000):
    matched_response = None

    def is_matching_response(response):
        parsed = urlparse(response.url)
        params = parse_qs(parsed.query)
        return (
            response.url.startswith(discovery.API_AUCTIONS_URL)
            and params.get("status") == ["closed"]
        )

    def capture_matching_response(response):
        nonlocal matched_response
        if matched_response is None and is_matching_response(response):
            matched_response = response

    print(f"Opening {discovery.PAST_AUCTIONS_URL}")
    print("If a browser challenge appears, complete it in the opened Chromium window.")
    print(f"Waiting up to {timeout_ms // 1000} seconds for the closed-auctions API response...")

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        try:
            page = browser.new_page()
            page.on("response", capture_matching_response)
            page.goto(
                discovery.PAST_AUCTIONS_URL,
                wait_until="domcontentloaded",
                timeout=discovery.PAGE_LOAD_TIMEOUT_MS,
            )

            if matched_response is None:
                try:
                    matched_response = page.wait_for_event(
                        "response",
                        predicate=is_matching_response,
                        timeout=timeout_ms,
                    )
                except TimeoutError as exc:
                    raise RuntimeError(
                        "Timed out waiting for Cars and Bids closed-auctions API response"
                    ) from exc

            if not matched_response.ok:
                raise RuntimeError(
                    "Cars and Bids completed auctions API response failed "
                    f"status={matched_response.status}"
                )

            return matched_response.json(), matched_response.url
        finally:
            browser.close()


if __name__ == "__main__":
    main()
