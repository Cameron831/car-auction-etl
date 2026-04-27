import argparse
import json
from urllib.parse import parse_qs, urlparse

from playwright.sync_api import TimeoutError, sync_playwright

from app.sources.carsandbids import discovery


def main():
    parser = argparse.ArgumentParser(
        description="Manually test live Cars and Bids discovery pagination."
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run Chromium headless. Default is headed for manual browser checks.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=180,
        help="Seconds to wait for the initial signed closed-auctions API response.",
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=3,
        help="Total pages to inspect, including the captured first page.",
    )
    parser.add_argument(
        "--print-first-auction",
        action="store_true",
        help="Print the first auction object from each page.",
    )
    args = parser.parse_args()

    if args.pages <= 0:
        raise SystemExit("--pages must be positive")

    results = inspect_pagination(
        pages=args.pages,
        headless=args.headless,
        timeout_ms=args.timeout_seconds * 1000,
        print_first_auction=args.print_first_auction,
    )
    print(json.dumps(results, indent=2))


def inspect_pagination(pages, headless=False, timeout_ms=180_000, print_first_auction=False):
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=headless)
        try:
            page = browser.new_page()
            first_payload, response_url = capture_first_page(page, timeout_ms)
            timestamp, signature = discovery._extract_signed_request_params(response_url)

            results = [
                build_page_result(
                    page_number=1,
                    offset=0,
                    payload=first_payload,
                    response_url=response_url,
                    print_first_auction=print_first_auction,
                )
            ]

            offset = discovery.DISCOVERY_PAGE_SIZE
            for page_number in range(2, pages + 1):
                payload = fetch_followup_page(
                    page,
                    offset=offset,
                    timestamp=timestamp,
                    signature=signature,
                )
                results.append(
                    build_page_result(
                        page_number=page_number,
                        offset=offset,
                        payload=payload,
                        response_url=None,
                        print_first_auction=print_first_auction,
                    )
                )
                offset += discovery.DISCOVERY_PAGE_SIZE

            return {
                "page_size": discovery.DISCOVERY_PAGE_SIZE,
                "timestamp": timestamp,
                "signature_length": len(signature),
                "pages": results,
            }
        finally:
            browser.close()


def capture_first_page(page, timeout_ms):
    matched_response = None

    def is_matching_response(response):
        return is_completed_auctions_response(response.url)

    def capture_matching_response(response):
        nonlocal matched_response
        if matched_response is None and is_matching_response(response):
            matched_response = response

    print(f"Opening {discovery.PAST_AUCTIONS_URL}")
    print("If a browser challenge appears, complete it in the opened Chromium window.")
    print(f"Waiting up to {timeout_ms // 1000} seconds for page 1...")

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


def fetch_followup_page(page, offset, timestamp, signature):
    print(f"Fetching follow-up page offset={offset} from inside the browser page...")
    response = page.evaluate(
        """async ({url, params}) => {
            const requestUrl = new URL(url);
            for (const [key, value] of Object.entries(params)) {
                requestUrl.searchParams.set(key, String(value));
            }
            const response = await fetch(requestUrl.toString(), {
                credentials: "include",
                headers: {"accept": "application/json"},
            });
            const text = await response.text();
            return {
                ok: response.ok,
                status: response.status,
                text,
            };
        }""",
        {
            "url": discovery.API_AUCTIONS_URL,
            "params": {
                "limit": discovery.DISCOVERY_PAGE_SIZE,
                "status": "closed",
                "offset": offset,
                "timestamp": timestamp,
                "signature": signature,
            },
        },
    )
    if not response["ok"]:
        raise RuntimeError(
            "Cars and Bids completed auctions API response failed "
            f"offset={offset} status={response['status']} body={response['text']}"
        )
    return json.loads(response["text"])


def build_page_result(page_number, offset, payload, response_url, print_first_auction):
    auctions = discovery._extract_auctions(payload)
    result = {
        "page": page_number,
        "offset": offset,
        "count": payload.get("count"),
        "total": payload.get("total"),
        "auctions": len(auctions),
        "first_id": auctions[0].get("id") if auctions and isinstance(auctions[0], dict) else None,
        "last_id": auctions[-1].get("id") if auctions and isinstance(auctions[-1], dict) else None,
    }
    if response_url is not None:
        result["response_url"] = response_url
    if print_first_auction:
        result["first_auction"] = auctions[0] if auctions else None
    return result


def is_completed_auctions_response(url):
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    return (
        url.startswith(discovery.API_AUCTIONS_URL)
        and params.get("status") == ["closed"]
    )


if __name__ == "__main__":
    main()
