CARSANDBIDS_CHROME_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)


def launch_carsandbids_browser_context(playwright, headless=True):
    browser = playwright.chromium.launch(headless=headless)
    context = browser.new_context(user_agent=CARSANDBIDS_CHROME_USER_AGENT)
    return browser, context
