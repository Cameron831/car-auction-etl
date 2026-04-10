from app.sources.bat.ingest import fetch_listing_html, save_listing_html

def test_fetch_listing_html_live():
    # Fetch a real BAT listing and verify that HTML is returned.
    html = fetch_listing_html("2004-bmw-m3-coupe-232")
    assert isinstance(html, str)
    assert len(html) > 0
    assert "<html" in html.lower()


def test_save_listing_html_live(tmp_path, mocker):
    # Redirect raw storage into pytest's temp directory for this live save test.
    mocker.patch("app.sources.bat.ingest.RAW_HTML_DIR", tmp_path / "data" / "raw" / "bat")

    # Fetch live HTML, persist it locally, and verify that a non-empty HTML file was created.
    html = fetch_listing_html("2004-bmw-m3-coupe-232")
    saved_path = save_listing_html("2004-bmw-m3-coupe-232", html)

    assert saved_path.exists()
    assert saved_path.suffix == ".html"
    assert saved_path.stat().st_size > 0
