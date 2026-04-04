from app.sources.bat.ingest import fetch_listing_html

def test_fetch_listing_html_live():
    html = fetch_listing_html("2004-bmw-m3-coupe-232")
    assert isinstance(html, str)
    assert len(html) > 0
    assert "<html" in html.lower()