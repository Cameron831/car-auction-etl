import logging

from bs4 import BeautifulSoup
import pytest
import requests
from app.sources.bat import ingest
from app.sources.bat.ingest import fetch_listing_html, save_listing_html

# test that the function returns the correct HTML and that the request is made with the correct URL
def test_fetch_listing_html(mocker, caplog):
    # create mock request
    mock_get = mocker.patch('app.sources.bat.ingest.requests.get')
    # set return value for mock request
    mock_get.return_value.text = "<html>Test</html>"
    mock_get.return_value.raise_for_status.return_value = None

    # call the function being tested
    caplog.set_level(logging.INFO)
    response = fetch_listing_html("test-id")

    # assert that the response is correct and that the mock request was called with the correct URL
    assert response == "<html>Test</html>"
    mock_get.assert_called_once_with("https://bringatrailer.com/listing/test-id", timeout=10)
    assert "Fetching BAT listing HTML for listing_id=test-id" in caplog.text
    assert "Fetched BAT listing HTML for listing_id=test-id" in caplog.text
    assert "<html>Test</html>" not in caplog.text

# test that an HTTP error is raised when the request fails
def test_fetch_listing_html_bad_response(mocker):
    # create mock request
    mock_get = mocker.patch('app.sources.bat.ingest.requests.get')
    # raise error for mock request
    mock_get.return_value.raise_for_status.side_effect = requests.HTTPError("404 Client Error")

    # assert that the error is raised when calling the function
    with pytest.raises(requests.HTTPError, match="404 Client Error"):
        fetch_listing_html("bad-id")

# test that a connection error is raised when the request fails to connect
def test_fetch_listing_html_connection_error(mocker):
    # create mock request
    mock_get = mocker.patch("app.sources.bat.ingest.requests.get")
    # raise connection error for mock request
    mock_get.side_effect = requests.ConnectionError("Connection failed")

    # assert that the error is raised when calling the function
    with pytest.raises(requests.ConnectionError, match="Connection failed"):
        fetch_listing_html("test-id")


def test_build_raw_listing_html_params_maps_listing_to_schema_columns():
    params = ingest.build_raw_listing_html_params(
        "test-id",
        "<html>Test</html>",
        "https://example.test/listing/test-id",
    )

    assert params == {
        "source_site": "bringatrailer",
        "source_listing_id": "test-id",
        "url": "https://example.test/listing/test-id",
        "raw_html": "<html>Test</html>",
    }


def test_build_raw_listing_html_params_defaults_bat_url():
    params = ingest.build_raw_listing_html_params("test-id", "<html>Test</html>")

    assert params["url"] == "https://bringatrailer.com/listing/test-id/"


def test_save_listing_html_executes_upsert_with_expected_conflict_target(mocker, caplog):
    calls = {}

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params):
            calls["sql"] = sql
            calls["params"] = params

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

    def fake_connect(database_url):
        calls["database_url"] = database_url
        return FakeConnection()

    mocker.patch.dict(
        "os.environ",
        {"DATABASE_URL": "postgresql://user:pass@localhost/db"},
    )
    mocker.patch.object(ingest.psycopg, "connect", side_effect=fake_connect)

    caplog.set_level(logging.INFO)
    save_listing_html(
        "test-id",
        "<html>Test</html>",
        "https://example.test/listing/test-id",
    )

    assert calls["database_url"] == "postgresql://user:pass@localhost/db"
    assert "ON CONFLICT (source_site, source_listing_id) DO UPDATE" in calls["sql"]
    assert "processed = FALSE" in calls["sql"]
    assert calls["params"] == {
        "source_site": "bringatrailer",
        "source_listing_id": "test-id",
        "url": "https://example.test/listing/test-id",
        "raw_html": "<html>Test</html>",
    }
    assert "Saved BAT raw listing HTML for listing_id=test-id" in caplog.text
    assert "<html>Test</html>" not in caplog.text
    assert "postgresql://user:pass@localhost/db" not in caplog.text


def test_save_listing_html_requires_database_url(mocker):
    mocker.patch.dict("os.environ", {}, clear=True)

    with pytest.raises(RuntimeError, match="DATABASE_URL must be set"):
        save_listing_html("test-id", "<html>Test</html>")


def test_evaluate_listing_eligibility_rejects_missing_or_unparseable_year():
    soup = BeautifulSoup("<html></html>", "html.parser")

    assert ingest.evaluate_listing_eligibility(soup, "factory-five-cobra-replica") == (
        False,
        "listing ID year missing",
    )


def test_evaluate_listing_eligibility_rejects_pre_1946_listing_id():
    soup = BeautifulSoup("<html></html>", "html.parser")

    assert ingest.evaluate_listing_eligibility(soup, "1941-ford-super-deluxe-coupe") == (
        False,
        "year before 1946",
    )


def test_evaluate_listing_eligibility_rejects_missing_country():
    soup = BeautifulSoup("<html><body></body></html>", "html.parser")

    assert ingest.evaluate_listing_eligibility(soup, "1967-porsche-911s-coupe") == (
        False,
        "listing outside US",
    )


def test_evaluate_listing_eligibility_rejects_non_usa_country():
    soup = BeautifulSoup(
        """
        <html>
            <body>
                <span class="show-country-name">CAN</span>
            </body>
        </html>
        """,
        "html.parser",
    )

    assert ingest.evaluate_listing_eligibility(soup, "1967-porsche-911s-coupe") == (
        False,
        "listing outside US",
    )


def test_evaluate_listing_eligibility_rejects_excluded_category():
    soup = BeautifulSoup(
        """
        <html>
            <body>
                <span class="show-country-name">USA</span>
                <a class="group-link" href="/parts/">
                    <strong class="group-title-label">Category</strong>
                    Parts
                </a>
            </body>
        </html>
        """,
        "html.parser",
    )

    assert ingest.evaluate_listing_eligibility(soup, "1967-porsche-911s-coupe") == (
        False,
        "excluded category: Parts",
    )


def test_evaluate_listing_eligibility_rejects_when_any_category_is_excluded():
    soup = BeautifulSoup(
        """
        <html>
            <body>
                <span class="show-country-name">USA</span>
                <a class="group-link" href="/convertible/">
                    <strong class="group-title-label">Category</strong>
                    Convertibles
                </a>
                <a class="group-link" href="/parts/">
                    <strong class="group-title-label">Category</strong>
                    Parts
                </a>
            </body>
        </html>
        """,
        "html.parser",
    )

    assert ingest.evaluate_listing_eligibility(soup, "1967-porsche-911s-coupe") == (
        False,
        "excluded category: Parts",
    )


def test_evaluate_listing_eligibility_rejects_projects_via_category():
    soup = BeautifulSoup(
        """
        <html>
            <body>
                <span class="show-country-name">USA</span>
                <a class="group-link" href="/project/">
                    <strong class="group-title-label">Category</strong>
                    Projects
                </a>
            </body>
        </html>
        """,
        "html.parser",
    )

    assert ingest.evaluate_listing_eligibility(soup, "1967-porsche-911-coupe") == (
        False,
        "excluded category: Projects",
    )


def test_evaluate_listing_eligibility_rejects_race_cars_via_category():
    soup = BeautifulSoup(
        """
        <html>
            <body>
                <span class="show-country-name">USA</span>
                <a class="group-link" href="/race-car/">
                    <strong class="group-title-label">Category</strong>
                    Race Cars
                </a>
            </body>
        </html>
        """,
        "html.parser",
    )

    assert ingest.evaluate_listing_eligibility(soup, "1997-porsche-911-gt2-evo") == (
        False,
        "excluded category: Race Cars",
    )


def test_evaluate_listing_eligibility_rejects_replica_when_listing_id_year_missing():
    soup = BeautifulSoup(
        """
        <html>
            <body>
                <span class="show-country-name">USA</span>
                <a class="group-link" href="/convertible/">
                    <strong class="group-title-label">Category</strong>
                    Convertibles
                </a>
            </body>
        </html>
        """,
        "html.parser",
    )

    assert ingest.evaluate_listing_eligibility(soup, "shelby-cobra-replica") == (
        False,
        "listing ID year missing",
    )


def test_evaluate_listing_eligibility_does_not_reject_missing_category():
    soup = BeautifulSoup(
        '<html><body><span class="show-country-name">USA</span></body></html>',
        "html.parser",
    )

    assert ingest.evaluate_listing_eligibility(soup, "1967-porsche-911s-coupe") == (True, None)


def test_evaluate_listing_eligibility_uses_listing_id_when_title_has_no_year():
    soup = BeautifulSoup(
        '<html><body><span class="show-country-name">USA</span></body></html>',
        "html.parser",
    )

    assert ingest.evaluate_listing_eligibility(soup, "2010-am-general-hmmwv-military-5") == (
        True,
        None,
    )


def test_evaluate_listing_eligibility_does_not_reject_empty_category():
    soup = BeautifulSoup(
        """
        <html>
            <body>
                <span class="show-country-name">USA</span>
                <a class="group-link" href="/category/">
                    <strong class="group-title-label">Category</strong>
                </a>
            </body>
        </html>
        """,
        "html.parser",
    )

    assert ingest.evaluate_listing_eligibility(soup, "1967-porsche-911s-coupe") == (True, None)


def test_evaluate_listing_eligibility_does_not_reject_non_excluded_category():
    soup = BeautifulSoup(
        """
        <html>
            <body>
                <span class="show-country-name">USA</span>
                <a class="group-link" href="/convertible/">
                    <strong class="group-title-label">Category</strong>
                    Convertibles
                </a>
            </body>
        </html>
        """,
        "html.parser",
    )

    assert ingest.evaluate_listing_eligibility(soup, "1967-porsche-911s-coupe") == (True, None)


def test_evaluate_listing_eligibility_allows_multiple_non_excluded_categories():
    soup = BeautifulSoup(
        """
        <html>
            <body>
                <span class="show-country-name">USA</span>
                <a class="group-link" href="/convertible/">
                    <strong class="group-title-label">Category</strong>
                    Convertibles
                </a>
                <a class="group-link" href="/truck-4x4/">
                    <strong class="group-title-label">Category</strong>
                    Truck & 4x4
                </a>
            </body>
        </html>
        """,
        "html.parser",
    )

    assert ingest.evaluate_listing_eligibility(soup, "1967-ford-f-250") == (True, None)


def test_evaluate_listing_eligibility_keeps_truck_and_4x4_in_scope():
    soup = BeautifulSoup(
        """
        <html>
            <body>
                <span class="show-country-name">USA</span>
                <a class="group-link" href="/truck-4x4/">
                    <strong class="group-title-label">Category</strong>
                    Truck & 4x4
                </a>
            </body>
        </html>
        """,
        "html.parser",
    )

    assert ingest.evaluate_listing_eligibility(soup, "1967-ford-f-250-4x4") == (True, None)
