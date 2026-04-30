import logging

from bs4 import BeautifulSoup
import pytest

from app.sources.bat import transform

def test_build_raw_listing_lookup_params_maps_listing_to_schema_columns():
    params = transform.build_raw_listing_lookup_params("test-id")

    assert params == {
        "source_site": "bringatrailer",
        "source_listing_id": "test-id",
    }


def test_load_listing_html_retrieves_raw_html_from_postgres(mocker):
    calls = {}

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params):
            calls["sql"] = sql
            calls["params"] = params

        def fetchone(self):
            return ("<html>Test</html>",)

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
    mocker.patch.object(transform.psycopg, "connect", side_effect=fake_connect)

    html = transform.load_listing_html("test-id")

    assert html == "<html>Test</html>"
    assert calls["database_url"] == "postgresql://user:pass@localhost/db"
    assert "FROM raw_listing_html" in calls["sql"]
    assert "source_site = %(source_site)s" in calls["sql"]
    assert "source_listing_id = %(source_listing_id)s" in calls["sql"]
    assert calls["params"] == {
        "source_site": "bringatrailer",
        "source_listing_id": "test-id",
    }


def test_load_listing_html_missing_record_raises_clear_error(mocker):
    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params):
            return None

        def fetchone(self):
            return None

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

    mocker.patch.dict(
        "os.environ",
        {"DATABASE_URL": "postgresql://user:pass@localhost/db"},
    )
    mocker.patch.object(transform.psycopg, "connect", return_value=FakeConnection())

    with pytest.raises(LookupError, match="Raw HTML record not found for listing ID: missing-id"):
        transform.load_listing_html("missing-id")


def test_load_listing_html_requires_database_url(mocker):
    mocker.patch.dict("os.environ", {}, clear=True)

    with pytest.raises(RuntimeError, match="DATABASE_URL must be set"):
        transform.load_listing_html("test-id")


def test_load_pending_raw_listing_html_selects_unprocessed_rows_in_stable_order(mocker):
    calls = {}
    expected_rows = [
        {"source_listing_id": "first"},
        {"source_listing_id": "second"},
    ]

    class FakeCursor:
        def __init__(self, rows):
            self._rows = rows

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params):
            calls["sql"] = sql
            calls["params"] = params

        def fetchall(self):
            return self._rows

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self, row_factory=None):
            calls["row_factory"] = row_factory
            return FakeCursor(expected_rows)

    mocker.patch.dict(
        "os.environ",
        {"DATABASE_URL": "postgresql://user:pass@localhost/db"},
    )
    mocker.patch.object(transform.psycopg, "connect", return_value=FakeConnection())

    rows = transform.load_pending_raw_listing_html()

    assert rows == expected_rows
    assert "FROM raw_listing_html" in calls["sql"]
    assert "processed = FALSE" in calls["sql"]
    assert "ORDER BY created_at ASC, id ASC" in calls["sql"]
    assert "LIMIT %(limit)s" not in calls["sql"]
    assert calls["params"] == {"source_site": "bringatrailer"}
    assert calls["row_factory"] is transform.dict_row


def test_load_pending_raw_listing_html_applies_optional_limit(mocker):
    calls = {}

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params):
            calls["sql"] = sql
            calls["params"] = params

        def fetchall(self):
            return [{"source_listing_id": "first"}]

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self, row_factory=None):
            return FakeCursor()

    mocker.patch.dict(
        "os.environ",
        {"DATABASE_URL": "postgresql://user:pass@localhost/db"},
    )
    mocker.patch.object(transform.psycopg, "connect", return_value=FakeConnection())

    rows = transform.load_pending_raw_listing_html(limit=1)

    assert rows == [{"source_listing_id": "first"}]
    assert "LIMIT %(limit)s" in calls["sql"]
    assert calls["params"] == {"source_site": "bringatrailer", "limit": 1}


def test_load_pending_raw_listing_html_returns_empty_for_non_positive_limit(mocker):
    connect = mocker.patch.object(transform.psycopg, "connect")
    mocker.patch.dict(
        "os.environ",
        {"DATABASE_URL": "postgresql://user:pass@localhost/db"},
    )

    assert transform.load_pending_raw_listing_html(limit=0) == []
    connect.assert_not_called()


def test_load_pending_raw_listing_html_requires_database_url(mocker):
    mocker.patch.dict("os.environ", {}, clear=True)

    with pytest.raises(RuntimeError, match="DATABASE_URL must be set"):
        transform.load_pending_raw_listing_html()


def test_transform_listing_html_logs_success_without_raw_html(mocker, caplog):
    mocker.patch.object(transform, "load_listing_html", return_value="<html>SENSITIVE_RAW_HTML</html>")
    mocker.patch.object(transform, "get_product_json_ld", return_value={"name": "One Owner 2004 BMW M3"})
    mocker.patch.object(transform, "get_listing_details", return_value=["Chassis: WBSBL93414PN57203"])
    mocker.patch.object(transform, "parse_listing_id_year", return_value=2004)
    mocker.patch.object(transform, "parse_make", return_value="BMW")
    mocker.patch.object(transform, "parse_model", return_value="M3")
    mocker.patch.object(transform, "find_detail_value", side_effect=["50,250 Miles", "Chassis: WBSBL93414PN57203", "6-Speed Manual Transmission"])
    mocker.patch.object(transform, "parse_mileage", return_value=50250)
    mocker.patch.object(transform, "extract_vin", return_value="WBSBL93414PN57203")
    mocker.patch.object(transform, "extract_sale_price", return_value=19750)
    mocker.patch.object(transform, "extract_sold_status", return_value=True)
    mocker.patch.object(transform, "extract_auction_end_date", return_value="2026-03-30")
    mocker.patch.object(transform, "normalize_transmission", return_value="manual")

    caplog.set_level(logging.INFO)
    transformed = transform.transform_listing_html("2004-test-id")

    assert transformed["listing_id"] == "2004-test-id"
    assert transformed["model_raw"] == "M3"
    assert transformed["model_normalized"] == "M3"
    assert "Transforming BAT listing HTML for listing_id=2004-test-id" in caplog.text
    assert "Transformed BAT listing HTML for listing_id=2004-test-id" in caplog.text
    assert "SENSITIVE_RAW_HTML" not in caplog.text

def test_transform_listing_html_allows_missing_model(mocker):
    html_content = """
    <html>
        <head>
            <script type="application/ld+json">
            {
                "@context": "http://schema.org",
                "@type": "Product",
                "name": "One Owner BMW M3",
                "offers": {
                    "@type": "Offer",
                    "priceCurrency": "USD",
                    "price": 19750
                }
            }
            </script>
        </head>
        <body>
            <button class="group-title">
                <strong class="group-title-label">Make</strong>
                BMW
            </button>
            <div class="item">
                <strong>Listing Details</strong>
                <ul>
                    <li>Chassis: WBSBL93414PN57203</li>
                    <li>50,250 Miles</li>
                    <li>6-Speed Manual Transmission</li>
                </ul>
            </div>
            <div class="listing-available-info">
                <span>Sold for <strong>USD $19,750</strong></span>
            </div>
            <span class="date date-localize" data-timestamp="1774898451"></span>
        </body>
    </html>
    """
    mocker.patch.object(transform, "load_listing_html", return_value=html_content)

    transformed = transform.transform_listing_html("2004-missing-model")

    assert transformed["make"] == "BMW"
    assert transformed["model_raw"] is None
    assert transformed["model_normalized"] is None
    assert transformed["year"] == 2004

def test_transform_listing_html_allows_missing_mileage_detail(mocker):
    html_content = """
    <html>
        <head>
            <script type="application/ld+json">
            {
                "@context": "http://schema.org",
                "@type": "Product",
                "name": "One Owner BMW M3",
                "offers": {
                    "@type": "Offer",
                    "priceCurrency": "USD",
                    "price": 19750
                }
            }
            </script>
        </head>
        <body>
            <button class="group-title">
                <strong class="group-title-label">Make</strong>
                BMW
            </button>
            <button class="group-title">
                <strong class="group-title-label">Model</strong>
                M3
            </button>
            <div class="item">
                <strong>Listing Details</strong>
                <ul>
                    <li>Chassis: WBSBL93414PN57203</li>
                    <li>6-Speed Manual Transmission</li>
                </ul>
            </div>
            <div class="listing-available-info">
                <span>Sold for <strong>USD $19,750</strong></span>
            </div>
            <span class="date date-localize" data-timestamp="1774898451"></span>
        </body>
    </html>
    """
    mocker.patch.object(transform, "load_listing_html", return_value=html_content)

    transformed = transform.transform_listing_html("2004-missing-mileage")

    assert transformed["mileage"] is None
    assert transformed["vin"] == "WBSBL93414PN57203"
    assert transformed["transmission"] == "manual"


@pytest.mark.parametrize(
    ("listing_details", "error"),
    [
        (
            """
            <li>50,250 Miles</li>
            <li>6-Speed Manual Transmission</li>
            """,
            "Could not parse VIN",
        ),
        (
            """
            <li>Chassis: WBSBL93414PN57203</li>
            <li>50,250 Miles</li>
            """,
            "Could not parse Transmission",
        ),
    ],
)
def test_transform_listing_html_preserves_required_detail_failures(
    mocker,
    listing_details,
    error,
):
    html_content = f"""
    <html>
        <head>
            <script type="application/ld+json">
            {{
                "@context": "http://schema.org",
                "@type": "Product",
                "name": "One Owner BMW M3",
                "offers": {{
                    "@type": "Offer",
                    "priceCurrency": "USD",
                    "price": 19750
                }}
            }}
            </script>
        </head>
        <body>
            <button class="group-title">
                <strong class="group-title-label">Make</strong>
                BMW
            </button>
            <div class="item">
                <strong>Listing Details</strong>
                <ul>
                    {listing_details}
                </ul>
            </div>
            <div class="listing-available-info">
                <span>Sold for <strong>USD $19,750</strong></span>
            </div>
            <span class="date date-localize" data-timestamp="1774898451"></span>
        </body>
    </html>
    """
    mocker.patch.object(transform, "load_listing_html", return_value=html_content)

    with pytest.raises(ValueError, match=error):
        transform.transform_listing_html("2004-required-field-failure")

def test_get_product_json_ld_returns_product_data(tmp_path):
    # create a test HTML file with a valid JSON-LD script tag
    html_content = """
    <html>
            <script type="application/ld+json">
            {
                "@context": "http://schema.org",
                "@type": "Product",
                "name": "Test Product"
            }
            </script>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    product_data = transform.get_product_json_ld(soup)
    assert product_data == {
        "@context": "http://schema.org",
        "@type": "Product",
        "name": "Test Product"
    }

def test_get_product_json_ld_multiple_script_tags(tmp_path):
    # create a test HTML file with multiple JSON-LD script tags, only one of which contains product data
    html_content = """
    <html>
            <script type="application/ld+json">
            {
                "@context": "http://schema.org",
                "@type": "Organization",
                "name": "Test Organization"
            }
            </script>
            <script type="application/ld+json">
            {
                "@context": "http://schema.org",
                "@type": "Product",
                "name": "Test Product"
            }
            </script>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    product_data = transform.get_product_json_ld(soup)
    assert product_data == {
        "@context": "http://schema.org",
        "@type": "Product",
        "name": "Test Product"
    }

def test_get_product_json_ld_no_product_data(tmp_path):
    # create a test HTML file with JSON-LD script tags that do not contain product data
    html_content = """
    <html>
            <script type="application/ld+json">
            {
                "@context": "http://schema.org",
                "@type": "Organization",
                "name": "Test Organization"
            }
            </script>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    with pytest.raises(ValueError, match="No valid Product JSON-LD found in listing HTML"):
        transform.get_product_json_ld(soup)
    
def test_get_product_json_ld_invalid_json(tmp_path):
    # create a test HTML file with a JSON-LD script tag that contains invalid JSON
    html_content = """
    <html>
            <script type="application/ld+json">
            {
                "@context": "http://schema.org",
                "@type": "Product",
                "name": "Test Product",
            }
            </script>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    with pytest.raises(ValueError, match="No valid Product JSON-LD found in listing HTML"):
        transform.get_product_json_ld(soup)

def test_get_product_json_ld_returns_product_from_array_payload():
    html_content = """
    <html>
        <script type="application/ld+json">
        [
            {
                "@context": "http://schema.org",
                "@type": "BreadcrumbList"
            },
            {
                "@context": "http://schema.org",
                "@type": "Product",
                "name": "Test Product"
            }
        ]
        </script>
    </html>
    """

    soup = BeautifulSoup(html_content, "html.parser")
    product_data = transform.get_product_json_ld(soup)
    assert product_data == {
        "@context": "http://schema.org",
        "@type": "Product",
        "name": "Test Product",
    }

def test_extract_listing_title_from_json_ld():
    soup = BeautifulSoup("<html></html>", "html.parser")
    product_data = {
        "@context": "http://schema.org",
        "@type": "Product",
        "name": "One Owner 2026 Make Model Title",
    }
    title = transform.extract_listing_title(soup, product_data)
    assert title == "2026 Make Model Title"

def test_extract_listing_title_from_meta_tag():
    html_content = """
    <html>
        <head>
            <meta name="parsely-title" content="One Owner 2026 Make Model Title">
        </head>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    product_data = {}
    title = transform.extract_listing_title(soup, product_data)
    assert title == "2026 Make Model Title"

def test_extract_listing_title_not_found():
    html_content = """
    <html>
        <head>
            <script type="application/ld+json">
            {
                "@context": "http://schema.org",
                "@type": "Product"
            }
            </script>
        </head>
        <body></body>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    product_data = {
        "@context": "http://schema.org",
        "@type": "Product"
    }
    with pytest.raises(ValueError, match="Could not parse listing title"):
        transform.extract_listing_title(soup, product_data)

def test_parse_listing_id_year_reads_leading_year():
    assert transform.parse_listing_id_year("2026-make-model") == 2026
    assert transform.parse_listing_id_year("2026") == 2026


@pytest.mark.parametrize(
    "listing_id",
    [
        "make-model-2026",
        "26-make-model",
        "20260-make-model",
        "",
        None,
    ],
)
def test_parse_listing_id_year_rejects_missing_leading_year(listing_id):
    with pytest.raises(ValueError, match="Could not parse year from listing ID"):
        transform.parse_listing_id_year(listing_id)

def test_parse_model_valid():
    html_content = """
    <html>
        <body>
            <button class="group-title">
                <strong class="group-title-label">Model</strong>
                Model Name
            </button>
        </body>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    model = transform.parse_model(soup)
    assert model == "Model Name"

def test_parse_model_valid_group_link():
    html_content = """
    <html>
        <body>
            <a class="group-link" href="https://bringatrailer.com/porsche/911-carrera-3-2/">
                <strong class="group-title-label">Model</strong>
                Porsche 911 Carrera 3.2
            </a>
        </body>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    model = transform.parse_model(soup)
    assert model == "Porsche 911 Carrera 3.2"

def test_parse_model_returns_first_matching_value():
    html_content = """
    <html>
        <body>
            <a class="group-link" href="/911/">
                <strong class="group-title-label">Model</strong>
                Porsche 911
            </a>
            <a class="group-link" href="/911-carrera/">
                <strong class="group-title-label">Model</strong>
                Porsche 911 Carrera
            </a>
        </body>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")

    assert transform.parse_model(soup) == "Porsche 911"

def test_parse_model_not_found():
    html_content = """
    <html>
        <body>
            <button class="group-title">
                <strong class="group-title-label">Make</strong>
                Make Name
            </button>
        </body>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    assert transform.parse_model(soup) is None

def test_parse_model_empty_group_returns_none():
    html_content = """
    <html>
        <body>
            <button class="group-title">
                <strong class="group-title-label">Model</strong>
            </button>
        </body>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    assert transform.parse_model(soup) is None

def test_parse_make_valid():
    html_content = """
    <html>
        <body>
            <button class="group-title">
                <strong class="group-title-label">Make</strong>
                Make Name
            </button>
        </body>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    make = transform.parse_make(soup)
    assert make == "Make Name"

def test_parse_make_valid_group_link():
    html_content = """
    <html>
        <body>
            <a class="group-link" href="https://bringatrailer.com/porsche/">
                <strong class="group-title-label">Make</strong>
                Porsche
            </a>
        </body>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    make = transform.parse_make(soup)
    assert make == "Porsche"

def test_parse_make_returns_first_matching_value():
    html_content = """
    <html>
        <body>
            <a class="group-link" href="/porsche/">
                <strong class="group-title-label">Make</strong>
                Porsche
            </a>
            <a class="group-link" href="/volkswagen/">
                <strong class="group-title-label">Make</strong>
                Volkswagen
            </a>
        </body>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")

    assert transform.parse_make(soup) == "Porsche"

def test_parse_make_not_found():
    html_content = """
    <html>
        <body>
            <button class="group-title">
                <strong class="group-title-label">Model</strong>
                Model Name
            </button>
        </body>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    with pytest.raises(ValueError, match="Could not find 'Make' group"):
        transform.parse_make(soup)

def test_parse_make_empty_group_raises():
    html_content = """
    <html>
        <body>
            <button class="group-title">
                <strong class="group-title-label">Make</strong>
            </button>
        </body>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    with pytest.raises(ValueError, match="Could not find 'Make' group"):
        transform.parse_make(soup)


def test_extract_group_value_reads_all_category_values_from_group_links():
    soup = BeautifulSoup(
        """
        <html>
            <body>
                <a class="group-link" href="/truck-4x4/">
                    <strong class="group-title-label">Category</strong>
                    Truck & 4x4
                </a>
                <a class="group-link" href="/convertible/">
                    <strong class="group-title-label">Category</strong>
                    Convertibles
                </a>
            </body>
        </html>
        """,
        "html.parser",
    )

    assert transform.extract_group_value(soup, "Category") == [
        "Truck & 4x4",
        "Convertibles",
    ]

def test_extract_group_value_returns_none_for_missing_group():
    soup = BeautifulSoup("<html><body></body></html>", "html.parser")

    assert transform.extract_group_value(soup, "Category") is None

def test_extract_group_value_returns_none_for_empty_group_value():
    soup = BeautifulSoup(
        """
        <html>
            <body>
                <button class="group-title">
                    <strong class="group-title-label">Category</strong>
                </button>
            </body>
        </html>
        """,
        "html.parser",
    )

    assert transform.extract_group_value(soup, "Category") is None


def test_get_listing_details_valid():
    html_content = """
    <html>
        <body>
            <div class="item">
                <strong>Listing Details</strong>
                <ul>
                    <li>Chassis: <a href="test-url">WBSBL93414PN57203</a></li>
                    <li>100k Miles</li>
                    <li>3.2-Liter S54 Inline-Six</li>
                </ul>
            </div>
        </body>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    details = transform.get_listing_details(soup)
    assert details == [
        "Chassis: WBSBL93414PN57203",
        "100k Miles",
        "3.2-Liter S54 Inline-Six",
    ]

def test_get_listing_details_not_found():
    html_content = """
    <html>
        <body>
            <div class="item">
                <strong>Other Details</strong>
                <ul>
                    <li>100k Miles</li>
                </ul>
            </div>
        </body>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    with pytest.raises(ValueError, match="Could not parse listing details"):
        transform.get_listing_details(soup)

def test_get_listing_details_no_list():
    html_content = """
    <html>
        <body>
            <div class="item">
                <strong>Listing Details</strong>
            </div>
        </body>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    with pytest.raises(ValueError, match="Could not parse listing details"):
        transform.get_listing_details(soup)

def test_parse_mileage_valid():
    assert transform.parse_mileage("100k Miles") == 100000
    assert transform.parse_mileage("50,000 Miles") == 50000
    assert transform.parse_mileage("100 miles") == 100

def test_parse_mileage_TMU():
    assert transform.parse_mileage("TMU") == None
    assert transform.parse_mileage("Mileage Unknown") == None
    assert transform.parse_mileage("miles shown") == None

def test_parse_mileage_invalid():
    with pytest.raises(ValueError, match="Could not parse mileage"):
        transform.parse_mileage("")

def test_find_detail_value_valid_miles_with_k():
    values = [
        "Chassis: WBSBL93414PN57203",
        "100k Miles",
        "3.2-Liter S54 Inline-Six",
    ]
    assert transform.find_detail_value(values, r"\bmiles?\b|\btmu\b|\bunknown\b", "Mileage") == "100k Miles"

def test_find_detail_value_valid_miles_without_k():
    values = [
        "Chassis: WBSBL93414PN57203",
        "2,500 Miles",
        "3.2-Liter S54 Inline-Six",
    ]
    assert transform.find_detail_value(values, r"\bmiles?\b|\btmu\b|\bunknown\b", "Mileage") == "2,500 Miles"

def test_find_detail_value_valid_miles_and_tmu():
    values = [
        "Chassis: WBSBL93414PN57203",
        "2,500 Miles, TMU",
        "3.2-Liter S54 Inline-Six",
    ]
    assert transform.find_detail_value(values, r"\bmiles?\b|\btmu\b|\bunknown\b", "Mileage") == "2,500 Miles, TMU"

def test_find_detail_value_valid_tmu_only():
    values = [
        "Chassis: WBSBL93414PN57203",
        "TMU",
        "3.2-Liter S54 Inline-Six",
    ]
    assert transform.find_detail_value(values, r"\bmiles?\b|\btmu\b|\bunknown\b", "Mileage") == "TMU"

def test_find_detail_value_not_found():
    values = [
        "Chassis: WBSBL93414PN57203",
        "3.2-Liter S54 Inline-Six",
    ]
    with pytest.raises(ValueError, match="Could not parse Mileage"):
        transform.find_detail_value(values, r"\bmiles?\b|\btmu\b|\bunknown\b", "Mileage")

def test_find_detail_value_empty_values():
    values = []
    with pytest.raises(ValueError, match="Could not parse Mileage"):
        transform.find_detail_value(values, r"\bmiles?\b|\btmu\b|\bunknown\b", "Mileage")

def test_find_detail_value_valid_transmission():
    values = [
        "Chassis: WBSBL93414PN57203",
        "Four speed manual transmission",
        "3.2-Liter S54 Inline-Six",
    ]
    assert transform.find_detail_value(values, transform.TRANSMISSION_DETAIL_PATTERN, "Transmission") == "Four speed manual transmission"

def test_find_detail_value_valid_tranaxle():
    values = [
        "Chassis: WBSBL93414PN57203",
        "6-Speed manual transaxle",
        "3.2-Liter S54 Inline-Six",
    ]
    assert transform.find_detail_value(values, transform.TRANSMISSION_DETAIL_PATTERN, "Transmission") == "6-Speed manual transaxle"

def test_find_detail_value_valid_gearbox():
    values = [
        "Chassis: WBSBL93414PN57203",
        "Automatic gearbox",
        "3.2-Liter S54 Inline-Six",
    ]
    assert transform.find_detail_value(values, transform.TRANSMISSION_DETAIL_PATTERN, "Transmission") == "Automatic gearbox"

def test_find_detail_value_valid_column_shifted_manual_without_transmission_keyword():
    values = [
        "Chassis: CE145Z164835",
        "Column-Shifted Three-Speed Manual",
        "235ci Inline-Six",
    ]
    assert transform.find_detail_value(values, transform.TRANSMISSION_DETAIL_PATTERN, "Transmission") == "Column-Shifted Three-Speed Manual"

def test_find_detail_value_valid_floor_shift_manual_without_transmission_keyword():
    values = [
        "Chassis: WBSBL93414PN57203",
        "Floor-Shift Four-Speed Manual",
        "3.2-Liter S54 Inline-Six",
    ]
    assert transform.find_detail_value(values, transform.TRANSMISSION_DETAIL_PATTERN, "Transmission") == "Floor-Shift Four-Speed Manual"

def test_find_detail_value_transmission_not_found():
    values = [
        "Chassis: WBSBL93414PN57203",
        "3.2-Liter S54 Inline-Six",
    ]
    with pytest.raises(ValueError, match="Could not parse Transmission"):
        transform.find_detail_value(values, transform.TRANSMISSION_DETAIL_PATTERN, "Transmission")

def test_find_detail_value_transmission_ignores_unrelated_manual_details():
    values = [
        "Chassis: CE145Z164835",
        "Manual Steering",
        "Manual Brakes",
        "235ci Inline-Six",
    ]
    with pytest.raises(ValueError, match="Could not parse Transmission"):
        transform.find_detail_value(values, transform.TRANSMISSION_DETAIL_PATTERN, "Transmission")

def test_extract_vin_valid():
    raw_vin = "Chassis: WBSBL93414PN57203"
    assert transform.extract_vin(raw_vin) == "WBSBL93414PN57203"

def test_extract_vin_invalid():
    raw_vin = "Chassis: INVALID VIN"
    with pytest.raises(ValueError, match="Could not parse VIN"):
        transform.extract_vin(raw_vin)

def test_extract_vin_no_chassis_prefix():
    raw_vin = "WBSBL93414PN57203"
    with pytest.raises(ValueError, match="Could not parse VIN"):
        transform.extract_vin(raw_vin)

def test_find_detail_value_VIN_valid():
    values = [
        "Chassis: WBSBL93414PN57203",
        "3.2-Liter S54 Inline-Six",
    ]
    assert transform.find_detail_value(values, r"^Chassis:", "VIN") == "Chassis: WBSBL93414PN57203"

def test_find_detail_value_VIN_invalid():
    values = [
        "2,500 Miles",
        "3.2-Liter S54 Inline-Six",
    ]
    with pytest.raises(ValueError, match="Could not parse VIN"):
        transform.find_detail_value(values, r"^Chassis:", "VIN")

def test_extact_bid_price_valid_product_data():
    html_content = """
    <html>
        <body></body>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    product_data = {
        "@context": "http://schema.org",
        "@type": "Product",
        "offers":{"@type":"Offer","priceCurrency":"USD","price":19750}
    }
    price = transform.extract_sale_price(soup, product_data)
    assert price == 19750

def test_extract_bid_price_valid_bid_label():
    html_content = """
    <html>
        <body>
            <table>
                <tr>
                    <td>Winning Bid</td>
                    <td>$19,750</td>
                </tr>
            </table>
        </body>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    product_data = {}
    price = transform.extract_sale_price(soup, product_data)
    assert price == 19750

def test_extract_bid_price_no_price_info():
    html_content = """
    <html>
        <body>
            <table>
                <tr>
                    <td>Winning Bid</td>
                    <td>Price Not Available</td>
                </tr>
            </table>
        </body>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    product_data = {}
    with pytest.raises(ValueError, match="Could not parse sale price"):
        transform.extract_sale_price(soup, product_data)

def test_extract_sold_status_returns_true_for_sold_for():
    html_content = """
    <html>
        <body>
            <div class="listing-available">
                    <div class="listing-available-info">
                        <span class="info-value noborder-tiny">Sold for <strong>USD $19,750</strong></span>
                </div>
            </div>
        </body>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    assert transform.extract_sold_status(soup) == True

def test_extract_sold_status_returns_false_for_bid_to():
    html_content = """
    <html>
        <body>
            <div class="listing-available">
                    <div class="listing-available-info">
                        <span class="info-value noborder-tiny">Bid to <strong>USD $19,750</strong></span>
                </div>
            </div>
        </body>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    assert transform.extract_sold_status(soup) == False


def test_extract_sold_status_rejects_withdrawn_on():
    html_content = """
    <html>
        <body>
            <div class="listing-available">
                    <div class="listing-available-info">
                        <span class="info-value noborder-tiny">Withdrawn on 4/29/26</span>
                </div>
            </div>
        </body>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    with pytest.raises(ValueError, match="Could not parse sold status"):
        transform.extract_sold_status(soup)


def test_extract_sold_status_no_available_info():
    html_content = """
    <html>
        <body>
            <div class="listing-available">
            </div>
        </body>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    with pytest.raises(ValueError, match="Could not parse sold status"):
        transform.extract_sold_status(soup)


def test_extract_auction_end_text_valid():
    html_content = """
    <html>
        <body>
            <div class="listing-available">
                <span class="date date-localize" data-timestamp="1774898451"</span>
            </div>
        </body>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    end_text = transform.extract_auction_end_date(soup)
    assert end_text == "2026-03-30"

def test_extract_auction_end_text_no_date_tag():
    html_content = """
    <html>
        <body>
            <div class="listing-available">
            </div>
        </body>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    with pytest.raises(ValueError, match="Could not find sale date"):
        transform.extract_auction_end_date(soup)

def test_extract_auction_end_text_no_timestamp():
    html_content = """
    <html>
        <body>
            <div class="listing-available">
                <span class="date date-localize"></span>
            </div>
        </body>
    </html>
    """
    soup = BeautifulSoup(html_content, "html.parser")
    with pytest.raises(ValueError, match="Sale date missing data-timestamp"):
        transform.extract_auction_end_date(soup)

def test_normalize_transmission_manual():
    assert transform.normalize_transmission("Four speed manual transmission") == "manual"
    assert transform.normalize_transmission("6-Speed manual transaxle") == "manual"

def test_normalize_transmission_automatic():
    assert transform.normalize_transmission("six speed gearbox") == "automatic"
    assert transform.normalize_transmission("5-Speed automatic transmission") == "automatic"  

def test_normalize_transmission_empty():
    with pytest.raises(ValueError, match="Could not normalize transmission"):
        transform.normalize_transmission("")
