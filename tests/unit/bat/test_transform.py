import json
from bs4 import BeautifulSoup
import pytest

from app.sources.bat.transform import (
    extract_auction_end_date,
    extract_group_value,
    extract_listing_title,
    extract_sale_price,
    extract_sold_status,
    extract_vin,
    find_detail_value,
    get_listing_details,
    get_product_json_ld,
    load_listing_html,
    normalize_transmission,
    parse_make,
    parse_mileage,
    parse_model,
    parse_year,
    store_transformed_data,
    transform_listing_html,
)

def test_load_listing_html(tmp_path, mocker):
    # redirect raw HTML storage into pytest's temporary directory
    mocker.patch("app.sources.bat.transform.RAW_HTML_DIR", tmp_path / "data" / "raw" / "bat")

    # create a test HTML file to load
    test_file = tmp_path / "data" / "raw" / "bat" / "test-id.html"
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.write_text("<html>Test</html>", encoding="utf-8")

    # call the function being tested and assert that the loaded HTML is correct
    html = load_listing_html("test-id")
    assert html == "<html>Test</html>"

def test_load_listing_html_file_not_found(tmp_path, mocker):
    # redirect raw HTML storage into pytest's temporary directory
    mocker.patch("app.sources.bat.transform.RAW_HTML_DIR", tmp_path / "data" / "raw" / "bat")

    # assert that a FileNotFoundError is raised when the file does not exist
    with pytest.raises(FileNotFoundError, match="Raw HTML file not found for listing ID: missing-id"):
        load_listing_html("missing-id")

def test_store_transformed_data_writes_file(tmp_path, mocker):
    # redirect transformed data storage into pytest's temporary directory
    mocker.patch("app.sources.bat.transform.TRANSFORMED_HTML_DIR", tmp_path / "data" / "transformed" / "bat")

    # create some test transformed data to store
    test_data = {"key": "value"}

    # call the function being tested and assert that the file was written with the correct contents
    saved_path = store_transformed_data("test-id", test_data)
    assert saved_path == tmp_path / "data" / "transformed" / "bat" / "test-id.json"
    assert saved_path.exists()
    assert saved_path.read_text(encoding="utf-8") == json.dumps(test_data, default=str, indent=2)

def test_store_transformed_data_overwrites_existing_file(tmp_path, mocker):
    # redirect transformed data storage into pytest's temporary directory
    mocker.patch("app.sources.bat.transform.TRANSFORMED_HTML_DIR", tmp_path / "data" / "transformed" / "bat")

    # create some test transformed data to store
    test_data_1 = {"key": "value1"}
    test_data_2 = {"key": "value2"}

    # store the first set of data and assert that it was written correctly
    saved_path_1 = store_transformed_data("test-id", test_data_1)
    assert saved_path_1 == tmp_path / "data" / "transformed" / "bat" / "test-id.json"
    assert saved_path_1.exists()
    assert saved_path_1.read_text(encoding="utf-8") == json.dumps(test_data_1, default=str, indent=2)
    # store the second set of data and assert that it overwrote the first
    saved_path_2 = store_transformed_data("test-id", test_data_2)
    assert saved_path_2 == tmp_path / "data" / "transformed" / "bat" / "test-id.json"
    assert saved_path_2.exists()
    assert saved_path_2.read_text(encoding="utf-8") == json.dumps(test_data_2, default=str, indent=2)
    
def test_store_transformed_data_creates_parent_directory(tmp_path, mocker):
    # create a nested target directory path that does not exist
    target_dir = tmp_path / "nested" / "transformed" / "bat"
    # redirect transformed data storage into pytest's temporary directory
    mocker.patch("app.sources.bat.transform.TRANSFORMED_HTML_DIR", target_dir)

    # assert that the target directory does not exist before calling the function
    assert not target_dir.exists()

    # create some test transformed data to store
    test_data = {"key": "value"}

    # call the function being tested and assert that the parent directory was created and the file was writte
    saved_path = store_transformed_data("test-id", test_data)
    assert target_dir.exists()
    assert target_dir.is_dir()
    assert saved_path == target_dir / "test-id.json"
    assert saved_path.exists()
    assert json.loads(saved_path.read_text(encoding="utf-8")) == {"key": "value"}

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
    product_data = get_product_json_ld(soup)
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
    product_data = get_product_json_ld(soup)
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
        get_product_json_ld(soup)
    
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
        get_product_json_ld(soup)

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
    product_data = get_product_json_ld(soup)
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
    title = extract_listing_title(soup, product_data)
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
    title = extract_listing_title(soup, product_data)
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
        extract_listing_title(soup, product_data)
    
def test_parse_year_valid_title():
    title = "2026 Make Model Title"
    year = parse_year(title)
    assert year == 2026

def test_parse_year_invalid_title():
    title = "Make Model Title"
    with pytest.raises(ValueError, match="Could not parse year from listing title"):
        parse_year(title)

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
    model = parse_model(soup)
    assert model == "Model Name"

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
    with pytest.raises(ValueError, match="Could not find 'Model' group"):
        parse_model(soup)

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
    make = parse_make(soup)
    assert make == "Make Name"

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
        parse_make(soup)
    
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
    details = get_listing_details(soup)
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
        get_listing_details(soup)

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
        get_listing_details(soup)

def test_parse_mileage_valid():
    assert parse_mileage("100k Miles") == 100000
    assert parse_mileage("50,000 Miles") == 50000
    assert parse_mileage("100 miles") == 100

def test_parse_mileage_TMU():
    assert parse_mileage("TMU") == None
    assert parse_mileage("Mileage Unknown") == None

def test_parse_mileage_invalid():
    with pytest.raises(ValueError, match="Could not parse mileage"):
        parse_mileage("")

def test_find_detail_value_valid_miles_with_k():
    values = [
        "Chassis: WBSBL93414PN57203",
        "100k Miles",
        "3.2-Liter S54 Inline-Six",
    ]
    assert find_detail_value(values, r"\bmiles?\b|\btmu\b|\bunknown\b", "Mileage") == "100k Miles"

def test_find_detail_value_valid_miles_without_k():
    values = [
        "Chassis: WBSBL93414PN57203",
        "2,500 Miles",
        "3.2-Liter S54 Inline-Six",
    ]
    assert find_detail_value(values, r"\bmiles?\b|\btmu\b|\bunknown\b", "Mileage") == "2,500 Miles"

def test_find_detail_value_valid_miles_and_tmu():
    values = [
        "Chassis: WBSBL93414PN57203",
        "2,500 Miles, TMU",
        "3.2-Liter S54 Inline-Six",
    ]
    assert find_detail_value(values, r"\bmiles?\b|\btmu\b|\bunknown\b", "Mileage") == "2,500 Miles, TMU"

def test_find_detail_value_valid_tmu_only():
    values = [
        "Chassis: WBSBL93414PN57203",
        "TMU",
        "3.2-Liter S54 Inline-Six",
    ]
    assert find_detail_value(values, r"\bmiles?\b|\btmu\b|\bunknown\b", "Mileage") == "TMU"

def test_find_detail_value_not_found():
    values = [
        "Chassis: WBSBL93414PN57203",
        "3.2-Liter S54 Inline-Six",
    ]
    with pytest.raises(ValueError, match="Could not parse Mileage"):
        find_detail_value(values, r"\bmiles?\b|\btmu\b|\bunknown\b", "Mileage")

def test_find_detail_value_empty_values():
    values = []
    with pytest.raises(ValueError, match="Could not parse Mileage"):
        find_detail_value(values, r"\bmiles?\b|\btmu\b|\bunknown\b", "Mileage")

def test_find_detail_value_valid_transmission():
    values = [
        "Chassis: WBSBL93414PN57203",
        "Four speed manual transmission",
        "3.2-Liter S54 Inline-Six",
    ]
    assert find_detail_value(values, r"\b(?:Transmission|Transaxle|Gearbox)\b", "Transmission") == "Four speed manual transmission"

def test_find_detail_value_valid_tranaxle():
    values = [
        "Chassis: WBSBL93414PN57203",
        "6-Speed manual transaxle",
        "3.2-Liter S54 Inline-Six",
    ]
    assert find_detail_value(values, r"\b(?:Transmission|Transaxle|Gearbox)\b", "Transmission") == "6-Speed manual transaxle"

def test_find_detail_value_valid_gearbox():
    values = [
        "Chassis: WBSBL93414PN57203",
        "Automatic gearbox",
        "3.2-Liter S54 Inline-Six",
    ]
    assert find_detail_value(values, r"\b(?:Transmission|Transaxle|Gearbox)\b", "Transmission") == "Automatic gearbox"

def test_find_detail_value_transmission_not_found():
    values = [
        "Chassis: WBSBL93414PN57203",
        "3.2-Liter S54 Inline-Six",
    ]
    with pytest.raises(ValueError, match="Could not parse Transmission"):
        find_detail_value(values, r"\b(?:Transmission|Transaxle|Gearbox)\b", "Transmission")

def test_extract_vin_valid():
    raw_vin = "Chassis: WBSBL93414PN57203"
    assert extract_vin(raw_vin) == "WBSBL93414PN57203"

def test_extract_vin_invalid():
    raw_vin = "Chassis: INVALID VIN"
    with pytest.raises(ValueError, match="Could not parse VIN"):
        extract_vin(raw_vin)

def test_extract_vin_no_chassis_prefix():
    raw_vin = "WBSBL93414PN57203"
    with pytest.raises(ValueError, match="Could not parse VIN"):
        extract_vin(raw_vin)

def test_find_detail_value_VIN_valid():
    values = [
        "Chassis: WBSBL93414PN57203",
        "3.2-Liter S54 Inline-Six",
    ]
    assert find_detail_value(values, r"^Chassis:", "VIN") == "Chassis: WBSBL93414PN57203"

def test_find_detail_value_VIN_invalid():
    values = [
        "2,500 Miles",
        "3.2-Liter S54 Inline-Six",
    ]
    with pytest.raises(ValueError, match="Could not parse VIN"):
        find_detail_value(values, r"^Chassis:", "VIN")

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
    price = extract_sale_price(soup, product_data)
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
    price = extract_sale_price(soup, product_data)
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
        extract_sale_price(soup, product_data)

def test_extract_sold_status_valid():
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
    assert extract_sold_status(soup) == True

def test_extract_sold_status_not_sold():
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
    assert extract_sold_status(soup) == False

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
        extract_sold_status(soup)


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
    end_text = extract_auction_end_date(soup)
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
        extract_auction_end_date(soup)

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
        extract_auction_end_date(soup)

def test_normalize_transmission_manual():
    assert normalize_transmission("Four speed manual transmission") == "manual"
    assert normalize_transmission("6-Speed manual transaxle") == "manual"

def test_normalize_transmission_automatic():
    assert normalize_transmission("six speed gearbox") == "automatic"
    assert normalize_transmission("5-Speed automatic transmission") == "automatic"  

def test_normalize_transmission_empty():
    with pytest.raises(ValueError, match="Could not normalize transmission"):
        normalize_transmission("")
