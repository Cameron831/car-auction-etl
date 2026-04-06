import pytest
import json
from app.sources.bat.transform import *

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
    