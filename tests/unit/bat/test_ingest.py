import pytest
import requests
from app.sources.bat.ingest import fetch_listing_html, save_listing_html

# test that the function returns the correct HTML and that the request is made with the correct URL
def test_fetch_listing_html(mocker):
    # create mock request
    mock_get = mocker.patch('app.sources.bat.ingest.requests.get')
    # set return value for mock request
    mock_get.return_value.text = "<html>Test</html>"
    mock_get.return_value.raise_for_status.return_value = None

    # call the function being tested
    response = fetch_listing_html("test-id")

    # assert that the response is correct and that the mock request was called with the correct URL
    assert response == "<html>Test</html>"
    mock_get.assert_called_once_with("https://bringatrailer.com/listing/test-id", timeout=10)

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


def test_save_listing_html_writes_file_and_returns_path(tmp_path, mocker):
    # redirect raw HTML storage into pytest's temporary directory
    mocker.patch("app.sources.bat.ingest.RAW_HTML_DIR", tmp_path / "data" / "raw" / "bat")

    # save test HTML and capture the returned file path
    saved_path = save_listing_html("test-id", "<html>Test</html>")

    # assert that the file was written to the expected location with the expected contents
    assert saved_path == tmp_path / "data" / "raw" / "bat" / "test-id.html"
    assert saved_path.exists()
    assert saved_path.read_text(encoding="utf-8") == "<html>Test</html>"


def test_save_listing_html_overwrites_existing_file(tmp_path, mocker):
    # redirect raw HTML storage into pytest's temporary directory
    mocker.patch("app.sources.bat.ingest.RAW_HTML_DIR", tmp_path / "data" / "raw" / "bat")

    # save the same listing twice to verify that the second write overwrites the first
    first_path = save_listing_html("test-id", "<html>First</html>")
    second_path = save_listing_html("test-id", "<html>Second</html>")

    # assert that both saves point to the same file and that the latest contents were written
    assert first_path == second_path
    assert second_path.read_text(encoding="utf-8") == "<html>Second</html>"
