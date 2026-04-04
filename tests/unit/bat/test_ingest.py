import pytest
import requests
from app.sources.bat.ingest import fetch_listing_html

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
    mock_get = mocker.patch("app.sources.bat.ingest.requests.get")
    mock_get.side_effect = requests.ConnectionError("Connection failed")

    with pytest.raises(requests.ConnectionError, match="Connection failed"):
        fetch_listing_html("test-id")