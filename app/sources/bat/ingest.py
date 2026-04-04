import requests

def fetch_listing_html(id):
    url = f"https://bringatrailer.com/listing/{id}"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.text