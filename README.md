# Car Auction ETL

Car Auction ETL is a Python CLI that scrapes completed vehicle auctions, stores raw source artifacts, transforms listing data, and loads normalized records into Postgres.

Status: end-to-end MVP complete.

## What It Proves

- Multiple auction sources: Bring a Trailer (`bat`) and Cars & Bids (`cab`).
- Live discovery of completed auction listings.
- Live single-listing ingestion from source pages and APIs.
- Raw source storage in Postgres (`raw_listing_html` and `raw_listing_json`).
- Source-specific transforms into normalized vehicle and sale fields.
- Postgres loading with uniqueness constraints by source and listing ID.
- Automated unit and integration tests.

Live scraping depends on source-site availability and page structure.

## End-to-End Flow

1. Discover completed auction listings.
2. Ingest raw source artifacts.
3. Transform source data into normalized fields.
4. Load normalized records into Postgres.

## CLI

Install dependencies before using the console script. Preferred top-level format:

```text
auction-etl bat ...
auction-etl cab ...
```

Single-listing examples:

```text
auction-etl bat run --listing-id 2016-porsche-boxster-spyder-55
auction-etl cab run --listing-id rGJlwggO
```

Discovery and batch examples:

```text
auction-etl bat discover --max-candidates 5
auction-etl bat ingest-discovered --batch-size 5
auction-etl bat transform-discovered --batch-size 5

auction-etl cab discover --max-candidates 5
auction-etl cab ingest-discovered --batch-size 5
auction-etl cab transform-discovered --batch-size 5
```

Expected summary output examples:

```text
Discovery summary: inspected=5 new=4 existing_or_updated=1 failed=0
Ingest summary: listing_id=2016-porsche-boxster-spyder-55 accepted=true raw_stored=true
Transform summary: listing_id=2016-porsche-boxster-spyder-55 transformed=true loaded=true
Run summary: listing_id=rGJlwggO accepted=true raw_stored=true transformed=true loaded=true
Ingest-discovered summary: selected=5 scrape_attempted=5 scrape_failed=0 rejected=0 raw_html_stored=5 accepted=5
Ingest-discovered summary: selected=5 scrape_attempted=5 scrape_failed=0 rejected=0 raw_json_stored=5 accepted=5
Transform-discovered summary: selected=5 transformed_and_loaded=5 transform_failed=0 load_failed=0
```

## Windows PowerShell Setup

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
.venv\Scripts\python.exe -m pip install -r requirements.txt
docker compose up -d postgres
$env:DATABASE_URL = "postgresql://auction_user:localdevpassword@127.0.0.1:5433/auction_etl"
```

Run live examples:

```powershell
auction-etl bat run --listing-id 2016-porsche-boxster-spyder-55
auction-etl cab run --listing-id rGJlwggO
auction-etl bat discover --max-candidates 5
auction-etl cab discover --max-candidates 5
```

Run tests:

```powershell
.venv\Scripts\python.exe -m pytest -q
```

## macOS/Linux Setup

```sh
python3.11 -m venv .venv
. .venv/bin/activate
.venv/bin/python -m pip install -r requirements.txt
docker compose up -d postgres
export DATABASE_URL="postgresql://auction_user:localdevpassword@127.0.0.1:5433/auction_etl"
```

Run live examples:

```sh
auction-etl bat run --listing-id 2016-porsche-boxster-spyder-55
auction-etl cab run --listing-id rGJlwggO
auction-etl bat discover --max-candidates 5
auction-etl cab discover --max-candidates 5
```

Run tests:

```sh
.venv/bin/python -m pytest -q
```

## Data Model

Core normalized listing fields:

- `source_site`
- `source_listing_id`
- `url`
- `make`
- `model_raw`
- `model_normalized`
- `year`
- `mileage`
- `vin`
- `sale_price`
- `sold`
- `auction_end_date`
- `transmission`
- `listing_details_raw`

Supporting tables:

- `discovered_listings`: source listing IDs, URLs, eligibility, discovery timestamps, and ingestion status.
- `raw_listing_html`: raw Bring a Trailer HTML artifacts.
- `raw_listing_json`: raw Cars & Bids JSON artifacts.

## Screenshot Placeholders

![Live BAT and Cars & Bids CLI output](docs/screenshots/live-cli-output.png)

Caption: proof that live single-listing commands run for both supported sources and print end-to-end summary lines.

![Database proof for normalized records](docs/screenshots/postgres-normalized-records.png)

Caption: proof that normalized rows from Bring a Trailer and Cars & Bids are loaded into Postgres.

![Passing test output](docs/screenshots/passing-tests.png)

Caption: optional proof that the automated test suite passes locally.

## Future Work

- Add durable screenshots for the proof points above.
- Expand normalized fields where source data is reliable.
- Add operational hardening around source-site changes and retry behavior.
