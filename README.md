# Car Auction ETL
A comprehensive project that ingests completed vehicle auction listings, stores raw source artifacts, and transforms key vehicle and sale data into normalized Postgres tables for analysis.

## MVP 
- recently completed bringatrailer auctions scraped daily
- CLI layer for orchestration/entry
- data normalized and stored in Postgres
- Robust CI/CD pipeline
- Docker containerization

## Phase 1
One hardcoded completed Bring a Trailer listing
- fetch
- save raw HTML locally
- parse fields
- insert into Postgres

Implementation Steps:
- [x] Pull raw html from a URL
- [x] Store raw html in local folder
- [x] Parse html for data outlined in schema
- [x] Basic CLI
- [ ] Structured error handling
- [x] Postgres/Docker for Postgres
- [ ] Store data into Postgres

## CLI
Run the current Phase 1 Bring a Trailer ETL pipeline from the repository root:

```powershell
python -m app.cli <bat-listing-id>
```

The command fetches the listing HTML, saves the raw HTML record, transforms it,
and loads the transformed listing into Postgres. `DATABASE_URL` must be set for
the save, transform, and load steps.

## Data Model
- source_site: string
- listing_id: string
- url: string
- auction_end_date: datetime
- make: string
- model: string
- year: int
- mileage: int
- vin: string
- sale_price: int
- sold: boolean
- transmission: string

Implement Later:
- seller_type: string
- interior_color: string
- drivetrain: string
- engine: string
- body_style: string
- exterior_color: string
- title_status: string

## Tools
- Docker
    - App Container
    - Postgres DB
- Postgres
- AWS
- Github Actions
- Redis
