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
- [ ] Basic CLI
- [ ] Structured error handling
- [x] Postgres/Docker for Postgres
- [ ] Store data into Postgres

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
