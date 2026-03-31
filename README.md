# Car Auction ETL
A comprehensive project that ingests completed vehicle auction listings, stores raw source artifacts, and transforms key vehicle and sale data into normalized Postgres tables for analysis.

## MVP 
recently completed cars and bids auctions scraped daily
CLI layer for orchestration/entry
data normalized and stored in Postgres
Robust CI/CD pipeline
Docker containerization

## Phase 1
One hardcoded completed Cars & Bids listing
- fetch
- save raw HTML locally
- parse fields
- insert into Postgres

<br>
Implementation Steps:
- [ ] Pull raw html from a URL
- [ ] Store raw html in local folder
- [ ] Parse html for data outlined in schema
- [ ] Basic CLI
- [ ] Postgres/Docker for Postgres
- [ ] Store data into Postgres

## Data Model
- source_site: string
- listing_id: string
- url: string
- auction_end_at: datetime
- make: string
- model: string
- year: int
- mileage: int
- VIN: string
- title_status: string
- sale_price: int
- sold: boolean
- transmission: string

<br>
Implement Later:
- seller_type: string
- interior_color: string
- drivetrain: string
- engine: string
- body_style: string
- exterior_color: string

## Tools
- Docker
    - App Container
    - Postgres DB
- Postgres
- AWS
- Github Actions
- Redis
