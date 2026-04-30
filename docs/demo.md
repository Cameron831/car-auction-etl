# CLI Demo

Run these examples after completing the README quick start and setting `DATABASE_URL`.

## Single Listing

```sh
auction-etl bat run --listing-id 2016-porsche-boxster-spyder-55
auction-etl cab run --listing-id rGJlwggO
```

Expected summary output:

```text
Run summary: listing_id=2016-porsche-boxster-spyder-55 accepted=true raw_stored=true transformed=true loaded=true
Run summary: listing_id=rGJlwggO accepted=true raw_stored=true transformed=true loaded=true
```

## Discovery Batch

```sh
auction-etl bat discover --max-candidates 5
auction-etl bat ingest-discovered --batch-size 5
auction-etl bat transform-discovered --batch-size 5

auction-etl cab discover --max-candidates 5
auction-etl cab ingest-discovered --batch-size 5
auction-etl cab transform-discovered --batch-size 5
```

Expected summary output:

```text
Discovery summary: inspected=5 new=4 existing_or_updated=1 failed=0
Ingest-discovered summary: selected=5 scrape_attempted=5 scrape_failed=0 rejected=0 raw_html_stored=5 accepted=5
Ingest-discovered summary: selected=5 scrape_attempted=5 scrape_failed=0 rejected=0 raw_json_stored=5 accepted=5
Transform-discovered summary: selected=5 transformed_and_loaded=5 transform_failed=0 load_failed=0
```

Live counts can vary when source sites change, listings become unavailable, or records already exist locally.
