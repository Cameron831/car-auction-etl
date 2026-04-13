# Change Summary: Issue #34

Updated `sql/schema.sql` to define the MVP canonical `listings` table for transformed Bring a Trailer listings.

Changes:
- Replaced the minimal `raw_url` and `title` shape with `url`, `make`, `model`, `year`, `mileage`, `vin`, `sale_price`, `sold`, `auction_end_date`, `transmission`, and `listing_details_raw`.
- Kept `id`, `source_site`, `source_listing_id`, `created_at`, `updated_at`, and the unique constraint on `(source_site, source_listing_id)`.
- Set `auction_end_date` to `DATE NOT NULL`, `sale_price` to `INTEGER NOT NULL`, and `listing_details_raw` to `JSONB`.
- Added conservative checks for `year`, `mileage`, `sale_price`, and `transmission`.
- Added indexes for `auction_end_date` and non-null `vin` values.

Verification:
- Reviewed `sql/schema.sql` against the approved acceptance criteria.
- Did not add tests, persistence code, Docker wiring, or raw HTML artifact storage.
