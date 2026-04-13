# Issue #34: Define MVP listings table schema

URL: https://github.com/Cameron831/car-auction-etl/issues/34

## Type

feature

## Priority

high

## Depends On

None

## Labels

- database
- schema

## Goal

Update the Postgres schema so the listings table can store the current transformed Bring a Trailer listing output.

## Scope

- Update `sql/schema.sql` with the MVP listings table columns.
- Preserve uniqueness on `source_site` and `source_listing_id`.
- Add low-risk constraints for `year`, `mileage`, `sale_price`, and `transmission`.
- Add indexes for `auction_end_date` and `vin` where useful.
- Keep raw HTML artifact storage out of the database schema.

## Acceptance Criteria

- `listings` includes `source_site`, `source_listing_id`, `url`, `make`, `model`, `year`, `mileage`, `vin`, `sale_price`, `sold`, `auction_end_date`, `transmission`, `listing_details_raw`, `created_at`, and `updated_at`.
- `auction_end_date` is `DATE`.
- `sale_price` is `NOT NULL` and represents the displayed amount, including bid-to amount for unsold listings.
- `listing_details_raw` is `JSONB`.
- Source uniqueness is enforced by `source_site` and `source_listing_id`.

## Notes

Use one canonical `listings` table for the MVP rather than normalizing into source, vehicle, auction, and sale tables.
