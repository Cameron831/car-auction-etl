# Execution Plan: Issue #34

## Workflow

Issue to PR

## Stage

Plan issue

## Input Artifact

- `.runs/postgres-schema/issue-34/00-issue.md`

## Output Artifact

- `.runs/postgres-schema/issue-34/01-execution-plan.md`

## Goal

Update `sql/schema.sql` so the canonical `listings` table can store the MVP Bring a Trailer transformed listing output, while preserving source uniqueness and keeping raw HTML artifact storage out of the schema.

## Scope

In scope:

- Update the existing `listings` table definition in `sql/schema.sql`.
- Add the MVP listing columns: `url`, `make`, `model`, `year`, `mileage`, `vin`, `sale_price`, `sold`, `auction_end_date`, `transmission`, and `listing_details_raw`.
- Keep `source_site` and `source_listing_id` uniqueness enforced.
- Add conservative constraints for `year`, `mileage`, `sale_price`, and `transmission`.
- Add indexes for `auction_end_date` and `vin` if useful.

Out of scope:

- Insert/upsert/application persistence changes.
- Docker or bootstrap wiring.
- Schema verification tests.
- Normalizing into separate source, vehicle, auction, or sale tables.
- Storing raw HTML artifacts in Postgres.

## Assumptions

- The current `listings` table can be evolved in place without a migration strategy as part of this issue.
- Existing uniqueness on `(source_site, source_listing_id)` remains the canonical source identity rule.
- The schema should match the current transformed output rather than over-normalizing for future sources.

## Implementation Steps

1. Edit `sql/schema.sql` to replace the minimal listings shape with the approved MVP canonical columns while keeping `id`, timestamps, and source uniqueness.
2. Replace the current `raw_url` and `title` shape with the accepted schema fields, including `url` and the transformed listing attributes.
3. Add low-risk constraints:
   - `year` sanity check.
   - `mileage >= 0` when present.
   - `sale_price >= 0` and `NOT NULL`.
   - `transmission` constrained to current normalized values when present, expected to be `manual` or `automatic`.
4. Add indexes on `auction_end_date` and `vin` if they are not already covered.
5. Review the final DDL against the issue acceptance criteria without adding tests, persistence code, or Docker initialization wiring.

## Files Expected To Change

- `sql/schema.sql`

## Risks

- Constraint definitions could be too strict if they do not match current transformed listing values.
- Because this is not a migration issue, any existing local database volume may require later initialization/reset handling in issue #35.

## Verification Notes

- Review `sql/schema.sql` to confirm the final column list matches the issue acceptance criteria.
- Confirm the uniqueness rule remains on `source_site` and `source_listing_id`.
- Confirm `auction_end_date` is `DATE`, `sale_price` is `NOT NULL`, and `listing_details_raw` is `JSONB`.
- Do not add schema tests or persistence code as part of this issue.

## Stop Point

Wait for user approval before starting implementation or editing code.
