# Final Change Summary: Issue #34

## Issue

#34: Define MVP listings table schema

## Files Changed

- `sql/schema.sql`
- `.runs/postgres-schema/issue-34/00-issue.md`
- `.runs/postgres-schema/issue-34/01-execution-plan.md`
- `.runs/postgres-schema/issue-34/02-change-summary.md`
- `.runs/postgres-schema/issue-34/03-change-diff.patch`
- `.runs/postgres-schema/issue-34/04-test-execution-plan.md`
- `.runs/postgres-schema/issue-34/05-test-diff.patch`
- `.runs/postgres-schema/issue-34/06-verification-log.md`
- `.runs/postgres-schema/issue-34/07-qa-review.md`
- `.runs/postgres-schema/issue-34/08-final-change-summary.md`

## Summary

- Updated `sql/schema.sql` to define the MVP canonical `listings` table for transformed Bring a Trailer listings.
- Replaced the minimal `raw_url` and `title` shape with `url`, `make`, `model`, `year`, `mileage`, `vin`, `sale_price`, `sold`, `auction_end_date`, `transmission`, and `listing_details_raw`.
- Preserved uniqueness on `(source_site, source_listing_id)`.
- Added low-risk constraints for `year`, `mileage`, `sale_price`, and `transmission`.
- Added indexes for `auction_end_date` and non-null `vin` values.
- Kept insert/upsert code, Docker initialization wiring, automated schema verification tests, and raw HTML database storage out of scope.

## Verification

- Review-only verification passed.
- No automated tests were added or run because schema verification tests are deferred to issue #36.
- QA review passed with no findings.

## Residual Risk

- PostgreSQL DDL execution has not been run in this issue. That risk is accepted for issue #34 and should be covered by the follow-up schema verification issue.

## Draft Commit Message

Define MVP listings schema

## Draft PR Title

Define MVP listings schema

## Draft PR Body

```markdown
## Summary

- define the MVP canonical `listings` table for transformed Bring a Trailer listings
- add vehicle, sale, auction date, transmission, and source detail columns
- preserve source listing uniqueness and add basic checks plus lookup indexes

## Verification

- Review-only verification passed against `sql/schema.sql`
- QA review passed with no findings
- Automated schema tests were not added or run because issue #36 covers schema verification checks

Closes #34
```

## Stop Point

Await explicit user approval before committing, pushing, or opening a pull request.
