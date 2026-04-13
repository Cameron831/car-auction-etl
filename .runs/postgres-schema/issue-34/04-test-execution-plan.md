# Test Execution Plan: Issue #34

## Workflow

Issue to PR

## Stage

Test planning

## Input Artifacts

- `.runs/postgres-schema/issue-34/01-execution-plan.md`
- `.runs/postgres-schema/issue-34/02-change-summary.md`
- `.runs/postgres-schema/issue-34/03-change-diff.patch`

## Output Artifact

- `.runs/postgres-schema/issue-34/04-test-execution-plan.md`

## Goal

Validate the approved `sql/schema.sql` change by review-only checks that the canonical `listings` table matches Issue #34, without adding schema verification tests in this issue.

## Recommendation

Do not add automated tests in this issue.

Reason: the approved execution plan explicitly kept schema verification tests out of scope, and issue #36 separately covers schema verification checks. For issue #34, validation should be limited to reviewing the DDL and confirming it matches the accepted schema shape.

## Scope

In scope:

- Manual validation of the final DDL against the issue acceptance criteria.
- Review of column set, constraints, indexes, and uniqueness in `sql/schema.sql`.

Out of scope:

- Automated schema verification tests.
- Insert/upsert/app persistence tests.
- Docker initialization wiring tests.
- Behavior beyond Issue #34.

## Verification Steps

1. Review `sql/schema.sql` directly.
2. Verify the `listings` columns include `source_site`, `source_listing_id`, `url`, `make`, `model`, `year`, `mileage`, `vin`, `sale_price`, `sold`, `auction_end_date`, `transmission`, `listing_details_raw`, `created_at`, and `updated_at`.
3. Confirm `auction_end_date` is `DATE NOT NULL`.
4. Confirm `sale_price` is `INTEGER NOT NULL` with a non-negative check.
5. Confirm `listing_details_raw` is `JSONB`.
6. Confirm uniqueness remains on `(source_site, source_listing_id)`.
7. Confirm indexes exist for `auction_end_date` and non-null `vin`.

## Commands

```powershell
Get-Content sql/schema.sql
Select-String -Path sql/schema.sql -Pattern "CREATE TABLE IF NOT EXISTS listings", "UNIQUE", "auction_end_date", "sale_price", "listing_details_raw", "CREATE INDEX"
```

`git diff -- sql/schema.sql` would normally be useful, but git repository detection has been unreliable in this sandbox due to the local safe-directory/ownership state. Use the patch artifact at `.runs/postgres-schema/issue-34/03-change-diff.patch` as the canonical diff if git remains unavailable.

## Risks

- Review-only validation does not execute PostgreSQL-specific enforcement.
- Adding automated schema checks here would duplicate issue #36 and expand the approved scope.

## Files Expected To Change

- None.

## Stop Point

Wait for user approval before running verification commands or creating the verification log.
