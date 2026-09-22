# Silver research expansion — production verification

Verified at **2026-09-22 16:34 KST**. Status: **completed with one held disclosure**.

## Persisted data (direct RDS counts)

| Dataset | Stored records | Assets | Uncertified records |
|---|---:|---:|---:|
| FMP full financial records | 921,234 | 8,202 | 0 |
| FMP company/profile observations | 926,739 | 8,963 | 0 |
| DART structured corporate-event details | 9,412 | 2,185 | 0 |

These are provider-record/observation counts, **not API-call counts or numeric
account-line counts**. FMP numeric accounts are exposed by `fmp_statement_line`.

Existing DART Silver financial lines were reused without recopying history:
`dart_standardized_statement_line` now maps **27 metrics through 66 exact
account/family rules**, including historical `ifrs_` aliases. A direct Samsung
Electronics sample confirmed total-assets coverage from 2015-12-31 through
2026-06-30. This sample does not assert complete coverage for every company.

`company_profile_observation` exposes **3,762 existing DART observations**, as
well as the FMP observations above. It preserves observation-time availability.

## Input reconciliation and exclusions

- Initial inventory: 15,763 files.
- 32 files under `financials/fmp/latest/` were collection-trigger filing lists,
  not statement bodies. Initial inventory classified them incorrectly; the
  discovery code was corrected. No financial body was lost by excluding them.
- Effective input files: **15,731**.
- Processed or checkpoint-skipped effective files: **15,730**.
- Held disclosure: `corp=122450`, merger receipt **20260921000430**. Its matching
  official acceptance-list object was not present in Bronze. No receipt-prefix
  date was invented and no additional OpenDART call was made.
- Input rows excluded across replayed files: **12,933,964**, including rows
  outside the existing Silver asset universe or without adequate PIT metadata.
  This is a file-level source-row accounting sum, not unique missing companies
  or unique missing financial facts. **6,786** input rows had unresolved asset
  identity at the applicable date. Excluded rows remain in Bronze.
- No unexpected body-publication errors and no orphaned RUNNING research runs
  remained in the final direct DB audit.

The ECS task exited **2**, because its original outcome report contained the
32 misclassified list inputs and the one held disclosure. This is **not** a
clean exit-0 claim. The final verification reconciles those classifications;
all persisted research records have certified quality-run lineage.

## Execution and evidence

- Provider API calls: **0**. Replay task received only Bronze-bucket and Silver-DB
  configuration secrets, not provider API keys.
- No price, total-return, or Gold recalculation was run by this backfill.
- The optimized resumed run completed its finite inventory in **1,089 seconds**;
  this excludes setup, canaries and deployment time.
- Completed files were skipped by checksum/version checkpoints. Only unseen
  observation keys were copied into Silver. Out-of-universe FMP records were
  filtered before expensive date conversion and hashing.
- Applied migrations: `018_research_expansion.sql` and
  `019_dart_legacy_account_namespace.sql`.
- Replay ECS task: `314d262bd7d84bd79f4ee34cbf30237d`;
  definition `teamalpha-data-research-backfill:2`.
- Daily Scheduler remains ENABLED and was verified on
  `teamalpha-data-daily-full:274`, image matching source commit
  `2cdf2899a6d8116e9644fecf8057979cda987e86`.
- CI test and deployment workflows succeeded for that commit.

Durable audit prefix:

```text
s3://soma-quant-bronze-31-159372032315-ap-northeast-2-an/ops/research-backfill/20260922-v2-resume1/
  inventory.json
  progress.json
  outcomes.json
  verification.json
```

## Remaining outside this change

This is not an assertion that every Bronze field is now a standardized Silver
column. The newly discovered PR13 FMP macro/external collections and complete
DART XML/HTML narrative extraction remain separate expansion work. Unmapped
custom DART accounts remain in `fundamental_statement_line`; no name-based
semantic mapping was guessed. Existing Gold factors were not switched or rebuilt.
