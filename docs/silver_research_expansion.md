# Bronze → Silver research expansion

Implementation order: DART exact-account mapping → FMP raw statements → company
observations → structured DART financing/reorganization disclosures.

## Deployment state and boundary

This change is additive. Apply migration `018_research_expansion.sql` through
`python -m pipeline.silver_quality.migrate` **before** starting the new workers.
Do not edit an applied migration. Verify the latest production branch/task
definition before deployment: the change was reconciled onto `9de9bde`, and is not
proof of what a newer running ECS task contains.

Code and local PostgreSQL verification do not mean production backfill is done.
AWS authentication, production-schema reconciliation, deployment, replay and
coverage verification are separate remaining operational steps.

No source-provider HTTP requests, price recalculation, return rebuilding, Gold
rewriting, or deletion of existing `fundamental` data is needed by this expansion.

## 1. Existing DART Silver accounts

`dart_account_metric_map` is an explicit, versioned allowlist of official account
IDs and statement families. `dart_standardized_statement_line` joins it to
certified `fundamental_statement_line` rows. Existing and newly certified lines
become visible without copying financial history or a separate backfill.

- Exact IDs only; colon/underscore namespace separators are normalized.
- Migration 019 adds exact aliases for the historical official `ifrs_`
  namespace found in older stored filings; 018 is not modified after application.
- Names, company-specific extension IDs, and segment/member details are not
  guessed. They remain in the original Silver table for further mapping review.
- All filing revisions and CFS/OFS variants survive.
- `current_amount` and `current_cumulative_amount` remain separate. A quarterly
  income value is not silently interchangeable with year-to-date cash flow.
- Use `metric_candidate_count = 1`; ambiguous matching lines are exposed for
  diagnosis, not arbitrarily summed or selected.
- IS and CIS remain different statement types. A factor must explicitly select
  its statement family and consolidation basis, not aggregate both.
- No implied currency/unit conversion or sign normalization.

Example point-in-time selection for a specified asset, metric and scope:

```sql
SELECT DISTINCT ON (period_end, fiscal_period)
       period_end, fiscal_period, current_amount, current_cumulative_amount,
       available_at, filing_id, currency, mapping_version
FROM dart_standardized_statement_line
WHERE asset_id = :asset_id AND metric = :metric
  AND statement_type = :statement_type AND fs_type = :fs_type
  AND available_at <= :as_of AND metric_candidate_count = 1
ORDER BY period_end, fiscal_period, available_at DESC, filing_id DESC;
```

This is an input interface, not a switch of the existing Gold factor engine.

## 2. FMP full statement preservation

`research_observation`, dataset `FMP_STATEMENT`, retains every field of an
admitted, dated financial row, including fields outside `FINANCIAL_METRICS`.
The original provider response stays in Bronze. JSON-null normalization replaces
dataframe NaN/Infinity; invalid numeric values never become factor numbers.

`fmp_statement_line` exposes numeric fields as source-named long-form accounts.
`reported_currency` is statement metadata, **not** a claim that every numeric
field is currency: EPS, shares and ratios still require explicit unit mapping.
Metadata year/CIK identifiers are excluded from numeric account extraction.
Missing/invalid availability and unknown assets are not filled by inference.

The daily FMP bundle collects new statement observations only from the target
snapshot-date partition while reading its existing inputs;
it does not fetch provider data again. The regular FMP backfill has a separate
research-financials partition per year. For already completed provider history,
use the targeted replay below instead of rerunning price/return backfill.

## 3. Company attributes and industry observations

`company_profile_observation` exposes existing DART company `raw_row` attributes
plus preserved FMP profile/screener snapshots. Sector, industry, CEO, employee
count and raw source attributes are available without putting current values on
past dates. DART industry codes and FMP industry labels remain distinct systems.
FMP profiles without a real manifest `received_at` cannot be backdated from an
IPO date or snapshot-directory label.

Each FMP source response is a separate observation: do not combine a screener
and profile as if they were one fully populated row. Apply source-file/provider
priority and as-of selection explicitly when building a factor.

## 4. Structured DART corporate events

The existing event parser carries its entire structured row into
`research_observation`, dataset `DART_EVENT`, in the same publish transaction.
`dart_corporate_event_detail` exposes issuance method, funds by purpose, new
common shares, merger purpose/counterparty/valuation text, plus the raw fields.
Issuer vs related-company scope and report title remain in metadata. Inherited
preferred-share price evidence does not generate another issuer filing.

Events become available on the day after the official disclosure acceptance
date, using the repository's existing UTC day convention. Corrections remain
distinct observations; raw event terms are not automatically actionable active
events. A factor must handle cancellation, correction lineage and related-company
scope. This change does not extract all document XML/HTML narrative or create
new KIS/investor-flow sources.

## Bounded backfill and resume

Use an explicit JSONL manifest of existing Bronze files (not a directory-wide
scan). Each entry has `path`, `sha256`, and `dataset` (`FMP_STATEMENT`,
`FMP_PROFILE`, or `DART_EVENT`). DART also requires `disclosure_file` and
`disclosure_sha256` for the official list containing exactly one matching
receipt. Local and S3 paths are supported.

```sh
python -m pipeline.silver.research_backfill --manifest /absolute/path/inputs.jsonl --max-files 100
```

- One file per transaction; each batch has a hard cap on newly processed files.
- Verify checksums before transformation. Checkpoints bind input hashes and
  transform version; DART checkpoints also bind official disclosure metadata.
- A certified completed checkpoint skips reading the Bronze body entirely.
- Probe only supplied observation keys in batches; COPY only unseen rows.
- Failed publication rolls back its data and checkpoint together.
- Unmapped/PIT-invalid rows are counted as warnings and stored as excluded counts
  in the checkpoint, not represented as successful row coverage. After a mapping
  repair, use `--recheck` with a targeted manifest to revisit those files without
  rewriting existing observations. Historical ticker reuse is rejected if
  identity is ambiguous.

After replay, reconcile input/admitted/excluded counts and inspect the certified
views by dataset, asset, period and availability date. Do not claim full coverage
from a schema migration, successful task exit, or a raw input count alone.

For an in-region ECS run, `pipeline.silver.research_backfill_ecs` discovers a
finite inventory under the three supported prefixes, saves it to an explicit
`ops/research-backfill/` audit prefix, and replays with four bounded workers and
one persistent DB connection per worker. Use `--max-files` for a canary.
Resume uses the same content checkpoints. Ambiguous/missing official disclosure
identities are reported as `REVIEW_REQUIRED`, not guessed. Outcomes/progress are
persisted every 25 files. Newly introduced FMP macro/external Bronze collections
are not included in these three dataset contracts.
