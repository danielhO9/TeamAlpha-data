# 2026-09-23 targeted daily recovery

## Completed

- KIS non-session calendar bounds fixed in `c0038bd`; 82 KIS tests passed.
- Ownership replay and pending-input manifests fixed in `5fbde97`; 99 related tests passed.
- Non-December annual statement period resolution fixed in `418fdaf`; 16 alternative-input tests passed. Only exact receipt/company matches in captured DART disclosure titles are accepted; no dates are inferred from filing dates.
- RDS daily Silver and KRX total-return contracts were already certified through 2026-09-22. Neither was rebuilt by this recovery.
- Daily task `ff595807b25140eb8976418b343ef7a6` had failed on ownership uniqueness: the same receipt changed only `corp_name` from 루멘스 to 루멘스바이오스. Previously published PIT records are retained, and substantive changes still fail for review.
- Restored the exact 92 Bronze inputs (80 ownership, 6 industry, 6 full statements) from the failed run, verifying their combined fingerprint against run `633110ea-0ea1-4f95-b6a5-499012df2357` before publishing.
- Ownership: 5,256 existing rows retained and 91 new rows published; industry: 5 rows published, 1 out-of-universe row excluded. Certified run `4772010d-2e77-442e-82c3-5e03574eda53`.
- Detailed financial lines: 6 files / 707 rows certified in run `e1b6dcbd-fd0b-4483-b9bd-5245a6b0ebdc`; 7 files / 1,122 rows certified in run `56cc185c-2920-4d1c-9de6-fe4ff72d0874`. Both had zero rejected rows after exact June-year-end evidence resolution.
- Financial statement bodies were reused from Bronze. A current-day regular-disclosure metadata lookup was necessary for corrected receipt IDs. Its partial-day snapshot is preserved separately; the newly created incomplete daily-list cache was removed so the next scheduled daily job will fetch the complete day.

## Running, not yet complete

ECS task `f2f7e592275e459488f66d15d7d1e629`, definition `teamalpha-data-targeted-recovery:1`, was confirmed RUNNING with the shared certification lock and `[targeted-recovery] KIS start through=20260922`.

Its narrowly scoped sequence is:

1. KIS collection and final coverage verification through September 22.
2. Gold approved daily factors for September 18, 21 and 22 only. A pre-existing populated date causes an explicit stop rather than unreviewed replacement.
3. FMP target dates September 17, 18 and 21, retaining the existing per-stage receipts and certification checks.

This task does not recollect core KRX/DART, rebuild total returns, rerun research history, or change factor approvals. The existing monitoring heartbeat was updated with its ID and the completed repairs. Do not infer completion from this launch record.

## Evidence

Bucket: `soma-quant-bronze-31-159372032315-ap-northeast-2-an`.

- `ops/recovery/20260923/alternative-inputs-20260922.json`
- `ops/recovery/20260923/regular-reports-20260923-asof-050500Z.json`
- `ops/recovery/20260923/targeted-recovery-script.py`
- Logs: `/ecs/teamalpha-data-daily-full`, stream `ecs/teamalpha-data-daily-full/f2f7e592275e459488f66d15d7d1e629`.

A broad post-publish count by `quality_run_id` hit the 20-second read-only statement timeout and was canceled; no result is claimed from that query. The row totals above are from the committed certified publishing results.
