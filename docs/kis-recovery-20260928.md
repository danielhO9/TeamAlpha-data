# KIS delayed-source recovery, 2026-09-28

## Evidence and cause

The latest failed run (September 26, target September 25) contained 208 failures:

- 103 `J investor missing 1 days; no zero fill`.
- 103 dependent `missing KRX component for non-NXT integrated series` failures.
- 2 provider `OPSQ1002 / SESSION FULL` errors.

The requested session window was September 17–23. September 24–25 are not sessions in the deployed XKRX calendar. For sampled tickers `008600` and `019490`, the immutable September 26 responses anchored at September 23 started at September 22. Identical requests on September 28 returned September 23 with provider-reported zero volume. This demonstrates delayed source availability for the samples, not grounds to manufacture zeros for missing observations.

Original failed result:
`daily_references/daily_results/827b57996c1c4fcbf79fc19beea5e9991bde893b6c5dde490b715460574e2a91.json`.

## Changes

Commit `dd2e5f5`:

- Persist a pending daily reference context keyed by sessions, policy, universe and reference state. Retries reuse that context and its successful partition checkpoints.
- Coalesce identical concurrent KIS history requests within a client, with a bounded 128-entry completed-result cache. A new client fetches fresh provider data; missing results are not cached across runs.
- Retry `OPSQ1002` with the existing bounded backoff and shared request budget.
- Emit bounded collection progress logs.
- Keep strict missing-date checks, final coverage verification and post-verification-only watermark advancement.

86 KIS tests passed locally. The CI test and deployment workflows passed. Scheduler definition `teamalpha-data-daily-full:278` used the verified image digest `sha256:de69a83e511317231cc973aa0cda034815a3718493e7c985343177bddd652876`.

## Recovery scope and execution

`tools/recover_kis_20260928.py` reuses the failed window's immutable manifest and checkpoints. It does not collect KRX/DART again or rebuild total returns. It checks complete KIS coverage, then advances the reference state and processes Gold September 23 and FMP September 22–24 only.

The first recovery launch (`997be77170614b84b82630f5dda95794`, definition `:2`) failed before collection because its manifest URI used the unsigned research hash instead of the signed object's hash. The lock was released; the launcher was corrected and both hashes were checked against S3.

Corrected task: `b95f4fa38fae42479248b9788c4a9a0d`, definition `teamalpha-data-targeted-recovery:3`. It was confirmed RUNNING and holding the certification lock. This launch record is not evidence of completion.

Script archive: `s3://soma-quant-bronze-31-159372032315-ap-northeast-2-an/ops/recovery/20260928/recover_kis.py`.

Log group `/ecs/teamalpha-data-daily-full`; stream `ecs/teamalpha-data-daily-full/b95f4fa38fae42479248b9788c4a9a0d`.

## Verified KIS completion (2026-09-28 10:12 KST)

- 5,548 planned partitions: 5,340 successful checkpoints skipped, only 208 processed, zero failures.
- Collection work ran from 01:11:56 to 01:12:30 UTC (approximately 34 seconds after context/inventory preparation).
- Final coverage: expected 40,101; actual 40,101; missing, unexpected, duplicate and invalid-ratio counts all zero.
- KIS reference advanced through September 23 only after verification.
- Gold September 23 started at 01:12:35 UTC. Gold/FMP downstream completion is not yet claimed by this record.
