# October 28 gap recovery

## Confirmed blockers

RDS had certified core Silver, KRX total return, KIS and all 49 APPROVED Gold factors through October 7; FMP through October 6. The October 28 scheduled run refused to skip 13 missing KRX sessions through October 27.

A persisted failed DQ run from October 16 records a conflicting `report_nm` for DART receipt `20261006000420`. The October 6 and October 8 immutable Bronze manifests confirm that every field is unchanged except the title's leading `[첨부추가]` marker. The original early-October CloudWatch event bodies are no longer available; the exact original failure timestamp is not claimed.

The checkout already contained an uncommitted, relevant fix and regression test for this marker. These were reviewed and reproduced in an isolated worktree based on production `origin/main` (`f000b1c`), without modifying the original checkout or its other unfinished Gold/schema work. Commit `c0990fe` permits only the known display marker while retaining raw titles and audit history; semantic title changes and `[첨부정정]` remain blocked. The 26 targeted tests and CI/deployment workflows passed.

A second preflight blocker was the unapplied migration `020_dart_historical_revenue.sql` from the already deployed PR #14. Under the shared certification lock, only this missing additive migration was committed and `migrate.assert_current` passed. Existing fundamentals, Gold approvals and immutable source data were not rewritten. An initial nested transaction invocation was rolled back automatically before the migration was rerun using the runner's own transaction boundary.

## Recovery execution

- Production daily scheduler: `teamalpha-data-daily-full:281`.
- Source: `c0990fee8b4f34beb52794fa2586f5080b9ebb32`.
- Verified image: `sha256:ec7022787e3cce0fc2f3e2755c81723bf568655033c9db26c985797e8dce3b61`.
- Recovery definition: `teamalpha-data-gap-recovery:1`.
- Task: `5a13306283aa4e9180fa9921072f37e8`.
- Command: `python -m pipeline.gap_replay --from 20261008 --to 20261027`.

The existing bounded replay runs weekdays sequentially under one shared certification lock. This includes the October 9 KRX holiday because non-KRX inputs can still need that calendar date. Intermediate source updates may leave the total-return contract explicitly BUILDING; final closure and freshness verification occur on the last replay day. Do not label this temporary state certified or launch a competing repair. The replay does not repeat certified dates through October 7 and defers total-return rebuilding to the final day.

The task was accepted by ECS without launch failures. Completion must be verified from its subsequent log and RDS; this document alone does not claim completed recovery. Logs: `/ecs/teamalpha-data-daily-full`, stream `ecs/teamalpha-data-daily-full/5a13306283aa4e9180fa9921072f37e8`.

## Resumed batched recovery

The first task certified October 8 core Silver and all 49 APPROVED Gold factors, then failed on FMP CUSIP `316773605`. Stored Bronze profiles contain active `FITB-PI` and inactive `FITBI` with the same CUSIP/ISIN; RDS already assigns those security identifiers to FITBI. No explicit ticker-change evidence justifies merging the assets. Commit `717609836b98f8260c639d7750e47e0d3e9641c3` extends the existing optional-identifier exclusion/audit policy to persisted ownership conflicts. Existing identifiers and raw facts remain unchanged; ambiguous candidate links are omitted and counted in DQ evidence.

The same commit adds bounded batched followups to gap replay: core dates first, final total-return closure, alternative inputs, one final KIS catch-up, one Gold range and remaining FMP dates. Normal scheduled defaults remain unchanged. Local full suite: 1,187 passed; targeted tests: 69 passed. CI and deployment passed.

- Resumed definition: `teamalpha-data-gap-recovery:2`.
- Task: `692b6f106a744b17b158334bb71b54ce`.
- Image digest: `sha256:4586f3758ff83798e2338cdc989cf1affb8f9aa9b31bdfc7dd0fe933b32a71e9`.
- Command: `python -m pipeline.gap_replay --from 20261009 --to 20261027 --resume-building --defer-followups --followup-from 20261008`.

ECS RUNNING and shared-lock acquisition were verified. Logs confirm resuming the BUILDING contract at October 8 and starting October 9. October 9 is a KRX holiday but remains a calendar processing target for other inputs. Final certification is still pending; do not repeat completed October 8 Gold or launch a competing replay. If only a followup fails after core completion, recover that unfinished stage rather than replaying completed core/Gold.

## Rolling evidence cache repair

At the user's request, task `692b6f106a744b17b158334bb71b54ce` was stopped deliberately (ECS STOPPED, exit 143) to remove repeated DART dependency discovery. Each calendar replay moved the 180-day start, invalidating the old same-start reuse condition. October 20 took about 62 minutes: about 47 minutes for evidence preparation and 14 minutes for Silver publication/validation.

Commit `51e33ba653a6d1a9bc8edaf80e9e0f290ea2670c` allows reuse for overlapping forward-moving ranges. Cached dependency selector evidence is reclassified against the new seed set, including dependencies entering/leaving the range; new candidates are still fetched. Content-addressed body and final provenance validation remain required. Tests cover the moving-window transition, newly selected dependencies and corrupted cached evidence. Full suite: 1,189 passed.

Read-only RDS checks after stopping confirmed core daily CERTIFIED through October 20 and all 49 APPROVED Gold factors through October 8. A separate `--gold-from` argument prevents losing deferred Gold dates when core replay resumes from October 21. Intended remaining range: core October 21–27, Gold October 9–27, other deferred followups from October 8. October 27 is the latest completed calendar day at the October 28 KST request. No completion claim is made before final certification.

CI and deployment passed; scheduler definition is `teamalpha-data-daily-full:283`. Restarted with `teamalpha-data-gap-recovery:3`, task `625b7c9e21814a6a8fed3a2dcc807aa7`, immutable image `sha256:1f732846805ff341b0b0e79c8f7dec0e38dcddbb5ecf606b7e9681eb2adff137`. Arguments: `--from 20261021 --to 20261027 --resume-building --defer-followups --followup-from 20261008 --gold-from 20261009`. No competing RUNNING task was present at launch; ECS accepted without failures. Existing EFS evidence remains mounted. Confirm actual probe reuse and final layer certification from subsequent logs/DB, not from launch success.

## Pre-publication invalidation resume guard

Task `625b7c9e21814a6a8fed3a2dcc807aa7` exited 1 before collection. The restart guard required a non-null return-contract quality ID, but the normal Bronze-action pre-publication invalidator deliberately clears that field. Read-only RDS evidence retained `invalidated_reason=DART_BRONZE_ACTION_CHANGE_OBSERVED_BEFORE_PUBLICATION:2026-10-21` and `invalidated_by_run_id=b47ba34f-a3b4-4f30-91d7-155fd7be7568`, joining to CERTIFIED daily October 20. The earlier task had been stopped between this invalidation and Silver publication.

Commit `6f0935db2182aa6de3492539f5099b7192af96e3` accepts this exact retained source proof for restart only: the invalidation date must equal the first resumed date, the retained run must be CERTIFIED daily at the last certified raw-price date, and all existing coverage, methodology and no-gap checks still apply. The contract remains BUILDING, with no fabricated quality ID or readiness flag. Targeted restart tests: 27 passed, including rejection of wrong reason, missing run, failed run, wrong mode and wrong date. The read-only production proof check passed.

CI test/deploy succeeded. Recovery definition `teamalpha-data-gap-recovery:4`, task `c97a142a1e07487aa509a16c5cc75c3e`, image `sha256:9eb735f75b64e76de7d1a9d71f2ee7f9abedbae17e824a0d05180ffc30cff5df` was launched with the same October 21–27 core and October 9–27 Gold ranges. No competing task was present. Final certification is pending.

## Observed speed and outstanding interrupted input

Subsequent production logs confirm October 22, 23 and 26 completed in about 29 minutes each, versus about 62 minutes previously. Probe reuse counts were 1,678/1,684 with only 6/0/1 new candidates. October 26 Silver is CERTIFIED; October 27 final evidence preparation reused 170,487 cache objects and downloaded 574 changed objects. This does not certify the final TR/KIS/Gold/FMP stages.

Read-only investigation confirmed an interrupted-input omission: stock 071950 (asset 4569, corp 00210856, 코아스), amended half-year filing `20261021000109`, exists in Bronze but June 30 fundamentals in RDS still contain only filing `20260812000288` (28 rows). The previous task fetched it before interruption; retry returned changed=0 and therefore omitted it from the Silver input list. Repair this exact input under the shared certification lock, retain quality gates, and account for affected Gold dates before claiming completion. Do not run the whole-year/backfill_s3 path (which is intended for an empty target).

Verified source keys and SHA256 at inspection:

- `financials/dart/year=2026/corp=071950/11012.json`: `1ac5d44b4d725c54da1d8fc1fdca003401b9b1e064dd0c4e3dbc6df78ffbac55` (15,547 bytes).
- `dividends/dart/alot-matter/year=2026/report=11012/corp=071950/rcept=20261021000109/response.json`: `90b52d33843588ebb4edb27683410567012087b80456972629b688f37b98d5d6` (3,145 bytes).
- Same receipt `manifest.json`: `ba652e1c6856294948b45b8871efc9e4fd95f21ac510ca64cfa8af46097adfaf` (605 bytes).

Financial absence is confirmed; independently reconcile dividend metrics before publishing. The active recovery still holds the shared lock, so no concurrent corrective write was made during this inspection.
