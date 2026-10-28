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
