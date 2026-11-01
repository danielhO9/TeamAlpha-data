# Reviewed Cheil Bio price-unit correction

Approved scope: deploy the persistent reader correction, back up and repair
Silver, recertify total returns, regenerate affected reference108 Gold. Existing
strategy artifacts are preserved; no strategy rerun or website publication.

052670 / asset 4188 consolidated 1500 old shares into one on its February 9,
2026 resumed listing. The quoted change still used the old-unit 2080 close.
Use the issuer's explicit ratio, not the rounded share-count quotient. Preserve
Bronze quotes and fail closed if the reviewed anchors change.

`pipeline.reviewed_price_repair_ecs` acquires the daily certification epoch and
return-writer locks, validates the actual Bronze row, and runs existing action
evidence preparation/preview before any price mutation. It saves a content-
addressed full-asset backup to private S3 and verifies the backup bytes. The
transaction multiplies only pre-event adjusted closes by 1500, invalidates the
return contract and clears only this asset's derived returns. The existing
incremental rebuild selects that damaged history; all inherited lineage and
full return-audit gates remain enabled. An independent event-return oracle
must pass after recertification, or the contract is demoted again.

Required task environment: `S3_BRONZE_BUCKET`, `REPAIR_OUTPUT_PREFIX`.
Use the current deployed image/resources and existing EFS evidence cache.
Do not run alongside another maintenance task. Preserve the daily scheduler.
Cloud success requires completion.json plus independent RDS readback, not
merely successful task launch.

Validation: 1209 existing/reader tests passed; 6 additional repair-state tests
passed. Actual February 9 Bronze parquet matches the reviewed anchors. No
production correction has occurred at this documentation checkpoint.
