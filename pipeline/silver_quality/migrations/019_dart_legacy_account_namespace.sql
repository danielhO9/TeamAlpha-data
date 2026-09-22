-- Historical DART filings use the official ifrs_ namespace rather than
-- ifrs-full_. Preserve exact account names/families; do not fuzzy-match labels.
-- Add aliases instead of rewriting 25M source lines or changing applied DDL.
INSERT INTO dart_account_metric_map(account_id,statement_type,metric,mapping_version)
SELECT replace(account_id,'ifrs-full_','ifrs_'),statement_type,metric,
       'dart-exact-id-v2-legacy'
FROM dart_account_metric_map
WHERE account_id LIKE 'ifrs-full%'
ON CONFLICT (account_id,statement_type) DO NOTHING;
