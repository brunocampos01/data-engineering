CREATE MATERIALIZED VIEW IF NOT EXISTS staging.sccd_ungp_worklog
AS SELECT * from raw.sccd_ungp_worklog ORDER BY recordkey, createdate
