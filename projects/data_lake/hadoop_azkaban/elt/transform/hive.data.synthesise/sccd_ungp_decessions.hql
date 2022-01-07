CREATE MATERIALIZED VIEW IF NOT EXISTS staging.sccd_ungp_ticket_decessions
AS SELECT * from raw.sccd_ungp_worklog where logtype in ('SOLICITA ALT SEVERIDADE', 'APROVAR SEVERIDADE', 'REPROVAR SEVERIDADE')
ORDER BY recordkey, createdate