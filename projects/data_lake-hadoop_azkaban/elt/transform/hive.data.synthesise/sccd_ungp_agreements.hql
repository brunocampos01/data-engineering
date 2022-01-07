CREATE MATERIALIZED VIEW IF NOT EXISTS staging.sccd_ungp_ticket_agreements
AS SELECT * from raw.sccd_ungp_worklog where logtype in ('ACORDO DE PRAZO', 'APROVAR ACORDO', 'REPROVAR ACORDO')
ORDER BY recordkey, createdate