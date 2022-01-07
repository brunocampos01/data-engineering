CREATE MATERIALIZED VIEW IF NOT EXISTS staging.sccd_ungp_ticket_failures
AS SELECT * from raw.sccd_ungp_worklog where logtype in ('REPROVAR PF', 'REPROVAR', 'REJEITA SOLUÇÃO', 'REJEITA HOMOLOGAÇÃO')
ORDER BY recordkey, createdate