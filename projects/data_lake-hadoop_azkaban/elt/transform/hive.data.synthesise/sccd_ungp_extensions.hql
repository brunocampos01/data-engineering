CREATE MATERIALIZED VIEW staging.sccd_ungp_ticket_extensions 
AS SELECT * from raw.sccd_ungp_worklog where logtype in ('SOLICITAR DEF. PRAZO', 'ACEITA PRORROGAÇÃO', 'REPROVAR PRORROGAÇÃO')
ORDER BY recordkey, createdate