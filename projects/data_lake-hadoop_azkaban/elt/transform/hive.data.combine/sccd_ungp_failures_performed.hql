CREATE VIEW IF NOT EXISTS staging.sccd_ungp_failures_performed
AS SELECT md5(concat(row_sequence(), recordkey)) as id,
		  recordkey as service,
		  cast(worklogid as string) worklog,
		  description,
		  createdate as reprovaldate,
		  createdate,
		  siteid as site,
		  md5(concat(description, createdate)) as md5,
		  date_format(createdate, 'y-MM') as year_month
from raw.sccd_ungp_worklog where logtype in ('REPROVAR PF', 'REPROVAR', 'REJEITA SOLUÇÃO', 'REJEITA HOMOLOGAÇÃO')
ORDER BY recordkey, createdate