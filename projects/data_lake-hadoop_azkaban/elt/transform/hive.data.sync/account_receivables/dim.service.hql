INSERT OVERWRITE TABLE account_receivables.dim_service
select
	md5(s.cdservico),
	s.cdservico,
	s.nmservico,
	s.declasse,
	s.derecorrencia,
	s.siglaunidade
from staging.hyperion_service s
