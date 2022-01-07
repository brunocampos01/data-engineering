INSERT OVERWRITE TABLE account_receivables.dim_segment
SELECT
	md5(concat(s.id,s.businessunit)),
	s.id,
	s.name,
	s.businessunit
FROM staging.hyperion_segment s
