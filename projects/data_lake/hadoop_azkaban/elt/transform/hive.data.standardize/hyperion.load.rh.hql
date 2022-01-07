INSERT OVERWRITE TABLE staging.hyperion_rh
SELECT
	cast(CONCAT(regexp_replace(ud01, 'FY', '20'),'0101') as int)	as cdData
  ,ud08															                            as cdCentroCusto
	,ud03 															                          as cdVersao
	,ud10 															                          as cdMovimento
	,ud09 															                          as cdTipoProfissional
	,case when ud02 = 'Orcado' then h.valor_mes_1 end			        as vlOrcado
	,case when ud02 = 'Realizado' then h.valor_mes_1 end 			    as vlRealizado
from raw.hyperion_rh as h
where ud08 rlike '^U[A-Z]-[A-Z][0-9]' or ud08 rlike '^U[A-Z][A-Z]-[A-Z][0-9]' or ud08 rlike '^I[A-Z][A-Z]-[A-Z][0-9]' or ud08 rlike '^U[A-Z][A-Z][A-Z]-[A-Z][0-9]';

INSERT INTO TABLE staging.hyperion_rh
SELECT
	cast(CONCAT(regexp_replace(ud01, 'FY', '20'),'0201') as int)	as cdData
  ,ud08															                            as cdCentroCusto
	,ud03 															                          as cdVersao
	,ud10 															                          as cdMovimento
	,ud09 															                          as cdTipoProfissional
	,case when ud02 = 'Orcado' then h.valor_mes_2 end			        as vlOrcado
	,case when ud02 = 'Realizado' then h.valor_mes_2 end 			    as vlRealizado
from raw.hyperion_rh as h
where ud08 rlike '^U[A-Z]-[A-Z][0-9]' or ud08 rlike '^U[A-Z][A-Z]-[A-Z][0-9]' or ud08 rlike '^I[A-Z][A-Z]-[A-Z][0-9]' or ud08 rlike '^U[A-Z][A-Z][A-Z]-[A-Z][0-9]';

INSERT INTO TABLE	staging.hyperion_rh
SELECT
	cast(CONCAT(regexp_replace(ud01, 'FY', '20'),'0301') as int)	as cdData
  ,ud08															                            as cdCentroCusto
	,ud03 															                          as cdVersao
	,ud10 															                          as cdMovimento
	,ud09 															                          as cdTipoProfissional
	,case when ud02 = 'Orcado' then h.valor_mes_3 end			        as vlOrcado
	,case when ud02 = 'Realizado' then h.valor_mes_3 end 			    as vlRealizado
from raw.hyperion_rh as h
where ud08 rlike '^U[A-Z]-[A-Z][0-9]' or ud08 rlike '^U[A-Z][A-Z]-[A-Z][0-9]' or ud08 rlike '^I[A-Z][A-Z]-[A-Z][0-9]' or ud08 rlike '^U[A-Z][A-Z][A-Z]-[A-Z][0-9]';

INSERT INTO TABLE	staging.hyperion_rh
SELECT
	cast(CONCAT(regexp_replace(ud01, 'FY', '20'),'0401') as int)	as cdData
  ,ud08															                            as cdCentroCusto
	,ud03 															                          as cdVersao
	,ud10 															                          as cdMovimento
	,ud09 															                          as cdTipoProfissional
	,case when ud02 = 'Orcado' then h.valor_mes_4 end			        as vlOrcado
	,case when ud02 = 'Realizado' then h.valor_mes_4 end 			    as vlRealizado
from raw.hyperion_rh as h
where ud08 rlike '^U[A-Z]-[A-Z][0-9]' or ud08 rlike '^U[A-Z][A-Z]-[A-Z][0-9]' or ud08 rlike '^I[A-Z][A-Z]-[A-Z][0-9]' or ud08 rlike '^U[A-Z][A-Z][A-Z]-[A-Z][0-9]';

INSERT INTO TABLE	staging.hyperion_rh
SELECT
	cast(CONCAT(regexp_replace(ud01, 'FY', '20'),'0501') as int)	as cdData
  ,ud08															                            as cdCentroCusto
	,ud03 															                          as cdVersao
	,ud10 															                          as cdMovimento
	,ud09 															                          as cdTipoProfissional
	,case when ud02 = 'Orcado' then h.valor_mes_5 end			        as vlOrcado
	,case when ud02 = 'Realizado' then h.valor_mes_5 end 			    as vlRealizado
from raw.hyperion_rh as h
where ud08 rlike '^U[A-Z]-[A-Z][0-9]' or ud08 rlike '^U[A-Z][A-Z]-[A-Z][0-9]' or ud08 rlike '^I[A-Z][A-Z]-[A-Z][0-9]' or ud08 rlike '^U[A-Z][A-Z][A-Z]-[A-Z][0-9]';

INSERT INTO TABLE	staging.hyperion_rh
SELECT
	cast(CONCAT(regexp_replace(ud01, 'FY', '20'),'0601') as int)	as cdData
  ,ud08															                            as cdCentroCusto
	,ud03 															                          as cdVersao
	,ud10 															                          as cdMovimento
	,ud09 															                          as cdTipoProfissional
	,case when ud02 = 'Orcado' then h.valor_mes_6 end			        as vlOrcado
	,case when ud02 = 'Realizado' then h.valor_mes_6 end 			    as vlRealizado
from raw.hyperion_rh as h
where ud08 rlike '^U[A-Z]-[A-Z][0-9]' or ud08 rlike '^U[A-Z][A-Z]-[A-Z][0-9]' or ud08 rlike '^I[A-Z][A-Z]-[A-Z][0-9]' or ud08 rlike '^U[A-Z][A-Z][A-Z]-[A-Z][0-9]';

INSERT INTO TABLE	staging.hyperion_rh
SELECT
	cast(CONCAT(regexp_replace(ud01, 'FY', '20'),'0701') as int)	as cdData
  ,ud08															                            as cdCentroCusto
	,ud03 															                          as cdVersao
	,ud10 															                          as cdMovimento
	,ud09 															                          as cdTipoProfissional
	,case when ud02 = 'Orcado' then h.valor_mes_7 end			        as vlOrcado
	,case when ud02 = 'Realizado' then h.valor_mes_7 end 			    as vlRealizado
from raw.hyperion_rh as h
where ud08 rlike '^U[A-Z]-[A-Z][0-9]' or ud08 rlike '^U[A-Z][A-Z]-[A-Z][0-9]' or ud08 rlike '^I[A-Z][A-Z]-[A-Z][0-9]' or ud08 rlike '^U[A-Z][A-Z][A-Z]-[A-Z][0-9]';

INSERT INTO TABLE	staging.hyperion_rh
SELECT
	cast(CONCAT(regexp_replace(ud01, 'FY', '20'),'0801') as int)	as cdData
  ,ud08															                            as cdCentroCusto
	,ud03 															                          as cdVersao
	,ud10 															                          as cdMovimento
	,ud09 															                          as cdTipoProfissional
	,case when ud02 = 'Orcado' then h.valor_mes_8 end			        as vlOrcado
	,case when ud02 = 'Realizado' then h.valor_mes_8 end 			    as vlRealizado
from raw.hyperion_rh as h
where ud08 rlike '^U[A-Z]-[A-Z][0-9]' or ud08 rlike '^U[A-Z][A-Z]-[A-Z][0-9]' or ud08 rlike '^I[A-Z][A-Z]-[A-Z][0-9]' or ud08 rlike '^U[A-Z][A-Z][A-Z]-[A-Z][0-9]';

INSERT INTO TABLE	staging.hyperion_rh
SELECT
	cast(CONCAT(regexp_replace(ud01, 'FY', '20'),'0901') as int)	as cdData
  ,ud08															                            as cdCentroCusto
	,ud03 															                          as cdVersao
	,ud10 															                          as cdMovimento
	,ud09 															                          as cdTipoProfissional
	,case when ud02 = 'Orcado' then h.valor_mes_9 end			        as vlOrcado
	,case when ud02 = 'Realizado' then h.valor_mes_9 end 			    as vlRealizado
from raw.hyperion_rh as h
where ud08 rlike '^U[A-Z]-[A-Z][0-9]' or ud08 rlike '^U[A-Z][A-Z]-[A-Z][0-9]' or ud08 rlike '^I[A-Z][A-Z]-[A-Z][0-9]' or ud08 rlike '^U[A-Z][A-Z][A-Z]-[A-Z][0-9]';

INSERT INTO TABLE	staging.hyperion_rh
SELECT
	cast(CONCAT(regexp_replace(ud01, 'FY', '20'),'1001') as int)	as cdData
  ,ud08															                            as cdCentroCusto
	,ud03 															                          as cdVersao
	,ud10 															                          as cdMovimento
	,ud09 															                          as cdTipoProfissional
	,case when ud02 = 'Orcado' then h.valor_mes_10 end			      as vlOrcado
	,case when ud02 = 'Realizado' then h.valor_mes_10 end 			  as vlRealizado
from raw.hyperion_rh as h
where ud08 rlike '^U[A-Z]-[A-Z][0-9]' or ud08 rlike '^U[A-Z][A-Z]-[A-Z][0-9]' or ud08 rlike '^I[A-Z][A-Z]-[A-Z][0-9]' or ud08 rlike '^U[A-Z][A-Z][A-Z]-[A-Z][0-9]';

INSERT INTO TABLE	staging.hyperion_rh
SELECT
	cast(CONCAT(regexp_replace(ud01, 'FY', '20'),'1101') as int)	as cdData
  ,ud08															                            as cdCentroCusto
	,ud03 															                          as cdVersao
	,ud10 															                          as cdMovimento
	,ud09 															                          as cdTipoProfissional
	,case when ud02 = 'Orcado' then h.valor_mes_11 end			      as vlOrcado
	,case when ud02 = 'Realizado' then h.valor_mes_11 end 			  as vlRealizado
from raw.hyperion_rh as h
where ud08 rlike '^U[A-Z]-[A-Z][0-9]' or ud08 rlike '^U[A-Z][A-Z]-[A-Z][0-9]' or ud08 rlike '^I[A-Z][A-Z]-[A-Z][0-9]' or ud08 rlike '^U[A-Z][A-Z][A-Z]-[A-Z][0-9]';

INSERT INTO TABLE	staging.hyperion_rh
SELECT
	cast(CONCAT(regexp_replace(ud01, 'FY', '20'),'1201') as int)	as cdData
  ,ud08															                            as cdCentroCusto
	,ud03 															                          as cdVersao
	,ud10 															                          as cdMovimento
	,ud09 															                          as cdTipoProfissional
	,case when ud02 = 'Orcado' then h.valor_mes_12 end			      as vlOrcado
	,case when ud02 = 'Realizado' then h.valor_mes_12 end 			  as vlRealizado
from raw.hyperion_rh as h
where ud08 rlike '^U[A-Z]-[A-Z][0-9]' or ud08 rlike '^U[A-Z][A-Z]-[A-Z][0-9]' or ud08 rlike '^I[A-Z][A-Z]-[A-Z][0-9]' or ud08 rlike '^U[A-Z][A-Z][A-Z]-[A-Z][0-9]';
