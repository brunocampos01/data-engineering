INSERT OVERWRITE TABLE staging.hyperion_financas
SELECT
	cast(CONCAT(regexp_replace(ud06, 'FY', '20'),'0101') as int)	as cdData
	,SPLIT(ud01, '-')[0]						 		as cdUnidadeNegocio
	,ud01															  as cdCentroCusto
	,ud02 															as cdProjeto
	,ud03 															as cdProduto
	,ud04 															as cdServico
	,ud05 															as cdSegmento
	,ud07 															as cdMoeda
	,ud08 															as cdVersao
	,ud09 															as cdEmpresa
	,ud10 															as cdInterCia
	,ud11 															as cdAlocacao
	,ud12															  as cdOperacao
	,ud13 															as cdConta
	,case when ud12 = 'Orcado' 		then h.valor_mes_1 end as vlOrcado
	,case when ud12 = 'Realizado' then h.valor_mes_1 end as vlRealizado
	,case when ud12 = 'Forecast' 	then h.valor_mes_1 end as vlForecast
from raw.hyperion_financas as h;

INSERT INTO TABLE staging.hyperion_financas
SELECT
	cast(CONCAT(regexp_replace(ud06, 'FY', '20'),'0201') as int)	as cdData
	,SPLIT(ud01, '-')[0]						 		as cdUnidadeNegocio
	,ud01 															as cdCentroCusto
	,ud02 															as cdProjeto
	,ud03 															as cdProduto
	,ud04 															as cdServico
	,ud05 															as cdSegmento
	,ud07 															as cdMoeda
	,ud08 															as cdVersao
	,ud09 															as cdEmpresa
	,ud10 															as cdInterCia
	,ud11 															as cdAlocacao
	,ud12															  as cdOperacao
	,ud13 															as cdConta
	,case when ud12 = 'Orcado' 		then h.valor_mes_2 end as vlOrcado
	,case when ud12 = 'Realizado' then h.valor_mes_2 end as vlRealizado
	,case when ud12 = 'Forecast' 	then h.valor_mes_2 end as vlForecast
from raw.hyperion_financas as h;

INSERT INTO TABLE staging.hyperion_financas
SELECT
	cast(CONCAT(regexp_replace(ud06, 'FY', '20'),'0301') as int)	as cdData
	,SPLIT(ud01, '-')[0]						 		as cdUnidadeNegocio
	,ud01 															as cdCentroCusto
	,ud02 															as cdProjeto
	,ud03 															as cdProduto
	,ud04 															as cdServico
	,ud05 															as cdSegmento
	,ud07 															as cdMoeda
	,ud08 															as cdVersao
	,ud09 															as cdEmpresa
	,ud10 															as cdInterCia
	,ud11 															as cdAlocacao
	,ud12															  as cdOperacao
	,ud13 															as cdConta
	,case when ud12 = 'Orcado' 		then h.valor_mes_3 end as vlOrcado
	,case when ud12 = 'Realizado' then h.valor_mes_3 end as vlRealizado
	,case when ud12 = 'Forecast' 	then h.valor_mes_3 end as vlForecast
from raw.hyperion_financas as h;

INSERT INTO TABLE staging.hyperion_financas
SELECT
	cast(CONCAT(regexp_replace(ud06, 'FY', '20'),'0401') as int)	as cdData
	,SPLIT(ud01, '-')[0]						 		as cdUnidadeNegocio
	,ud01 															as cdCentroCusto
	,ud02 															as cdProjeto
	,ud03 															as cdProduto
	,ud04 															as cdServico
	,ud05 															as cdSegmento
	,ud07 															as cdMoeda
	,ud08 															as cdVersao
	,ud09 															as cdEmpresa
	,ud10 															as cdInterCia
	,ud11 															as cdAlocacao
	,ud12															  as cdOperacao
	,ud13 															as cdConta
	,case when ud12 = 'Orcado' 		then h.valor_mes_4 end as vlOrcado
	,case when ud12 = 'Realizado' then h.valor_mes_4 end as vlRealizado
	,case when ud12 = 'Forecast' 	then h.valor_mes_4 end as vlForecast
from raw.hyperion_financas as h;

INSERT INTO TABLE staging.hyperion_financas
SELECT
	cast(CONCAT(regexp_replace(ud06, 'FY', '20'),'0501') as int)	as cdData
	,SPLIT(ud01, '-')[0]						 		as cdUnidadeNegocio
	,ud01 															as cdCentroCusto
	,ud02 															as cdProjeto
	,ud03 															as cdProduto
	,ud04 															as cdServico
	,ud05 															as cdSegmento
	,ud07 															as cdMoeda
	,ud08 															as cdVersao
	,ud09 															as cdEmpresa
	,ud10 															as cdInterCia
	,ud11 															as cdAlocacao
	,ud12															  as cdOperacao
	,ud13 															as cdConta
	,case when ud12 = 'Orcado' 		then h.valor_mes_5 end as vlOrcado
	,case when ud12 = 'Realizado' then h.valor_mes_5 end as vlRealizado
	,case when ud12 = 'Forecast' 	then h.valor_mes_5 end as vlForecast
from raw.hyperion_financas as h;

INSERT INTO TABLE staging.hyperion_financas
SELECT
	cast(CONCAT(regexp_replace(ud06, 'FY', '20'),'0601') as int)	as cdData
	,SPLIT(ud01, '-')[0]						 		as cdUnidadeNegocio
	,ud01 															as cdCentroCusto
	,ud02 															as cdProjeto
	,ud03 															as cdProduto
	,ud04 															as cdServico
	,ud05 															as cdSegmento
	,ud07 															as cdMoeda
	,ud08 															as cdVersao
	,ud09 															as cdEmpresa
	,ud10 															as cdInterCia
	,ud11 															as cdAlocacao
	,ud12															  as cdOperacao
	,ud13 															as cdConta
	,case when ud12 = 'Orcado' 		then h.valor_mes_6 end as vlOrcado
	,case when ud12 = 'Realizado' then h.valor_mes_6 end as vlRealizado
	,case when ud12 = 'Forecast' 	then h.valor_mes_6 end as vlForecast
from raw.hyperion_financas as h;

INSERT INTO TABLE staging.hyperion_financas
SELECT
	cast(CONCAT(regexp_replace(ud06, 'FY', '20'),'0701') as int)	as cdData
	,SPLIT(ud01, '-')[0]						 		as cdUnidadeNegocio
	,ud01 															as cdCentroCusto
	,ud02 															as cdProjeto
	,ud03 															as cdProduto
	,ud04 															as cdServico
	,ud05 															as cdSegmento
	,ud07 															as cdMoeda
	,ud08 															as cdVersao
	,ud09 															as cdEmpresa
	,ud10 															as cdInterCia
	,ud11 															as cdAlocacao
	,ud12															  as cdOperacao
	,ud13 															as cdConta
	,case when ud12 = 'Orcado' 		then h.valor_mes_7 end as vlOrcado
	,case when ud12 = 'Realizado' then h.valor_mes_7 end as vlRealizado
	,case when ud12 = 'Forecast' 	then h.valor_mes_7 end as vlForecast
from raw.hyperion_financas as h;

INSERT INTO TABLE staging.hyperion_financas
SELECT
	cast(CONCAT(regexp_replace(ud06, 'FY', '20'),'0801') as int)	as cdData
	,SPLIT(ud01, '-')[0]						 		as cdUnidadeNegocio
	,ud01 															as cdCentroCusto
	,ud02 															as cdProjeto
	,ud03 															as cdProduto
	,ud04 															as cdServico
	,ud05 															as cdSegmento
	,ud07 															as cdMoeda
	,ud08 															as cdVersao
	,ud09 															as cdEmpresa
	,ud10 															as cdInterCia
	,ud11 															as cdAlocacao
	,ud12															  as cdOperacao
	,ud13 															as cdConta
	,case when ud12 = 'Orcado' 		then h.valor_mes_8 end as vlOrcado
	,case when ud12 = 'Realizado' then h.valor_mes_8 end as vlRealizado
	,case when ud12 = 'Forecast' 	then h.valor_mes_8 end as vlForecast
from raw.hyperion_financas as h;

INSERT INTO TABLE staging.hyperion_financas
SELECT
	cast(CONCAT(regexp_replace(ud06, 'FY', '20'),'0901') as int)	as cdData
	,SPLIT(ud01, '-')[0]						 		as cdUnidadeNegocio
	,ud01 															as cdCentroCusto
	,ud02 															as cdProjeto
	,ud03 															as cdProduto
	,ud04 															as cdServico
	,ud05 															as cdSegmento
	,ud07 															as cdMoeda
	,ud08 															as cdVersao
	,ud09 															as cdEmpresa
	,ud10 															as cdInterCia
	,ud11 															as cdAlocacao
	,ud12															  as cdOperacao
	,ud13 															as cdConta
	,case when ud12 = 'Orcado' 		then h.valor_mes_9 end as vlOrcado
	,case when ud12 = 'Realizado' then h.valor_mes_9 end as vlRealizado
	,case when ud12 = 'Forecast' 	then h.valor_mes_9 end as vlForecast
from raw.hyperion_financas as h;

INSERT INTO TABLE staging.hyperion_financas
SELECT
	cast(CONCAT(regexp_replace(ud06, 'FY', '20'),'1001') as int)	as cdData
	,SPLIT(ud01, '-')[0]						 		as cdUnidadeNegocio
	,ud01 															as cdCentroCusto
	,ud02 															as cdProjeto
	,ud03 															as cdProduto
	,ud04 															as cdServico
	,ud05 															as cdSegmento
	,ud07 															as cdMoeda
	,ud08 															as cdVersao
	,ud09 															as cdEmpresa
	,ud10 															as cdInterCia
	,ud11 															as cdAlocacao
	,ud12															  as cdOperacao
	,ud13 															as cdConta
	,case when ud12 = 'Orcado' 		then h.valor_mes_10 end as vlOrcado
	,case when ud12 = 'Realizado' then h.valor_mes_10 end as vlRealizado
	,case when ud12 = 'Forecast' 	then h.valor_mes_10 end as vlForecast
from raw.hyperion_financas as h;

INSERT INTO TABLE staging.hyperion_financas
SELECT
	cast(CONCAT(regexp_replace(ud06, 'FY', '20'),'1101') as int)	as cdData
	,SPLIT(ud01, '-')[0]						 		as cdUnidadeNegocio
	,ud01 															as cdCentroCusto
	,ud02 															as cdProjeto
	,ud03 															as cdProduto
	,ud04 															as cdServico
	,ud05 															as cdSegmento
	,ud07 															as cdMoeda
	,ud08 															as cdVersao
	,ud09 															as cdEmpresa
	,ud10 															as cdInterCia
	,ud11 															as cdAlocacao
	,ud12															  as cdOperacao
	,ud13 															as cdConta
	,case when ud12 = 'Orcado' 		then h.valor_mes_11 end as vlOrcado
	,case when ud12 = 'Realizado' then h.valor_mes_11 end as vlRealizado
	,case when ud12 = 'Forecast' 	then h.valor_mes_11 end as vlForecast
from raw.hyperion_financas as h;

INSERT INTO TABLE staging.hyperion_financas
SELECT
	cast(CONCAT(regexp_replace(ud06, 'FY', '20'),'1201') as int)	as cdData
	,SPLIT(ud01, '-')[0]						 		as cdUnidadeNegocio
	,ud01 															as cdCentroCusto
	,ud02 															as cdProjeto
	,ud03 															as cdProduto
	,ud04 															as cdServico
	,ud05 															as cdSegmento
	,ud07 															as cdMoeda
	,ud08 															as cdVersao
	,ud09 															as cdEmpresa
	,ud10 															as cdInterCia
	,ud11 															as cdAlocacao
	,ud12															  as cdOperacao
	,ud13 															as cdConta
	,case when ud12 = 'Orcado' 		then h.valor_mes_12 end as vlOrcado
	,case when ud12 = 'Realizado' then h.valor_mes_12 end as vlRealizado
	,case when ud12 = 'Forecast' 	then h.valor_mes_12 end as vlForecast
from raw.hyperion_financas as h;
