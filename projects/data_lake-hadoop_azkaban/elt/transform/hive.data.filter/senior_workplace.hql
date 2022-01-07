CREATE VIEW IF NOT EXISTS staging.senior_workplace AS
SELECT
  CAST(usu_codpos 	   	AS int)         AS id,
  CAST(usu_despos 	   	AS string)      AS name,
  CAST(usu_endpos 	   	AS string)      AS adress,
  CAST(usu_estado 	   	AS string)      AS state,
  CASE
  	WHEN usu_cidade = "Sao Paulo" THEN "SÃO PAULO"
  	WHEN usu_cidade = "Florianopolis" THEN "FLORIANÓPOLIS"
  	WHEN usu_cidade = "Goiania" THEN "GOIÂNIA"
  	WHEN usu_cidade = "BRASILIA" THEN "BRASÍLIA"
  	WHEN usu_cidade = "Cuiaba" THEN "CUIABÁ"
  	WHEN usu_cidade = "Brasilia" THEN "BRASÍLIA"
  	WHEN usu_cidade = "Sao Jose do Rio Preto" THEN "SÃO JOSÉ DO RIO PRETO"
  	WHEN usu_cidade = "Ribeirao Preto" THEN "RIBEIRÃO PRETO"
  	WHEN usu_cidade = "Santo Andre" THEN "SANTO ANDRÉ"
  	WHEN usu_cidade = "Taubate" THEN "TAUBATÉ"
  	WHEN usu_cidade = "Vitoria" THEN "VITÓRIA"
  	WHEN usu_codpos = 95 THEN NULL
    ELSE CAST(UPPER(usu_cidade) AS string)
  END 									AS city
FROM raw.senior_localtrabalho AS localtrabalho
ORDER BY id DESC;
