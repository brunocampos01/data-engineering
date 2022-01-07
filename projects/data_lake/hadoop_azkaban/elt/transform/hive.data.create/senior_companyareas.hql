CREATE VIEW IF NOT EXISTS staging.senior_companyareas AS
SELECT DISTINCT
  CAST(usu_unidade     AS  int)		               AS id,
  CASE
     WHEN usu_unidade = 1  THEN "BR001"
     WHEN usu_unidade = 2  THEN "BR002"
     WHEN usu_unidade = 3  THEN "BR003"
     WHEN usu_unidade = 4  THEN "BR004"
     WHEN usu_unidade = 5  THEN "BR005"
     WHEN usu_unidade = 6  THEN "BR005"
     WHEN usu_unidade = 7  THEN "BR005"
     WHEN usu_unidade = 8  THEN "BR005"
     WHEN usu_unidade = 9  THEN "BR005"
     WHEN usu_unidade = 10 THEN "BR005"
     WHEN usu_unidade = 11 THEN "BR006"
     WHEN usu_unidade = 12 THEN "BR006"
     WHEN usu_unidade = 13 THEN "BR005"
     WHEN usu_unidade = 14 THEN "BR005"
     WHEN usu_unidade = 15 THEN "BR007"
     ELSE NULL
  END                                           AS controllershipCode,
  CASE
     WHEN usu_unidade = 1  THEN "Justiça"
     WHEN usu_unidade = 2  THEN "Gestão Pública"
     WHEN usu_unidade = 3  THEN "Indústria da Construção"
     WHEN usu_unidade = 4  THEN "Gestão Municipal"
     WHEN usu_unidade = 5  THEN "ADM/FIN"
     WHEN usu_unidade = 6  THEN "DHO"
     WHEN usu_unidade = 7  THEN "TI"
     WHEN usu_unidade = 8  THEN "Marketing"
     WHEN usu_unidade = 9  THEN "Pesquisa e Desenvolvimento"
     WHEN usu_unidade = 10 THEN "Qualidade"
     WHEN usu_unidade = 11 THEN "Investimento"
     WHEN usu_unidade = 12 THEN "ARCO"
     WHEN usu_unidade = 13 THEN "ARCO"
     WHEN usu_unidade = 14 THEN "ARCO"
     WHEN usu_unidade = 15 THEN "Saúde"
     ELSE NULL
  END                                            AS areaName,
  CASE
     WHEN usu_unidade = 1  THEN "UNJ"
     WHEN usu_unidade = 2  THEN "UNGP"
     WHEN usu_unidade = 3  THEN "UNIC"
     WHEN usu_unidade = 4  THEN "UNGP"
     WHEN usu_unidade = 5  THEN "UC"
     WHEN usu_unidade = 6  THEN "UC"
     WHEN usu_unidade = 7  THEN "UC"
     WHEN usu_unidade = 8  THEN "UC"
     WHEN usu_unidade = 9  THEN "UC"
     WHEN usu_unidade = 10 THEN "UC"
     WHEN usu_unidade = 11 THEN "INV"
     WHEN usu_unidade = 12 THEN "UC"
     WHEN usu_unidade = 13 THEN "UC"
     WHEN usu_unidade = 14 THEN "UC"
     WHEN usu_unidade = 15 THEN "UNHC"
     ELSE NULL
  END                                            AS businessUnityInitial
FROM raw.senior_centrosdecusto AS setoresnegocio
WHERE usu_unidade IS NOT NULL;
