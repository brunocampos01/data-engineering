CREATE TABLE IF NOT EXISTS staging.senior_contracttype (
    id int NOT NULL,
    contracttype string
)
COMMENT 'Classificação dos tipos de contratos dos colaboradores da xpto_company'
STORED AS ORC
TBLPROPERTIES ("transactional" = "true", 'orc.compression' = 'ZLIB');

INSERT INTO TABLE staging.senior_contracttype
VALUES
	(0, 'Terceiro'),
	(1, 'Empregado'),
	(2, 'Diretor Executivo'),
	(5, 'Estagiário'),
	(6, 'Jovem aprendiz');
