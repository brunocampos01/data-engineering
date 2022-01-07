CREATE TABLE IF NOT EXISTS staging.senior_agerange (
    id int NOT NULL,
    agerange string
)
COMMENT 'Classificação da faixa etária dos colaboradores em anos'
STORED AS ORC
TBLPROPERTIES ("transactional" = "true", 'orc.compression' = 'ZLIB');

INSERT INTO TABLE staging.senior_agerange
VALUES
	(1, 'menos de 18 anos'),
	(2, 'de 18 a ~26 anos'),
	(3, 'de 26 a ~36 anos'),
	(4, 'de 36 a ~45 anos'),
	(5, 'mais de 45 anos');
