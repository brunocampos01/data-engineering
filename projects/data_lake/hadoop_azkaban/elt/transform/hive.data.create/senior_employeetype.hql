CREATE TABLE IF NOT EXISTS staging.senior_employeetype (
    id int NOT NULL,
    employeetype string,
    employeetypeinitial string
)
COMMENT 'Tipo de Colaborador entre Pessoas Física e Pessoa Jurídica para classificação de CLT, PJ, estagiários e jovens aprendizes'
STORED AS ORC
TBLPROPERTIES ("transactional" = "true", 'orc.compression' = 'ZLIB');

INSERT INTO TABLE staging.senior_employeetype
VALUES
	(1, 'Pessoa Física', 'PF'),
	(2, 'Pessoa Jurídica', 'PJ');
