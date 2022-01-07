CREATE TABLE IF NOT EXISTS staging.senior_positiontype (
    id int NOT NULL,
    positiontype string,
    positioncategory string,
    hierarchy int
)
COMMENT 'Tipo de Cargo para classificação do organograma de cargos'
STORED AS ORC
TBLPROPERTIES ("transactional" = "true", 'orc.compression' = 'ZLIB');

INSERT INTO TABLE staging.senior_positiontype
VALUES
	(0, NULL, null, 0),
	(1, 'Diretoria', 'Liderança', 2),
	(2, 'Gerência', 'Liderança', 3),
	(3, 'Coordenadoria', 'Liderança', 4),
	(4, 'Especialista', 'Colaborador', 5),
	(5, 'Colaborador', 'Colaborador', 6),
	(6, 'Estagiário', 'Colaborador', 7),
	(7, 'Jovem Aprendiz', 'Colaborador', 8),
	(8, 'Diretoria Executiva', 'Liderança', 1);
