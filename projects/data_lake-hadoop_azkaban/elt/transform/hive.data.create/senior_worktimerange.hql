CREATE TABLE IF NOT EXISTS staging.senior_worktimerange (
    id int NOT NULL,
    worktimerange string
)
COMMENT 'Classificação do tempo de empresa de colaboradores em anos'
STORED AS ORC
TBLPROPERTIES ("transactional" = "true", 'orc.compression' = 'ZLIB');

INSERT INTO TABLE staging.senior_worktimerange
VALUES
	(1, 'abaixo de 1 ano'),
	(2, 'de 1 a ~2 anos'),
	(3, 'de 2 a ~5 anos'),
	(4, 'de 5 a ~10 anos'),
	(5, 'mais que 10 anos');
