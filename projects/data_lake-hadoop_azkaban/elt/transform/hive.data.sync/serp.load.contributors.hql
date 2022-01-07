CREATE EXTERNAL TABLE staging.update_serp_contributors (
    login string,
    email string,
    nome string,
    idcentrocusto string,
    nmcentrocusto string,
    idempresa string,
    nmempresa string,
    idunidade string,
    nmunidade string,
    nucargahoraria string,
    nmcargo string,
    nmperfil string,
    dtadmissao string,
    dtdemissao string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/xpto_company/serp/contributors';

CREATE TABLE IF NOT EXISTS staging.serp_contributors
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
STORED AS ORC
tblproperties ('transactional' = 'true', 'orc.compression' = 'ZLIB')
AS (SELECT update_serp_contributors.*, current_timestamp as load_date, FALSE as archived FROM staging.update_serp_contributors);

CREATE TEMPORARY TABLE staging.diff_serp_contributors
STORED AS ORC
TBLPROPERTIES ("auto.purge"="true")
AS (
   SELECT
        update_serp_contributors.*,
        CASE
            WHEN (update_serp_contributors.login <> serp_contributors.login
            OR update_serp_contributors.nome <> serp_contributors.nome
            OR update_serp_contributors.idcentrocusto <> serp_contributors.idcentrocusto
            OR update_serp_contributors.nmcentrocusto <> serp_contributors.nmcentrocusto
            OR update_serp_contributors.idempresa <> serp_contributors.idempresa
            OR update_serp_contributors.nmempresa <> serp_contributors.nmempresa
            OR update_serp_contributors.idunidade <> serp_contributors.idunidade
            OR update_serp_contributors.nmunidade <> serp_contributors.nmunidade
            OR update_serp_contributors.nucargahoraria <> serp_contributors.nucargahoraria
            OR update_serp_contributors.nmcargo <> serp_contributors.nmcargo
            OR update_serp_contributors.nmperfil <> serp_contributors.nmperfil
            OR update_serp_contributors.dtadmissao <> serp_contributors.dtadmissao
            OR update_serp_contributors.dtdemissao <> serp_contributors.dtdemissao) THEN TRUE
            ELSE FALSE
        END AS changed
    FROM staging.serp_contributors, staging.update_serp_contributors
    WHERE serp_contributors.login = update_serp_contributors.login
    AND NOT serp_contributors.archived
);

MERGE INTO staging.serp_contributors c USING (
    SELECT *, FALSE AS to_filed FROM staging.diff_serp_contributors
    UNION ALL
    SELECT *, TRUE AS to_filed FROM staging.diff_serp_contributors WHERE changed
)
AS updates ON updates.login = c.login AND NOT c.archived
WHEN matched AND updates.changed AND updates.to_filed
    THEN UPDATE SET archived = TRUE
WHEN matched AND updates.changed AND NOT updates.to_filed THEN INSERT VALUES (
    updates.login, updates.email, updates.nome, updates.idcentrocusto, updates.nmcentrocusto, updates.idempresa,
    updates.nmempresa, updates.idunidade, updates.nmunidade, updates.nucargahoraria, updates.nmcargo, updates.nmperfil, updates.dtadmissao,
    updates.dtdemissao, current_timestamp, FALSE)
WHEN NOT matched THEN INSERT VALUES (
    updates.login, updates.email, updates.nome, updates.idcentrocusto, updates.nmcentrocusto, updates.idempresa,
    updates.nmempresa, updates.idunidade, updates.nmunidade, updates.nucargahoraria, updates.nmcargo, updates.nmperfil, updates.dtadmissao,
    updates.dtdemissao, current_timestamp, FALSE);

-- ------------------------------------------------------ ATUAL

CREATE EXTERNAL TABLE IF NOT EXISTS staging.update_serp_contributors (
    login string,
    email string,
    nome string,
    idcentrocusto string,
    nmcentrocusto string,
    idempresa string,
    nmempresa string,
    idunidade string,
    nmunidade string,
    nucargahoraria string,
    nmcargo string,
    nmperfil string,
    dtadmissao string,
    dtdemissao string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hadoop/xpto_company/serp/contributors';

CREATE TABLE IF NOT EXISTS staging.serp_contributors
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
STORED AS ORC
tblproperties ('transactional' = 'true', 'orc.compression' = 'ZLIB')
AS (SELECT update_serp_contributors.*, current_timestamp as load_date, FALSE as archived FROM staging.update_serp_contributors);

DROP TABLE staging.diff_serp_contributors;

CREATE TABLE staging.diff_serp_contributors
STORED AS ORC
TBLPROPERTIES ("auto.purge"="true")
AS (
   SELECT
        update_serp_contributors.*,
        CASE
            WHEN (update_serp_contributors.login <> serp_contributors.login
            OR update_serp_contributors.nome <> serp_contributors.nome
            OR update_serp_contributors.idcentrocusto <> serp_contributors.idcentrocusto
            OR update_serp_contributors.nmcentrocusto <> serp_contributors.nmcentrocusto
            OR update_serp_contributors.idempresa <> serp_contributors.idempresa
            OR update_serp_contributors.nmempresa <> serp_contributors.nmempresa
            OR update_serp_contributors.idunidade <> serp_contributors.idunidade
            OR update_serp_contributors.nmunidade <> serp_contributors.nmunidade
            OR update_serp_contributors.nucargahoraria <> serp_contributors.nucargahoraria
            OR update_serp_contributors.nmcargo <> serp_contributors.nmcargo
            OR update_serp_contributors.nmperfil <> serp_contributors.nmperfil
            OR update_serp_contributors.dtadmissao <> serp_contributors.dtadmissao
            OR update_serp_contributors.dtdemissao <> serp_contributors.dtdemissao) THEN TRUE
            ELSE FALSE
        END AS changed
    FROM staging.serp_contributors, staging.update_serp_contributors
    WHERE serp_contributors.login = update_serp_contributors.login
    AND NOT serp_contributors.archived
);

UPDATE staging.serp_contributors SET archived = TRUE WHERE login in (SELECT login from staging.diff_serp_contributors WHERE changed)

INSERT INTO TABLE staging.serp_contributors SELECT updates.login, updates.email, updates.nome, updates.idcentrocusto, updates.nmcentrocusto, updates.idempresa,
    updates.nmempresa, updates.idunidade, updates.nmunidade, updates.nucargahoraria, updates.nmcargo, updates.nmperfil, updates.dtadmissao,
    updates.dtdemissao, current_timestamp, FALSE FROM staging.diff_serp_contributors updates WHERE changed

INSERT INTO TABLE staging.serp_contributors SELECT updates.login, updates.email, updates.nome, updates.idcentrocusto, updates.nmcentrocusto, updates.idempresa,
    updates.nmempresa, updates.idunidade, updates.nmunidade, updates.nucargahoraria, updates.nmcargo, updates.nmperfil, updates.dtadmissao,
    updates.dtdemissao, current_timestamp, FALSE FROM staging.update_serp_contributors updates LEFT JOIN staging.serp_contributors c ON c.login = updates.login WHERE c.login is null

select * from staging.diff_serp_contributors
select * from staging.serp_contributors where login = 'CRISTIANO.BARROS'
select * from staging.update_serp_contributors
