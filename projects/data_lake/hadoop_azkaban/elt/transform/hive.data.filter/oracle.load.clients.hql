CREATE VIEW staging.oracle_clients AS
select distinct c.cliente_nome as nomeCliente
from raw.oracle_ar_capa as c
