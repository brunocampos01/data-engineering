CREATE VIEW staging.oracle_contract AS
select distinct l.LIN_NR_CONTRATO as nrContrato
from raw.oracle_ar_lancamentos as l
