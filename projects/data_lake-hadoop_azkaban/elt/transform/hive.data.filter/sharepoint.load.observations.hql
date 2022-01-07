CREATE VIEW staging.sharepoint_observations AS
select
	distinct
	deObservacao as name
from raw.sharepoint_ajustes_manuais as sam
