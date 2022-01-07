INSERT OVERWRITE TABLE budget_management.dim_observation
select
  md5(so.name),
  so.name
from staging.sharepoint_observations as so
