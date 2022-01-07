INSERT OVERWRITE TABLE budget_management.dim_version
select
  md5(stgv.name),
  stgv.name
from staging.hyperion_version as stgv
