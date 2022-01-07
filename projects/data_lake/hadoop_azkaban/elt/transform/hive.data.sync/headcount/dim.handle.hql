MERGE INTO headcount.dim_handle dimh
using staging.hyperion_handle stgh
ON (md5(stgh.name) = dimh.id)
WHEN NOT MATCHED THEN INSERT VALUES (
    md5(stgh.name),
    stgh.name);
