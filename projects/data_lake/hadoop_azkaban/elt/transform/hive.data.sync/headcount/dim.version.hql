MERGE INTO headcount.dim_version dimv
using staging.hyperion_version stgv
ON (md5(stgv.name) = dimv.id)
WHEN NOT MATCHED THEN INSERT VALUES (
    md5(stgv.name),
    stgv.name);
