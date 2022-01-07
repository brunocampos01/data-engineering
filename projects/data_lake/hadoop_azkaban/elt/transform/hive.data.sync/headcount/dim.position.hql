MERGE INTO headcount.dim_position dimp
using staging.hyperion_position stgp
ON (md5(stgp.name) = dimp.id)
WHEN NOT MATCHED THEN INSERT VALUES (
    md5(stgp.name),
    stgp.name);
