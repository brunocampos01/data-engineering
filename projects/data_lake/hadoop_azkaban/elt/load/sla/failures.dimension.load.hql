merge into sla.dim_failures failures
using staging.sccd_ungp_failures_performed as performed
on performed.worklog = failures.worklog
    when matched and performed.md5 != hash then update set 
        description = performed.description,
        reprovaldate = performed.reprovaldate
    when not matched then insert values (
        performed.id,
        md5(performed.service),
        performed.service,
        performed.worklog,
        performed.site,
        performed.description,
        performed.reprovaldate,
        performed.md5,
        performed.year_month);
