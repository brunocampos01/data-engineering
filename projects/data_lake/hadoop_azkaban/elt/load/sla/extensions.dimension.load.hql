merge into sla.dim_extensions extensions
using staging.sccd_ungp_extensions_performed as performed
on performed.worklog = extensions.worklog
    when matched and performed.md5 != hash then update set 
        description = performed.description,
        acceptdate = performed.acceptdate,
        concededate = performed.concededate,
        approval = performed.approval,
        submitdate = performed.submitdate,
        submitter = performed.submitter,
        submiteddate = performed.submiteddate,
        currentdeadline = performed.currentdeadline
    when not matched then insert values (
        performed.id,
        md5(performed.serviceid),
        performed.serviceid,
        performed.worklog,
        performed.site,
        performed.description,
        performed.acceptdate,
        performed.concededate,
        performed.approval,
        performed.submitdate,
        performed.submitter,
        performed.submiteddate,
        performed.currentdeadline,
        performed.md5,
        performed.year_month);
