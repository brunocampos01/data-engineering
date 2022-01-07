merge into sla.dim_decessions decessions
using staging.sccd_ungp_decessions_performed as performed
on performed.worklog = decessions.worklog
    when matched and performed.md5 != hash then update set 
        description = performed.description, 
        approvaldescription = performed.approvaldescription, 
        submitdate = performed.submitdate,
        submitter = performed.submitter,
        acceptdate = performed.acceptdate,
        approval = performed.approval,
        original = performed.original,
        renew = performed.renew,
        currentdeadline = performed.currentdeadline
    when not matched then insert values (
        performed.id,
        md5(performed.serviceid),
        performed.serviceid,
        performed.worklog, 
        performed.site,
        performed.description,
        performed.approvaldescription, 
        performed.submitdate,
        performed.submitter,
        performed.acceptdate,
        performed.approval,
        performed.original,
        performed.renew,
        performed.currentdeadline,
        performed.md5,
        performed.year_month);
