merge into sla.dim_agreements agreements 
using staging.sccd_ungp_agreements_performed as performed
on performed.worklog = agreements.worklog
    when matched and performed.md5 != hash then update set 
        br_agreement = md5(performed.serviceid), 
        description = performed.description,
        acceptdate = performed.acceptdate,
        accepteddate = performed.accepteddate,
        approval = performed.approval,
        submitter = performed.submitter,
        submitdate = performed.submitdate,
        currentdeadline = performed.currentdeadline
    when not matched then insert values (
        performed.id, 
        md5(performed.serviceid), 
        performed.serviceid, 
        performed.worklog, 
        performed.site, 
        performed.description, 
        performed.acceptdate, 
        performed.accepteddate,
        performed.approval,
        performed.submitter,
        performed.submitdate,
        performed.currentdeadline,
        performed.md5,
        performed.year_month);
