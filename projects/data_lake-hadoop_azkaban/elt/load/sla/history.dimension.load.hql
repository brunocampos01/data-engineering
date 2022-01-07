merge into sla.dim_history history 
using staging.sccd_ungp_tickets_history updates 
on history.id = updates.id 
    when matched and updates.hash != history.hash then update set
        id = updates.id,
        br_history = md5(updates.tkstatusid),
        service = updates.ticketid,
        status = updates.`maxvalue`,
        description = updates.statusdescription,
        changedate = updates.changedate,
        timetracking = updates.statustracking
    when not matched then insert values (
        updates.id,
        md5(updates.ticketid),
        updates.ticketid,
        updates.siteid,
        updates.`maxvalue`,
        updates.statusdescription,
        updates.changedate,
        updates.statustracking,
        updates.hash,
        updates.year_month);
