drop table if exists sla.temp_fact_sla;

create temporary table sla.temp_fact_sla as select fact_data.sk_service as id, fact_data.* from (
    select distinct
       services.id as sk_service,
       contracts.id as sk_contract,
       history.br_history as sk_history,
       cast(NULL as string) as sk_product,
       extensions.id as sk_extensions,
       agreements.id as sk_agreements,
       decessions.id as sk_decessions,
       failures.id as sk_failures,
       sla.siteid as site,
       sla.fulfillment,
       sla.contractualfulfillment, 
       sla.holdtime, 
       sla.leadtime, 
       sla.cycletime, 
       sla.reactiontime, 
       cast(NULL as string) as agregatedtime,
       sla.failures,
       sla.extensions,
       sla.agreements,
       sla.decessions,
       CURRENT_TIMESTAMP() as chargedate,
       hash(sla.contractualfulfillment, sla.holdtime, sla.leadtime, sla.cycletime, sla.reactiontime, sla.failures, sla.extensions, sla.agreements, sla.decessions) as hash,
       FALSE as archived,
       sla.sysorigin,
       sla.year_month
    from staging.sccd_ungp_ticket_progress sla
    inner join sla.dim_service services on sla.ticketid = services.service
    left join sla.dim_contract contracts on contracts.description = services.contract
    left join sla.dim_history history on history.br_history = services.id
    left join sla.dim_agreements agreements on agreements.id = services.id
    left join sla.dim_decessions decessions on decessions.id = services.id
    left join sla.dim_extensions extensions on extensions.id = services.id
    left join sla.dim_failures failures on failures.id = services.id
) fact_data;

merge into sla.fact_sla sla
using sla.temp_fact_sla updates
on sla.id = updates.id
    when matched and sla.hash != updates.hash then update set
       sk_contract = updates.sk_contract,
       sk_agreements = updates.sk_agreements,
       sk_decessions = updates.sk_decessions,
       sk_extensions = updates.sk_extensions,
       sk_failures = updates.sk_failures,
       fulfillment = updates.fulfillment,
       contractualfulfillment = updates.contractualfulfillment,
       holdtime = updates.holdtime,
       leadtime = updates.leadtime,
       cycletime = updates.cycletime,
       reactiontime = updates.reactiontime,
       agregatedtime = updates.agregatedtime,
       failures = updates.failures,
       extensions = updates.extensions,
       agreements = updates.agreements,
       decessions = updates.decessions,
       chargedate = updates.chargedate,
       hash = updates.hash
    when not matched then insert values (
       updates.id,
       updates.sk_service,
       updates.sk_contract,
       updates.sk_history,
       updates.sk_product,
       updates.sk_extensions,
       updates.sk_agreements,
       updates.sk_decessions,
       updates.sk_failures,
       updates.site,
       updates.contractualfulfillment, 
       updates.fulfillment,
       updates.holdtime, 
       updates.leadtime, 
       updates.cycletime, 
       updates.reactiontime, 
       updates.agregatedtime,
       updates.failures, 
       updates.extensions, 
       updates.agreements, 
       updates.decessions,
       updates.chargedate,
       updates.hash,
       updates.archived,
       updates.sysorigin,
       updates.year_month);
