create view staging.sccd_ungp_contracts as 
    select contracts.*, hash(*) as hash from (
        (select md5(concat(row_sequence(), contractnum)) as id, 
            contractnum as legacy,
            NULL as contract,
            description,
            sof_status as status, 
            sof_gestor as manager, 
            enddate as expiration, 
            renewaldate as extension, 
            NULL as warranty, 
            initcap(orgid) as enterprise,
            sof_siteid as site,
            NULL as sector,
            NULL as department,
            90 as nms,
            1 as reductor,
            5 as tolerance,
            sof_cliente as customer,
            sof_cliente as initials,
            date_format(statusdate, 'y-MM') as year_month
        from raw.sccd_ungp_contracts 
        where sof_siteid ='UNGP') contracts
)
