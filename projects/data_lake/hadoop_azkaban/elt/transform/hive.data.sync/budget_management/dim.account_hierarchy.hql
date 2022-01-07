INSERT OVERWRITE TABLE budget_management.dim_account_hierarchy
select
if ((cast (trim(nmconta) as string) rlike '[0-9^]'),
      md5(trim(split(nmconta, '-')[0])),
      md5(nmconta)
    ) as id,
deorigem as source,
nmhierarquia as hierarchy,
nuordem as orderNumber,
nmconta as account,
nmcontanivel01 as accountlevel01,
nmcontanivel02 as accountlevel02,
nmcontanivel03 as accountlevel03,
nmcontanivel04 as accountlevel04,
nmcontanivel05 as accountlevel05,
nmcontanivel06 as accountlevel06,
nmcontanivel07 as accountlevel07,
nmcontanivel08 as accountlevel08,
nmcontanivel09 as accountlevel09,
nmcontanivel10 as accountlevel10,
nuordemnivel01 as orderlevel01,
nuordemnivel02 as orderlevel02,
nuordemnivel03 as orderlevel03,
nuordemnivel04 as orderlevel04,
nuordemnivel05 as orderlevel05,
nuordemnivel06 as orderlevel06,
nuordemnivel07 as orderlevel07,
nuordemnivel08 as orderlevel08,
nuordemnivel09 as orderlevel09,
nuordemnivel10 as orderlevel10
from staging.account_hierarchy
order by nuordem
