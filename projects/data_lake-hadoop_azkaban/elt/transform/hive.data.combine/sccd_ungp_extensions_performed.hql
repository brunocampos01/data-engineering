create view staging.sccd_ungp_extensions_performed as with request as (
	select row_number() over (partition by recordkey order by createdate asc) as mocked_key, * from (
		(select *,
			   lead(logtype) over record_window1 as nextlog, 
			   lead(recordkey) over record_window1 as nextrecord
		from staging.sccd_ungp_ticket_extensions
		where clientviewable = 1
		--and recordkey = 178023
		window record_window1 as (partition by recordkey order by createdate asc)) a 
	) where logtype = 'SOLICITAR DEF. PRAZO' and nextlog = 'ACEITA PRORROGAÇÃO'
), approval as (
	select row_number() over record_window2 as mocked_key, * 
	from staging.sccd_ungp_ticket_extensions 
	where logtype = 'ACEITA PRORROGAÇÃO' 
	and clientviewable = 1
	--and recordkey = 178023
	window record_window2 as (partition by recordkey order by createdate asc)
)
select md5(concat(row_sequence(), request.recordkey)) as id,
	   cast(request.recordkey as string) as serviceid,
	   cast(request.worklogid as string) as worklog,
	   request.siteid as site,
	   request.description,
	   request.createdate as submitdate,
	   request.createby as submitter,
	   request.sof_ungp_dt_prorrog_prazo as submiteddate,
	   approval.createdate as acceptdate,
	   nvl(approval.sof_ungp_dt_prorrog_aceita, request.sof_ungp_dt_prorrog_prazo) as concededate,
	   approval.createby as approval,
	   approval.sof_conclusao_ajustada_atual as currentdeadline,
	   md5(concat(request.description, request.createdate, request.createby, request.sof_ungp_dt_prorrog_prazo, approval.createdate, nvl(approval.sof_ungp_dt_prorrog_aceita, request.sof_ungp_dt_prorrog_prazo), approval.createby, approval.sof_conclusao_ajustada_atual)) as md5,
	   date_format(request.createdate, 'y-MM') as year_month
from request, approval
where request.mocked_key = approval.mocked_key and request.nextrecord = approval.recordkey and request.recordkey = approval.recordkey