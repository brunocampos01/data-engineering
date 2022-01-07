create view staging.sccd_ungp_agreements_performed as with request as (
	select row_number() over (partition by recordkey order by createdate asc) as mocked_key, * from (
		(select *,
			   lead(logtype) over record_window1 as nextlog, 
			   lead(recordkey) over record_window1 as nextrecord
		from staging.sccd_ungp_ticket_agreements
		where clientviewable = 1
		window record_window1 as (partition by recordkey order by createdate asc)) a 
	) where logtype = 'ACORDO DE PRAZO' and nextlog = 'APROVAR ACORDO'
), approval as (
	select row_number() over record_window2 as mocked_key, * 
	from staging.sccd_ungp_ticket_agreements 
	where logtype = 'APROVAR ACORDO' 
	and clientviewable = 1
	window record_window2 as (partition by recordkey order by createdate asc)
)
select md5(concat(row_sequence(), request.recordkey)) as id,
	   request.recordkey as serviceid,
	   cast(request.worklogid as string) as worklog,
	   request.siteid as site,
	   request.description,
	   request.createdate as submitdate,
	   request.createby as submitter,
	   approval.createdate as acceptdate,
	   request.sof_ungp_dt_acordo as accepteddate,
	   request.sof_conclusao_ajustada_atual as currentdeadline,
	   approval.createby as approval,
	   md5(concat(request.recordkey, request.siteid, request.description, approval.createby, approval.createdate, request.sof_ungp_dt_acordo)) as md5,
	   date_format(request.createdate, 'y-MM') as year_month
from request, approval
where request.mocked_key = approval.mocked_key and request.nextrecord = approval.recordkey and request.recordkey = approval.recordkey