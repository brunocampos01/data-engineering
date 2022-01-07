create view staging.sccd_ungp_decessions_performed as with request as (
	select row_number() over (partition by recordkey order by createdate asc) as mocked_key, * from (
		(select *,
			   lead(logtype) over record_window1 as nextlog, 
			   lead(recordkey) over record_window1 as nextrecord
		from staging.sccd_ungp_ticket_decessions
		where clientviewable = 1
		window record_window1 as (partition by recordkey order by createdate asc)) a 
	) where logtype = 'SOLICITA ALT SEVERIDADE' and nextlog = 'APROVAR SEVERIDADE'
), approval as (
	select row_number() over record_window2 as mocked_key, * 
	from staging.sccd_ungp_ticket_decessions
	where logtype = 'APROVAR SEVERIDADE' 
	and clientviewable = 1
	window record_window2 as (partition by recordkey order by createdate asc)
)
select md5(concat(row_sequence(), request.recordkey)) as id,
	   request.recordkey as serviceid,
	   cast(request.worklogid as string) as worklog,
	   request.siteid as site,
	   request.description,
	   approval.description as approvaldescription,
	   request.createdate as submitdate,
	   request.createby as submitter,
	   initcap(request.sof_ungp_assunto_log) as original,
	   initcap(request.sof_ungp_assunto_log) as renew,
	   approval.createdate as acceptdate,
	   request.sof_conclusao_ajustada_atual as currentdeadline,
	   approval.createby as approval,
	   md5(concat(request.description, approval.description, request.createdate, request.createby, initcap(request.sof_ungp_assunto_log), initcap(request.sof_ungp_assunto_log), approval.createdate, approval.createby)) as md5,
	   date_format(request.createdate, 'y-MM') as year_month
from request, approval
where request.mocked_key = approval.mocked_key and request.nextrecord = approval.recordkey and request.recordkey = approval.recordkey