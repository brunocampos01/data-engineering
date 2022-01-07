alter view staging.serp_appointments as with appointments as (
	select ap.*, 
		   min(flowid) over(partition by itemid) as firstflowid,
		   min(changedate) over(partition by itemid) as firstchange
	from (
		select pa.name as costproject, 
		   cast(i.dtapontamento as timestamp) as appointment, 
		   i.idquadrohorarioitem as itemid, 
		   cast(i.nuhoras as decimal) / 60 as effort, 
		   i.idtarefa as taskid,
		   i.dejustificativa as vindicatory,
		   q.cdusuario as employee,
		   q.cdusuario as username,
		   f.idfluxo as flowid,
		   f.idsituacaofluxo as flowstateid,
		   cast(f.dtacaofluxo as timestamp) as changedate,
		   f.cdusuarioresponsavel as approval, 
		   s.dssituacaofluxo as state,
		   row_number() over(partition by i.idquadrohorarioitem, f.idsituacaofluxo order by f.dtacaofluxo desc) as version,
		   lead(f.idfluxo) over(partition by i.idquadrohorarioitem order by f.dtacaofluxo) as nextflowid
		from raw.serp_quadrohorarioitem i 
		inner join raw.serp_quadrohorario q on q.idquadrohorario = i.idquadrohorario
		inner join raw.serp_quadrohorariofluxo f on i.idquadrohorarioitem = f.idquadrohorarioitem
		inner join raw.serp_situacaofluxo s on f.idsituacaofluxo = s.idsituacaofluxo
		inner join raw.cifs_oracle_pa_projetos pa on pa.project_id = i.idprojeto
	) ap where ap.version = 1 
) select ax.costproject, 
		 ax.appointment, 
		 ax.taskid, 
		 ax.itemid, 
		 ax.vindicatory, 
		 ax.effort, 
		 ax.username, 
		 ax.firstflowid,
		 a1.changedate as creationdate, 
		 a1.state as creationstate, 
		 a2.changedate as submitdate, 
		 a2.state as submitstate, 
		 a3.changedate as approvaldate, 
		 a3.state as approvalstate,
		 a3.approval,
		 c.idunidade as businessunity,
		 c.idmatricula as enrolment,
		 initcap(c.nmcolaborador) as employee,
		 (c.dtdemissao IS NULL) as activeemployee,
		 c.nmunidade as businessunitydescription,
		 c.idempresa as enterprisecode,
		 c.nmempresa as enterprise,
		 c.idcentrocusto as costcerter,
		 coalesce(a4.state, a3.state, a2.state, a1.state) as actualstate
		 from appointments ax
left join appointments a1 on ax.username = a1.username and ax.taskid = a1.taskid and ax.appointment = a1.appointment and a1.flowstateid = 1 and ax.firstflowid = a1.flowid
left join appointments a2 on ax.username = a2.username and ax.taskid = a2.taskid and ax.appointment = a2.appointment and a2.flowstateid = 2 
left join appointments a3 on ax.username = a3.username and ax.taskid = a3.taskid and ax.appointment = a3.appointment and a3.flowstateid = 3 
left join appointments a4 on ax.username = a4.username and ax.taskid = a4.taskid and ax.appointment = a4.appointment and a4.flowstateid = 6
inner join raw.serp_colaboradores c on ax.username = split(c.dsemail, '@')[0]
where ax.flowid = ax.firstflowid
order by ax.appointment desc