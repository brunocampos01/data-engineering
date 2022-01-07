create view staging.serp_effort_external_appointments as 
	select vrtc.idquadrohorarioitem as itemid, 
		   vrtc.workitemid as request, 
		   vrtc.url as url,
		   vrtc.dejustificativa as justification, 
		   cast(((vrtc.nuhoras) / 60) as decimal) as ext_effort, 
		   'RTC' as extsys from raw.serp_vinculortc vrtc
	union all
	select vsccd.idquadrohorarioitem as itemid, 
		   vsccd.chamado as request, 
		   NULL as url, 
		   vsccd.dejustificativa as justification, 
		   cast(((vsccd.nuhoras) / 60) as decimal) as ext_effort, 
		   'SCCD' as extsys from raw.serp_vinculosccd vsccd