CREATE VIEW staging.sccd_ungp_ticket_progress
AS SELECT md5(ticketid) as id, *, hash(*) as hash from (
	SELECT srs.*,
		   externalsystem.description as modal,
		   nvl(extensions.extensions, 0) as extensions,
		   nvl(agreements.agreements, 0) as agreements,
		   nvl(decessions.decessions, 0) as decessions,
		   nvl(failures.failures, 0) as failures,
		   (actualfinish <= adjustedtargetresolutiontime) as fulfillment,
		   (actualfinish <= targetfinish) as contractualfulfillment,
		   (nvl(cast(unix_timestamp(actualfinish) - unix_timestamp(creationdate) as int), 0.0) / 3600) as leadtime,
		   (nvl(cast(unix_timestamp(actualfinish) - unix_timestamp(actualstart) as int), 0.0) / 3600) as cycletime,
		   (nvl(cast(unix_timestamp(actualstart) - unix_timestamp(creationdate) as int), 0.0) / 3600) as reactiontime,
		   (nvl(cast(unix_timestamp(actualfinish) - unix_timestamp(adjustedtargetresolutiontime) as int), 0.0) / 3600) as exceededtime,
		   nvl(holdtime.holdtime, 0.0) as holdtime,
		   (nvl(cast(unix_timestamp(actualfinish) - unix_timestamp(adjustedtargetresolutiontime) as int), 0.0) / 3600) - holdtime.holdtime as exceededtimediscount, 
		   if(srs.classificationid not in ('SOF_UNGP_SR_DUVIDA', 'SOF_UNGP_SR_EVOLUTIVA', 'SOF_UNGP_SR_INCIDENTEINFRAESTRUTURA', 'SOF_UNGP_SR_INCIDENTESISTEMA', 'SOF_UNGP_SR_PROBLEMASISTEMA', 'SOF_UNGP_SR_SERVICO'), 'Nível 1 - Atendimento de Suporte Local', 'Nível 2 - Atendimento de Suporte Remoto') as servicelevel,
		   if(actualfinish IS NULL, 'Aberto', 'Resolvido') as state,
		   product.description as product, 
           module.description as module, 
           feature.description as feature,
           'Smart Cloud Control Desk' as sysorigin,
		   date_format(srs.creationdate, 'y-MM') as year_month
	FROM raw.sccd_ungp_srs srs
	INNER JOIN staging.sccd_ungp_domain externalsystem 
		ON srs.externalsystem = externalsystem.value 
		AND externalsystem.domainid = 'TSDTKTSOURCE'
		AND externalsystem.langcode = 'PT'
	LEFT JOIN raw.sccd_ungp_ci product on srs.cinum = product.cinum
	LEFT JOIN raw.sccd_ungp_ci module on srs.sof_ungp_modulo = module.cinum
	LEFT JOIN raw.sccd_ungp_ci feature on srs.sof_ungp_func = feature.cinum
	--LEFT JOIN raw.sccd_ungp_contract_collection cc on cc.sof_cinum = srs.cinum
	LEFT JOIN (
		SELECT ticketid, sum(statustracking) as holdtime
		FROM staging.sccd_ungp_tickets_history WHERE `maxvalue` = 'SLAHOLD' GROUP BY ticketid
	) holdtime on holdtime.ticketid = srs.ticketid
	LEFT JOIN ( 
		select serviceid, count(serviceid) as extensions from staging.sccd_ungp_extensions_performed group by serviceid
	) extensions ON extensions.serviceid = srs.ticketid
	LEFT JOIN (
		select serviceid, count(serviceid) as agreements from staging.sccd_ungp_agreements_performed group by serviceid
	) agreements ON agreements.serviceid = srs.ticketid
	LEFT JOIN (
		select serviceid, count(serviceid) as decessions from staging.sccd_ungp_decessions_performed group by serviceid
	) decessions ON decessions.serviceid = srs.ticketid
	LEFT JOIN (
		select service, count(service) as failures from staging.sccd_ungp_failures_performed group by service
	) failures ON failures.service = srs.ticketid
	DISTRIBUTE BY srs.classificationid SORT BY ticketid, creationdate ASC
) progress