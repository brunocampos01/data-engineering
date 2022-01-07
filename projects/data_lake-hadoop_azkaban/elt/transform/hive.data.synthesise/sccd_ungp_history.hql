CREATE VIEW IF NOT EXISTS staging.sccd_ungp_tickets_history
AS SELECT md5(concat(row_sequence(), statushistory.tkstatusid)) as id, statushistory.*, hash(statushistory.*) as hash FROM (SELECT history.ticketid,
		  history.class,
		  history.changeby,
		  history.changedate,
		  history.memo,
		  history.siteid,
		  history.tkstatusid,
		  history.statustracking as trackedtime,
		  cast(tracking.statustracking[0] as int) + (cast(tracking.statustracking[1] as int) / 60) + (cast(tracking.statustracking[2] as int) / 3600) as statustracking,
		  history.owner,
		  history.ownergroup,
		  history.assignedownergroup,
		  sd.domainid,
		  sd.maxvalue,
		  sd.value,
		  sd.internal,
		  sd.defaults,
		  sd.orgid,
		  sd.description as statusdescription,
		  sd.langcode,
		  date_format(history.changedate, 'y-MM') as year_month
FROM raw.sccd_ungp_ticket_history history
INNER JOIN staging.sccd_ungp_domain sd ON history.status = sd.value AND sd.domainid = 'SRSTATUS' AND sd.langcode = 'PT'
INNER JOIN (select tkstatusid, split(history.statustracking, ':') as statustracking from raw.sccd_ungp_ticket_history history) tracking ON history.tkstatusid = tracking.tkstatusid
ORDER BY history.ticketid, history.changedate) statushistory