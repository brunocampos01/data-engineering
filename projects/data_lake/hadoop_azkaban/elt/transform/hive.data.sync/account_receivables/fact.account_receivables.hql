INSERT OVERWRITE TABLE account_receivables.fact_account_receivables
SELECT
	ar.sk_date_issuance,
	ar.sk_due_date,
	ar.sk_date_received,
	cc.id,
	bu.id,
	p.id,
	sg.id,
	a.id,
	c.id,
	sv.id,
	i.id,
	md5(ar.clientname),
	md5(ar.contractnumber),
	ar.nrrps,
	ar.nrnfse,
	ar.vlrecebido,
	ar.vlbruto,
	ar.vlliquido,
	ar.vlimpostoretido,
	ar.vloriginalreceber,
	ar.vlrecebidobruto,
	vlajustes,
	vlajustesimposto,
	vlsaldoreceber,
	vlacrescimo,
	CURRENT_TIMESTAMP(),
	0
FROM staging.account_receivables as ar
JOIN account_receivables.dim_business_unit as bu on ar.businessunitcode = bu.code
LEFT JOIN account_receivables.dim_cost_center cc on concat(bu.initials,'-',ar.costcentercode) = cc.costcentercode
LEFT JOIN account_receivables.dim_product p on ar.productcode = p.name
LEFT JOIN account_receivables.dim_segment sg on ar.segmentcode = cast(replace(sg.code,'SG','') as int) and sg.businessunitname = bu.initials
LEFT JOIN account_receivables.dim_account a on ar.accountcode = a.code
LEFT JOIN account_receivables.dim_company c on ar.companycode = c.code
LEFT JOIN account_receivables.dim_service sv on ar.servicecode = cast(replace(sv.code,'S','') as int) and sv.businessUnitInitials = bu.initials
LEFT JOIN account_receivables.dim_intercia i on ar.interciacode = i.code
