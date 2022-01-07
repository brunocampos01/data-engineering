CREATE VIEW IF NOT EXISTS staging.senior_employeementcontracts AS
SELECT
  colaboradores.id,
  colaboradores.name 					AS name,
  empresa.shortname					AS enterpriseShortName,
  registro.employeetype				AS employeetype,
  colaboradores.registerCode,
  colaboradores.cpf,
  contrato.contracttype,
  colaboradores.gender,
  colaboradores.pcd,
  instrucao.educationlevel,
  colaboradores.lasthiringdate,
  colaboradores.hiringdate,
  situacao.worksituation,
  colaboradores.dismissaldate,
  DATEDIFF(COALESCE(colaboradores.dismissaldate, CURRENT_TIMESTAMP), colaboradores.hiringdate)    	 AS daysworktime,
  DATEDIFF(COALESCE(colaboradores.dismissaldate, CURRENT_TIMESTAMP), colaboradores.hiringdate)/365 	 AS yearsworktime,
  desligamentos.dismissalcauses,
  desligamentos.dismissalinitiative,
  colaboradores.birthdaydate,
  DATEDIFF(CURRENT_TIMESTAMP, colaboradores.birthdaydate)/365		AS age,
  localtrabalho.name				AS workplacename,
  localtrabalho.city				AS workplacecity,
  localtrabalho.state				AS workplacestate,
  cargos.positiontitle,
  cargos.positionseniority,
  cargos.fullpositiontitle,
  cargos.positiontitleprofile,
  cargos.positiontype,
  cargos.positioncategory,
  cargos.positiongrade,
  ccu.id							AS costcentercode,
  CASE
  	WHEN (colaboradores.idcompanyarea IS NULL) OR (colaboradores.idcompanyarea = 0) THEN c_sem_un.name
  	ELSE unidade.areaname
  END				AS companyareaname,
  CASE
  	WHEN (colaboradores.idcompanyarea IS NULL) OR (colaboradores.idcompanyarea = 0) THEN c_sem_un.workunity
  	ELSE unidade.businessunityinitial
  END 				AS businessunity,
  colaboradores.teamstructurecode,
  colaboradores.teamcode,
  liderdireto.id						AS iddirectleader,
  liderdireto.name						AS directleadername,
  diretoriaexecutiva.id					AS idexecutiveborder,
  diretoriaexecutiva.name				AS executivebordername,
  diretoria.id							AS idboard,
  diretoria.name						AS boardname,
  gestaonivel1.id						AS idmanagementlevel1,
  gestaonivel1.name						AS managementlevel1name
FROM staging.senior_pre_employeementcontracts AS colaboradores
LEFT JOIN staging.senior_enterprises AS empresa ON colaboradores.identerprise = empresa.id
LEFT JOIN staging.senior_employeetype AS registro ON colaboradores.idemployeetype = registro.id
LEFT JOIN staging.senior_contracttype AS contrato ON colaboradores.idcontracttype = contrato.id
LEFT JOIN staging.senior_educationlevel AS instrucao ON colaboradores.ideducationlevel = instrucao.id
LEFT JOIN staging.senior_worksituation AS situacao ON colaboradores.idworksituation = situacao.id
LEFT JOIN staging.senior_workplace AS localtrabalho ON colaboradores.idworkplace = localtrabalho.id
LEFT JOIN staging.senior_companyareas AS unidade ON colaboradores.idcompanyarea = unidade.id
LEFT JOIN staging.senior_dismissals AS desligamentos ON colaboradores.id = desligamentos.id
LEFT JOIN staging.senior_costcenter AS ccu ON colaboradores.identerprise = ccu.identerprise AND colaboradores.idccusenior = ccu.idccusenior
LEFT JOIN staging.senior_positionsstructure AS cargos ON colaboradores.positionsstructurecode = cargos.positionstructurecode AND colaboradores.idposition = cargos.id
LEFT JOIN staging.senior_employees_nonbusinessunity AS c_sem_un ON colaboradores.id = c_sem_un.id
LEFT JOIN (SELECT * FROM staging.senior_pre_employeementcontracts WHERE idworksituation <> 7) AS liderdireto ON colaboradores.teamstructurecode = liderdireto.teamstructurecode AND colaboradores.directleaderteamcode = liderdireto.teamCode
LEFT JOIN (SELECT * FROM staging.senior_pre_employeementcontracts WHERE idworksituation <> 7) AS diretoriaexecutiva ON colaboradores.teamstructurecode = diretoriaexecutiva.teamstructurecode AND colaboradores.executiveboardteamcode = diretoriaexecutiva.teamCode
LEFT JOIN (SELECT * FROM staging.senior_pre_employeementcontracts WHERE idworksituation <> 7) AS diretoria ON colaboradores.teamstructurecode = diretoria.teamstructurecode AND colaboradores.boardteamcode = diretoria.teamCode
LEFT JOIN (SELECT * FROM staging.senior_pre_employeementcontracts WHERE idworksituation <> 7) AS gestaonivel1 ON colaboradores.teamstructurecode = gestaonivel1.teamstructurecode AND colaboradores.managementlevel1teamcode = gestaonivel1.teamCode
ORDER BY colaboradores.hiringdate DESC;
