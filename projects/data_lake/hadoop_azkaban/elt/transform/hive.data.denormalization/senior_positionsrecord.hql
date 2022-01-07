CREATE VIEW IF NOT EXISTS staging.senior_positionsrecord AS
SELECT
  pr.idEmployeeSenior,
  employee.cpf,
  pr.positionStructurecode,
  pr.idposition,
  pr.positiontitle,
  pr.positionseniority,
  pr.fullpositiontitle,
  pr.positiontitleprofile,
  pr.positiontype,
  pr.positioncategory,
  pr.positiongrade,
  pr.positionUpdateDate			AS positionStartDate,
  CASE
		WHEN (((LEAD(pr.positionUpdateDate) OVER (PARTITION BY pr.idemployeesenior ORDER BY pr.positionUpdateDate)) IS NULL) AND
			 (employee.dismissaldate IS NOT NULL))
			THEN employee.dismissaldate
		ELSE LEAD(pr.positionUpdateDate) OVER (PARTITION BY pr.idemployeesenior ORDER BY pr.positionUpdateDate)
	END													AS positionEndDate,
	IF(
		employee.hiringdate =
			pr.positionUpdateDate,
		'true',
		'false')										AS flagHiringStartJob
FROM staging.senior_pre_positionsrecord AS pr
	LEFT JOIN staging.senior_pre_employeementcontracts AS employee ON employee.id = pr.idemployeesenior
ORDER BY idEmployeeSenior DESC

-------------------------------------------------------------------------------------------

CREATE VIEW IF NOT EXISTS test.senior_jobsTitlesRecord AS
SELECT
	md5(concat(row_sequence(),
			h_cargos.idcolaboradorsenior,
			cargos.idestrutura,
			cargos.id))				AS skJobsTitlesRecord,
	concat(h_cargos.idcolaboradorsenior,
			cargos.idestrutura,
			cargos.id)				AS nkJobsTitlesRecord,
	h_cargos.idcolaboradorsenior 	AS employeeCode,
	h_cargos.dataalteracaocargo		AS updateDate,
	cargos.id 						AS jobCode,
	cargos.idestrutura				AS jobStructurecode,
	cargos.nomesemnivel       		AS jobTitle,
	cargos.nivel					AS jobSeniority,
	cargos.nomecomnivel				AS fullJobTitle,
	cargos.perfil					AS jobTitleProfile,
	cargos.tipo						AS typeJob,
	cargos.categoria				AS jobCategory,
	CASE
	 WHEN (h_cargos.idestruturacargo = colaboradores.idestruturacargo AND h_cargos.idcargo = colaboradores.idcargo) THEN 1
	 ELSE 0
	END											AS flagLastJob,
	CASE
	 WHEN (h_cargos.dataalteracaocargo = colaboradores.dataadmissaotempodeempresa) THEN 1
	 WHEN (h_cargos.dataalteracaocargo = colaboradores.dataadmissao) THEN 1
	 ELSE 0
	END											AS flagFirstJob
FROM staging.senior_historicocargos AS h_cargos
JOIN staging.senior_cargos AS cargos
	ON h_cargos.idestruturacargo = cargos.idestrutura
	AND h_cargos.idcargo = cargos.id
LEFT JOIN staging.senior_colaboradores AS colaboradores ON h_cargos.idcolaboradorsenior = colaboradores.id;
