CREATE VIEW IF NOT EXISTS staging.senior_managementlevel1 AS
SELECT DISTINCT
  employee.id,
  employee.name AS managementlevel1name,
  employee.teamstructurecode,
  employee.teamcode,
  employee.enterpriseshortname,
  employee.employeetype,
  employee.registercode,
  employee.contracttype,
  employee.gender,
  employee.pcd,
  employee.educationlevel,
  employee.hiringdate,
  employee.worksituation,
  employee.dismissaldate,
  employee.dismissalcauses,
  employee.birthdaydate,
  employee.workplacename,
  employee.workplacecity,
  employee.workplacestate,
  employee.cpf,
  employee.costcentercode,
  employee.companyareaname,
  employee.businessunity,
  employee.positiontitle,
  employee.positionseniority,
  employee.fullpositiontitle,
  employee.positiontitleprofile,
  employee.positiontype,
  employee.positioncategory,
  employee.positiongrade
FROM staging.senior_employeementcontracts AS employee
	INNER JOIN staging.senior_employeementcontracts AS managementlevel1 ON employee.id = managementlevel1.idmanagementlevel1
	WHERE
		employee.positiontype = 'Coordenadoria' OR
		employee.positiontype = 'Gerência';
