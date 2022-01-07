CREATE DATABASE IF NOT EXISTS temp_senior;

----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS temp_senior.dim_business_structure (
  id int NOT NULL,
  nkbusinessstructure string,
  costCenterCode string,
  costCenterCodeSenior string,
  costCenterName string,
  costCenterType string,
  costCenterStatus string,
  costCenterApportionmentgroup string,
  costCenterClassification string,
  wageBillClassification string,
  costCenterManager string,
  costCenterManageremail string,
  costCenterResponsible string,
  costCenterResponsibleemail string,
  businessUnitId string,
  businessUnitInitials string,
  businessUnitName string,
  verticalName string,
  verticalType string,
  verticalInitials string,
  boardName string,
  resultCenterName string,
  resultCenterClassification string,
  resultCenterMode string,
  resultCenterPhase string,
  managementName string,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de centro de custo'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS temp_senior.dim_xpto_companyTeams (
  id string NOT NULL,
  nkxpto_companyTeams string,
  teamStructureCode int,
  teamCode string,
  teamPositionHierarchy string,
  hierarchyCode int,
  teamName string,
  fatherTeamPositionHierarchy string,
  fatherTeamCode string,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de equipes da xpto_company'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS temp_senior.dim_jobstitlesrecord (
  id string NOT NULL,
  nkjobstitlesrecord string,
  employeecode string,
  updatedate date,
  jobcode string,
  jobstructurecode int,
  jobtitle string,
  jobseniority string,
  fulljobtitle string,
  jobtitleprofile string,
  typejob string,
  jobcategory string,
  flaglastjob int,
  flagfirstjob int,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão de movimentação de cargos dos colaboradores da xpto_company'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS temp_senior.dim_enterprises (
  id string NOT NULL,
  nkEnterpriseCode string,
  controllershipCode string,
  fullName string,
  cnpj string,
  boardgroup string,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão das empresas pertencentes à xpto_company'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");


CREATE TABLE IF NOT EXISTS temp_senior.dim_leaders (
  id string NOT NULL,
  nkLeaderCode string,
  name string,
  teamStructureCode int,
  teamCode string,
  companyName string,
  employeeType string,
  contractType string,
  gender string,
  pcd string,
  education string,
  hiringDate date,
  workSituation string,
  dismissalDate date,
  dismissalCause string,
  birthdayDate date,
  workplace string,
  city string,
  state string,
  cpf string,
  costCenterCode string,
  workArea string,
  workUnity string,
  jobTitle string,
  jobSeniority string,
  fullJobTitle string,
  jobTitleProfile string,
  typeJob string,
  jobCategory string,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão dos líderes'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

-----------------------------------------------------------------------------------------------------------------
