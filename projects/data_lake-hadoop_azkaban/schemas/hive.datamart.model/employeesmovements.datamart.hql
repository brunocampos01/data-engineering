CREATE DATABASE IF NOT EXISTS employeesmovements;

----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS employeesmovements.dim_controllershipbusinessstructure (
  id string,
  nkbusinessstructure string,
  costCenterCode string,
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

CREATE TABLE IF NOT EXISTS employeesmovements.dim_xpto_companyTeams (
  id string,
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

CREATE TABLE IF NOT EXISTS employeesmovements.dim_jobstitlesrecord (
  id string,
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

CREATE TABLE IF NOT EXISTS employeesmovements.dim_enterprises (
  id string,
  nkEnterpriseCode string,
  controllershipCode string,
  fullName string,
  cnpj string,
  boardgroup string,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão das empresas pertencentes à xpto_company'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS employeesmovements.dim_xpto_companyDirectLeaders (
  id string,
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

CREATE TABLE IF NOT EXISTS employeesmovements.dim_executiveboard (
  id string,
  nkExecutiveBoardCode string,
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
) COMMENT 'Dimensão da Diretoria Executiva da xpto_company'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS employeesmovements.dim_board (
  id string,
  nkBoardCode string,
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
) COMMENT 'Dimensão da Diretoria da xpto_company'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS employeesmovements.dim_managementlevel1 (
  id string ,
  nkManagementCode string,
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
) COMMENT 'Dimensão do primeiro nível de gestão abaixo da diretoria da xpto_company'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS employeesmovements.dim_softplayers (
  id string,
  nkSoftplayerCode string,
  cpf string,
  name string,
  employeeType string,
  contractType string,
  seniorCode int,
  gender string,
  pcd string,
  education string,
  workSituation string,
  birthdayDate date,
  workplace string,
  city string,
  state string,
  PRIMARY KEY (id) DISABLE NOVALIDATE
) COMMENT 'Dimensão das informações individuais dos Softplayers'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS employeesmovements.fact_dismissals (
  sk_softplayers string,
  sk_businessstructure string,
  sk_enterprises string,
  nk_softplayercode string	COMMENT 'Código de identificação individual do colaborador no sistema Senior',
  headcount_counter string COMMENT 'Códiga para contagem de Headcount com CPF e Data de Admissão',
  hiringdate date COMMENT 'Data de admissão em tempo de empresa do colaborador',
  dismissaldate date COMMENT 'Data de desligamento do colaborador, caso seja o caso',
  dismissalcause string COMMENT 'Causas de desligamento',
  dismissalinitiative string COMMENT 'Iniciativa de desligamento',
  jobTitle string,
  jobSeniority string,
  fullJobTitle string,
  jobTitleProfile string,
  typeJob string,
  jobCategory string,
  workdays int COMMENT 'Total de dias em que o colaborador está ou permaneceu na xpto_company, contando a partir da data de admissão até o dia atual, ou até a data de desligamento.',
  workyears double COMMENT 'Total de anos em que o colaborador está ou permaneceu na xpto_company, contando a partir da data de admissão até o dia atual, ou até a data de desligamento.',
  workyearsrange string COMMENT 'Faixa de tempo de empresa em anos (workyears) em que o colaborador se encontra.',
  retention string COMMENT 'Flag para cálculo de retenção do colaborador para avaliação do período de experiência.',
  age double COMMENT 'Idade em anos do colaborador.',
  agerange string COMMENT 'Faixa etária (age) em que o colaborador se encontra.',
  workarea string COMMENT 'Setor em que o colaborador trabalha na xpto_company.',
  workunity string COMMENT 'Unidade de negócio em sigla em que o colaborador trabalha na xpto_company'
)
COMMENT 'Fato das demissões dos ex-colaboradores na xpto_company'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES ("transactional"="true", "orc.compress"="ZLIB");

CREATE TABLE IF NOT EXISTS employeesmovements.fact_employeechanges (
  sk_softplayers string,
  sk_businessstructure string,
  sk_enterprises string,
  sk_directleader string,
  sk_directexecutiveboard string,
  sk_board string,
  sk_managementlevel1 string,
  sk_xpto_companyteams string,
  sk_jobtitlesrecord string,
  nk_softplayercode string	COMMENT 'Código de identificação individual do colaborador no sistema Senior',
  headcount_counter string COMMENT 'Id para contagem de Headcount com CPF e Data de Admissão',
  hiringdate date COMMENT 'Data de admissão em tempo de empresa do colaborador',
  dismissaldate date COMMENT 'Data de desligamento do colaborador, caso seja o caso',
  jobupdatedate date COMMENT 'Data de atualização do cargo do colaborador. Em toda admissão há a inserção do cargo.',
  workdays int COMMENT 'Total de dias em que o colaborador está ou permaneceu na xpto_company, contando a partir da data de admissão até o dia atual, ou até a data de desligamento.',
  workyears double COMMENT 'Total de anos em que o colaborador está ou permaneceu na xpto_company, contando a partir da data de admissão até o dia atual, ou até a data de desligamento.',
  workyears double COMMENT 'Total de anos em que o colaborador está ou permaneceu na xpto_company, contando a partir da data de admissão até o dia atual, ou até a data de desligamento.',
  workyearsrange string COMMENT 'Faixa de tempo de empresa em anos (workyears) em que o colaborador se encontra.',
  retention string COMMENT 'Flag para cálculo de retenção do colaborador para avaliação do período de experiência.',
  age double COMMENT 'Idade em anos do colaborador.',
  agerange string COMMENT 'Faixa etária (age) em que o colaborador se encontra.',
  workarea string COMMENT 'Setor em que o colaborador trabalha na xpto.',
  workunity string COMMENT 'Unidade de negócio em sigla em que o colaborador trabalha na xpto',
  CONSTRAINT sk_softplayers FOREIGN KEY (sk_softplayers) REFERENCES employeesmovements.dim_softplayers(id) DISABLE NOVALIDATE,
  CONSTRAINT sk_businessstructure FOREIGN KEY (sk_businessstructure) REFERENCES employeesmovements.dim_controllershipbusinessstructure(id) DISABLE NOVALIDATE,
  CONSTRAINT sk_enterprises FOREIGN KEY (sk_enterprises) REFERENCES employeesmovements.dim_enterprises(id) DISABLE NOVALIDATE,
  CONSTRAINT sk_directleader FOREIGN KEY (sk_directleader) REFERENCES employeesmovements.dim_xptodirectleaders(id) DISABLE NOVALIDATE,
  CONSTRAINT sk_directexecutiveboard FOREIGN KEY (sk_directexecutiveboard) REFERENCES employeesmovements.dim_executiveboard(id) DISABLE NOVALIDATE,
  CONSTRAINT sk_board FOREIGN KEY (sk_board) REFERENCES employeesmovements.dim_board(id) DISABLE NOVALIDATE,
  CONSTRAINT sk_managementlevel1 FOREIGN KEY (sk_managementlevel1) REFERENCES employeesmovements.dim_managementlevel1(id) DISABLE NOVALIDATE,
  CONSTRAINT sk_xpto_companyteams FOREIGN KEY (sk_xptoteams) REFERENCES employeesmovements.dim_xptoteams(id) DISABLE NOVALIDATE,
  CONSTRAINT sk_jobtitlesrecord FOREIGN KEY (sk_jobtitlesrecord) REFERENCES employeesmovements.dim_jobstitlesrecord(id) DISABLE NOVALIDATE
)
COMMENT 'Fato das movimentações dos colaboradores na xpto'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS ORC TBLPROPERTIES ("transactional"="true", "orc.compress"="ZLIB");
