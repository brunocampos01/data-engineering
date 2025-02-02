CREATE VIEW IF NOT EXISTS test.senior_board AS
SELECT
  md5(concat(row_sequence(),
        diretoria.id))		             	AS skBoardCode,
  diretoria.id 							  	AS nkBoardCode,
  diretoria.nomediretor		 				AS name,
  diretoria.estruturaequipe					AS teamStructureCode,
  diretoria.equipenome						AS teamCode,
  diretoria.empresa 						AS companyName,
  diretoria.registro						AS employeeType,
  diretoria.idCadastroColaborador			AS registerCode,
  diretoria.tipocontrato 					AS contractType,
  diretoria.generoColaborador				AS gender,
  diretoria.pcd								AS pcd,
  diretoria.instrucao						AS education,
  diretoria.dataAdmissaoTempoDeEmpresa		AS hiringDate,
  diretoria.situacao						AS workSituation,
  diretoria.dataDesligamento				AS dismissalDate,
  diretoria.causasdesligamento				AS dismissalCause,
  diretoria.dataNascimento					AS birthdayDate,
  diretoria.localtrabalho					AS workplace,
  diretoria.cidade							AS city,
  diretoria.estado							AS state,
  diretoria.cpf        						AS cpf,
  diretoria.centroDeCusto					AS costCenterCode,
  diretoria.nomesetor						AS workArea,
  diretoria.siglaunidade					AS workUnity,
  diretoria.cargosemnivel					AS jobTitle,
  diretoria.nivelcargo						AS jobSeniority,
  diretoria.cargocomnivel					AS fullJobTitle,
  diretoria.perfilcargo						AS jobTitleProfile,
  diretoria.tipoCargo						AS typeJob,
  diretoria.categoriaCargo 					AS jobCategory
FROM staging.senior_diretoria AS diretoria;
