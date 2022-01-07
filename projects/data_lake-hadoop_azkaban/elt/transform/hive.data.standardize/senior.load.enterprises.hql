CREATE VIEW IF NOT EXISTS test.senior_enterprises AS
SELECT
  md5(concat(row_sequence(),
        empresas.nomeabreviado))		            AS skEnterpriseCode,
  empresas.nomeabreviado					      	AS nkEnterpriseCode,
  empresas.idEmpresaControladoria 					AS controllershipCode,
  empresas.nome    									AS fullName,
  hype.decnpj       								AS cnpj,
  hype.degrupo										AS boardgroup
FROM staging.senior_empresas AS empresas
LEFT JOIN staging.hyperion_company AS hype ON empresas.idEmpresaControladoria = hype.cdempresa
ORDER BY nkEnterpriseCode DESC;
