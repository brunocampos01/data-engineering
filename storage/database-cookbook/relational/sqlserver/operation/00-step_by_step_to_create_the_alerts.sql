/******************************************************************************
--	Instructions for using the script
*******************************************************************************/
--	1) F5 to run script

/******************************************************************************
--	Sequence of execution od Scripts para criate ALERTS.
******************************************************************************/

-------------------------------------------------------------------------------
-- 1)	Criar o Operator para colocar na Notification of fail dos JOBS que serão criados e também nos ALERTS de Severidade
--		Cria a Base Traces
-------------------------------------------------------------------------------
USE [msdb]

GO

EXEC [msdb].[dbo].[sp_add_operator]
     @name = N'Alerta_BD',
     @enabled = 1,
     @pager_days = 0,
     @email_address = N'E-mail@provedor.com' -- Para colocar mais destinatarios, basta separar o email com ponto e v�rgula ";"
GO

/* 
-- Caso no tenha a base "Traces", execute o codigo abaixo (lembre de alterar o path tamb�m).
USE master

GO

-------------------------------------------------------------------------------
--	1.1) Alterar o path para um local existente no seu servidor.
-------------------------------------------------------------------------------
CREATE DATABASE [Traces] 
	ON  PRIMARY ( 
		NAME = N'Traces', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL12.MSSQLSERVER\MSSQL\DATA\Traces.mdf' , 
		SIZE = 102400KB , FILEGROWTH = 102400KB 
	)
	LOG ON ( 
		NAME = N'Traces_log', FILENAME = N'C:\Program Files\Microsoft SQL Server\MSSQL12.MSSQLSERVER\MSSQL\DATA\Traces_log.ldf' , 
		SIZE = 30720KB , FILEGROWTH = 30720KB 
	)
GO

-------------------------------------------------------------------------------
-- 1.2) Utilizar o Recovery Model SIMPLE, pois no tem muito impacto perder 1 dia de informa��o nessa base de log.
-------------------------------------------------------------------------------
ALTER DATABASE [Traces] SET RECOVERY SIMPLE

GO
*/

-------------------------------------------------------------------------------
-- 2)	Open the  script "..\path\ALERTS - 2 - Create da Tabela de Controle dos ALERTS.txt", read as instruções e running.
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- 3)	Open the  script "..\path\ALERTS - 3 - Pre Requisito - QueriesDemoradas.txt", read as instruções e running.
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- 4)	Open the  script "..\path\ALERTS - 4 - Create das Procedures dos ALERTS.txt", read as instruções e running.
-------------------------------------------------------------------------------

-------------------------------------------------------------------------------
-- 5)	Open the  script "..\path\ALERTS - 5 - Create dos JOBS dos ALERTS.txt", read as instruções e running.
--------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------------
-- 6)	Open the  script "..\path\ALERTS - 6 - Create dos ALERTS de Severidade.txt", read as instruções e running.
--------------------------------------------------------------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------------------------------------
-- 7)	Open the  script "..\path\ALERTS - 7 - Teste ALERTS.txt", read as instruções e running.
--------------------------------------------------------------------------------------------------------------------------------