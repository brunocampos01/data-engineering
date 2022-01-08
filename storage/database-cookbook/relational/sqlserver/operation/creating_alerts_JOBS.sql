/******************************************************************************
--	Instructions for using the script
*******************************************************************************/
--	1) F5 to run script
USE [msdb]

GO

/******************************************************************************
-- CREATE JOB: [DBA - Database Alerts]
*******************************************************************************/
BEGIN TRANSACTION
    DECLARE
        @ReturnCode INT
    SELECT @ReturnCode = 0

    ---------------------------------------------------------------------------
    -- Select  category at JOB
    ---------------------------------------------------------------------------
    IF NOT EXISTS(SELECT [name]
                  FROM [msdb].[dbo].[syscategories]
                  WHERE [name] = N'Database Maintenance'
                    AND [category_class] = 1)
        BEGIN
            EXEC @ReturnCode = [msdb].[dbo].[sp_add_category]
                               @class = N'JOB', @type = N'LOCAL',
                               @name = N'Database Maintenance'

            IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
        END

    DECLARE
        @jobId BINARY(16)
    EXEC @ReturnCode = [msdb].[dbo].[sp_add_job]
                       @job_name = N'DBA - Alertas Banco de Dados',
                       @enabled = 1,
                       @notify_level_eventlog = 0,
                       @notify_level_email = 0,
                       @notify_level_netsend = 0,
                       @notify_level_page = 0,
                       @delete_level = 0,
                       @description = N'No description available.',
                       @category_name = N'Database Maintenance',
                       @owner_login_name = N'sa',
                       @job_id = @jobId OUTPUT

    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

    ---------------------------------------------------------------------------
    -- Cria o Step 1 do JOB - DBA - Alertas Banco de Dados
    ---------------------------------------------------------------------------
    EXEC @ReturnCode = [msdb].[dbo].[sp_add_jobstep]
                       @job_id = @jobId,
                       @step_name = N'DBA - Alertas Banco de Dados',
                       @step_id = 1,
                       @cmdexec_success_code = 0,
                       @on_success_action = 1,
                       @on_success_step_id = 0,
                       @on_fail_action = 2,
                       @on_fail_step_id = 0,
                       @retry_attempts = 0,
                       @retry_interval = 0,
                       @os_run_priority = 0,
                       @subsystem = N'TSQL',
                       @command = N'-- Executado a cada minuto
EXEC [dbo].[stpAlerta_Arquivo_Log_Full]
--EXEC [dbo].[stpAlerta_MaxSize_Arquivo_SQL]
EXEC [dbo].[stpAlerta_Erro_Banco_Dados]

-- Executado apenas no hor�rio comercial
IF ( DATEPART(HOUR, GETDATE()) >= 6 AND DATEPART(HOUR, GETDATE()) < 23 )
BEGIN
	EXEC [dbo].[stpAlerta_Processo_Bloqueado]
	EXEC [dbo].[stpAlerta_Consumo_CPU]
END

-- Executado a cada 5 minutos
IF ( DATEPART(mi, GETDATE()) %5 = 0 )
BEGIN
	EXEC [dbo].[stpAlerta_Espaco_Disco]
	EXEC [dbo].[stpAlerta_Tempdb_Utilizacao_Arquivo_MDF]
END

-- Executado a cada 20 minutos
IF ( DATEPART(mi, GETDATE()) %20 = 0 )
BEGIN
	EXEC [dbo].[stpAlerta_SQL_Server_Reiniciado]
END

-- Executado a cada 1 hora
IF ( DATEPART(mi, GETDATE()) %59 = 0 )
BEGIN
 EXEC [dbo].[stpAlerta_Conexao_SQLServer]
END',
                       @database_name = N'Traces',
                       @flags = 0

    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

    EXEC @ReturnCode = [msdb].[dbo].[sp_update_job] @job_id = @jobId,
                       @start_step_id = 1

    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

    ---------------------------------------------------------------------------
    -- Cria o Schedule do JOB
    ---------------------------------------------------------------------------
    DECLARE
        @Dt_Atual VARCHAR(8) = CONVERT(VARCHAR(8), GETDATE(), 112)

    EXEC @ReturnCode = [msdb].[dbo].[sp_add_jobschedule]
                       @job_id = @jobId,
                       @name = N'DBA - Alertas Banco de Dados',
                       @enabled = 1,
                       @freq_type = 4,
                       @freq_interval = 1,
                       @freq_subday_type = 4,
                       @freq_subday_interval = 1,
                       @freq_relative_interval = 0,
                       @freq_recurrence_factor = 0,
                       @active_start_date = @Dt_Atual,
                       @active_end_date = 99991231,
                       @active_start_time = 30,
                       @active_end_time = 235959

    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

    EXEC @ReturnCode = [msdb].[dbo].[sp_add_jobserver] @job_id = @jobId,
                       @server_name = N'(local)'

    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION

GOTO EndSave

QuitWithRollback:
IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION

EndSave:

GO

USE [msdb]

GO

/******************************************************************************
-- CRIA JOB: [DBA - Envia Email Processos em Execu��o]
*******************************************************************************/
BEGIN TRANSACTION
    DECLARE
        @ReturnCode INT
    SELECT @ReturnCode = 0

    ---------------------------------------------------------------------------
    -- Seleciona a Categoria do JOB
    ---------------------------------------------------------------------------
    IF NOT EXISTS(SELECT [name]
                  FROM [msdb].[dbo].[syscategories]
                  WHERE [name] = N'Database Maintenance'
                    AND [category_class] = 1)
        BEGIN
            EXEC @ReturnCode = [msdb].[dbo].[sp_add_category]
                               @class = N'JOB', @type = N'LOCAL',
                               @name = N'Database Maintenance'

            IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
        END

    DECLARE
        @jobId BINARY(16)
    EXEC @ReturnCode = [msdb].[dbo].[sp_add_job]
                       @job_name = N'DBA - Envia Email Processos em Execu��o',
                       @enabled = 1,
                       @notify_level_eventlog = 0,
                       @notify_level_email = 2,
                       @notify_level_netsend = 0,
                       @notify_level_page = 0,
                       @delete_level = 0,
                       @description = N'No description available.',
                       @category_name = N'Database Maintenance',
                       @owner_login_name = N'sa',
                       @notify_email_operator_name = N'Alerta_BD',
                       @job_id = @jobId OUTPUT

    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

    ---------------------------------------------------------------------------
    -- Cria o Step 1 do JOB - Envia Email DBA
    ---------------------------------------------------------------------------
    EXEC @ReturnCode = [msdb].[dbo].[sp_add_jobstep]
                       @job_id = @jobId,
                       @step_name = N'Envia Email Whoisactive',
                       @step_id = 1,
                       @cmdexec_success_code = 0,
                       @on_success_action = 1,
                       @on_success_step_id = 0,
                       @on_fail_action = 2,
                       @on_fail_step_id = 0,
                       @retry_attempts = 0,
                       @retry_interval = 0,
                       @os_run_priority = 0,
                       @subsystem = N'TSQL',
                       @command = N'EXEC [dbo].[stpEnvia_Email_Processos_Execucao]',
                       @database_name = N'Traces',
                       @flags = 0

    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

    EXEC @ReturnCode = [msdb].[dbo].[sp_update_job] @job_id = @jobId,
                       @start_step_id = 1

    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

    EXEC @ReturnCode = [msdb].[dbo].[sp_add_jobserver] @job_id = @jobId,
                       @server_name = N'(local)'

    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION

GOTO EndSave

QuitWithRollback:
IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION

EndSave:

GO

USE [msdb]
GO

/****** Object:  Job [DBA - Alertas Di�rios]    Script Date: 06/07/2017 18:48:55 ******/
BEGIN TRANSACTION
    DECLARE
        @ReturnCode INT
    SELECT @ReturnCode = 0
/****** Object:  JobCategory [Database Maintenance]    Script Date: 06/07/2017 18:48:55 ******/
    IF NOT EXISTS(SELECT name
                  FROM msdb.dbo.syscategories
                  WHERE name = N'Database Maintenance'
                    AND category_class = 1)
        BEGIN
            EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB',
                               @type=N'LOCAL', @name=N'Database Maintenance'
            IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

        END

    DECLARE
        @jobId BINARY(16)
    EXEC @ReturnCode = msdb.dbo.sp_add_job
                       @job_name=N'DBA - Alertas Di�rios',
                       @enabled=1,
                       @notify_level_eventlog=0,
                       @notify_level_email=2,
                       @notify_level_netsend=0,
                       @notify_level_page=0,
                       @delete_level=0,
                       @description=N'No description available.',
                       @category_name=N'Database Maintenance',
                       @owner_login_name=N'sa',
                       @notify_email_operator_name=N'Alerta_BD',
                       @job_id = @jobId OUTPUT
    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Databases sem Backup]    Script Date: 06/07/2017 18:48:55 ******/
    EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId,
                       @step_name=N'Database sem Backup',
                       @step_id=1,
                       @cmdexec_success_code=0,
                       @on_success_action=3,
                       @on_success_step_id=0,
                       @on_fail_action=2,
                       @on_fail_step_id=0,
                       @retry_attempts=0,
                       @retry_interval=0,
                       @os_run_priority=0, @subsystem=N'TSQL',
                       @command=N'EXEC [dbo].[stpAlerta_Database_Sem_Backup]',
                       @database_name=N'Traces',
                       @flags=0
    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Jobs que Falharam]    Script Date: 06/07/2017 18:48:55 ******/
    EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId,
                       @step_name=N'Job Falha',
                       @step_id=2,
                       @cmdexec_success_code=0,
                       @on_success_action=3,
                       @on_success_step_id=0,
                       @on_fail_action=2,
                       @on_fail_step_id=0,
                       @retry_attempts=0,
                       @retry_interval=0,
                       @os_run_priority=0, @subsystem=N'TSQL',
                       @command=N'EXEC [dbo].[stpAlerta_Job_Falha]',
                       @database_name=N'Traces',
                       @flags=0
    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Alerta Base Criada]    Script Date: 06/07/2017 18:48:55 ******/
    EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId,
                       @step_name=N'Alerta Base Criada',
                       @step_id=3,
                       @cmdexec_success_code=0,
                       @on_success_action=1,
                       @on_success_step_id=0,
                       @on_fail_action=2,
                       @on_fail_step_id=0,
                       @retry_attempts=0,
                       @retry_interval=0,
                       @os_run_priority=0, @subsystem=N'TSQL',
                       @command=N'EXEC [dbo].[stpAlerta_Database_Criada]',
                       @database_name=N'Traces',
                       @flags=0
    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
    EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId,
                       @start_step_id = 1
    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
    EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId,
                       @name=N'DI�RIO - 05:50',
                       @enabled=1,
                       @freq_type=4,
                       @freq_interval=1,
                       @freq_subday_type=1,
                       @freq_subday_interval=0,
                       @freq_relative_interval=0,
                       @freq_recurrence_factor=0,
                       @active_start_date=20160716,
                       @active_end_date=99991231,
                       @active_start_time=55000,
                       @active_end_time=235959
    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
    EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId,
                       @server_name = N'(local)'
    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO

USE [msdb]
GO

/****** Object:  Job [DBA - CHECKDB Databases]    Script Date: 27/01/2017 14:49:04 ******/
BEGIN TRANSACTION
    DECLARE
        @ReturnCode INT
    SELECT @ReturnCode = 0
/****** Object:  JobCategory [Database Maintenance]    Script Date: 27/01/2017 14:49:04 ******/
    IF NOT EXISTS(SELECT name
                  FROM msdb.dbo.syscategories
                  WHERE name = N'Database Maintenance'
                    AND category_class = 1)
        BEGIN
            EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB',
                               @type=N'LOCAL', @name=N'Database Maintenance'
            IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

        END

    DECLARE
        @jobId BINARY(16)
    EXEC @ReturnCode = msdb.dbo.sp_add_job
                       @job_name=N'DBA - CHECKDB Databases',
                       @enabled=0,
                       @notify_level_eventlog=0,
                       @notify_level_email=2,
                       @notify_level_netsend=0,
                       @notify_level_page=0,
                       @delete_level=0,
                       @description=N'No description available.',
                       @category_name=N'Database Maintenance',
                       @owner_login_name=N'sa',
                       @notify_email_operator_name=N'Alerta_BD',
                       @job_id = @jobId OUTPUT
    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [DBA - CheckDB Databases]    Script Date: 27/01/2017 14:49:04 ******/
    EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId,
                       @step_name=N'DBA - CheckDB Databases',
                       @step_id=1,
                       @cmdexec_success_code=0,
                       @on_success_action=1,
                       @on_success_step_id=0,
                       @on_fail_action=3,
                       @on_fail_step_id=0,
                       @retry_attempts=0,
                       @retry_interval=0,
                       @os_run_priority=0, @subsystem=N'TSQL',
                       @command=N'EXEC [dbo].[stpCHECKDB_Databases]',
                       @database_name=N'Traces',
                       @flags=0
    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Alerta Corrup��o Databases]    Script Date: 27/01/2017 14:49:04 ******/
    EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId,
                       @step_name=N'Alerta Corrup��o Databases',
                       @step_id=2,
                       @cmdexec_success_code=0,
                       @on_success_action=2,
                       @on_success_step_id=0,
                       @on_fail_action=2,
                       @on_fail_step_id=0,
                       @retry_attempts=0,
                       @retry_interval=0,
                       @os_run_priority=0, @subsystem=N'TSQL',
                       @command=N'EXEC [dbo].[stpAlerta_CheckDB]',
                       @database_name=N'Traces',
                       @flags=0
    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
    EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId,
                       @start_step_id = 1
    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
    EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId,
                       @name=N'DBA - CheckDB Databases',
                       @enabled=1,
                       @freq_type=4,
                       @freq_interval=1,
                       @freq_subday_type=1,
                       @freq_subday_interval=0,
                       @freq_relative_interval=0,
                       @freq_recurrence_factor=0,
                       @active_start_date=20170127,
                       @active_end_date=99991231,
                       @active_start_time=20000,
                       @active_end_time=235959
    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
    EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId,
                       @server_name = N'(local)'
    IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:

GO