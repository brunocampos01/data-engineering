/******************************************************************************
--	Instructions for using the script
*******************************************************************************/
--	1) F5 to run script

SET NOCOUNT ON

GO

EXEC [sp_configure] 'show advanced option', 1

RECONFIGURE with OVERRIDE

EXEC [sp_configure] 'Ole Automation Procedures', 1

RECONFIGURE with OVERRIDE

EXEC [sp_configure] 'show advanced option', 0

RECONFIGURE with OVERRIDE

GO

USE [Traces]

/******************************************************************************
--	The control table that will be used to store the History of the Suspect Pages table.
*******************************************************************************/
IF (OBJECT_ID('[dbo].[Historico_Suspect_Pages]') IS NOT NULL)
    DROP TABLE [dbo].[Historico_Suspect_Pages]

CREATE TABLE [dbo].[Historico_Suspect_Pages]
(
    [database_id]  [int]      NOT NULL,
    [file_id]      [int]      NOT NULL,
    [page_id]      [bigint]   NOT NULL,
    [event_type]   [int]      NOT NULL,
    [Dt_Corrupcao] [datetime] NOT NULL
) ON [PRIMARY]


/******************************************************************************
--	CREATE JOB
*******************************************************************************/

GO
IF (OBJECT_ID('[dbo].[stpALERT_Processo_Bloqueado]') IS NOT NULL)
    DROP PROCEDURE [dbo].[stpALERT_Processo_Bloqueado]
GO

/******************************************************************************
--	ALERT: PROCESS LOCKED
*******************************************************************************/

CREATE PROCEDURE [dbo].[stpALERT_Processo_Bloqueado]
AS
BEGIN
    SET NOCOUNT ON

    -- PROCESS LOCKED
    DECLARE
        @Id_ALERT_Parametro INT = (SELECT Id_ALERT_Parametro
                                   FROM [Traces].[dbo].ALERT_Parametro (NOLOCK)
                                   WHERE Nm_ALERT = 'Processo Bloqueado')

    -- Declares the variables
    DECLARE
        @Subject             VARCHAR(500), @Fl_Tipo TINYINT, @Qtd_Segundos INT, @Consulta VARCHAR(8000), @Importance AS VARCHAR(6), @Dt_Atual DATETIME,
        @EmailBody           VARCHAR(MAX), @ALERTLockHeader VARCHAR(MAX), @ALERTLockTable VARCHAR(MAX), @EmptyBodyEmail VARCHAR(MAX),
        @ALERTLockRaizHeader VARCHAR(MAX), @ALERTLockRaizTable VARCHAR(MAX), @Processo_Bloqueado_Parametro INT, @Qt_Tempo_Raiz_Lock INT,
        @EmailDestination    VARCHAR(500), @ProfileEmail VARCHAR(200)

    ---------------------------------------------------------------------------
    -- Retrieves Alert Parameters
    ---------------------------------------------------------------------------
    SELECT @Processo_Bloqueado_Parametro = Vl_Parametro, -- Minutos,
           @EmailDestination = Ds_Email,
           @ProfileEmail = Ds_Profile_Email
    FROM [dbo].[ALERT_Parametro]
    WHERE [Id_ALERT_Parametro] = @Id_ALERT_Parametro
    -- Blocked Process

    -- Quantity in Minutes
    SELECT @Qt_Tempo_Raiz_Lock = 1
    -- Query that is generating the lock (running more than 1 minute)

    ---------------------------------------------------------------------------
    --	Creates Table to store sp_whoisactive
    ---------------------------------------------------------------------------
    IF (OBJECT_ID('tempdb..#Resultado_WhoisActive') IS NOT NULL)
        DROP TABLE #Resultado_WhoisActive

    CREATE TABLE #Resultado_WhoisActive
    (
        [dd hh:mm:ss.mss]     VARCHAR(20),
        [database_name]       NVARCHAR(128),
        [login_name]          NVARCHAR(128),
        [host_name]           NVARCHAR(128),
        [start_time]          DATETIME,
        [status]              VARCHAR(30),
        [session_id]          INT,
        [blocking_session_id] INT,
        [wait_info]           VARCHAR(MAX),
        [open_tran_count]     INT,
        [CPU]                 VARCHAR(MAX),
        [reads]               VARCHAR(MAX),
        [writes]              VARCHAR(MAX),
        [sql_command]         XML
    )

    -- Set date
    SELECT @Dt_Atual = GETDATE()

    ---------------------------------------------------------------------------
    --	Load data in sp_whoisactive
    ---------------------------------------------------------------------------
    -- Return all thes processes is running
    EXEC [dbo].[sp_whoisactive]
         @get_outer_command = 1,
         @output_column_list = '[dd hh:mm:ss.mss][database_name][login_name][host_name][start_time][status][session_id][blocking_session_id][wait_info][open_tran_count][CPU][reads][writes][sql_command]',
         @destination_table = '#Resultado_WhoisActive'

    -- Changes the column that has the SQL command
    ALTER TABLE #Resultado_WhoisActive
        ALTER COLUMN [sql_command] VARCHAR(MAX)

    UPDATE #Resultado_WhoisActive
    SET [sql_command] = REPLACE(REPLACE(REPLACE(REPLACE(
                                                        CAST([sql_command] AS VARCHAR(1000)),
                                                        '<?query --',
                                                        ''), '--?>',
                                                ''), '&gt;', '>'),
                                '&lt;', '')

    -- select * from #Resultado_WhoisActive

    -- Checks if there is no running process
    IF NOT EXISTS(SELECT TOP 1 * FROM #Resultado_WhoisActive)
        BEGIN
            INSERT INTO #Resultado_WhoisActive
            SELECT NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL
        END

    -- Check the last Type of Alert registered -> 0: CLEAR / 1: ALERT
    SELECT @Fl_Tipo = [Fl_Tipo]
    FROM [dbo].[ALERT]
    WHERE [Id_ALERT] = (SELECT MAX(Id_ALERT)
                        FROM [dbo].[ALERT]
                        WHERE [Id_ALERT_Parametro] = @Id_ALERT_Parametro)

    /**************************************************************************
	--	Check for any Locked Process
	**************************************************************************/
    IF EXISTS(
            SELECT NULL
            FROM #Resultado_WhoisActive A
                     JOIN #Resultado_WhoisActive B
                          ON A.[blocking_session_id] = B.[session_id]
            WHERE DATEDIFF(SECOND, A.[start_time], @Dt_Atual) >
                  @Processo_Bloqueado_Parametro * 60 -- The query being blocked is running for another 2 minutes
              AND DATEDIFF(SECOND, B.[start_time], @Dt_Atual) >
                  @Qt_Tempo_Raiz_Lock * 60 -- The query you are blocking is running more than 1 minute
        )
        BEGIN
            -- Start - Alert
            IF ISNULL(@Fl_Tipo, 0) = 0 --Sends the Alert only once
                BEGIN
                    -----------------------------------------------------------
                    --	Checks the number of blocked processes
                    -----------------------------------------------------------
                    -- Declare the variable and return the amount of Slow Queries
                    DECLARE
                        @QtdProcessosBloqueados INT = (
                            SELECT COUNT(*)
                            FROM #Resultado_WhoisActive A
                                     JOIN #Resultado_WhoisActive B
                                          ON A.[blocking_session_id] = B.[session_id]
                            WHERE DATEDIFF(SECOND, A.[start_time],
                                           @Dt_Atual) >
                                  @Processo_Bloqueado_Parametro * 60
                              AND DATEDIFF(SECOND, B.[start_time],
                                           @Dt_Atual) >
                                  @Qt_Tempo_Raiz_Lock * 60
                        )

                    DECLARE
                        @QtdProcessosBloqueadosLocks INT = (
                            SELECT COUNT(*)
                            FROM #Resultado_WhoisActive A
                            WHERE [blocking_session_id] IS NOT NULL
                        )

                    -----------------------------------------------------------
                    --	Check Locks Level
                    -----------------------------------------------------------
                    ALTER TABLE #Resultado_WhoisActive
                        ADD Nr_Nivel_Lock TINYINT

                    -- Level 0
                    UPDATE A
                    SET Nr_Nivel_Lock = 0
                    FROM #Resultado_WhoisActive A
                    WHERE blocking_session_id IS NULL
                      AND session_id IN
                          (SELECT DISTINCT blocking_session_id
                           FROM #Resultado_WhoisActive
                           WHERE blocking_session_id IS NOT NULL)

                    UPDATE A
                    SET Nr_Nivel_Lock = 1
                    FROM #Resultado_WhoisActive A
                    WHERE Nr_Nivel_Lock IS NULL
                      AND blocking_session_id IN
                          (SELECT DISTINCT session_id
                           FROM #Resultado_WhoisActive
                           WHERE Nr_Nivel_Lock = 0)

                    UPDATE A
                    SET Nr_Nivel_Lock = 2
                    FROM #Resultado_WhoisActive A
                    WHERE Nr_Nivel_Lock IS NULL
                      AND blocking_session_id IN
                          (SELECT DISTINCT session_id
                           FROM #Resultado_WhoisActive
                           WHERE Nr_Nivel_Lock = 1)

                    UPDATE A
                    SET Nr_Nivel_Lock = 3
                    FROM #Resultado_WhoisActive A
                    WHERE Nr_Nivel_Lock IS NULL
                      AND blocking_session_id IN
                          (SELECT DISTINCT session_id
                           FROM #Resultado_WhoisActive
                           WHERE Nr_Nivel_Lock = 2)

                    -- Treatment when you do not have a Root Lock
                    IF NOT EXISTS(select *
                                  from #Resultado_WhoisActive
                                  where Nr_Nivel_Lock IS NOT NULL)
                        BEGIN
                            UPDATE A
                            SET Nr_Nivel_Lock = 0
                            FROM #Resultado_WhoisActive A
                            WHERE session_id IN
                                  (SELECT DISTINCT blocking_session_id
                                   FROM #Resultado_WhoisActive
                                   WHERE blocking_session_id IS NOT NULL)

                            UPDATE A
                            SET Nr_Nivel_Lock = 1
                            FROM #Resultado_WhoisActive A
                            WHERE Nr_Nivel_Lock IS NULL
                              AND blocking_session_id IN
                                  (SELECT DISTINCT session_id
                                   FROM #Resultado_WhoisActive
                                   WHERE Nr_Nivel_Lock = 0)

                            UPDATE A
                            SET Nr_Nivel_Lock = 2
                            FROM #Resultado_WhoisActive A
                            WHERE Nr_Nivel_Lock IS NULL
                              AND blocking_session_id IN
                                  (SELECT DISTINCT session_id
                                   FROM #Resultado_WhoisActive
                                   WHERE Nr_Nivel_Lock = 1)

                            UPDATE A
                            SET Nr_Nivel_Lock = 3
                            FROM #Resultado_WhoisActive A
                            WHERE Nr_Nivel_Lock IS NULL
                              AND blocking_session_id IN
                                  (SELECT DISTINCT session_id
                                   FROM #Resultado_WhoisActive
                                   WHERE Nr_Nivel_Lock = 2)
                        END

                    /******************************************************
			         --	CREATE EMAIL - ALERT
			        ******************************************************/

                    -----------------------------------------------------------
                    --	ALERT - HEADER - ROOT LOCK
                    -----------------------------------------------------------
                    SET @ALERTLockRaizHeader =
                            '<font color=black bold=true size=5>'
                    SET @ALERTLockRaizHeader = @ALERTLockRaizHeader +
                                               '<BR /> TOP 50 - Processos Raiz Lock <BR />'
                    SET @ALERTLockRaizHeader =
                            @ALERTLockRaizHeader + '</font>'

                    -----------------------------------------------------------
                    --	ALERT - BODY - ROOT LOCK
                    -----------------------------------------------------------
                    SET @ALERTLockRaizTable = CAST((
                        SELECT td = [Nr_Nivel_Lock] + '</td>'
                                        + '<td>' + [Duração] + '</td>'
                                        + '<td>' + [database_name] +
                                    '</td>'
                                        + '<td>' + [login_name] +
                                    '</td>'
                                        + '<td>' + [host_name] + '</td>'
                                        + '<td>' + [start_time] +
                                    '</td>'
                                        + '<td>' + [status] + '</td>'
                                        + '<td>' + [session_id] +
                                    '</td>'
                                        + '<td>' +
                                    [blocking_session_id] + '</td>'
                                        + '<td>' + [Wait] + '</td>'
                                        + '<td>' + [open_tran_count] +
                                    '</td>'
                                        + '<td>' + [CPU] + '</td>'
                                        + '<td>' + [reads] + '</td>'
                                        + '<td>' + [writes] + '</td>'
                                        + '<td>' + [sql_command] +
                                    '</td>'

                        FROM (
                                 -- EMAIL Table Details
                                 SELECT TOP 50 CAST(Nr_Nivel_Lock AS VARCHAR) AS [Nr_Nivel_Lock],
                                               ISNULL([dd hh:mm:ss.mss], '-') AS [Duração],
                                               ISNULL([database_name], '-')   AS [database_name],
                                               ISNULL([login_name], '-')      AS [login_name],
                                               ISNULL([host_name], '-')       AS [host_name],
                                               ISNULL(
                                                       CONVERT(VARCHAR(20), [start_time], 120),
                                                       '-')                   AS [start_time],
                                               ISNULL([status], '-')          AS [status],
                                               ISNULL(
                                                       CAST([session_id] AS VARCHAR),
                                                       '-')                   AS [session_id],
                                               ISNULL(
                                                       CAST([blocking_session_id] AS VARCHAR),
                                                       '-')                   AS [blocking_session_id],
                                               ISNULL([wait_info], '-')       AS [Wait],
                                               ISNULL(
                                                       CAST([open_tran_count] AS VARCHAR),
                                                       '-')                   AS [open_tran_count],
                                               ISNULL([CPU], '-')             AS [CPU],
                                               ISNULL([reads], '-')           AS [reads],
                                               ISNULL([writes], '-')          AS [writes],
                                               ISNULL(
                                                       SUBSTRING([sql_command], 1, 300),
                                                       '-')                   AS [sql_command]
                                 FROM #Resultado_WhoisActive
                                 WHERE Nr_Nivel_Lock IS NOT NULL
                                 ORDER BY [Nr_Nivel_Lock], [start_time]
                             ) AS D
                        ORDER BY [Nr_Nivel_Lock], [start_time]
                                 FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
                        )

                    -- Corrects Table Formatting
                    SET @ALERTLockRaizTable = REPLACE(
                            REPLACE(REPLACE(@ALERTLockRaizTable,
                                            '&lt;', '<'), '&gt;', '>'),
                            '<td>', '<td align = center>')

                    -- EMAIL Table Titles
                    SET @ALERTLockRaizTable =
                                '<table cellspacing="2" cellpadding="5" border="3">'
                                + '<tr>
							<th bgcolor=#0B0B61 width="80"><font color=white>Nivel Lock</font></th>
							<th bgcolor=#0B0B61 width="140"><font color=white>[dd hh:mm:ss.mss]</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Database</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Login</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Host Name</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Hora Início</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Status</font></th>
							<th bgcolor=#0B0B61 width="30"><font color=white>ID Sessão</font></th>
							<th bgcolor=#0B0B61 width="60"><font color=white>ID Sessão Bloqueando</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Wait</font></th>
							<th bgcolor=#0B0B61 width="60"><font color=white>Transações Abertas</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>CPU</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Reads</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Writes</font></th>
							<th bgcolor=#0B0B61 width="300"><font color=white>Query</font></th>
						</tr>'
                                + REPLACE(REPLACE(@ALERTLockRaizTable,
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'


                    -----------------------------------------------------------
                    --	ALERT - HEADER
                    -----------------------------------------------------------
                    SET @ALERTLockHeader =
                            '<font color=black bold=true size=5>'
                    SET @ALERTLockHeader = @ALERTLockHeader +
                                           '<BR /> TOP 50 - Processos executando no Database <BR />'
                    SET @ALERTLockHeader =
                            @ALERTLockHeader + '</font>'

                    -----------------------------------------------------------
                    --	ALERT - BODY
                    -----------------------------------------------------------
                    SET @ALERTLockTable = CAST((
                        SELECT td = [Duração] + '</td>'
                                        + '<td>' + [database_name] +
                                    '</td>'
                                        + '<td>' + [login_name] +
                                    '</td>'
                                        + '<td>' + [host_name] + '</td>'
                                        + '<td>' + [start_time] +
                                    '</td>'
                                        + '<td>' + [status] + '</td>'
                                        + '<td>' + [session_id] +
                                    '</td>'
                                        + '<td>' +
                                    [blocking_session_id] + '</td>'
                                        + '<td>' + [Wait] + '</td>'
                                        + '<td>' + [open_tran_count] +
                                    '</td>'
                                        + '<td>' + [CPU] + '</td>'
                                        + '<td>' + [reads] + '</td>'
                                        + '<td>' + [writes] + '</td>'
                                        + '<td>' + [sql_command] +
                                    '</td>'

                        FROM (
                                 -- EMAIL Table Details
                                 SELECT TOP 50 ISNULL([dd hh:mm:ss.mss], '-') AS [Duração],
                                               ISNULL([database_name], '-')   AS [database_name],
                                               ISNULL([login_name], '-')      AS [login_name],
                                               ISNULL([host_name], '-')       AS [host_name],
                                               ISNULL(
                                                       CONVERT(VARCHAR(20), [start_time], 120),
                                                       '-')                   AS [start_time],
                                               ISNULL([status], '-')          AS [status],
                                               ISNULL(
                                                       CAST([session_id] AS VARCHAR),
                                                       '-')                   AS [session_id],
                                               ISNULL(
                                                       CAST([blocking_session_id] AS VARCHAR),
                                                       '-')                   AS [blocking_session_id],
                                               ISNULL([wait_info], '-')       AS [Wait],
                                               ISNULL(
                                                       CAST([open_tran_count] AS VARCHAR),
                                                       '-')                   AS [open_tran_count],
                                               ISNULL([CPU], '-')             AS [CPU],
                                               ISNULL([reads], '-')           AS [reads],
                                               ISNULL([writes], '-')          AS [writes],
                                               ISNULL(
                                                       SUBSTRING([sql_command], 1, 300),
                                                       '-')                   AS [sql_command]
                                 FROM #Resultado_WhoisActive
                                 ORDER BY [start_time]
                             ) AS D
                        ORDER BY [start_time] FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
                        )

                    -- Corrects Table Formatting
                    SET @ALERTLockTable = REPLACE(REPLACE(
                                                          REPLACE(@ALERTLockTable, '&lt;', '<'),
                                                          '&gt;', '>'),
                                                  '<td>',
                                                  '<td align = center>')

                    -- EMAIL Table Titles
                    SET @ALERTLockTable =
                                '<table cellspacing="2" cellpadding="5" border="3">'
                                + '<tr>
							<th bgcolor=#0B0B61 width="140"><font color=white>[dd hh:mm:ss.mss]</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Database</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Login</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Host Name</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Hora Início</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Status</font></th>
							<th bgcolor=#0B0B61 width="30"><font color=white>ID Sessão</font></th>
							<th bgcolor=#0B0B61 width="60"><font color=white>ID Sessão Bloqueando</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Wait</font></th>
							<th bgcolor=#0B0B61 width="60"><font color=white>Transaçõees Abertas</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>CPU</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Reads</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Writes</font></th>
							<th bgcolor=#0B0B61 width="300"><font color=white>Query</font></th>
						</tr>'
                                + REPLACE(
                                        REPLACE(@ALERTLockTable, '&lt;', '<'),
                                        '&gt;', '>')
                                + '</table>'

                    -----------------------------------------------------------
                    -- Insert a blank in EMAIL
                    -----------------------------------------------------------
                    SET @EmptyBodyEmail = ''
                    SET @EmptyBodyEmail =
                                '<table cellpadding="5" cellspacing="5" border="0">' +
                                '<tr>
							<th width="500">               </th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  ISNULL(@EmptyBodyEmail, ''),
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'

                    /**********************************************************
			        --	Set EMAIL Variables
			        **********************************************************/
                    SELECT @Importance = 'High',
                           @Subject = 'ALERT: Existe(m) ' +
                                      CAST(@QtdProcessosBloqueados AS VARCHAR) +
                                      ' Processo(s) Bloqueado(s) a mais de ' +
                                      CAST(
                                              (@Processo_Bloqueado_Parametro) AS VARCHAR) +
                                      ' minuto(s)' +
                                      ' e um total de ' + CAST(
                                              @QtdProcessosBloqueadosLocks AS VARCHAR) +
                                      ' Lock(s) no Servidor: ' +
                                      @@SERVERNAME,
                           @EmailBody =
                           @ALERTLockRaizHeader + @EmptyBodyEmail +
                           @ALERTLockRaizTable + @EmptyBodyEmail
                               + @ALERTLockHeader + @EmptyBodyEmail +
                           @ALERTLockTable + @EmptyBodyEmail

                    /**********************************************************
			        --	SEND EMAIL - ALERT
			        **********************************************************/
                    EXEC [msdb].[dbo].[sp_send_dbmail]
                         @profile_name = @ProfileEmail,
                         @recipients = @EmailDestination,
                         @subject = @Subject,
                         @body = @EmailBody,
                         @body_format = 'HTML',
                         @importance = @Importance

                    /**********************************************************
			        -- Inserts a Record in the Alerts Control Table -> Fl_Type = 1 : ALERT
			        **********************************************************/
                    INSERT INTO [dbo].[ALERT] ([Id_ALERT_Parametro],
                                               [Ds_Mensagem],
                                               [Fl_Tipo])
                    SELECT @Id_ALERT_Parametro, @Subject, 1
                END
        END -- END - ALERT
    ELSE
        BEGIN
            -- START - CLEAR
            IF @Fl_Tipo = 1
                BEGIN
                    /**********************************************************
			        --	CREATE EMAIL - CLEAR
			        **********************************************************/

                    -----------------------------------------------------------
                    --	CLEAR - HEADER
                    -----------------------------------------------------------
                    SET @ALERTLockHeader =
                            '<font color=black bold=true size=5>'
                    SET @ALERTLockHeader = @ALERTLockHeader +
                                           '<BR /> Processos executando no Database <BR />'
                    SET @ALERTLockHeader =
                            @ALERTLockHeader + '</font>'

                    -----------------------------------------------------------
                    --	CLEAR - BODY
                    -----------------------------------------------------------
                    SET @ALERTLockTable = CAST((
                        SELECT td = [Duração] + '</td>'
                                        + '<td>' + [database_name] +
                                    '</td>'
                                        + '<td>' + [login_name] +
                                    '</td>'
                                        + '<td>' + [host_name] + '</td>'
                                        + '<td>' + [start_time] +
                                    '</td>'
                                        + '<td>' + [status] + '</td>'
                                        + '<td>' + [session_id] +
                                    '</td>'
                                        + '<td>' +
                                    [blocking_session_id] + '</td>'
                                        + '<td>' + [Wait] + '</td>'
                                        + '<td>' + [open_tran_count] +
                                    '</td>'
                                        + '<td>' + [CPU] + '</td>'
                                        + '<td>' + [reads] + '</td>'
                                        + '<td>' + [writes] + '</td>'
                                        + '<td>' + [sql_command] +
                                    '</td>'

                        FROM (
                                 -- EMAIL Table Details
                                 SELECT ISNULL([dd hh:mm:ss.mss], '-') AS [Duração],
                                        ISNULL([database_name], '-')   AS [database_name],
                                        ISNULL([login_name], '-')      AS [login_name],
                                        ISNULL([host_name], '-')       AS [host_name],
                                        ISNULL(
                                                CONVERT(VARCHAR(20), [start_time], 120),
                                                '-')                   AS [start_time],
                                        ISNULL([status], '-')          AS [status],
                                        ISNULL(
                                                CAST([session_id] AS VARCHAR),
                                                '-')                   AS [session_id],
                                        ISNULL(
                                                CAST([blocking_session_id] AS VARCHAR),
                                                '-')                   AS [blocking_session_id],
                                        ISNULL([wait_info], '-')       AS [Wait],
                                        ISNULL(
                                                CAST([open_tran_count] AS VARCHAR),
                                                '-')                   AS [open_tran_count],
                                        ISNULL([CPU], '-')             AS [CPU],
                                        ISNULL([reads], '-')           AS [reads],
                                        ISNULL([writes], '-')          AS [writes],
                                        ISNULL(
                                                SUBSTRING([sql_command], 1, 300),
                                                '-')                   AS [sql_command]
                                 FROM #Resultado_WhoisActive
                             ) AS D
                        ORDER BY [start_time] FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
                        )

                    -- Corrects Table Formatting
                    SET @ALERTLockTable = REPLACE(REPLACE(
                                                          REPLACE(@ALERTLockTable, '&lt;', '<'),
                                                          '&gt;', '>'),
                                                  '<td>',
                                                  '<td align = center>')

                    -- EMAIL Table Titles
                    SET @ALERTLockTable =
                                '<table cellspacing="2" cellpadding="5" border="3">'
                                + '<tr>
							<th bgcolor=#0B0B61 width="140"><font color=white>[dd hh:mm:ss.mss]</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Database</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Login</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Host Name</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Hora In�cio</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Status</font></th>
							<th bgcolor=#0B0B61 width="30"><font color=white>ID Session</font></th>
							<th bgcolor=#0B0B61 width="60"><font color=white>ID Session Bloqueando</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Wait</font></th>
							<th bgcolor=#0B0B61 width="60"><font color=white>transactions Abertas</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>CPU</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Reads</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Writes</font></th>
							<th bgcolor=#0B0B61 width="300"><font color=white>Query</font></th>
						</tr>'
                                + REPLACE(
                                        REPLACE(@ALERTLockTable, '&lt;', '<'),
                                        '&gt;', '>')
                                + '</table>'

                    -----------------------------------------------------------
                    -- Insert a blank page in EMAIL
                    -----------------------------------------------------------
                    SET @EmptyBodyEmail = ''
                    SET @EmptyBodyEmail =
                                '<table cellpadding="5" cellspacing="5" border="0">' +
                                '<tr>
							<th width="500">               </th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  ISNULL(@EmptyBodyEmail, ''),
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'

                    /**********************************************************
			        --	Set EMAIL Variables
			        ***********************************************************/
                    SELECT @Importance = 'High',
                           @Subject =
                           'CLEAR: Não existe mais algum Processo Bloqueado a mais de ' +
                           CAST(
                                   (@Processo_Bloqueado_Parametro) AS VARCHAR) +
                           ' minuto(s) no Servidor: ' + @@SERVERNAME,
                           @EmailBody =
                           @ALERTLockHeader + @EmptyBodyEmail +
                           @ALERTLockTable + @EmptyBodyEmail

                    /**********************************************************
			        --	SEND EMAIL - CLEAR
			        ***********************************************************/
                    EXEC [msdb].[dbo].[sp_send_dbmail]
                         @profile_name = @ProfileEmail,
                         @recipients = @EmailDestination,
                         @subject = @Subject,
                         @body = @EmailBody,
                         @body_format = 'HTML',
                         @importance = @Importance

                    /**********************************************************
			        -- Inserts a Record in the Alerts Control Table -> Fl_Type = 0 : CLEAR
			        ***********************************************************/
                    INSERT INTO [dbo].[ALERT] ([Id_ALERT_Parametro],
                                               [Ds_Mensagem],
                                               [Fl_Tipo])
                    SELECT @Id_ALERT_Parametro, @Subject, 0
                END
        END -- END - CLEAR
END



GO
IF (OBJECT_ID('[dbo].[stpALERT_Arquivo_Log_Full]') IS NOT NULL)
    DROP PROCEDURE [dbo].[stpALERT_Arquivo_Log_Full]
GO

/**********************************************************
--	ALERT: ARQUIVO DE LOG FULL
**********************************************************/

CREATE PROCEDURE [dbo].[stpALERT_Arquivo_Log_Full]
AS
BEGIN
    SET NOCOUNT ON

    -- Arquivo de Log Full
    DECLARE
        @Id_ALERT_Parametro INT = (SELECT Id_ALERT_Parametro
                                   FROM [Traces].[dbo].ALERT_Parametro (NOLOCK)
                                   WHERE Nm_ALERT = 'Arquivo de Log Full')

    -- Declares the variables
    DECLARE
        @Tamanho_Minimo_ALERT_log   INT, @ALERTLogHeader VARCHAR(MAX), @ALERTLogTable VARCHAR(MAX), @EmptyBodyEmail VARCHAR(MAX),
        @Importance AS              VARCHAR(6), @EmailBody VARCHAR(MAX), @Subject VARCHAR(500), @Fl_Tipo TINYINT, @Log_Full_Parametro TINYINT,
        @ResultadoWhoisactiveHeader VARCHAR(MAX), @ResultadoWhoisactiveTable VARCHAR(MAX), @EmailDestination VARCHAR(500), @ProfileEmail VARCHAR(200)

    -----------------------------------------------------------
    -- Retrieves ALERT parameters
    -----------------------------------------------------------
    SELECT @Log_Full_Parametro = Vl_Parametro, -- Percentual
           @EmailDestination = Ds_Email,
           @ProfileEmail = Ds_Profile_Email
    FROM [dbo].[ALERT_Parametro]
    WHERE [Id_ALERT_Parametro] = @Id_ALERT_Parametro
    -- Arquivo de Log Full

    -- Seta as variaveis
    SELECT @Tamanho_Minimo_ALERT_log = 500000
    -- 500 MB

    -- Verifica o �ltimo Tipo do ALERT registrado -> 0: CLEAR / 1: ALERT
    SELECT @Fl_Tipo = [Fl_Tipo]
    FROM [dbo].[ALERT]
    WHERE [Id_ALERT] = (SELECT MAX(Id_ALERT)
                        FROM [dbo].[ALERT]
                        WHERE [Id_ALERT_Parametro] = @Id_ALERT_Parametro)

    -- Create a tabela que ira armazenar os dados dos processos
    IF (OBJECT_ID('tempdb..#Resultado_WhoisActive') IS NOT NULL)
        DROP TABLE #Resultado_WhoisActive

    CREATE TABLE #Resultado_WhoisActive
    (
        [dd hh:mm:ss.mss]     VARCHAR(20),
        [database_name]       NVARCHAR(128),
        [login_name]          NVARCHAR(128),
        [host_name]           NVARCHAR(128),
        [start_time]          DATETIME,
        [status]              VARCHAR(30),
        [session_id]          INT,
        [blocking_session_id] INT,
        [wait_info]           VARCHAR(MAX),
        [open_tran_count]     INT,
        [CPU]                 VARCHAR(MAX),
        [reads]               VARCHAR(MAX),
        [writes]              VARCHAR(MAX),
        [sql_command]         XML
    )

    -- Create a tabela que ira armazenar os dados dos arquivos de log
    IF (OBJECT_ID('tempdb..#ALERT_Arquivo_Log_Full') IS NOT NULL)
        DROP TABLE #ALERT_Arquivo_Log_Full

    SELECT db.[name]                                            AS [DatabaseName],
           CAST(
                   ls.[cntr_value] / 1024.00 AS DECIMAL(18, 2)) AS [cntr_value],
           CAST(CAST(lu.[cntr_value] AS FLOAT) /
                CASE
                    WHEN CAST(ls.[cntr_value] AS FLOAT) = 0
                        THEN 1
                    ELSE CAST(ls.[cntr_value] AS FLOAT)
                    END AS DECIMAL(18, 2)) *
           100                                                  AS [Percente_Log_Used]
    INTO #ALERT_Arquivo_Log_Full
    FROM [sys].[databases] AS db
             JOIN [sys].[dm_os_performance_counters] AS lu
                  ON db.[name] = lu.[instance_name]
             JOIN [sys].[dm_os_performance_counters] AS ls
                  ON db.[name] = ls.[instance_name]
    WHERE lu.[counter_name] LIKE 'Log File(s) Used Size (KB)%'
      AND ls.[counter_name] LIKE 'Log File(s) Size (KB)%'
      AND ls.[cntr_value] > @Tamanho_Minimo_ALERT_log
    -- Maior que 100 MB

    /**********************************************************
	-- Verifica se existe algum LOG com muita utiliza��o
	**********************************************************/
    IF EXISTS(
            SELECT *
            FROM #ALERT_Arquivo_Log_Full
            WHERE [Percente_Log_Used] > @Log_Full_Parametro
        )
        BEGIN
            -- START - ALERT
            IF ISNULL(@Fl_Tipo, 0) = 0 -- Envia o ALERT apenas uma vez
                BEGIN
                    -----------------------------------------------------------
                    --	ALERT - DADOS - WHOISACTIVE
                    -----------------------------------------------------------
                    -- Retorna todos os processos que est�o sendo executados no momento
                    EXEC [dbo].[sp_whoisactive]
                         @get_outer_command = 1,
                         @output_column_list = '[dd hh:mm:ss.mss][database_name][login_name][host_name][start_time][status][session_id][blocking_session_id][wait_info][open_tran_count][CPU][reads][writes][sql_command]',
                         @destination_table = '#Resultado_WhoisActive'

                    -- Altera a coluna que possui o comando SQL
                    ALTER TABLE #Resultado_WhoisActive
                        ALTER COLUMN [sql_command] VARCHAR(MAX)

                    UPDATE #Resultado_WhoisActive
                    SET [sql_command] = REPLACE(REPLACE(REPLACE(REPLACE(
                                                                        CAST([sql_command] AS VARCHAR(1000)),
                                                                        '<?query --',
                                                                        ''),
                                                                '--?>',
                                                                ''),
                                                        '&gt;', '>'),
                                                '&lt;', '')

                    -- select * from #Resultado_WhoisActive

                    -- Verifica se n�o existe nenhum processo em Execu��o
                    IF NOT EXISTS(
                            SELECT TOP 1 * FROM #Resultado_WhoisActive)
                        BEGIN
                            INSERT INTO #Resultado_WhoisActive
                            SELECT NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL
                        END

                    /**********************************************************
			--	ALERT - CREATE EMAIL
			**********************************************************/

                    -----------------------------------------------------------
                    --	ALERT - HEADER - LOG FULL
                    -----------------------------------------------------------
                    SET @ALERTLogHeader =
                            '<font color=black bold=true size= 5>'
                    SET @ALERTLogHeader = @ALERTLogHeader +
                                          '<BR /> Informa��es dos Arquivos de Log <BR />'
                    SET @ALERTLogHeader = @ALERTLogHeader + '</font>'

                    -----------------------------------------------------------
                    --	ALERT - BODY - LOG FULL
                    -----------------------------------------------------------
                    SET @ALERTLogTable = CAST((
                        SELECT td = [DatabaseName] + '</td>'
                                        + '<td>' +
                                    CAST([cntr_value] AS VARCHAR) +
                                    '</td>'
                                        + '<td>' +
                                    CAST([Percente_Log_Used] AS VARCHAR) +
                                    '</td>'
                        FROM (
                                 -- EMAIL Table Details
                                 SELECT [DatabaseName],
                                        [cntr_value],
                                        [Percente_Log_Used]
                                 FROM #ALERT_Arquivo_Log_Full
                                 WHERE [Percente_Log_Used] > @Log_Full_Parametro
                             ) AS D
                        ORDER BY [Percente_Log_Used] DESC FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
                        )

                    -- Corrects Table Formatting
                    SET @ALERTLogTable = REPLACE(REPLACE(
                                                         REPLACE(@ALERTLogTable, '&lt;', '<'),
                                                         '&gt;', '>'),
                                                 '<td>',
                                                 '<td align=center>')

                    -- EMAIL Table Titles
                    SET @ALERTLogTable =
                                '<table cellspacing="2" cellpadding="5" border="3">'
                                + '<tr>
							<th bgcolor=#0B0B61 width="200"><font color=white>Database</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Tamanho Log (MB)</font></th>
							<th bgcolor=#0B0B61 width="250"><font color=white>Percentual Log Utilizado (%)</font></th>
						</tr>'
                                + REPLACE(
                                        REPLACE(@ALERTLogTable, '&lt;', '<'),
                                        '&gt;', '>')
                                + '</table>'

                    -----------------------------------------------------------
                    --	ALERT - HEADER - WHOISACTIVE
                    -----------------------------------------------------------
                    SET @ResultadoWhoisactiveHeader =
                            '<font color=black bold=true size=5>'
                    SET @ResultadoWhoisactiveHeader =
                                @ResultadoWhoisactiveHeader +
                                '<BR /> Processos executando no Database <BR />'
                    SET @ResultadoWhoisactiveHeader =
                            @ResultadoWhoisactiveHeader + '</font>'

                    -----------------------------------------------------------
                    --	ALERT - BODY - WHOISACTIVE
                    -----------------------------------------------------------
                    SET @ResultadoWhoisactiveTable = CAST((
                        SELECT td = [Duração] + '</td>'
                                        + '<td>' + [database_name] +
                                    '</td>'
                                        + '<td>' + [login_name] +
                                    '</td>'
                                        + '<td>' + [host_name] + '</td>'
                                        + '<td>' + [start_time] +
                                    '</td>'
                                        + '<td>' + [status] + '</td>'
                                        + '<td>' + [session_id] +
                                    '</td>'
                                        + '<td>' +
                                    [blocking_session_id] + '</td>'
                                        + '<td>' + [Wait] + '</td>'
                                        + '<td>' + [open_tran_count] +
                                    '</td>'
                                        + '<td>' + [CPU] + '</td>'
                                        + '<td>' + [reads] + '</td>'
                                        + '<td>' + [writes] + '</td>'
                                        + '<td>' + [sql_command] +
                                    '</td>'

                        FROM (
                                 -- EMAIL Table Details
                                 SELECT ISNULL([dd hh:mm:ss.mss], '-') AS [Duração],
                                        ISNULL([database_name], '-')   AS [database_name],
                                        ISNULL([login_name], '-')      AS [login_name],
                                        ISNULL([host_name], '-')       AS [host_name],
                                        ISNULL(
                                                CONVERT(VARCHAR(20), [start_time], 120),
                                                '-')                   AS [start_time],
                                        ISNULL([status], '-')          AS [status],
                                        ISNULL(
                                                CAST([session_id] AS VARCHAR),
                                                '-')                   AS [session_id],
                                        ISNULL(
                                                CAST([blocking_session_id] AS VARCHAR),
                                                '-')                   AS [blocking_session_id],
                                        ISNULL([wait_info], '-')       AS [Wait],
                                        ISNULL(
                                                CAST([open_tran_count] AS VARCHAR),
                                                '-')                   AS [open_tran_count],
                                        ISNULL([CPU], '-')             AS [CPU],
                                        ISNULL([reads], '-')           AS [reads],
                                        ISNULL([writes], '-')          AS [writes],
                                        ISNULL(
                                                SUBSTRING([sql_command], 1, 300),
                                                '-')                   AS [sql_command]
                                 FROM #Resultado_WhoisActive
                             ) AS D
                        ORDER BY [start_time] FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
                        )

                    -- Corrects Table Formatting
                    SET @ResultadoWhoisactiveTable = REPLACE(
                            REPLACE(REPLACE(@ResultadoWhoisactiveTable,
                                            '&lt;', '<'), '&gt;', '>'),
                            '<td>', '<td align = center>')

                    -- EMAIL Table Titles
                    SET @ResultadoWhoisactiveTable =
                                '<table cellspacing="2" cellpadding="5" border="3">'
                                + '<tr>
							<th bgcolor=#0B0B61 width="140"><font color=white>[dd hh:mm:ss.mss]</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Database</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Login</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Host Name</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Hora In�cio</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Status</font></th>
							<th bgcolor=#0B0B61 width="30"><font color=white>ID Session</font></th>
							<th bgcolor=#0B0B61 width="60"><font color=white>ID Session Bloqueando</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Wait</font></th>
							<th bgcolor=#0B0B61 width="60"><font color=white>transactions Abertas</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>CPU</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Reads</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Writes</font></th>
							<th bgcolor=#0B0B61 width="1000"><font color=white>Query</font></th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  @ResultadoWhoisactiveTable,
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'

                    -----------------------------------------------------------
                    -- Insert a blank page in EMAIL
                    -----------------------------------------------------------
                    SET @EmptyBodyEmail = ''
                    SET @EmptyBodyEmail =
                                '<table cellpadding="5" cellspacing="5" border="0">' +
                                '<tr>
							<th width="500">               </th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  ISNULL(@EmptyBodyEmail, ''),
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'

                    /**********************************************************
			--	Set EMAIL Variables
			**********************************************************/
                    SELECT @Importance = 'High',
                           @Subject =
                           'ALERT: Existe algum Arquivo de Log com mais de ' +
                           CAST((@Log_Full_Parametro) AS VARCHAR) +
                           '% Usage on Server: ' +
                           @@SERVERNAME,
                           @EmailBody =
                           @ALERTLogHeader + @EmptyBodyEmail +
                           @ALERTLogTable + @EmptyBodyEmail +
                           @ResultadoWhoisactiveHeader +
                           @EmptyBodyEmail +
                           @ResultadoWhoisactiveTable + @EmptyBodyEmail

                    /**********************************************************
			--	ALERT - ENVIA O EMAIL
			**********************************************************/
                    EXEC [msdb].[dbo].[sp_send_dbmail]
                         @profile_name = @ProfileEmail,
                         @recipients = @EmailDestination,
                         @subject = @Subject,
                         @body = @EmailBody,
                         @body_format = 'HTML',
                         @importance = @Importance

                    /**********************************************************
			-- Inserts a Record in the ALERT Control Table -> Fl_Tipo = 1 : ALERT
			**********************************************************/
                    INSERT INTO [dbo].[ALERT] ([Id_ALERT_Parametro],
                                               [Ds_Mensagem],
                                               [Fl_Tipo])
                    SELECT @Id_ALERT_Parametro, @Subject, 1
                END
        END -- END - ALERT
    ELSE
        BEGIN
            -- START - CLEAR
            IF @Fl_Tipo = 1
                BEGIN
                    -----------------------------------------------------------
                    --	CLEAR - DADOS - WHOISACTIVE
                    -----------------------------------------------------------
                    -- Retorna todos os processos que est�o sendo executados no momento
                    EXEC [dbo].[sp_whoisactive]
                         @get_outer_command = 1,
                         @output_column_list = '[dd hh:mm:ss.mss][database_name][login_name][host_name][start_time][status][session_id][blocking_session_id][wait_info][open_tran_count][CPU][reads][writes][sql_command]',
                         @destination_table = '#Resultado_WhoisActive'

                    -- Altera a coluna que possui o comando SQL
                    ALTER TABLE #Resultado_WhoisActive
                        ALTER COLUMN [sql_command] VARCHAR(MAX)

                    UPDATE #Resultado_WhoisActive
                    SET [sql_command] = REPLACE(REPLACE(REPLACE(REPLACE(
                                                                        CAST([sql_command] AS VARCHAR(1000)),
                                                                        '<?query --',
                                                                        ''),
                                                                '--?>',
                                                                ''),
                                                        '&gt;', '>'),
                                                '&lt;', '')

                    -- select * from #Resultado_WhoisActive

                    -- Verifica se n�o existe nenhum processo em Execu��o
                    IF NOT EXISTS(
                            SELECT TOP 1 * FROM #Resultado_WhoisActive)
                        BEGIN
                            INSERT INTO #Resultado_WhoisActive
                            SELECT NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL
                        END

                    /**********************************************************
			--	CLEAR - CREATE EMAIL
			**********************************************************/

                    -----------------------------------------------------------
                    --	CLEAR - HEADER
                    -----------------------------------------------------------
                    SET @ALERTLogHeader =
                            '<font color=black bold=true size=5>'
                    SET @ALERTLogHeader = @ALERTLogHeader +
                                          '<BR /> Informa��es dos Arquivos de Log <BR />'
                    SET @ALERTLogHeader = @ALERTLogHeader + '</font>'

                    -----------------------------------------------------------
                    --	CLEAR - BODY
                    -----------------------------------------------------------
                    SET @ALERTLogTable = CAST((
                        SELECT td = [DatabaseName] + '</td>'
                                        + '<td>' +
                                    CAST([cntr_value] AS VARCHAR) +
                                    '</td>'
                                        + '<td>' +
                                    CAST([Percente_Log_Used] AS VARCHAR) +
                                    '</td>'

                        FROM (
                                 -- EMAIL Table Details
                                 SELECT [DatabaseName],
                                        [cntr_value],
                                        [Percente_Log_Used]
                                 FROM #ALERT_Arquivo_Log_Full
                             ) AS D
                        ORDER BY [Percente_Log_Used] DESC FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
                        )

                    -- Corrects Table Formatting
                    SET @ALERTLogTable = REPLACE(REPLACE(
                                                         REPLACE(@ALERTLogTable, '&lt;', '<'),
                                                         '&gt;', '>'),
                                                 '<td>',
                                                 '<td align = center>')

                    -- EMAIL Table Titles
                    SET @ALERTLogTable =
                                '<table cellspacing="2" cellpadding="5" border="3">'
                                + '<tr>
							<th bgcolor=#0B0B61 width="200"><font color=white>Database</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Tamanho Log (MB)</font></th>
							<th bgcolor=#0B0B61 width="250"><font color=white>Percentual Log Utilizado (%)</font></th>
						</tr>'
                                + REPLACE(
                                        REPLACE(@ALERTLogTable, '&lt;', '<'),
                                        '&gt;', '>')
                                + '</table>'

                    -----------------------------------------------------------
                    --	CLEAR - HEADER - WHOISACTIVE
                    -----------------------------------------------------------
                    SET @ResultadoWhoisactiveHeader =
                            '<font color=black bold=true size=5>'
                    SET @ResultadoWhoisactiveHeader =
                                @ResultadoWhoisactiveHeader +
                                '<BR /> Processos executando no Database <BR />'
                    SET @ResultadoWhoisactiveHeader =
                            @ResultadoWhoisactiveHeader + '</font>'

                    -----------------------------------------------------------
                    --	CLEAR - BODY - WHOISACTIVE
                    -----------------------------------------------------------
                    SET @ResultadoWhoisactiveTable = CAST((
                        SELECT td = [Duração] + '</td>'
                                        + '<td>' + [database_name] +
                                    '</td>'
                                        + '<td>' + [login_name] +
                                    '</td>'
                                        + '<td>' + [host_name] + '</td>'
                                        + '<td>' + [start_time] +
                                    '</td>'
                                        + '<td>' + [status] + '</td>'
                                        + '<td>' + [session_id] +
                                    '</td>'
                                        + '<td>' +
                                    [blocking_session_id] + '</td>'
                                        + '<td>' + [Wait] + '</td>'
                                        + '<td>' + [open_tran_count] +
                                    '</td>'
                                        + '<td>' + [CPU] + '</td>'
                                        + '<td>' + [reads] + '</td>'
                                        + '<td>' + [writes] + '</td>'
                                        + '<td>' + [sql_command] +
                                    '</td>'

                        FROM (
                                 -- EMAIL Table Details
                                 SELECT ISNULL([dd hh:mm:ss.mss], '-') AS [Duração],
                                        ISNULL([database_name], '-')   AS [database_name],
                                        ISNULL([login_name], '-')      AS [login_name],
                                        ISNULL([host_name], '-')       AS [host_name],
                                        ISNULL(
                                                CONVERT(VARCHAR(20), [start_time], 120),
                                                '-')                   AS [start_time],
                                        ISNULL([status], '-')          AS [status],
                                        ISNULL(
                                                CAST([session_id] AS VARCHAR),
                                                '-')                   AS [session_id],
                                        ISNULL(
                                                CAST([blocking_session_id] AS VARCHAR),
                                                '-')                   AS [blocking_session_id],
                                        ISNULL([wait_info], '-')       AS [Wait],
                                        ISNULL(
                                                CAST([open_tran_count] AS VARCHAR),
                                                '-')                   AS [open_tran_count],
                                        ISNULL([CPU], '-')             AS [CPU],
                                        ISNULL([reads], '-')           AS [reads],
                                        ISNULL([writes], '-')          AS [writes],
                                        ISNULL(
                                                SUBSTRING([sql_command], 1, 300),
                                                '-')                   AS [sql_command]
                                 FROM #Resultado_WhoisActive
                             ) AS D
                        ORDER BY [start_time] FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
                        )

                    -- Corrects Table Formatting
                    SET @ResultadoWhoisactiveTable = REPLACE(
                            REPLACE(REPLACE(@ResultadoWhoisactiveTable,
                                            '&lt;', '<'), '&gt;', '>'),
                            '<td>', '<td align = center>')

                    -- EMAIL Table Titles
                    SET @ResultadoWhoisactiveTable =
                                '<table cellspacing="2" cellpadding="5" border="3">'
                                + '<tr>
							<th bgcolor=#0B0B61 width="140"><font color=white>[dd hh:mm:ss.mss]</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Database</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Login</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Host Name</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Hora In�cio</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Status</font></th>
							<th bgcolor=#0B0B61 width="30"><font color=white>ID Session</font></th>
							<th bgcolor=#0B0B61 width="60"><font color=white>ID Session Bloqueando</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Wait</font></th>
							<th bgcolor=#0B0B61 width="60"><font color=white>transactions Abertas</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>CPU</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Reads</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Writes</font></th>
							<th bgcolor=#0B0B61 width="1000"><font color=white>Query</font></th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  @ResultadoWhoisactiveTable,
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'

                    -----------------------------------------------------------
                    -- Insert a blank page in EMAIL
                    -----------------------------------------------------------
                    SET @EmptyBodyEmail = ''
                    SET @EmptyBodyEmail =
                                '<table cellpadding="5" cellspacing="5" border="0">' +
                                '<tr>
							<th width="500">               </th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  ISNULL(@EmptyBodyEmail, ''),
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'

                    /**********************************************************
			        --	Set EMAIL Variables
			        **********************************************************/
                    SELECT @Importance = 'High',
                           @Subject =
                           'CLEAR: N�o existe mais algum Arquivo de Log com mais de ' +
                           CAST((@Log_Full_Parametro) AS VARCHAR) +
                           '% Usage on Server: ' +
                           @@SERVERNAME,
                           @EmailBody =
                           @ALERTLogHeader + @EmptyBodyEmail +
                           @ALERTLogTable + @EmptyBodyEmail +
                           @ResultadoWhoisactiveHeader +
                           @EmptyBodyEmail +
                           @ResultadoWhoisactiveTable + @EmptyBodyEmail

                    /************************************************************************************
			        --	ALERT - ENVIA O EMAIL
			        **********************************************************/
                    EXEC [msdb].[dbo].[sp_send_dbmail]
                         @profile_name = @ProfileEmail,
                         @recipients = @EmailDestination,
                         @subject = @Subject,
                         @body = @EmailBody,
                         @body_format = 'HTML',
                         @importance = @Importance

                    /**********************************************************
			-- Inserts a Record in the ALERT Control Table -> Fl_Tipo = 0 : CLEAR
			**********************************************************/
                    INSERT INTO [dbo].[ALERT] ([Id_ALERT_Parametro],
                                               [Ds_Mensagem],
                                               [Fl_Tipo])
                    SELECT @Id_ALERT_Parametro, @Subject, 0
                END
        END -- END - CLEAR
END



GO
IF (OBJECT_ID('[dbo].[stpALERT_Espaco_Disco]') IS NOT NULL)
    DROP PROCEDURE [dbo].[stpALERT_Espaco_Disco]
GO

/**********************************************************
--	ALERT: ESPA�O DISCO
**********************************************************/

CREATE PROCEDURE [dbo].[stpALERT_Espaco_Disco]
AS
BEGIN
    SET NOCOUNT ON

    -- Create as tabelas que ir�o armazenar as informa��es do Espa�o em Disco
    IF (OBJECT_ID('tempdb..#dbspace') IS NOT NULL)
        DROP TABLE #dbspace

    CREATE TABLE #dbspace
    (
        [name]    SYSNAME,
        [caminho] VARCHAR(200),
        [tamanho] VARCHAR(10),
        [drive]   VARCHAR(30)
    )

    IF (OBJECT_ID('tempdb..#espacodisco') IS NOT NULL)
        DROP TABLE #espacodisco

    CREATE TABLE [#espacodisco]
    (
        [Drive]            VARCHAR(10),
        [Tamanho (MB)]     INT,
        [Usado (MB)]       INT,
        [Livre (MB)]       INT,
        [Livre (%)]        INT,
        [Usado (%)]        INT,
        [Ocupado SQL (MB)] INT,
        [Data]             SMALLDATETIME
    )

    IF (OBJECT_ID('tempdb..#space') IS NOT NULL)
        DROP TABLE #space

    CREATE TABLE #space
    (
        [drive]  CHAR(1),
        [mbfree] INT
    )

    -- Popula as tabelas com as informa��es sobre o Espa�o em Disco
    EXEC sp_MSforeachdb
         'Use [?] INSERT INTO #dbspace SELECT CONVERT(VARCHAR(25), DB_Name()) ''Database'', CONVERT(VARCHAR(60), FileName), CONVERT(VARCHAR(8), Size / 128) ''Size in MB'', CONVERT(VARCHAR(30), Name) FROM sysfiles'

    -- Declares the variables
    DECLARE
        @hr           INT, @fso INT, @mbtotal INT, @TotalSpace INT, @MBFree INT, @Percentage INT,
        @SQLDriveSize INT, @size float, @drive VARCHAR(1), @fso_Method VARCHAR(255)

    SELECT @mbtotal = 0,
           @mbtotal = 0

    EXEC @hr = [master].[dbo].[sp_OACreate]
               'Scripting.FilesystemObject', @fso OUTPUT

    INSERT INTO #space
        EXEC [master].[dbo].[xp_fixeddrives]

    -- Utiliza o Cursor para gerar as informa��es de cada Disco
    DECLARE CheckDrives CURSOR FOR SELECT drive, mbfree FROM #space
    OPEN CheckDrives
    FETCH NEXT FROM CheckDrives INTO @drive, @MBFree
    WHILE (@@FETCH_STATUS = 0)
    BEGIN
        SET @fso_Method = 'Drives("' + @drive + ':").TotalSize'

        SELECT @SQLDriveSize = SUM(CONVERT(INT, [tamanho]))
        FROM #dbspace
        WHERE SUBSTRING([caminho], 1, 1) = @drive

        EXEC @hr = [sp_OAMethod] @fso, @fso_Method, @size OUTPUT

        SET @mbtotal = @size / (1024 * 1024)

        INSERT INTO #espacodisco
        VALUES (@drive + ':', @mbtotal, @mbtotal - @MBFree, @MBFree,
                (100 * ROUND(@MBFree, 2) / ROUND(@mbtotal, 2)),
                (100 - 100 * ROUND(@MBFree, 2) / ROUND(@mbtotal, 2)),
                @SQLDriveSize, GETDATE())

        FETCH NEXT FROM CheckDrives INTO @drive,@MBFree
    END
    CLOSE CheckDrives
    DEALLOCATE CheckDrives

    -- Tabela com os dados resumidos sobre o Espa�o em Disco
    IF (OBJECT_ID('_DTS_Espacodisco ') IS NOT NULL)
        DROP TABLE _DTS_Espacodisco

    SELECT [Drive],
           [Tamanho (MB)],
           [Usado (MB)],
           [Livre (MB)],
           [Livre (%)],
           [Usado (%)],
           ISNULL([Ocupado SQL (MB)], 0) AS [Ocupado SQL (MB)]
    INTO [dbo].[_DTS_Espacodisco]
    FROM #espacodisco

    -- Create a tabela que ira armazenar os dados dos processos
    IF (OBJECT_ID('tempdb..#Resultado_WhoisActive') IS NOT NULL)
        DROP TABLE #Resultado_WhoisActive

    CREATE TABLE #Resultado_WhoisActive
    (
        [dd hh:mm:ss.mss]     VARCHAR(20),
        [database_name]       NVARCHAR(128),
        [login_name]          NVARCHAR(128),
        [host_name]           NVARCHAR(128),
        [start_time]          DATETIME,
        [status]              VARCHAR(30),
        [session_id]          INT,
        [blocking_session_id] INT,
        [wait_info]           VARCHAR(MAX),
        [open_tran_count]     INT,
        [CPU]                 VARCHAR(MAX),
        [reads]               VARCHAR(MAX),
        [writes]              VARCHAR(MAX),
        [sql_command]         XML
    )

    -- Declares the variables
    DECLARE
        @Subject                    VARCHAR(500), @Fl_Tipo TINYINT, @Importance AS VARCHAR(6), @EmailBody VARCHAR(MAX),
        @ALERTDiscoHeader           VARCHAR(MAX), @ALERTDiscoTable VARCHAR(MAX), @EmptyBodyEmail VARCHAR(MAX), @EmailDestination VARCHAR(500),
        @ResultadoWhoisactiveHeader VARCHAR(MAX), @ResultadoWhoisactiveTable VARCHAR(MAX), @Espaco_Disco_Parametro INT, @ProfileEmail VARCHAR(200)

    -----------------------------------------------------------
    -- Retrieves ALERT parameters
    -----------------------------------------------------------
    -- Disc Space
    DECLARE
        @Id_ALERT_Parametro INT = (SELECT Id_ALERT_Parametro
                                   FROM [Traces].[dbo].ALERT_Parametro (NOLOCK)
                                   WHERE Nm_ALERT = 'Disc Space')

    SELECT @Espaco_Disco_Parametro = Vl_Parametro, -- Percentual
           @EmailDestination = Ds_Email,
           @ProfileEmail = Ds_Profile_Email
    FROM [dbo].[ALERT_Parametro]
    WHERE [Id_ALERT_Parametro] = @Id_ALERT_Parametro
    -- Disc Space

    -- Verifica o �ltimo Tipo do ALERT registrado -> 0: CLEAR / 1: ALERT
    SELECT @Fl_Tipo = [Fl_Tipo]
    FROM [dbo].[ALERT]
    WHERE [Id_ALERT] = (SELECT MAX(Id_ALERT)
                        FROM [dbo].[ALERT]
                        WHERE [Id_ALERT_Parametro] = @Id_ALERT_Parametro)

    /**********************************************************
	--	Verifica o Espa�o Livre em Disco
	**********************************************************/
    IF EXISTS(
            SELECT NULL
            FROM [dbo].[_DTS_Espacodisco]
            WHERE [Usado (%)] > @Espaco_Disco_Parametro
        )
        BEGIN
            -- START - ALERT
            IF ISNULL(@Fl_Tipo, 0) = 0 -- Envia o ALERT apenas uma vez
                BEGIN
                    -----------------------------------------------------------
                    --	ALERT - DADOS - WHOISACTIVE
                    -----------------------------------------------------------
                    -- Retorna todos os processos que est�o sendo executados no momento
                    EXEC [dbo].[sp_whoisactive]
                         @get_outer_command = 1,
                         @output_column_list = '[dd hh:mm:ss.mss][database_name][login_name][host_name][start_time][status][session_id][blocking_session_id][wait_info][open_tran_count][CPU][reads][writes][sql_command]',
                         @destination_table = '#Resultado_WhoisActive'

                    -- Altera a coluna que possui o comando SQL
                    ALTER TABLE #Resultado_WhoisActive
                        ALTER COLUMN [sql_command] VARCHAR(MAX)

                    UPDATE #Resultado_WhoisActive
                    SET [sql_command] = REPLACE(REPLACE(REPLACE(REPLACE(
                                                                        CAST([sql_command] AS VARCHAR(1000)),
                                                                        '<?query --',
                                                                        ''),
                                                                '--?>',
                                                                ''),
                                                        '&gt;', '>'),
                                                '&lt;', '')

                    -- select * from #Resultado_WhoisActive

                    -- Verifica se n�o existe nenhum processo em Execu��o
                    IF NOT EXISTS(
                            SELECT TOP 1 * FROM #Resultado_WhoisActive)
                        BEGIN
                            INSERT INTO #Resultado_WhoisActive
                            SELECT NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL
                        END

                    /**********************************************************
			        --	CREATE EMAIL - ALERT
			        **********************************************************/

                    -----------------------------------------------------------
                    --	ALERT - HEADER
                    -----------------------------------------------------------
                    SET @ALERTDiscoHeader =
                            '<font color=black bold=true size=5>'
                    SET @ALERTDiscoHeader = @ALERTDiscoHeader +
                                            '<BR /> Espa�o em Disco no Servidor <BR />'
                    SET @ALERTDiscoHeader =
                            @ALERTDiscoHeader + '</font>'

                    -----------------------------------------------------------
                    --	ALERT - BODY
                    -----------------------------------------------------------
                    SET @ALERTDiscoTable = CAST((
                        SELECT td = [Drive] + '</td>'
                                        + '<td>' + [Tamanho (MB)] +
                                    '</td>'
                                        + '<td>' + [Usado (MB)] +
                                    '</td>'
                                        + '<td>' + [Livre (MB)] +
                                    '</td>'
                                        + '<td>' + [Livre (%)] + '</td>'
                                        + '<td>' + [Usado (%)] + '</td>'
                                        + '<td>' + [Ocupado SQL (MB)] +
                                    '</td>'
                        FROM (
                                 -- EMAIL Table Details
                                 SELECT [Drive],
                                        CAST([Tamanho (MB)] AS VARCHAR)     AS [Tamanho (MB)],
                                        CAST([Usado (MB)] AS VARCHAR)       AS [Usado (MB)],
                                        CAST([Livre (MB)] AS VARCHAR)       AS [Livre (MB)],
                                        CAST([Livre (%)] AS VARCHAR)        AS [Livre (%)],
                                        CAST([Usado (%)] AS VARCHAR)        AS [Usado (%)],
                                        CAST([Ocupado SQL (MB)] AS VARCHAR) AS [Ocupado SQL (MB)]
                                 FROM [Traces].[dbo].[_DTS_Espacodisco]
                             ) AS D
                        ORDER BY [Drive] FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
                        )

                    -- Corrects Table Formatting
                    SET @ALERTDiscoTable = REPLACE(REPLACE(
                                                           REPLACE(@ALERTDiscoTable, '&lt;', '<'),
                                                           '&gt;',
                                                           '>'),
                                                   '<td>',
                                                   '<td align = center>')

                    -- EMAIL Table Titles
                    SET @ALERTDiscoTable =
                                '<table cellspacing="2" cellpadding="5" border="3">'
                                + '<tr>
							<th bgcolor=#0B0B61 width="100"><font color=white>Drive (%)</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Tamanho (MB)</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Usado (MB)</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Livre (MB)</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Livre (%)</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Usado (%)</font></th>
							<th bgcolor=#0B0B61 width="150"><font color=white>Ocupado SQL (MB)</font></th>
						</tr>'
                                + REPLACE(
                                        REPLACE(@ALERTDiscoTable, '&lt;', '<'),
                                        '&gt;', '>')
                                + '</table>'

                    -----------------------------------------------------------
                    --	ALERT - HEADER - WHOISACTIVE
                    -----------------------------------------------------------
                    SET @ResultadoWhoisactiveHeader =
                            '<font color=black bold=true size=5>'
                    SET @ResultadoWhoisactiveHeader =
                                @ResultadoWhoisactiveHeader +
                                '<BR /> Processos executando no Database <BR />'
                    SET @ResultadoWhoisactiveHeader =
                            @ResultadoWhoisactiveHeader + '</font>'

                    -----------------------------------------------------------
                    --	ALERT - BODY - WHOISACTIVE
                    -----------------------------------------------------------
                    SET @ResultadoWhoisactiveTable = CAST((
                        SELECT td = [Duração] + '</td>'
                                        + '<td>' + [database_name] +
                                    '</td>'
                                        + '<td>' + [login_name] +
                                    '</td>'
                                        + '<td>' + [host_name] + '</td>'
                                        + '<td>' + [start_time] +
                                    '</td>'
                                        + '<td>' + [status] + '</td>'
                                        + '<td>' + [session_id] +
                                    '</td>'
                                        + '<td>' +
                                    [blocking_session_id] + '</td>'
                                        + '<td>' + [Wait] + '</td>'
                                        + '<td>' + [open_tran_count] +
                                    '</td>'
                                        + '<td>' + [CPU] + '</td>'
                                        + '<td>' + [reads] + '</td>'
                                        + '<td>' + [writes] + '</td>'
                                        + '<td>' + [sql_command] +
                                    '</td>'

                        FROM (
                                 -- EMAIL Table Details
                                 SELECT ISNULL([dd hh:mm:ss.mss], '-') AS [Duração],
                                        ISNULL([database_name], '-')   AS [database_name],
                                        ISNULL([login_name], '-')      AS [login_name],
                                        ISNULL([host_name], '-')       AS [host_name],
                                        ISNULL(
                                                CONVERT(VARCHAR(20), [start_time], 120),
                                                '-')                   AS [start_time],
                                        ISNULL([status], '-')          AS [status],
                                        ISNULL(
                                                CAST([session_id] AS VARCHAR),
                                                '-')                   AS [session_id],
                                        ISNULL(
                                                CAST([blocking_session_id] AS VARCHAR),
                                                '-')                   AS [blocking_session_id],
                                        ISNULL([wait_info], '-')       AS [Wait],
                                        ISNULL(
                                                CAST([open_tran_count] AS VARCHAR),
                                                '-')                   AS [open_tran_count],
                                        ISNULL([CPU], '-')             AS [CPU],
                                        ISNULL([reads], '-')           AS [reads],
                                        ISNULL([writes], '-')          AS [writes],
                                        ISNULL(
                                                SUBSTRING([sql_command], 1, 300),
                                                '-')                   AS [sql_command]
                                 FROM #Resultado_WhoisActive
                             ) AS D
                        ORDER BY [start_time] FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
                        )

                    -- Corrects Table Formatting
                    SET @ResultadoWhoisactiveTable = REPLACE(
                            REPLACE(REPLACE(@ResultadoWhoisactiveTable,
                                            '&lt;', '<'), '&gt;', '>'),
                            '<td>', '<td align = center>')

                    -- EMAIL Table Titles
                    SET @ResultadoWhoisactiveTable =
                                '<table cellspacing="2" cellpadding="5" border="3">'
                                + '<tr>
							<th bgcolor=#0B0B61 width="140"><font color=white>[dd hh:mm:ss.mss]</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Database</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Login</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Host Name</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Hora In�cio</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Status</font></th>
							<th bgcolor=#0B0B61 width="30"><font color=white>ID Session</font></th>
							<th bgcolor=#0B0B61 width="60"><font color=white>ID Session Bloqueando</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Wait</font></th>
							<th bgcolor=#0B0B61 width="60"><font color=white>transactions Abertas</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>CPU</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Reads</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Writes</font></th>
							<th bgcolor=#0B0B61 width="1000"><font color=white>Query</font></th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  @ResultadoWhoisactiveTable,
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'

                    -----------------------------------------------------------
                    -- Insert a blank page in EMAIL
                    -----------------------------------------------------------
                    SET @EmptyBodyEmail = ''
                    SET @EmptyBodyEmail =
                                '<table cellpadding="5" cellspacing="5" border="0">' +
                                '<tr>
							<th width="500">               </th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  ISNULL(@EmptyBodyEmail, ''),
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'

                    /**********************************************************
			        --	SEND EMAIL - ALERT
			        **********************************************************/
                    SELECT @Importance = 'High',
                           @Subject =
                           'ALERT: Existe algum volume de disco com mais de ' +
                           CAST((@Espaco_Disco_Parametro) AS VARCHAR) +
                           '% Usage on Server: ' +
                           @@SERVERNAME,
                           @EmailBody =
                           @ALERTDiscoHeader + @EmptyBodyEmail +
                           @ALERTDiscoTable + @EmptyBodyEmail +
                           @ResultadoWhoisactiveHeader +
                           @EmptyBodyEmail +
                           @ResultadoWhoisactiveTable + @EmptyBodyEmail


                    /*********************************************************/

                    EXEC [msdb].[dbo].[sp_send_dbmail]
                         @profile_name = @ProfileEmail,
                         @recipients = @EmailDestination,
                         @subject = @Subject,
                         @body = @EmailBody,
                         @body_format = 'HTML',
                         @importance = @Importance

                    /**********************************************************
			        -- Inserts a Record in the ALERT Control Table -> Fl_Tipo = 1 : ALERT
			        **********************************************************/
                    INSERT INTO [dbo].[ALERT] ([Id_ALERT_Parametro],
                                               [Ds_Mensagem],
                                               [Fl_Tipo])
                    SELECT @Id_ALERT_Parametro, @Subject, 1
                END
        END -- END - ALERT
    ELSE
        BEGIN
            -- START - CLEAR
            IF @Fl_Tipo = 1
                BEGIN
                    -----------------------------------------------------------
                    --	CLEAR - DADOS - WHOISACTIVE
                    -----------------------------------------------------------
                    -- Retorna todos os processos que est�o sendo executados no momento
                    EXEC [dbo].[sp_whoisactive]
                         @get_outer_command = 1,
                         @output_column_list = '[dd hh:mm:ss.mss][database_name][login_name][host_name][start_time][status][session_id][blocking_session_id][wait_info][open_tran_count][CPU][reads][writes][sql_command]',
                         @destination_table = '#Resultado_WhoisActive'

                    -- Altera a coluna que possui o comando SQL
                    ALTER TABLE #Resultado_WhoisActive
                        ALTER COLUMN [sql_command] VARCHAR(MAX)

                    UPDATE #Resultado_WhoisActive
                    SET [sql_command] = REPLACE(REPLACE(REPLACE(REPLACE(
                                                                        CAST([sql_command] AS VARCHAR(1000)),
                                                                        '<?query --',
                                                                        ''),
                                                                '--?>',
                                                                ''),
                                                        '&gt;', '>'),
                                                '&lt;', '')

                    -- select * from #Resultado_WhoisActive

                    -- Verifica se n�o existe nenhum processo em Execu��o
                    IF NOT EXISTS(
                            SELECT TOP 1 * FROM #Resultado_WhoisActive)
                        BEGIN
                            INSERT INTO #Resultado_WhoisActive
                            SELECT NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL
                        END

                    /**********************************************************
			--	CREATE EMAIL - CLEAR
			**********************************************************/

                    -----------------------------------------------------------
                    --	CLEAR - HEADER
                    -----------------------------------------------------------
                    SET @ALERTDiscoHeader =
                            '<font color=black bold=true size=5>'
                    SET @ALERTDiscoHeader = @ALERTDiscoHeader +
                                            '<BR /> Espa�o em Disco no Servidor <BR />'
                    SET @ALERTDiscoHeader =
                            @ALERTDiscoHeader + '</font>'

                    -----------------------------------------------------------
                    --	CLEAR - BODY
                    -----------------------------------------------------------
                    SET @ALERTDiscoTable = CAST((
                        SELECT td = [Drive] + '</td>'
                                        + '<td>' + [Tamanho (MB)] +
                                    '</td>'
                                        + '<td>' + [Usado (MB)] +
                                    '</td>'
                                        + '<td>' + [Livre (MB)] +
                                    '</td>'
                                        + '<td>' + [Livre (%)] + '</td>'
                                        + '<td>' + [Usado (%)] + '</td>'
                                        + '<td>' + [Ocupado SQL (MB)] +
                                    '</td>'
                        FROM (
                                 -- EMAIL Table Details
                                 SELECT [Drive],
                                        CAST([Tamanho (MB)] AS VARCHAR)     AS [Tamanho (MB)],
                                        CAST([Usado (MB)] AS VARCHAR)       AS [Usado (MB)],
                                        CAST([Livre (MB)] AS VARCHAR)       AS [Livre (MB)],
                                        CAST([Livre (%)] AS VARCHAR)        AS [Livre (%)],
                                        CAST([Usado (%)] AS VARCHAR)        AS [Usado (%)],
                                        CAST([Ocupado SQL (MB)] AS VARCHAR) AS [Ocupado SQL (MB)]
                                 FROM [Traces].[dbo].[_DTS_Espacodisco]
                             ) AS D
                        ORDER BY [Drive] FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
                        )

                    -- Corrects Table Formatting
                    SET @ALERTDiscoTable = REPLACE(REPLACE(
                                                           REPLACE(@ALERTDiscoTable, '&lt;', '<'),
                                                           '&gt;',
                                                           '>'),
                                                   '<td>',
                                                   '<td align = center>')

                    -- EMAIL Table Titles
                    SET @ALERTDiscoTable =
                                '<table cellspacing="2" cellpadding="5" border="3">'
                                + '<tr>
							<th bgcolor=#0B0B61 width="100"><font color=white>Drive (%)</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Tamanho (MB)</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Usado (MB)</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Livre (MB)</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Livre (%)</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Usado (%)</font></th>
							<th bgcolor=#0B0B61 width="150"><font color=white>Ocupado SQL (MB)</font></th>
						</tr>'
                                + REPLACE(
                                        REPLACE(@ALERTDiscoTable, '&lt;', '<'),
                                        '&gt;', '>')
                                + '</table>'

                    -----------------------------------------------------------
                    --	CLEAR - HEADER - WHOISACTIVE
                    -----------------------------------------------------------
                    SET @ResultadoWhoisactiveHeader =
                            '<font color=black bold=true size=5>'
                    SET @ResultadoWhoisactiveHeader =
                                @ResultadoWhoisactiveHeader +
                                '<BR /> Processos executando no Database <BR />'
                    SET @ResultadoWhoisactiveHeader =
                            @ResultadoWhoisactiveHeader + '</font>'

                    -----------------------------------------------------------
                    --	CLEAR - BODY - WHOISACTIVE
                    -----------------------------------------------------------
                    SET @ResultadoWhoisactiveTable = CAST((
                        SELECT td = [Duração] + '</td>'
                                        + '<td>' + [database_name] +
                                    '</td>'
                                        + '<td>' + [login_name] +
                                    '</td>'
                                        + '<td>' + [host_name] + '</td>'
                                        + '<td>' + [start_time] +
                                    '</td>'
                                        + '<td>' + [status] + '</td>'
                                        + '<td>' + [session_id] +
                                    '</td>'
                                        + '<td>' +
                                    [blocking_session_id] + '</td>'
                                        + '<td>' + [Wait] + '</td>'
                                        + '<td>' + [open_tran_count] +
                                    '</td>'
                                        + '<td>' + [CPU] + '</td>'
                                        + '<td>' + [reads] + '</td>'
                                        + '<td>' + [writes] + '</td>'
                                        + '<td>' + [sql_command] +
                                    '</td>'

                        FROM (
                                 -- EMAIL Table Details
                                 SELECT ISNULL([dd hh:mm:ss.mss], '-') AS [Duração],
                                        ISNULL([database_name], '-')   AS [database_name],
                                        ISNULL([login_name], '-')      AS [login_name],
                                        ISNULL([host_name], '-')       AS [host_name],
                                        ISNULL(
                                                CONVERT(VARCHAR(20), [start_time], 120),
                                                '-')                   AS [start_time],
                                        ISNULL([status], '-')          AS [status],
                                        ISNULL(
                                                CAST([session_id] AS VARCHAR),
                                                '-')                   AS [session_id],
                                        ISNULL(
                                                CAST([blocking_session_id] AS VARCHAR),
                                                '-')                   AS [blocking_session_id],
                                        ISNULL([wait_info], '-')       AS [Wait],
                                        ISNULL(
                                                CAST([open_tran_count] AS VARCHAR),
                                                '-')                   AS [open_tran_count],
                                        ISNULL([CPU], '-')             AS [CPU],
                                        ISNULL([reads], '-')           AS [reads],
                                        ISNULL([writes], '-')          AS [writes],
                                        ISNULL(
                                                SUBSTRING([sql_command], 1, 300),
                                                '-')                   AS [sql_command]
                                 FROM #Resultado_WhoisActive
                             ) AS D
                        ORDER BY [start_time] FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
                        )

                    -- Corrects Table Formatting
                    SET @ResultadoWhoisactiveTable = REPLACE(
                            REPLACE(REPLACE(@ResultadoWhoisactiveTable,
                                            '&lt;', '<'), '&gt;', '>'),
                            '<td>', '<td align = center>')

                    -- EMAIL Table Titles
                    SET @ResultadoWhoisactiveTable =
                                '<table cellspacing="2" cellpadding="5" border="3">'
                                + '<tr>
							<th bgcolor=#0B0B61 width="140"><font color=white>[dd hh:mm:ss.mss]</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Database</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Login</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Host Name</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Hora In�cio</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Status</font></th>
							<th bgcolor=#0B0B61 width="30"><font color=white>ID Session</font></th>
							<th bgcolor=#0B0B61 width="60"><font color=white>ID Session Bloqueando</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Wait</font></th>
							<th bgcolor=#0B0B61 width="60"><font color=white>transactions Abertas</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>CPU</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Reads</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Writes</font></th>
							<th bgcolor=#0B0B61 width="1000"><font color=white>Query</font></th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  @ResultadoWhoisactiveTable,
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'

                    -----------------------------------------------------------
                    -- Insert a blank page in EMAIL
                    -----------------------------------------------------------
                    SET @EmptyBodyEmail = ''
                    SET @EmptyBodyEmail =
                                '<table cellpadding="5" cellspacing="5" border="0">' +
                                '<tr>
							<th width="500">               </th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  ISNULL(@EmptyBodyEmail, ''),
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'

                    /**********************************************************
			--	ENVIA O EMAIL - CLEAR
			**********************************************************/
                    SELECT @Importance = 'High',
                           @Subject =
                           'CLEAR: N�o existe mais algum volume de disco com mais de ' +
                           CAST((@Espaco_Disco_Parametro) AS VARCHAR) +
                           '% Usage on Server: ' +
                           @@SERVERNAME,
                           @EmailBody =
                           @ALERTDiscoHeader + @EmptyBodyEmail +
                           @ALERTDiscoTable + @EmptyBodyEmail +
                           @ResultadoWhoisactiveHeader +
                           @EmptyBodyEmail +
                           @ResultadoWhoisactiveTable + @EmptyBodyEmail

                    EXEC [msdb].[dbo].[sp_send_dbmail]
                         @profile_name = @ProfileEmail,
                         @recipients = @EmailDestination,
                         @subject = @Subject,
                         @body = @EmailBody,
                         @body_format = 'HTML',
                         @importance = @Importance

                    /**********************************************************
			-- Inserts a Record in the ALERT Control Table -> Fl_Tipo = 0 : CLEAR
			**********************************************************/
                    INSERT INTO [dbo].[ALERT] ([Id_ALERT_Parametro],
                                               [Ds_Mensagem],
                                               [Fl_Tipo])
                    SELECT @Id_ALERT_Parametro, @Subject, 0
                END
        END -- END - CLEAR
END


GO
IF (OBJECT_ID('[dbo].[stpALERT_Consumo_CPU]') IS NOT NULL)
    DROP PROCEDURE [dbo].[stpALERT_Consumo_CPU]
GO

/**********************************************************
--	ALERT: CONSUMO CPU
**********************************************************/

CREATE PROCEDURE [dbo].[stpALERT_Consumo_CPU]
AS
BEGIN
    SET NOCOUNT ON

    -- Consumo CPU
    DECLARE
        @Id_ALERT_Parametro INT = (SELECT Id_ALERT_Parametro
                                   FROM [Traces].[dbo].ALERT_Parametro (NOLOCK)
                                   WHERE Nm_ALERT = 'Consumo CPU')

    -- Declares the variables
    DECLARE
        @Subject                    VARCHAR(500), @Fl_Tipo TINYINT, @Importance AS VARCHAR(6), @EmailBody VARCHAR(MAX), @CPU_Parametro INT,
        @ALERTCPUAgarradosHeader    VARCHAR(MAX), @ALERTCPUAgarradosTable VARCHAR(MAX), @EmptyBodyEmail VARCHAR(MAX),
        @ResultadoWhoisactiveHeader VARCHAR(MAX), @ResultadoWhoisactiveTable VARCHAR(MAX), @EmailDestination VARCHAR(500),
        @ProfileEmail               VARCHAR(200)

    -----------------------------------------------------------
    -- Retrieves ALERT parameters
    -----------------------------------------------------------
    SELECT @CPU_Parametro = Vl_Parametro, -- Percentual
           @EmailDestination = Ds_Email,
           @ProfileEmail = Ds_Profile_Email
    FROM [dbo].[ALERT_Parametro]
    WHERE [Id_ALERT_Parametro] = @Id_ALERT_Parametro
    -- Consumo CPU

    -- Verifica o �ltimo Tipo do ALERT registrado -> 0: CLEAR / 1: ALERT
    SELECT @Fl_Tipo = [Fl_Tipo]
    FROM [dbo].[ALERT]
    WHERE [Id_ALERT] = (SELECT MAX(Id_ALERT)
                        FROM [dbo].[ALERT]
                        WHERE [Id_ALERT_Parametro] = @Id_ALERT_Parametro)

    -----------------------------------------------------------
    --	Create Tabela para armazenar os Dados da sp_whoisactive
    -----------------------------------------------------------
    -- Create a tabela que ira armazenar os dados dos processos
    IF (OBJECT_ID('tempdb..#Resultado_WhoisActive') IS NOT NULL)
        DROP TABLE #Resultado_WhoisActive

    CREATE TABLE #Resultado_WhoisActive
    (
        [dd hh:mm:ss.mss]     VARCHAR(20),
        [database_name]       NVARCHAR(128),
        [login_name]          NVARCHAR(128),
        [host_name]           NVARCHAR(128),
        [start_time]          DATETIME,
        [status]              VARCHAR(30),
        [session_id]          INT,
        [blocking_session_id] INT,
        [wait_info]           VARCHAR(MAX),
        [open_tran_count]     INT,
        [CPU]                 VARCHAR(MAX),
        [reads]               VARCHAR(MAX),
        [writes]              VARCHAR(MAX),
        [sql_command]         XML
    )

    -----------------------------------------------------------
    -- Verifica a utiliza��o da CPU
    -----------------------------------------------------------
    IF (OBJECT_ID('tempdb..#CPU_Utilization') IS NOT NULL)
        DROP TABLE #CPU_Utilization

    SELECT TOP (2) record_id,
                   [SQLProcessUtilization],
                   100 - SystemIdle - SQLProcessUtilization as OtherProcessUtilization,
                   [SystemIdle],
                   100 - SystemIdle                         AS CPU_Utilization
    INTO #CPU_Utilization
    FROM (
             SELECT record.value('(./Record/@id)[1]', 'int') AS [record_id],
                    record.value(
                            '(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]',
                            'int')                           AS [SystemIdle],
                    record.value(
                            '(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]',
                            'int')                           AS [SQLProcessUtilization],
                    [timestamp]
             FROM (
                      SELECT [timestamp],
                             CONVERT(XML, [record]) AS [record]
                      FROM [sys].[dm_os_ring_buffers]
                      WHERE [ring_buffer_type] =
                            N'RING_BUFFER_SCHEDULER_MONITOR'
                        AND [record] LIKE '%<SystemHealth>%'
                  ) AS X
         ) AS Y
    ORDER BY record_id DESC

    /**********************************************************
	--	Verifica se o Consumo de CPU est� maior do que o parametro
	**********************************************************/
    IF (
           select CPU_Utilization
           from #CPU_Utilization
           where record_id =
                 (select max(record_id) from #CPU_Utilization)
       ) > @CPU_Parametro
        BEGIN
            -- START - ALERT
            IF (
                   select CPU_Utilization
                   from #CPU_Utilization
                   where record_id =
                         (select min(record_id) from #CPU_Utilization)
               ) > @CPU_Parametro
                BEGIN
                    IF ISNULL(@Fl_Tipo, 0) = 0 -- Envia o ALERT apenas uma vez
                        BEGIN
                            -----------------------------------------------------------
                            --	ALERT - DADOS - WHOISACTIVE
                            -----------------------------------------------------------
                            -- Retorna todos os processos que est�o sendo executados no momento
                            EXEC [dbo].[sp_whoisactive]
                                 @get_outer_command = 1,
                                 @output_column_list = '[dd hh:mm:ss.mss][database_name][login_name][host_name][start_time][status][session_id][blocking_session_id][wait_info][open_tran_count][CPU][reads][writes][sql_command]',
                                 @destination_table = '#Resultado_WhoisActive'

                            -- Altera a coluna que possui o comando SQL
                            ALTER TABLE #Resultado_WhoisActive
                                ALTER COLUMN [sql_command] VARCHAR(MAX)

                            UPDATE #Resultado_WhoisActive
                            SET [sql_command] = REPLACE(REPLACE(REPLACE(
                                                                        REPLACE(
                                                                                CAST([sql_command] AS VARCHAR(1000)),
                                                                                '<?query --',
                                                                                ''),
                                                                        '--?>',
                                                                        ''),
                                                                '&gt;',
                                                                '>'),
                                                        '&lt;', '')

                            -- select * from #Resultado_WhoisActive

                            -- Verifica se n�o existe nenhum processo em Execu��o
                            IF NOT EXISTS(
                                    SELECT TOP 1 * FROM #Resultado_WhoisActive)
                                BEGIN
                                    INSERT INTO #Resultado_WhoisActive
                                    SELECT NULL,
                                           NULL,
                                           NULL,
                                           NULL,
                                           NULL,
                                           NULL,
                                           NULL,
                                           NULL,
                                           NULL,
                                           NULL,
                                           NULL,
                                           NULL,
                                           NULL,
                                           NULL
                                END

                            /**********************************************************
				--	CREATE EMAIL - ALERT
				**********************************************************/

                            -----------------------------------------------------------
                            --	ALERT - HEADER
                            -----------------------------------------------------------
                            SET @ALERTCPUAgarradosHeader =
                                    '<font color=black bold=true size=5>'
                            SET @ALERTCPUAgarradosHeader =
                                        @ALERTCPUAgarradosHeader +
                                        '<BR /> Consumo de CPU no Servidor <BR />'
                            SET @ALERTCPUAgarradosHeader =
                                    @ALERTCPUAgarradosHeader + '</font>'

                            -----------------------------------------------------------
                            --	ALERT - BODY
                            -----------------------------------------------------------
                            SET @ALERTCPUAgarradosTable = CAST((
                                SELECT td = [SQLProcessUtilization] +
                                            '</td>'
                                                + '<td>' +
                                            OtherProcessUtilization +
                                            '</td>'
                                                + '<td>' +
                                            [SystemIdle] + '</td>'
                                                + '<td>' +
                                            CPU_Utilization + '</td>'

                                FROM (
                                         -- EMAIL Table Details
                                         select TOP 1 CAST([SQLProcessUtilization] AS VARCHAR)                          [SQLProcessUtilization],
                                                      CAST(
                                                              (100 - SystemIdle - SQLProcessUtilization) AS VARCHAR) as OtherProcessUtilization,
                                                      CAST([SystemIdle] AS VARCHAR)                                  AS [SystemIdle],
                                                      CAST(100 - SystemIdle AS VARCHAR)                              AS CPU_Utilization
                                         from #CPU_Utilization
                                         order by record_id DESC
                                     ) AS D FOR XML PATH ('tr'), TYPE) AS VARCHAR(MAX)
                                )

                            -- Corrects Table Formatting
                            SET @ALERTCPUAgarradosTable =
                                    REPLACE(REPLACE(REPLACE(
                                                            @ALERTCPUAgarradosTable,
                                                            '&lt;',
                                                            '<'),
                                                    '&gt;', '>'),
                                            '<td>',
                                            '<td align = center>')

                            -- EMAIL Table Titles
                            SET @ALERTCPUAgarradosTable =
                                        '<table cellspacing="2" cellpadding="5" border="3">'
                                        + '<tr>
								<th bgcolor=#0B0B61 width="200"><font color=white>SQL Server (%)</font></th>
								<th bgcolor=#0B0B61 width="200"><font color=white>Outros Processos (%)</font></th>
								<th bgcolor=#0B0B61 width="200"><font color=white>Livre (%)</font></th>
								<th bgcolor=#0B0B61 width="200"><font color=white>Utiliza��o Total (%)</font></th>
							</tr>'
                                        + REPLACE(REPLACE(
                                                          @ALERTCPUAgarradosTable,
                                                          '&lt;', '<'),
                                                  '&gt;', '>')
                                        + '</table>'

                            -----------------------------------------------------------
                            --	ALERT - HEADER - WHOISACTIVE
                            -----------------------------------------------------------
                            SET @ResultadoWhoisactiveHeader =
                                    '<font color=black bold=true size=5>'
                            SET @ResultadoWhoisactiveHeader =
                                        @ResultadoWhoisactiveHeader +
                                        '<BR /> Processos executando no Database <BR />'
                            SET @ResultadoWhoisactiveHeader =
                                    @ResultadoWhoisactiveHeader + '</font>'

                            -----------------------------------------------------------
                            --	ALERT - BODY - WHOISACTIVE
                            -----------------------------------------------------------
                            SET @ResultadoWhoisactiveTable = CAST((
                                SELECT td = [Duração] + '</td>'
                                                + '<td>' +
                                            [database_name] + '</td>'
                                                + '<td>' +
                                            [login_name] + '</td>'
                                                + '<td>' + [host_name] +
                                            '</td>'
                                                + '<td>' +
                                            [start_time] + '</td>'
                                                + '<td>' + [status] +
                                            '</td>'
                                                + '<td>' +
                                            [session_id] + '</td>'
                                                + '<td>' +
                                            [blocking_session_id] +
                                            '</td>'
                                                + '<td>' + [Wait] +
                                            '</td>'
                                                + '<td>' +
                                            [open_tran_count] + '</td>'
                                                + '<td>' + [CPU] +
                                            '</td>'
                                                + '<td>' + [reads] +
                                            '</td>'
                                                + '<td>' + [writes] +
                                            '</td>'
                                                + '<td>' +
                                            [sql_command] + '</td>'

                                FROM (
                                         -- EMAIL Table Details
                                         SELECT ISNULL([dd hh:mm:ss.mss], '-') AS [Duração],
                                                ISNULL([database_name], '-')   AS [database_name],
                                                ISNULL([login_name], '-')      AS [login_name],
                                                ISNULL([host_name], '-')       AS [host_name],
                                                ISNULL(
                                                        CONVERT(VARCHAR(20), [start_time], 120),
                                                        '-')                   AS [start_time],
                                                ISNULL([status], '-')          AS [status],
                                                ISNULL(
                                                        CAST([session_id] AS VARCHAR),
                                                        '-')                   AS [session_id],
                                                ISNULL(
                                                        CAST([blocking_session_id] AS VARCHAR),
                                                        '-')                   AS [blocking_session_id],
                                                ISNULL([wait_info], '-')       AS [Wait],
                                                ISNULL(
                                                        CAST([open_tran_count] AS VARCHAR),
                                                        '-')                   AS [open_tran_count],
                                                ISNULL([CPU], '-')             AS [CPU],
                                                ISNULL([reads], '-')           AS [reads],
                                                ISNULL([writes], '-')          AS [writes],
                                                ISNULL(
                                                        SUBSTRING([sql_command], 1, 300),
                                                        '-')                   AS [sql_command]
                                         FROM #Resultado_WhoisActive
                                     ) AS D
                                ORDER BY [start_time] FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
                                )

                            -- Corrects Table Formatting
                            SET @ResultadoWhoisactiveTable =
                                    REPLACE(REPLACE(REPLACE(
                                                            @ResultadoWhoisactiveTable,
                                                            '&lt;',
                                                            '<'),
                                                    '&gt;', '>'),
                                            '<td>',
                                            '<td align = center>')

                            -- EMAIL Table Titles
                            SET @ResultadoWhoisactiveTable =
                                        '<table cellspacing="2" cellpadding="5" border="3">'
                                        + '<tr>
								<th bgcolor=#0B0B61 width="140"><font color=white>[dd hh:mm:ss.mss]</font></th>
								<th bgcolor=#0B0B61 width="100"><font color=white>Database</font></th>
								<th bgcolor=#0B0B61 width="120"><font color=white>Login</font></th>
								<th bgcolor=#0B0B61 width="120"><font color=white>Host Name</font></th>
								<th bgcolor=#0B0B61 width="200"><font color=white>Hora In�cio</font></th>
								<th bgcolor=#0B0B61 width="120"><font color=white>Status</font></th>
								<th bgcolor=#0B0B61 width="30"><font color=white>ID Session</font></th>
								<th bgcolor=#0B0B61 width="60"><font color=white>ID Session Bloqueando</font></th>
								<th bgcolor=#0B0B61 width="120"><font color=white>Wait</font></th>
								<th bgcolor=#0B0B61 width="60"><font color=white>transactions Abertas</font></th>
								<th bgcolor=#0B0B61 width="120"><font color=white>CPU</font></th>
								<th bgcolor=#0B0B61 width="120"><font color=white>Reads</font></th>
								<th bgcolor=#0B0B61 width="120"><font color=white>Writes</font></th>
								<th bgcolor=#0B0B61 width="1000"><font color=white>Query</font></th>
							</tr>'
                                        + REPLACE(REPLACE(
                                                          @ResultadoWhoisactiveTable,
                                                          '&lt;', '<'),
                                                  '&gt;', '>')
                                        + '</table>'

                            -----------------------------------------------------------
                            -- Insert a blank page in EMAIL
                            -----------------------------------------------------------
                            SET @EmptyBodyEmail = ''
                            SET @EmptyBodyEmail =
                                        '<table cellpadding="5" cellspacing="5" border="0">' +
                                        '<tr>
								<th width="500">               </th>
							</tr>'
                                        + REPLACE(REPLACE(
                                                          ISNULL(@EmptyBodyEmail, ''),
                                                          '&lt;', '<'),
                                                  '&gt;', '>')
                                        + '</table>'

                            /**********************************************************
				--	Set EMAIL Variables
				**********************************************************/
                            SELECT @Importance = 'High',
                                   @Subject =
                                   'ALERT: O Consumo de CPU est� acima de ' +
                                   CAST((@CPU_Parametro) AS VARCHAR) +
                                   '% no Servidor: ' + @@SERVERNAME,
                                   @EmailBody =
                                   @ALERTCPUAgarradosHeader +
                                   @EmptyBodyEmail +
                                   @ALERTCPUAgarradosTable +
                                   @EmptyBodyEmail +
                                   @ResultadoWhoisactiveHeader +
                                   @EmptyBodyEmail +
                                   @ResultadoWhoisactiveTable +
                                   @EmptyBodyEmail

                            /**********************************************************
				--	SEND EMAIL - ALERT
				**********************************************************/
                            EXEC [msdb].[dbo].[sp_send_dbmail]
                                 @profile_name = @ProfileEmail,
                                 @recipients = @EmailDestination,
                                 @subject = @Subject,
                                 @body = @EmailBody,
                                 @body_format = 'HTML',
                                 @importance = @Importance

                            /**********************************************************
				-- Inserts a Record in the ALERT Control Table -> Fl_Tipo = 1 : ALERT
				**********************************************************/
                            INSERT INTO [dbo].[ALERT] ([Id_ALERT_Parametro],
                                                       [Ds_Mensagem],
                                                       [Fl_Tipo])
                            SELECT @Id_ALERT_Parametro, @Subject, 1
                        END
                END
        END -- END - ALERT
    ELSE
        BEGIN
            -- START - CLEAR
            IF @Fl_Tipo = 1
                BEGIN
                    -----------------------------------------------------------
                    --	ALERT - DADOS - WHOISACTIVE
                    -----------------------------------------------------------
                    -- Retorna todos os processos que est�o sendo executados no momento
                    EXEC [dbo].[sp_whoisactive]
                         @get_outer_command = 1,
                         @output_column_list = '[dd hh:mm:ss.mss][database_name][login_name][host_name][start_time][status][session_id][blocking_session_id][wait_info][open_tran_count][CPU][reads][writes][sql_command]',
                         @destination_table = '#Resultado_WhoisActive'

                    -- Altera a coluna que possui o comando SQL
                    ALTER TABLE #Resultado_WhoisActive
                        ALTER COLUMN [sql_command] VARCHAR(MAX)

                    UPDATE #Resultado_WhoisActive
                    SET [sql_command] = REPLACE(REPLACE(REPLACE(REPLACE(
                                                                        CAST([sql_command] AS VARCHAR(1000)),
                                                                        '<?query --',
                                                                        ''),
                                                                '--?>',
                                                                ''),
                                                        '&gt;', '>'),
                                                '&lt;', '')

                    -- select * from #Resultado_WhoisActive

                    -- Verifica se n�o existe nenhum processo em Execu��o
                    IF NOT EXISTS(
                            SELECT TOP 1 * FROM #Resultado_WhoisActive)
                        BEGIN
                            INSERT INTO #Resultado_WhoisActive
                            SELECT NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL,
                                   NULL
                        END

                    /**********************************************************
			--	CREATE EMAIL - CLEAR
			**********************************************************/

                    -----------------------------------------------------------
                    --	CLEAR - HEADER
                    -----------------------------------------------------------
                    SET @ALERTCPUAgarradosHeader =
                            '<font color=black bold=true size=5>'
                    SET @ALERTCPUAgarradosHeader =
                                @ALERTCPUAgarradosHeader +
                                '<BR /> Consumo de CPU no Servidor <BR />'
                    SET @ALERTCPUAgarradosHeader =
                            @ALERTCPUAgarradosHeader + '</font>'

                    -----------------------------------------------------------
                    --	CLEAR - BODY
                    -----------------------------------------------------------
                    SET @ALERTCPUAgarradosTable = CAST((
                        SELECT td = [SQLProcessUtilization] + '</td>'
                                        + '<td>' +
                                    OtherProcessUtilization + '</td>'
                                        + '<td>' + [SystemIdle] +
                                    '</td>'
                                        + '<td>' + CPU_Utilization +
                                    '</td>'

                        FROM (
                                 -- EMAIL Table Details
                                 select TOP 1 CAST([SQLProcessUtilization] AS VARCHAR)                          [SQLProcessUtilization],
                                              CAST(
                                                      (100 - SystemIdle - SQLProcessUtilization) AS VARCHAR) as OtherProcessUtilization,
                                              CAST([SystemIdle] AS VARCHAR)                                  AS [SystemIdle],
                                              CAST(100 - SystemIdle AS VARCHAR)                              AS CPU_Utilization
                                 from #CPU_Utilization
                                 order by record_id DESC
                             ) AS D FOR XML PATH ('tr'), TYPE) AS VARCHAR(MAX)
                        )

                    -- Corrects Table Formatting
                    SET @ALERTCPUAgarradosTable = REPLACE(
                            REPLACE(REPLACE(@ALERTCPUAgarradosTable,
                                            '&lt;', '<'), '&gt;', '>'),
                            '<td>', '<td align = center>')

                    -- EMAIL Table Titles
                    SET @ALERTCPUAgarradosTable =
                                '<table cellspacing="2" cellpadding="5" border="3">'
                                + '<tr>
								<th bgcolor=#0B0B61 width="200"><font color=white>SQL Server (%)</font></th>
								<th bgcolor=#0B0B61 width="200"><font color=white>Outros Processos (%)</font></th>
								<th bgcolor=#0B0B61 width="200"><font color=white>Livre (%)</font></th>
								<th bgcolor=#0B0B61 width="200"><font color=white>Utiliza��o Total (%)</font></th>
							</tr>'
                                + REPLACE(REPLACE(
                                                  @ALERTCPUAgarradosTable,
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'

                    -----------------------------------------------------------
                    --	ALERT - HEADER - WHOISACTIVE
                    -----------------------------------------------------------
                    SET @ResultadoWhoisactiveHeader =
                            '<font color=black bold=true size=5>'
                    SET @ResultadoWhoisactiveHeader =
                                @ResultadoWhoisactiveHeader +
                                '<BR /> Processos executando no Database <BR />'
                    SET @ResultadoWhoisactiveHeader =
                            @ResultadoWhoisactiveHeader + '</font>'

                    -----------------------------------------------------------
                    --	ALERT - BODY - WHOISACTIVE
                    -----------------------------------------------------------
                    SET @ResultadoWhoisactiveTable = CAST((
                        SELECT td = [Duração] + '</td>'
                                        + '<td>' + [database_name] +
                                    '</td>'
                                        + '<td>' + [login_name] +
                                    '</td>'
                                        + '<td>' + [host_name] + '</td>'
                                        + '<td>' + [start_time] +
                                    '</td>'
                                        + '<td>' + [status] + '</td>'
                                        + '<td>' + [session_id] +
                                    '</td>'
                                        + '<td>' +
                                    [blocking_session_id] + '</td>'
                                        + '<td>' + [Wait] + '</td>'
                                        + '<td>' + [open_tran_count] +
                                    '</td>'
                                        + '<td>' + [CPU] + '</td>'
                                        + '<td>' + [reads] + '</td>'
                                        + '<td>' + [writes] + '</td>'
                                        + '<td>' + [sql_command] +
                                    '</td>'

                        FROM (
                                 -- EMAIL Table Details
                                 SELECT ISNULL([dd hh:mm:ss.mss], '-') AS [Duração],
                                        ISNULL([database_name], '-')   AS [database_name],
                                        ISNULL([login_name], '-')      AS [login_name],
                                        ISNULL([host_name], '-')       AS [host_name],
                                        ISNULL(
                                                CONVERT(VARCHAR(20), [start_time], 120),
                                                '-')                   AS [start_time],
                                        ISNULL([status], '-')          AS [status],
                                        ISNULL(
                                                CAST([session_id] AS VARCHAR),
                                                '-')                   AS [session_id],
                                        ISNULL(
                                                CAST([blocking_session_id] AS VARCHAR),
                                                '-')                   AS [blocking_session_id],
                                        ISNULL([wait_info], '-')       AS [Wait],
                                        ISNULL(
                                                CAST([open_tran_count] AS VARCHAR),
                                                '-')                   AS [open_tran_count],
                                        ISNULL([CPU], '-')             AS [CPU],
                                        ISNULL([reads], '-')           AS [reads],
                                        ISNULL([writes], '-')          AS [writes],
                                        ISNULL(
                                                SUBSTRING([sql_command], 1, 300),
                                                '-')                   AS [sql_command]
                                 FROM #Resultado_WhoisActive
                             ) AS D
                        ORDER BY [start_time] FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
                        )

                    -- Corrects Table Formatting
                    SET @ResultadoWhoisactiveTable = REPLACE(
                            REPLACE(REPLACE(@ResultadoWhoisactiveTable,
                                            '&lt;', '<'), '&gt;', '>'),
                            '<td>', '<td align = center>')

                    -- EMAIL Table Titles
                    SET @ResultadoWhoisactiveTable =
                                '<table cellspacing="2" cellpadding="5" border="3">'
                                + '<tr>
							<th bgcolor=#0B0B61 width="140"><font color=white>[dd hh:mm:ss.mss]</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Database</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Login</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Host Name</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Hora In�cio</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Status</font></th>
							<th bgcolor=#0B0B61 width="30"><font color=white>ID Session</font></th>
							<th bgcolor=#0B0B61 width="60"><font color=white>ID Session Bloqueando</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Wait</font></th>
							<th bgcolor=#0B0B61 width="60"><font color=white>transactions Abertas</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>CPU</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Reads</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Writes</font></th>
							<th bgcolor=#0B0B61 width="1000"><font color=white>Query</font></th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  @ResultadoWhoisactiveTable,
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'

                    -----------------------------------------------------------
                    -- Insert a blank page in EMAIL
                    -----------------------------------------------------------
                    SET @EmptyBodyEmail = ''
                    SET @EmptyBodyEmail =
                                '<table cellpadding="5" cellspacing="5" border="0">' +
                                '<tr>
							<th width="500">               </th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  ISNULL(@EmptyBodyEmail, ''),
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'

                    /**********************************************************
			--	Set EMAIL Variables
			**********************************************************/
                    SELECT @Importance = 'High',
                           @Subject =
                           'CLEAR: O Consumo de CPU est� abaixo de ' +
                           CAST((@CPU_Parametro) AS VARCHAR) +
                           '% no Servidor: ' + @@SERVERNAME,
                           @EmailBody =
                           @ALERTCPUAgarradosHeader + @EmptyBodyEmail +
                           @ALERTCPUAgarradosTable + @EmptyBodyEmail +
                           @ResultadoWhoisactiveHeader +
                           @EmptyBodyEmail +
                           @ResultadoWhoisactiveTable + @EmptyBodyEmail

                    /**********************************************************
			--	ENVIA O EMAIL - CLEAR
			**********************************************************/
                    EXEC [msdb].[dbo].[sp_send_dbmail]
                         @profile_name = @ProfileEmail,
                         @recipients = @EmailDestination,
                         @subject = @Subject,
                         @body = @EmailBody,
                         @body_format = 'HTML',
                         @importance = @Importance

                    /**********************************************************
			-- Inserts a Record in the ALERT Control Table -> Fl_Tipo = 0 : CLEAR
			**********************************************************/
                    INSERT INTO [dbo].[ALERT] ([Id_ALERT_Parametro],
                                               [Ds_Mensagem],
                                               [Fl_Tipo])
                    SELECT @Id_ALERT_Parametro, @Subject, 0
                END
        END -- END - CLEAR
END

GO

/*

IF ( OBJECT_ID('[dbo].[stpALERT_MaxSize_Arquivo_SQL]') IS NOT NULL )
	DROP PROCEDURE [dbo].[stpALERT_MaxSize_Arquivo_SQL]
GO

/**********************************************************
--	ALERT: MAXSIZE ARQUIVO SQL
**********************************************************/

CREATE PROCEDURE [dbo].[stpALERT_MaxSize_Arquivo_SQL]
AS
BEGIN
	SET NOCOUNT ON

	-- MaxSize Arquivo SQL
	DECLARE @Id_ALERT_Parametro INT = (SELECT Id_ALERT_Parametro FROM [Traces].[dbo].ALERT_Parametro (NOLOCK) WHERE Nm_ALERT = 'MaxSize Arquivo SQL')

	-- Create as tabelas que ir�o armazenar as informa��es sobre os arquivos
	IF ( OBJECT_ID('tempdb..##MDFs_Sizes_ALERTs') IS NOT NULL )
		DROP TABLE ##MDFs_Sizes_ALERTs

	CREATE TABLE ##MDFs_Sizes_ALERTs(
		[Server]			VARCHAR(50),
		[Nm_Database]		VARCHAR(100),
		[NomeLogico]		VARCHAR(100),
		[Total_Utilizado]	NUMERIC(15,2),
		[Espaco_Livre (MB)] NUMERIC(15,2),
		[physical_name]		VARCHAR(4000)
	)

	IF ( OBJECT_ID('tempdb..#Logs_Sizes') IS NOT NULL )
		DROP TABLE #Logs_Sizes

	CREATE TABLE #Logs_Sizes(
		[Server]		VARCHAR(50),
		[Nm_Database]	VARCHAR(100) NOT NULL,
		[Log_Size(KB)]	BIGINT NOT NULL,
		[Log_Used(KB)]	BIGINT NOT NULL,
		[Log_Used(%)]	DECIMAL(22, 2) NULL
	)

	-- Popula os dados
	EXEC sp_MSforeachdb '
		Use [?]
		INSERT INTO ##MDFs_Sizes_ALERTs
		SELECT	@@SERVERNAME,
				db_name() AS NomeBase,
				[Name] AS NomeLogico,
				CONVERT(DECIMAL(15,2), ROUND(FILEPROPERTY(a.[Name], ''SpaceUsed'') / 128.000, 2)) AS [Total_Utilizado (MB)],
				CONVERT(DECIMAL(15,2), ROUND((a.Size-FILEPROPERTY(a.[Name], ''SpaceUsed'')) / 128.000, 2)) AS [Available Space (MB)],
				[Filename] AS physical_name
		FROM [dbo].[sysfiles] a (NOLOCK)
		JOIN [sysfilegroups] b (NOLOCK) ON a.[groupid] = b.[groupid]
		ORDER BY b.[groupname]'

	-- Busca as informa��es sobre os arquivos LDF
	INSERT INTO #Logs_Sizes([Server], [Nm_Database], [Log_Size(KB)], [Log_Used(KB)], [Log_Used(%)])
	SELECT	@@SERVERNAME,
			db.[name]									AS [Database Name],
			ls.[cntr_value]								AS [Log Size (KB)] ,
			lu.[cntr_value]								AS [Log Used (KB)] ,
			CAST( CAST(	lu.[cntr_value] AS FLOAT) /
						CASE WHEN CAST(ls.[cntr_value] AS FLOAT) = 0
							THEN 1
							ELSE CAST(ls.[cntr_value] AS FLOAT)
						END AS DECIMAL(18,2)) * 100		AS [Log Used %]
	FROM [sys].[databases] AS db WITH(NOLOCK)
	JOIN [sys].[dm_os_performance_counters] AS lu WITH(NOLOCK) ON db.[name] = lu.[instance_name]
	JOIN [sys].[dm_os_performance_counters] AS ls WITH(NOLOCK) ON db.[name] = ls.[instance_name]
	WHERE	lu.[counter_name] LIKE 'Log File(s) Used Size (KB)%'
			AND ls.[counter_name] LIKE 'Log File(s) Size (KB)%'

	-- Create a tabela com os dados resumidos
	IF ( OBJECT_ID('_Resultado_ALERT_SQLFile') IS NOT NULL )
		DROP TABLE _Resultado_ALERT_SQLFile

	SELECT	@@SERVERNAME					AS [Server],
			DB_NAME(A.[database_id])		AS [Nm_Database],
			[name]							AS Logical_Name,
			CASE WHEN RIGHT(A.[physical_name], 3) = 'mdf' OR RIGHT(A.[physical_name], 3) = 'ndf'
					THEN B.[Total_Utilizado]
					ELSE (C.[Log_Used(KB)]) / 1024.0
			END								AS [Used(MB)],
			(
				(	CASE WHEN A.[Max_Size] = -1
						THEN -1
						ELSE ( A.[Max_Size] / 1024 ) * 8
					END ) - (	CASE WHEN [is_percent_growth] = 1
										THEN ( ( A.[Max_Size] / 1024 ) * 8 ) * ((A.[Growth] / 100.00))
										ELSE CAST(( A.[Growth] * 8 ) / 1024.00 AS NUMERIC(15, 2))
								END )
			) * .85 -
			CASE WHEN RIGHT(A.[physical_name], 3) = 'mdf' OR RIGHT(A.[physical_name], 3) = 'ndf'
					THEN B.[Total_Utilizado]
					ELSE (C.[Log_Used(KB)]) / 1024.0
			END								AS [ALERT],
			CASE WHEN A.[name] = 'tempdev'
					THEN ([Espaco_Livre (MB)] + [Total_Utilizado])
					ELSE ([Size] / 1024.0) * 8
			END								AS [Size(MB)],
			CASE WHEN RIGHT(A.[physical_name], 3) = 'mdf' OR RIGHT(A.[physical_name], 3) = 'ndf'
					THEN [Espaco_Livre (MB)]
					ELSE ([Log_Size(KB)] - [Log_Used(KB)]) / 1024.0
			END								AS [Free_Space(MB)],
			CASE	WHEN A.[name] = 'tempdev'
						THEN ([Espaco_Livre (MB)] / ([Espaco_Livre (MB)] + [Total_Utilizado])) * 100.00
					WHEN RIGHT(A.[physical_name], 3) = 'mdf' OR RIGHT(A.[physical_name], 3) = 'ndf'
						THEN (([Espaco_Livre (MB)] / ((Size/1024.0) * 8.0))) * 100.0
					ELSE (100.00 - C.[Log_Used(%)])
			END								AS [Free_Space(%)],
			CASE WHEN A.[Max_Size] = -1
					THEN -1
					ELSE (A.[Max_Size] / 1024) * 8
			END								AS [MaxSize(MB)],
			CASE WHEN [is_percent_growth] = 1
					THEN CAST(A.[Growth] AS VARCHAR) + ' %'
					ELSE CAST (CAST((A.[Growth] * 8) / 1024.00 AS NUMERIC(15, 2)) AS VARCHAR) + ' MB'
			END								AS [Growth]
	INTO [dbo].[_Resultado_ALERT_SQLFile]
	FROM [sys].[master_files] A WITH(NOLOCK)
	JOIN ##MDFs_Sizes_ALERTs B ON A.[physical_name] = B.[physical_name]
	JOIN #Logs_Sizes C ON C.[Nm_Database] = db_name(A.[database_id])
	WHERE   A.[type_desc] <> 'FULLTEXT'
			AND A.[Max_Size] NOT IN ( -1 )
			AND CASE WHEN A.[Max_Size] = -1 THEN -1
					 ELSE ( A.[Max_Size] / 1024 ) * 8
				END <> 2097152

	-- Declares the variables
	DECLARE @Subject VARCHAR(500), @Fl_Tipo TINYINT, @Importance AS VARCHAR(6), @EmailBody VARCHAR(MAX), @Maxsize_Parametro INT,
			@ALERTMDFLDFHeader VARCHAR(MAX), @ALERTMDFLDFTable VARCHAR(MAX), @EmptyBodyEmail VARCHAR(MAX), @EmailDestination VARCHAR(500),
			@ProfileEmail VARCHAR(200)

	-- Verifica o �ltimo Tipo do ALERT registrado -> 0: CLEAR / 1: ALERT
	SELECT @Fl_Tipo = [Fl_Tipo]
	FROM [dbo].[ALERT]
	WHERE [Id_ALERT] = (SELECT MAX(Id_ALERT) FROM [dbo].[ALERT] WHERE [Id_ALERT_Parametro] = @Id_ALERT_Parametro )

	-----------------------------------------------------------
	-- Retrieves ALERT parameters
	-----------------------------------------------------------
	SELECT	@Maxsize_Parametro = Vl_Parametro * 1000,		-- Tamanho (MB)
			@EmailDestination = Ds_Email,
			@ProfileEmail = Ds_Profile_Email
	FROM [dbo].[ALERT_Parametro]
	WHERE [Id_ALERT_Parametro] = @Id_ALERT_Parametro		-- MaxSize Arquivo SQL

	/**********************************************************
	--	Verifica se existe algum arquivo MDF ou LDF pr�ximo do MaxSize
	**********************************************************/
	IF EXISTS	(
					SELECT NULL
					FROM [dbo].[_Resultado_ALERT_SQLFile]
					WHERE	(
								CASE WHEN [MaxSize(MB)] >= 150000
										THEN	CASE WHEN (([MaxSize(MB)] - [Used(MB)]) < @Maxsize_Parametro)
														THEN 1
														ELSE 0
												END
										ELSE	CASE WHEN [ALERT] < 0
													THEN 1
													ELSE 0
												END
								END
							) = 1
				)
	BEGIN	-- START - ALERT
		IF ISNULL(@Fl_Tipo, 0) = 0	-- Envia o ALERT apenas uma vez
		BEGIN
			/**********************************************************
			--	CREATE EMAIL - ALERT
			**********************************************************/

			-----------------------------------------------------------
			--	ALERT - HEADER
			-----------------------------------------------------------
			SET @ALERTMDFLDFHeader = '<font color=black bold=true size=5>'
			SET @ALERTMDFLDFHeader = @ALERTMDFLDFHeader + '<BR /> Informa��es arquivos .LDF e .MDF com "MaxSize" especificado <BR />'
			SET @ALERTMDFLDFHeader = @ALERTMDFLDFHeader + '</font>'

			-----------------------------------------------------------
			--	ALERT - BODY
			-----------------------------------------------------------
			SET @ALERTMDFLDFTable = CAST( (
				SELECT td =				[Nm_Database]			+ '</td>'
							+ '<td>' +	[Logical_Name]			+ '</td>'
							+ '<td>' +	[Tamanho_Atual (MB)]	+ '</td>'
							+ '<td>' +	[Livre (MB)]			+ '</td>'
							+ '<td>' +	[Utilizado (MB)]		+ '</td>'
							+ '<td>' +	[MaxSize(MB)]			+ '</td>'
							+ '<td>' +	[Growth]				+ '</td>'
				FROM (
						-- EMAIL Table Details
						SELECT	DISTINCT
								[Nm_Database],
								[Logical_Name],
								CAST([Size(MB)] AS VARCHAR)			AS [Tamanho_Atual (MB)],
								CAST([Free_Space(MB)] AS VARCHAR)	AS [Livre (MB)],
								CAST([Used(MB)] AS VARCHAR)			AS [Utilizado (MB)],
								CAST([MaxSize(MB)] AS VARCHAR)		AS [MaxSize(MB)],
								[Growth]
						FROM [Traces].[dbo].[_Resultado_ALERT_SQLFile]
						WHERE	(
									CASE WHEN [MaxSize(MB)] >= 150000
											THEN	CASE WHEN (([MaxSize(MB)] - [Used(MB)]) < 15000)
															THEN 1
															ELSE 0
													END
									ELSE	CASE WHEN [ALERT] < 0
													THEN 1
													ELSE 0
											END
									END
								) = 1

					  ) AS D ORDER BY [Livre (MB)]
				FOR XML PATH( 'tr' ), TYPE) AS VARCHAR(MAX)
			)

			-- Corrects Table Formatting
			SET @ALERTMDFLDFTable = REPLACE( REPLACE( REPLACE( @ALERTMDFLDFTable, '&lt;', '<'), '&gt;', '>'), '<td>', '<td align = center>')

			-- EMAIL Table Titles
			SET @ALERTMDFLDFTable =
					'<table cellspacing="2" cellpadding="5" border="3">'
					+	'<tr>
							<th bgcolor=#0B0B61 width="70"><font color=white>Database</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Nome L�gico</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Tamanho Atual (MB)</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Livre (MB)</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Utilizado (MB)</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>MaxSize (MB)</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Crescimento</font></th>
						</tr>'
					+ REPLACE( REPLACE( @ALERTMDFLDFTable, '&lt;', '<'), '&gt;', '>')
					+ '</table>'

			-----------------------------------------------------------
			-- Insert a blank page in EMAIL
			-----------------------------------------------------------
			SET @EmptyBodyEmail =	''
			SET @EmptyBodyEmail =
					'<table cellpadding="5" cellspacing="5" border="0">' +
						'<tr>
							<th width="500">               </th>
						</tr>'
						+ REPLACE( REPLACE( ISNULL(@EmptyBodyEmail,''), '&lt;', '<'), '&gt;', '>')
					+ '</table>'

			/**********************************************************
			--	SEND EMAIL - ALERT
			**********************************************************/
			SELECT	@Importance =	'High',
					@Subject =		'ALERT: Existe algum arquivo MDF ou LDF com risco de estouro do Maxsize no Servidor: ' + @@SERVERNAME,
					@EmailBody =	@ALERTMDFLDFHeader + @EmptyBodyEmail + @ALERTMDFLDFTable + @EmptyBodyEmail


			EXEC [msdb].[dbo].[sp_send_dbmail]
					@profile_name = @ProfileEmail,
					@recipients =	@EmailDestination,
					@subject =		@Subject,
					@body =			@EmailBody,
					@body_format =	'HTML',
					@importance =	@Importance

			/**********************************************************
			-- Inserts a Record in the ALERT Control Table -> Fl_Tipo = 1 : ALERT
			**********************************************************/
			INSERT INTO [dbo].[ALERT] ( [Id_ALERT_Parametro], [Ds_Mensagem], [Fl_Tipo] )
			SELECT @Id_ALERT_Parametro, @Subject, 1
		END
	END		-- END - ALERT
	ELSE
	BEGIN	-- START - CLEAR
		IF @Fl_Tipo = 1
		BEGIN
			/**********************************************************
			--	CREATE EMAIL - CLEAR
			**********************************************************/

			-----------------------------------------------------------
			--	CLEAR - HEADER
			-----------------------------------------------------------
			SET @ALERTMDFLDFHeader = '<font color=black bold=true size=5>'
			SET @ALERTMDFLDFHeader = @ALERTMDFLDFHeader + '<BR /> Informa��es arquivos .LDF e .MDF com "MaxSize" especificado <BR />'
			SET @ALERTMDFLDFHeader = @ALERTMDFLDFHeader + '</font>'

			-----------------------------------------------------------
			--	CLEAR - BODY
			-----------------------------------------------------------
			SET @ALERTMDFLDFTable = CAST( (
				SELECT td =				[Nm_Database]			+ '</td>'
							+ '<td>' +	[Logical_Name]			+ '</td>'
							+ '<td>' +	[Tamanho_Atual (MB)]	+ '</td>'
							+ '<td>' +	[Livre (MB)]			+ '</td>'
							+ '<td>' +	[Utilizado (MB)]		+ '</td>'
							+ '<td>' +	[MaxSize(MB)]			+ '</td>'
							+ '<td>' +	[Growth]				+ '</td>'
				FROM (
						-- EMAIL Table Details
						SELECT	DISTINCT
								[Nm_Database],
								[Logical_Name],
								CAST([Size(MB)] AS VARCHAR)			AS [Tamanho_Atual (MB)],
								CAST([Free_Space(MB)] AS VARCHAR)	AS [Livre (MB)],
								CAST([Used(MB)] AS VARCHAR)			AS [Utilizado (MB)],
								CAST([MaxSize(MB)] AS VARCHAR)		AS [MaxSize(MB)],
								[Growth]
						FROM [Traces].[dbo].[_Resultado_ALERT_SQLFile]

				  ) AS D ORDER BY [Utilizado (MB)] DESC
				FOR XML PATH( 'tr' ), TYPE) AS VARCHAR(MAX)
			)

			-- Corrects Table Formatting
			SET @ALERTMDFLDFTable = REPLACE( REPLACE( REPLACE( @ALERTMDFLDFTable, '&lt;', '<'), '&gt;', '>'), '<td>', '<td align = center>')

			-- EMAIL Table Titles
			SET @ALERTMDFLDFTable =
					'<table cellspacing="2" cellpadding="5" border="3">'
					+	'<tr>
							<th bgcolor=#0B0B61 width="70"><font color=white>Database</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Nome L�gico</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Tamanho Atual (MB)</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Livre (MB)</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Utilizado (MB)</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>MaxSize (MB)</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Crescimento</font></th>
						</tr>'
					+ REPLACE( REPLACE( @ALERTMDFLDFTable, '&lt;', '<'), '&gt;', '>')
					+ '</table>'

			-----------------------------------------------------------
			-- Insert a blank page in EMAIL
			-----------------------------------------------------------
			SET @EmptyBodyEmail =	''
			SET @EmptyBodyEmail =
					'<table cellpadding="5" cellspacing="5" border="0">' +
						'<tr>
							<th width="500">               </th>
						</tr>'
						+ REPLACE( REPLACE( ISNULL(@EmptyBodyEmail,''), '&lt;', '<'), '&gt;', '>')
					+ '</table>'

			/**********************************************************
			--	ENVIA O EMAIL - CLEAR
			**********************************************************/
			SELECT	@Importance =	'High',
					@Subject =		'CLEAR: N�o existe mais algum arquivo MDF ou LDF com risco de estouro do Maxsize no Servidor: ' + @@SERVERNAME,
					@EmailBody =	@ALERTMDFLDFHeader + @EmptyBodyEmail + @ALERTMDFLDFTable + @EmptyBodyEmail

			EXEC [msdb].[dbo].[sp_send_dbmail]
					@profile_name = @ProfileEmail,
					@recipients =	@EmailDestination,
					@subject =		@Subject,
					@body =			@EmailBody,
					@body_format =	'HTML',
					@importance =	@Importance

			/**********************************************************
			-- Inserts a Record in the ALERT Control Table -> Fl_Tipo = 0 : CLEAR
			**********************************************************/
			INSERT INTO [dbo].[ALERT] ( [Id_ALERT_Parametro], [Ds_Mensagem], [Fl_Tipo] )
			SELECT @Id_ALERT_Parametro, @Subject, 0
		END
	END		-- END - CLEAR
END
*/

GO
IF (OBJECT_ID(
        '[dbo].[stpALERT_Tempdb_Utilizacao_Arquivo_MDF]') IS NOT NULL)
    DROP PROCEDURE [dbo].[stpALERT_Tempdb_Utilizacao_Arquivo_MDF]
GO

/**********************************************************
--	ALERT: TEMPDB UTILIZACAO ARQUIVO MDF
**********************************************************/

CREATE PROCEDURE [dbo].[stpALERT_Tempdb_Utilizacao_Arquivo_MDF]
AS
BEGIN
    SET NOCOUNT ON

    -- Tamanho Arquivo MDF Tempdb
    DECLARE
        @Id_ALERT_Parametro INT = (SELECT Id_ALERT_Parametro
                                   FROM [Traces].[dbo].ALERT_Parametro (NOLOCK)
                                   WHERE Nm_ALERT = 'Tempdb Utilizacao Arquivo MDF')

    declare
        @Tempo_Conexoes_Hs tinyint, @Tempdb_Parametro int, @EmailDestination VARCHAR(500), @Tamanho_Tempdb INT, @ProfileEmail VARCHAR(200)

    -----------------------------------------------------------
    -- Retrieves ALERT parameters
    -----------------------------------------------------------
    SELECT @Tempdb_Parametro = Vl_Parametro, -- Percentual
           @EmailDestination = Ds_Email,
           @ProfileEmail = Ds_Profile_Email
    FROM [dbo].[ALERT_Parametro]
    WHERE [Id_ALERT_Parametro] = @Id_ALERT_Parametro
    -- Tempdb Utilizacao Arquivo

    -- Conex�es mais antigas que 1 hora
    SELECT @Tempo_Conexoes_Hs = 1,
           @Tamanho_Tempdb = 10000
    --	10 GB

    -- Declares the variables
    DECLARE
        @Subject                    VARCHAR(500), @Fl_Tipo TINYINT, @Importance AS VARCHAR(6), @EmailBody VARCHAR(MAX), @ALERTTamanhoMDFTempdbHeader VARCHAR(MAX),
        @ALERTTamanhoMDFTempdbTable VARCHAR(MAX), @ALERTTempdbUtilizacaoArquivoHeader VARCHAR(MAX), @ALERTTamanhoMDFTempdbConexoesTable VARCHAR(MAX),
        @EmptyBodyEmail             VARCHAR(MAX), @ALERTTempdbProcessoExecHeader VARCHAR(MAX), @ALERTTempdbProcessoExecTable VARCHAR(MAX), @Dt_Atual DATETIME

    -- Verifica o �ltimo Tipo do ALERT registrado -> 0: CLEAR / 1: ALERT
    SELECT @Fl_Tipo = [Fl_Tipo]
    FROM [dbo].[ALERT]
    WHERE [Id_ALERT] = (SELECT MAX(Id_ALERT)
                        FROM [dbo].[ALERT]
                        WHERE [Id_ALERT_Parametro] = @Id_ALERT_Parametro)

    -- Busca as informa��es do Tempdb
    IF (OBJECT_ID('tempdb..#ALERT_Tamanho_MDF_Tempdb') IS NOT NULL)
        DROP TABLE #ALERT_Tamanho_MDF_Tempdb

    select file_id,
           reserved_MB                 = CAST(
                           (unallocated_extent_page_count +
                            version_store_reserved_page_count +
                            user_object_reserved_page_count +
                            internal_object_reserved_page_count +
                            mixed_extent_page_count) * 8 /
                           1024. AS numeric(15, 2)),
           unallocated_extent_MB       = CAST(
                   unallocated_extent_page_count * 8 / 1024. AS NUMERIC(15, 2)),
           internal_object_reserved_MB = CAST(
                       internal_object_reserved_page_count * 8 /
                       1024. AS NUMERIC(15, 2)),
           version_store_reserved_MB   = CAST(
                   version_store_reserved_page_count * 8 / 1024. AS NUMERIC(15, 2)),
           user_object_reserved_MB     = convert(numeric(10, 2),
                   round(user_object_reserved_page_count * 8 / 1024.,
                         2))
    into #ALERT_Tamanho_MDF_Tempdb
    from tempdb.sys.dm_db_file_space_usage

    IF (OBJECT_ID(
            'tempdb..#ALERT_Tamanho_MDF_Tempdb_Conexoes') IS NOT NULL)
        DROP TABLE #ALERT_Tamanho_MDF_Tempdb_Conexoes

    -- Busca as transactions que est�o abertas
    CREATE TABLE #ALERT_Tamanho_MDF_Tempdb_Conexoes
    (
        [session_id]             [smallint]      NULL,
        [login_time]             [varchar](40)   NULL,
        [login_name]             [nvarchar](128) NULL,
        [host_name]              [nvarchar](128) NULL,
        [open_transaction_Count] [int]           NULL,
        [status]                 [nvarchar](30)  NULL,
        [cpu_time]               [int]           NULL,
        [total_elapsed_time]     [int]           NULL,
        [reads]                  [bigint]        NULL,
        [writes]                 [bigint]        NULL,
        [logical_reads]          [bigint]        NULL
    ) ON [PRIMARY]

    -- Query ALERT Tempdb - Conex�es abertas - Incluir no ALERT TempDb
    INSERT INTO #ALERT_Tamanho_MDF_Tempdb_Conexoes
    SELECT TOP 50 session_id,
                  convert(varchar(20), login_time, 120) AS login_time,
                  login_name,
                  host_name,
        /*open_transaction_Count,*/
                  NULL,
                  status,
                  cpu_time,
                  total_elapsed_time,
                  reads,
                  writes,
                  logical_reads
    FROM sys.dm_exec_sessions
    WHERE session_id > 50
      --and open_transaction_Count > 0
      and dateadd(hour, -@Tempo_Conexoes_Hs, getdate()) > login_time
    ORDER BY logical_reads DESC

    -- Tratamento caso n�o retorne nenhuma conex�o
    IF NOT EXISTS(SELECT TOP 1 session_id
                  FROM #ALERT_Tamanho_MDF_Tempdb_Conexoes)
        BEGIN
            INSERT INTO #ALERT_Tamanho_MDF_Tempdb_Conexoes
            VALUES (NULL, 'Sem conexao aberta a mais de 1 hora', NULL,
                    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        END

    -----------------------------------------------------------
    --	Create Tabela para armazenar os Dados da sp_whoisactive
    -----------------------------------------------------------
    -- Create a tabela que ira armazenar os dados dos processos
    IF (OBJECT_ID('tempdb..#Resultado_WhoisActive') IS NOT NULL)
        DROP TABLE #Resultado_WhoisActive

    CREATE TABLE #Resultado_WhoisActive
    (
        [dd hh:mm:ss.mss]     VARCHAR(20),
        [database_name]       NVARCHAR(128),
        [login_name]          NVARCHAR(128),
        [host_name]           NVARCHAR(128),
        [start_time]          DATETIME,
        [status]              VARCHAR(30),
        [session_id]          INT,
        [blocking_session_id] INT,
        [wait_info]           VARCHAR(MAX),
        [open_tran_count]     INT,
        [CPU]                 VARCHAR(MAX),
        [reads]               VARCHAR(MAX),
        [writes]              VARCHAR(MAX),
        [sql_command]         XML
    )

    -- Seta a hora atual
    SELECT @Dt_Atual = GETDATE()

    -----------------------------------------------------------
    --	Carrega os Dados da sp_whoisactive
    -----------------------------------------------------------
    -- Retorna todos os processos que est�o sendo executados no momento
    EXEC [dbo].[sp_whoisactive]
         @get_outer_command = 1,
         @output_column_list = '[dd hh:mm:ss.mss][database_name][login_name][host_name][start_time][status][session_id][blocking_session_id][wait_info][open_tran_count][CPU][reads][writes][sql_command]',
         @destination_table = '#Resultado_WhoisActive'

    -- Altera a coluna que possui o comando SQL
    ALTER TABLE #Resultado_WhoisActive
        ALTER COLUMN [sql_command] VARCHAR(MAX)

    UPDATE #Resultado_WhoisActive
    SET [sql_command] = REPLACE(REPLACE(REPLACE(REPLACE(
                                                        CAST([sql_command] AS VARCHAR(1000)),
                                                        '<?query --',
                                                        ''), '--?>',
                                                ''), '&gt;', '>'),
                                '&lt;', '')

    -- select * from #Resultado_WhoisActive

    -- Verifica se n�o existe nenhum processo em Execu��o
    IF NOT EXISTS(SELECT TOP 1 * FROM #Resultado_WhoisActive)
        BEGIN
            INSERT INTO #Resultado_WhoisActive
            SELECT NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL
        END

    /**********************************************************
	--	Verifica se o Consumo do Arquivo do Tempdb est� muito grande
	**********************************************************/
    IF EXISTS(
            select TOP 1 unallocated_extent_MB
            from #ALERT_Tamanho_MDF_Tempdb
            where reserved_MB > @Tamanho_Tempdb
              and unallocated_extent_MB <
                  reserved_MB * (1 - (@Tempdb_Parametro / 100.0))
        )
        BEGIN
            -- START - ALERT
            IF ISNULL(@Fl_Tipo, 0) = 0 -- Envia o ALERT apenas uma vez
                BEGIN
                    /**********************************************************
			        --	CREATE EMAIL - ALERT - TAMANHO ARQUIVO MDF TEMPDB
			        **********************************************************/

                    -----------------------------------------------------------
                    --	ALERT - HEADER
                    -----------------------------------------------------------
                    SET @ALERTTamanhoMDFTempdbHeader =
                            '<font color=black bold=true size=5>'
                    SET @ALERTTamanhoMDFTempdbHeader =
                                @ALERTTamanhoMDFTempdbHeader +
                                '<BR /> Tamanho Arquivo MDF Tempdb <BR />'
                    SET @ALERTTamanhoMDFTempdbHeader =
                            @ALERTTamanhoMDFTempdbHeader + '</font>'

                    -----------------------------------------------------------
                    --	ALERT - BODY
                    -----------------------------------------------------------
                    SET @ALERTTamanhoMDFTempdbTable = CAST((
                        SELECT td = CAST(file_id AS VARCHAR) + '</td>'
                                        + '<td>' +
                                    CAST(reserved_MB AS VARCHAR) +
                                    '</td>'
                                        + '<td>' +
                                    CAST(Pr_Utilizado AS VARCHAR) +
                                    '</td>'
                                        + '<td>' +
                                    CAST(unallocated_extent_MB AS VARCHAR) +
                                    '</td>'
                                        + '<td>' + CAST(
                                            internal_object_reserved_MB AS VARCHAR) +
                                    '</td>'
                                        + '<td>' +
                                    CAST(version_store_reserved_MB AS VARCHAR) +
                                    '</td>'
                                        + '<td>' +
                                    CAST(user_object_reserved_MB AS VARCHAR) +
                                    '</td>'
                        FROM (
                                 -- EMAIL Table Details
                                 select file_id,
                                        reserved_MB,
                                        CAST(
                                                ((1 - (unallocated_extent_MB / reserved_MB)) *
                                                 100) AS NUMERIC(15, 2)) AS Pr_Utilizado,
                                        unallocated_extent_MB,
                                        internal_object_reserved_MB,
                                        version_store_reserved_MB,
                                        user_object_reserved_MB
                                 from #ALERT_Tamanho_MDF_Tempdb
                             ) AS D FOR XML PATH ('tr'), TYPE) AS VARCHAR(MAX)
                        )

                    -- Corrects Table Formatting
                    SET @ALERTTamanhoMDFTempdbTable = REPLACE(
                            REPLACE(REPLACE(
                                            @ALERTTamanhoMDFTempdbTable,
                                            '&lt;', '<'), '&gt;', '>'),
                            '<td>', '<td align = center>')

                    -- EMAIL Table Titles
                    SET @ALERTTamanhoMDFTempdbTable =
                                '<table cellspacing="2" cellpadding="5" border="3">'
                                + '<tr>
							<th bgcolor=#0B0B61 width="200"><font color=white>File ID</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Espa�o Reservado (MB)</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Percentual Utilizado (%)</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Espa�o N�o Alocado (MB)</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Espa�o Objetos Internos (MB)</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Espa�o Version Store (MB)</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Espa�o Objetos de Usu�rio (MB)</font></th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  @ALERTTamanhoMDFTempdbTable,
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'

                    /**********************************************************
			        --	CREATE EMAIL - ALERT - CONEXOES COM TRANSACAO ABERTA
			        **********************************************************/

                    -----------------------------------------------------------
                    --	ALERT - HEADER
                    -----------------------------------------------------------
                    SET @ALERTTempdbUtilizacaoArquivoHeader =
                            '<font color=black bold=true size=5>'
                    SET @ALERTTempdbUtilizacaoArquivoHeader =
                                @ALERTTempdbUtilizacaoArquivoHeader +
                                '<BR /> TOP 50 - Conex�es com Transa��o Aberta <BR />'
                    SET @ALERTTempdbUtilizacaoArquivoHeader =
                                @ALERTTempdbUtilizacaoArquivoHeader +
                                '</font>'

                    -----------------------------------------------------------
                    --	ALERT - BODY
                    -----------------------------------------------------------
                    SET @ALERTTamanhoMDFTempdbConexoesTable = CAST((
                        SELECT td = session_id + '</td>'
                                        + '<td>' + login_time + '</td>'
                                        + '<td>' + login_name + '</td>'
                                        + '<td>' + host_name + '</td>'
                                        + '<td>' +
                                    open_transaction_Count + '</td>'
                                        + '<td>' + status + '</td>'
                                        + '<td>' + cpu_time + '</td>'
                                        + '<td>' + total_elapsed_time +
                                    '</td>'
                                        + '<td>' + reads + '</td>'
                                        + '<td>' + writes + '</td>'
                                        + '<td>' + logical_reads +
                                    '</td>'
                        FROM (
                                 -- EMAIL Table Details
                                 SELECT ISNULL(CAST(session_id AS VARCHAR), '-') AS session_id,
                                        ISNULL(login_time, '-')                  AS login_time,
                                        ISNULL(login_name, '-')                  AS login_name,
                                        ISNULL(host_name, '-')                   AS host_name,
                                        ISNULL(
                                                CAST(open_transaction_Count AS VARCHAR),
                                                '-')                             AS open_transaction_Count,
                                        ISNULL(status, '-')                      AS status,
                                        ISNULL(CAST(cpu_time AS VARCHAR), '-')   AS cpu_time,
                                        ISNULL(
                                                CAST(total_elapsed_time AS VARCHAR),
                                                '-')                             AS total_elapsed_time,
                                        ISNULL(CAST(reads AS VARCHAR), '-')      AS reads,
                                        ISNULL(CAST(writes AS VARCHAR), '-')     AS writes,
                                        ISNULL(
                                                CAST(logical_reads AS VARCHAR),
                                                '-')                             AS logical_reads
                                 FROM #ALERT_Tamanho_MDF_Tempdb_Conexoes
                             ) AS D
                        ORDER BY CAST(
                                         REPLACE([logical_reads], '-', 0) AS BIGINT) DESC FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
                        )

                    -- Corrects Table Formatting
                    SET @ALERTTamanhoMDFTempdbConexoesTable = REPLACE(
                            REPLACE(REPLACE(
                                            @ALERTTamanhoMDFTempdbConexoesTable,
                                            '&lt;', '<'), '&gt;', '>'),
                            '<td>', '<td align = center>')

                    -- EMAIL Table Titles
                    SET @ALERTTamanhoMDFTempdbConexoesTable =
                                '<table cellspacing="2" cellpadding="5" border="3">'
                                + '<tr>
							<th bgcolor=#0B0B61 width="100"><font color=white>session_id</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>login_time</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>login_name</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>host_name</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>open_transaction_Count</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>status</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>cpu_time</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>total_elapsed_time</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>reads</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>writes</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>logical_reads</font></th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  @ALERTTamanhoMDFTempdbConexoesTable,
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'
                    -----------------------------------------------------------
                    --	ALERT - HEADER
                    -----------------------------------------------------------
                    SET @ALERTTempdbProcessoExecHeader =
                            '<font color=black bold=true size=5>'
                    SET @ALERTTempdbProcessoExecHeader =
                                @ALERTTempdbProcessoExecHeader +
                                '<BR /> TOP 50 - Processos executando no Database <BR />'
                    SET @ALERTTempdbProcessoExecHeader =
                            @ALERTTempdbProcessoExecHeader + '</font>'

                    -----------------------------------------------------------
                    --	ALERT - BODY
                    -----------------------------------------------------------
                    SET @ALERTTempdbProcessoExecTable = CAST((
                        SELECT td = [Duração] + '</td>'
                                        + '<td>' + [database_name] +
                                    '</td>'
                                        + '<td>' + [login_name] +
                                    '</td>'
                                        + '<td>' + [host_name] + '</td>'
                                        + '<td>' + [start_time] +
                                    '</td>'
                                        + '<td>' + [status] + '</td>'
                                        + '<td>' + [session_id] +
                                    '</td>'
                                        + '<td>' +
                                    [blocking_session_id] + '</td>'
                                        + '<td>' + [Wait] + '</td>'
                                        + '<td>' + [open_tran_count] +
                                    '</td>'
                                        + '<td>' + [CPU] + '</td>'
                                        + '<td>' + [reads] + '</td>'
                                        + '<td>' + [writes] + '</td>'
                                        + '<td>' + [sql_command] +
                                    '</td>'

                        FROM (
                                 -- EMAIL Table Details
                                 SELECT TOP 50 ISNULL([dd hh:mm:ss.mss], '-') AS [Duração],
                                               ISNULL([database_name], '-')   AS [database_name],
                                               ISNULL([login_name], '-')      AS [login_name],
                                               ISNULL([host_name], '-')       AS [host_name],
                                               ISNULL(
                                                       CONVERT(VARCHAR(20), [start_time], 120),
                                                       '-')                   AS [start_time],
                                               ISNULL([status], '-')          AS [status],
                                               ISNULL(
                                                       CAST([session_id] AS VARCHAR),
                                                       '-')                   AS [session_id],
                                               ISNULL(
                                                       CAST([blocking_session_id] AS VARCHAR),
                                                       '-')                   AS [blocking_session_id],
                                               ISNULL([wait_info], '-')       AS [Wait],
                                               ISNULL(
                                                       CAST([open_tran_count] AS VARCHAR),
                                                       '-')                   AS [open_tran_count],
                                               ISNULL([CPU], '-')             AS [CPU],
                                               ISNULL([reads], '-')           AS [reads],
                                               ISNULL([writes], '-')          AS [writes],
                                               ISNULL(
                                                       SUBSTRING([sql_command], 1, 300),
                                                       '-')                   AS [sql_command]
                                 FROM #Resultado_WhoisActive
                                 ORDER BY [start_time]
                             ) AS D
                        ORDER BY [start_time] FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
                        )

                    -- Corrects Table Formatting
                    SET @ALERTTempdbProcessoExecTable = REPLACE(
                            REPLACE(REPLACE(
                                            @ALERTTempdbProcessoExecTable,
                                            '&lt;', '<'), '&gt;', '>'),
                            '<td>', '<td align = center>')

                    -- EMAIL Table Titles
                    SET @ALERTTempdbProcessoExecTable =
                                '<table cellspacing="2" cellpadding="5" border="3">'
                                + '<tr>
							<th bgcolor=#0B0B61 width="140"><font color=white>[dd hh:mm:ss.mss]</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Database</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Login</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Host Name</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Hora In�cio</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Status</font></th>
							<th bgcolor=#0B0B61 width="30"><font color=white>ID Session</font></th>
							<th bgcolor=#0B0B61 width="60"><font color=white>ID Session Bloqueando</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Wait</font></th>
							<th bgcolor=#0B0B61 width="60"><font color=white>transactions Abertas</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>CPU</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Reads</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Writes</font></th>
							<th bgcolor=#0B0B61 width="300"><font color=white>Query</font></th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  @ALERTTempdbProcessoExecTable,
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'

                    -----------------------------------------------------------
                    -- Insert a blank page in EMAIL
                    -----------------------------------------------------------
                    SET @EmptyBodyEmail = ''
                    SET @EmptyBodyEmail =
                                '<table cellpadding="5" cellspacing="5" border="0">' +
                                '<tr>
							<th width="500">               </th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  ISNULL(@EmptyBodyEmail, ''),
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'

                    /**********************************************************
			        --	Set EMAIL Variables
			        **********************************************************/
                    SELECT @Importance = 'High',
                           @Subject =
                           'ALERT: O Tamanho do Arquivo MDF do Tempdb est� acima de 70% no Servidor: ' +
                           @@SERVERNAME,
                           @EmailBody = @ALERTTamanhoMDFTempdbHeader +
                                        @EmptyBodyEmail +
                                        @ALERTTamanhoMDFTempdbTable +
                                        @EmptyBodyEmail +
                                        @ALERTTempdbUtilizacaoArquivoHeader +
                                        @EmptyBodyEmail +
                                        @ALERTTamanhoMDFTempdbConexoesTable +
                                        @EmptyBodyEmail +
                                        @ALERTTempdbProcessoExecHeader +
                                        @EmptyBodyEmail +
                                        @ALERTTempdbProcessoExecTable +
                                        @EmptyBodyEmail


                    /**********************************************************
			        --	SEND EMAIL - ALERT
			        **********************************************************/
                    EXEC [msdb].[dbo].[sp_send_dbmail]
                         @profile_name = @ProfileEmail,
                         @recipients = @EmailDestination,
                         @subject = @Subject,
                         @body = @EmailBody,
                         @body_format = 'HTML',
                         @importance = @Importance

                    /**********************************************************
			        -- Inserts a Record in the ALERT Control Table -> Fl_Tipo = 1 : ALERT
			        **********************************************************/
                    INSERT INTO [dbo].[ALERT] ([Id_ALERT_Parametro],
                                               [Ds_Mensagem],
                                               [Fl_Tipo])
                    SELECT @Id_ALERT_Parametro, @Subject, 1
                END
        END -- END - ALERT
    ELSE
        BEGIN
            -- START - CLEAR
            IF @Fl_Tipo = 1
                BEGIN
                    /**********************************************************
			        --	CREATE EMAIL - CLEAR - TAMANHO ARQUIVO MDF TEMPDB
			        **********************************************************/

                    -----------------------------------------------------------
                    --	CLEAR - HEADER
                    -----------------------------------------------------------
                    SET @ALERTTamanhoMDFTempdbHeader =
                            '<font color=black bold=true size=5>'
                    SET @ALERTTamanhoMDFTempdbHeader =
                                @ALERTTamanhoMDFTempdbHeader +
                                '<BR /> Tamanho Arquivo MDF Tempdb <BR />'
                    SET @ALERTTamanhoMDFTempdbHeader =
                            @ALERTTamanhoMDFTempdbHeader + '</font>'

                    -----------------------------------------------------------
                    --	CLEAR - BODY
                    -----------------------------------------------------------
                    SET @ALERTTamanhoMDFTempdbTable = CAST((
                        SELECT td = CAST(file_id AS VARCHAR) + '</td>'
                                        + '<td>' +
                                    CAST(reserved_MB AS VARCHAR) +
                                    '</td>'
                                        + '<td>' +
                                    CAST(Pr_Utilizado AS VARCHAR) +
                                    '</td>'
                                        + '<td>' +
                                    CAST(unallocated_extent_MB AS VARCHAR) +
                                    '</td>'
                                        + '<td>' + CAST(
                                            internal_object_reserved_MB AS VARCHAR) +
                                    '</td>'
                                        + '<td>' +
                                    CAST(version_store_reserved_MB AS VARCHAR) +
                                    '</td>'
                                        + '<td>' +
                                    CAST(user_object_reserved_MB AS VARCHAR) +
                                    '</td>'
                        FROM (
                                 -- EMAIL Table Details
                                 select file_id,
                                        reserved_MB,
                                        CAST(
                                                ((1 - (unallocated_extent_MB / reserved_MB)) *
                                                 100) AS NUMERIC(15, 2)) AS Pr_Utilizado,
                                        unallocated_extent_MB,
                                        internal_object_reserved_MB,
                                        version_store_reserved_MB,
                                        user_object_reserved_MB
                                 from #ALERT_Tamanho_MDF_Tempdb
                             ) AS D FOR XML PATH ('tr'), TYPE) AS VARCHAR(MAX)
                        )

                    -- Corrects Table Formatting
                    SET @ALERTTamanhoMDFTempdbTable = REPLACE(
                            REPLACE(REPLACE(
                                            @ALERTTamanhoMDFTempdbTable,
                                            '&lt;', '<'), '&gt;', '>'),
                            '<td>', '<td align = center>')

                    -- EMAIL Table Titles
                    SET @ALERTTamanhoMDFTempdbTable =
                                '<table cellspacing="2" cellpadding="5" border="3">'
                                + '<tr>
							<th bgcolor=#0B0B61 width="200"><font color=white>File ID</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Espa�o Reservado (MB)</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Percentual Utilizado (%)</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Espa�o N�o Alocado (MB)</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Espa�o Objetos Internos (MB)</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Espa�o Version Store (MB)</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Espa�o Objetos de Usu�rio (MB)</font></th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  @ALERTTamanhoMDFTempdbTable,
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'

                    /**********************************************************
			        --	CREATE EMAIL - CLEAR - CONEXOES COM TRANSACAO ABERTA
			        **********************************************************/

                    -----------------------------------------------------------
                    --	CLEAR - HEADER
                    -----------------------------------------------------------
                    SET @ALERTTempdbUtilizacaoArquivoHeader =
                            '<font color=black bold=true size=5>'
                    SET @ALERTTempdbUtilizacaoArquivoHeader =
                                @ALERTTempdbUtilizacaoArquivoHeader +
                                '<BR /> TOP 50 - Conex�es com Transa��o Aberta <BR />'
                    SET @ALERTTempdbUtilizacaoArquivoHeader =
                                @ALERTTempdbUtilizacaoArquivoHeader +
                                '</font>'

                    -----------------------------------------------------------
                    --	CLEAR - BODY
                    -----------------------------------------------------------
                    SET @ALERTTamanhoMDFTempdbConexoesTable = CAST((
                        SELECT td = session_id + '</td>'
                                        + '<td>' + login_time + '</td>'
                                        + '<td>' + login_name + '</td>'
                                        + '<td>' + host_name + '</td>'
                                        + '<td>' +
                                    open_transaction_Count + '</td>'
                                        + '<td>' + status + '</td>'
                                        + '<td>' + cpu_time + '</td>'
                                        + '<td>' + total_elapsed_time +
                                    '</td>'
                                        + '<td>' + reads + '</td>'
                                        + '<td>' + writes + '</td>'
                                        + '<td>' + logical_reads +
                                    '</td>'
                        FROM (
                                 -- EMAIL Table Details
                                 SELECT ISNULL(CAST(session_id AS VARCHAR), '-') AS session_id,
                                        ISNULL(login_time, '-')                  AS login_time,
                                        ISNULL(login_name, '-')                  AS login_name,
                                        ISNULL(host_name, '-')                   AS host_name,
                                        ISNULL(
                                                CAST(open_transaction_Count AS VARCHAR),
                                                '-')                             AS open_transaction_Count,
                                        ISNULL(status, '-')                      AS status,
                                        ISNULL(CAST(cpu_time AS VARCHAR), '-')   AS cpu_time,
                                        ISNULL(
                                                CAST(total_elapsed_time AS VARCHAR),
                                                '-')                             AS total_elapsed_time,
                                        ISNULL(CAST(reads AS VARCHAR), '-')      AS reads,
                                        ISNULL(CAST(writes AS VARCHAR), '-')     AS writes,
                                        ISNULL(
                                                CAST(logical_reads AS VARCHAR),
                                                '-')                             AS logical_reads
                                 FROM #ALERT_Tamanho_MDF_Tempdb_Conexoes
                             ) AS D
                        ORDER BY CAST(
                                         REPLACE([logical_reads], '-', 0) AS BIGINT) DESC FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
                        )

                    -- Corrects Table Formatting
                    SET @ALERTTamanhoMDFTempdbConexoesTable = REPLACE(
                            REPLACE(REPLACE(
                                            @ALERTTamanhoMDFTempdbConexoesTable,
                                            '&lt;', '<'), '&gt;', '>'),
                            '<td>', '<td align = center>')

                    -- EMAIL Table Titles
                    SET @ALERTTamanhoMDFTempdbConexoesTable =
                                '<table cellspacing="2" cellpadding="5" border="3">'
                                + '<tr>
							<th bgcolor=#0B0B61 width="100"><font color=white>session_id</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>login_time</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>login_name</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>host_name</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>open_transaction_Count</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>status</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>cpu_time</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>total_elapsed_time</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>reads</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>writes</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>logical_reads</font></th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  @ALERTTamanhoMDFTempdbConexoesTable,
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'

                    -----------------------------------------------------------
                    --	ALERT - HEADER
                    -----------------------------------------------------------
                    SET @ALERTTempdbProcessoExecHeader =
                            '<font color=black bold=true size=5>'
                    SET @ALERTTempdbProcessoExecHeader =
                                @ALERTTempdbProcessoExecHeader +
                                '<BR /> TOP 50 - Processos executando no Database <BR />'
                    SET @ALERTTempdbProcessoExecHeader =
                            @ALERTTempdbProcessoExecHeader + '</font>'

                    -----------------------------------------------------------
                    --	ALERT - BODY
                    -----------------------------------------------------------
                    SET @ALERTTempdbProcessoExecTable = CAST((
                        SELECT td = [Duração] + '</td>'
                                        + '<td>' + [database_name] +
                                    '</td>'
                                        + '<td>' + [login_name] +
                                    '</td>'
                                        + '<td>' + [host_name] + '</td>'
                                        + '<td>' + [start_time] +
                                    '</td>'
                                        + '<td>' + [status] + '</td>'
                                        + '<td>' + [session_id] +
                                    '</td>'
                                        + '<td>' +
                                    [blocking_session_id] + '</td>'
                                        + '<td>' + [Wait] + '</td>'
                                        + '<td>' + [open_tran_count] +
                                    '</td>'
                                        + '<td>' + [CPU] + '</td>'
                                        + '<td>' + [reads] + '</td>'
                                        + '<td>' + [writes] + '</td>'
                                        + '<td>' + [sql_command] +
                                    '</td>'

                        FROM (
                                 -- EMAIL Table Details
                                 SELECT TOP 50 ISNULL([dd hh:mm:ss.mss], '-') AS [Duração],
                                               ISNULL([database_name], '-')   AS [database_name],
                                               ISNULL([login_name], '-')      AS [login_name],
                                               ISNULL([host_name], '-')       AS [host_name],
                                               ISNULL(
                                                       CONVERT(VARCHAR(20), [start_time], 120),
                                                       '-')                   AS [start_time],
                                               ISNULL([status], '-')          AS [status],
                                               ISNULL(
                                                       CAST([session_id] AS VARCHAR),
                                                       '-')                   AS [session_id],
                                               ISNULL(
                                                       CAST([blocking_session_id] AS VARCHAR),
                                                       '-')                   AS [blocking_session_id],
                                               ISNULL([wait_info], '-')       AS [Wait],
                                               ISNULL(
                                                       CAST([open_tran_count] AS VARCHAR),
                                                       '-')                   AS [open_tran_count],
                                               ISNULL([CPU], '-')             AS [CPU],
                                               ISNULL([reads], '-')           AS [reads],
                                               ISNULL([writes], '-')          AS [writes],
                                               ISNULL(
                                                       SUBSTRING([sql_command], 1, 300),
                                                       '-')                   AS [sql_command]
                                 FROM #Resultado_WhoisActive
                                 ORDER BY [start_time]
                             ) AS D
                        ORDER BY [start_time] FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
                        )

                    -- Corrects Table Formatting
                    SET @ALERTTempdbProcessoExecTable = REPLACE(
                            REPLACE(REPLACE(
                                            @ALERTTempdbProcessoExecTable,
                                            '&lt;', '<'), '&gt;', '>'),
                            '<td>', '<td align = center>')

                    -- EMAIL Table Titles
                    SET @ALERTTempdbProcessoExecTable =
                                '<table cellspacing="2" cellpadding="5" border="3">'
                                + '<tr>
							<th bgcolor=#0B0B61 width="140"><font color=white>[dd hh:mm:ss.mss]</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Database</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Login</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Host Name</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Hora In�cio</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Status</font></th>
							<th bgcolor=#0B0B61 width="30"><font color=white>ID Session</font></th>
							<th bgcolor=#0B0B61 width="60"><font color=white>ID Session Bloqueando</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Wait</font></th>
							<th bgcolor=#0B0B61 width="60"><font color=white>transactions Abertas</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>CPU</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Reads</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Writes</font></th>
							<th bgcolor=#0B0B61 width="300"><font color=white>Query</font></th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  @ALERTTempdbProcessoExecTable,
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'

                    -----------------------------------------------------------
                    -- Insert a blank page in EMAIL
                    -----------------------------------------------------------
                    SET @EmptyBodyEmail = ''
                    SET @EmptyBodyEmail =
                                '<table cellpadding="5" cellspacing="5" border="0">' +
                                '<tr>
							<th width="500">               </th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  ISNULL(@EmptyBodyEmail, ''),
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'

                    /**********************************************************
			        --	Set EMAIL Variables
			        **********************************************************/
                    SELECT @Importance = 'High',
                           @Subject =
                           'CLEAR: O Tamanho do Arquivo MDF do Tempdb est� abaixo de 70% no Servidor: ' +
                           @@SERVERNAME,
                           @EmailBody = @ALERTTamanhoMDFTempdbHeader +
                                        @EmptyBodyEmail +
                                        @ALERTTamanhoMDFTempdbTable +
                                        @EmptyBodyEmail +
                                        @ALERTTempdbUtilizacaoArquivoHeader +
                                        @EmptyBodyEmail +
                                        @ALERTTamanhoMDFTempdbConexoesTable +
                                        @EmptyBodyEmail +
                                        @ALERTTempdbProcessoExecHeader +
                                        @EmptyBodyEmail +
                                        @ALERTTempdbProcessoExecTable +
                                        @EmptyBodyEmail

                    /**********************************************************
			        --	ENVIA O EMAIL - CLEAR
			        **********************************************************/
                    EXEC [msdb].[dbo].[sp_send_dbmail]
                         @profile_name = @ProfileEmail,
                         @recipients = @EmailDestination,
                         @subject = @Subject,
                         @body = @EmailBody,
                         @body_format = 'HTML',
                         @importance = @Importance

                    /**********************************************************
			        -- Inserts a Record in the ALERT Control Table -> Fl_Tipo = 0 : CLEAR
			    **********************************************************/
                    INSERT INTO [dbo].[ALERT] ([Id_ALERT_Parametro],
                                               [Ds_Mensagem],
                                               [Fl_Tipo])
                    SELECT @Id_ALERT_Parametro, @Subject, 0
                END
        END -- END - CLEAR
END
GO

GO
IF (OBJECT_ID('[dbo].[stpALERT_Conexao_SQLServer]') IS NOT NULL)
    DROP PROCEDURE [dbo].[stpALERT_Conexao_SQLServer]
GO

/**********************************************************
--	ALERT: CONEXAO SQL SERVER
**********************************************************/

CREATE PROCEDURE [dbo].[stpALERT_Conexao_SQLServer]
AS
BEGIN
    SET NOCOUNT ON

    -- Conex�es SQL Server
    DECLARE
        @Id_ALERT_Parametro INT = (SELECT Id_ALERT_Parametro
                                   FROM [Traces].[dbo].ALERT_Parametro (NOLOCK)
                                   WHERE Nm_ALERT = 'Conex�o SQL Server')

    -- Declares the variables
    DECLARE
        @Dt_Atual       DATETIME, @EmailBody VARCHAR(MAX), @ALERTConexaoSQLServerHeader VARCHAR(MAX), @ALERTConexaoSQLServerTable VARCHAR(MAX),
        @EmptyBodyEmail VARCHAR(MAX), @Importance AS VARCHAR(6), @Subject VARCHAR(500), @Qtd_Conexoes INT, @Conexoes_SQLServer_Parametro INT,
        @Fl_Tipo        INT, @EmailDestination VARCHAR(500), @ALERTConexaoProcessosExecHeader VARCHAR(MAX), @ALERTConexaoProcessosExecTable VARCHAR(MAX),
        @ProfileEmail   VARCHAR(200)

    -----------------------------------------------------------
    -- Retrieves ALERT parameters
    -----------------------------------------------------------
    SELECT @Conexoes_SQLServer_Parametro = Vl_Parametro, -- Quantidade
           @EmailDestination = Ds_Email,
           @ProfileEmail = Ds_Profile_Email
    FROM [dbo].[ALERT_Parametro]
    WHERE [Id_ALERT_Parametro] = @Id_ALERT_Parametro -- Conex�es SQL Server

    SELECT @Qtd_Conexoes = count(*)
    FROM sys.dm_exec_sessions
    WHERE session_id > 50

    -- Verifica o �ltimo Tipo do ALERT registrado -> 0: CLEAR / 1: ALERT
    SELECT @Fl_Tipo = [Fl_Tipo]
    FROM [dbo].[ALERT]
    WHERE [Id_ALERT] = (SELECT MAX(Id_ALERT)
                        FROM [dbo].[ALERT]
                        WHERE [Id_ALERT_Parametro] = @Id_ALERT_Parametro)

    -----------------------------------------------------------
    --	Create Tabela para armazenar os Dados da sp_whoisactive
    -----------------------------------------------------------
    -- Create a tabela que ira armazenar os dados dos processos
    IF (OBJECT_ID('tempdb..#Resultado_WhoisActive') IS NOT NULL)
        DROP TABLE #Resultado_WhoisActive

    CREATE TABLE #Resultado_WhoisActive
    (
        [dd hh:mm:ss.mss]     VARCHAR(20),
        [database_name]       NVARCHAR(128),
        [login_name]          NVARCHAR(128),
        [host_name]           NVARCHAR(128),
        [start_time]          DATETIME,
        [status]              VARCHAR(30),
        [session_id]          INT,
        [blocking_session_id] INT,
        [wait_info]           VARCHAR(MAX),
        [open_tran_count]     INT,
        [CPU]                 VARCHAR(MAX),
        [reads]               VARCHAR(MAX),
        [writes]              VARCHAR(MAX),
        [sql_command]         XML
    )

    -- Seta a hora atual
    SELECT @Dt_Atual = GETDATE()

    -----------------------------------------------------------
    --	Carrega os Dados da sp_whoisactive
    -----------------------------------------------------------
    -- Retorna todos os processos que est�o sendo executados no momento
    EXEC [dbo].[sp_whoisactive]
         @get_outer_command = 1,
         @output_column_list = '[dd hh:mm:ss.mss][database_name][login_name][host_name][start_time][status][session_id][blocking_session_id][wait_info][open_tran_count][CPU][reads][writes][sql_command]',
         @destination_table = '#Resultado_WhoisActive'

    -- Altera a coluna que possui o comando SQL
    ALTER TABLE #Resultado_WhoisActive
        ALTER COLUMN [sql_command] VARCHAR(MAX)

    UPDATE #Resultado_WhoisActive
    SET [sql_command] = REPLACE(REPLACE(REPLACE(REPLACE(
                                                        CAST([sql_command] AS VARCHAR(1000)),
                                                        '<?query --',
                                                        ''), '--?>',
                                                ''), '&gt;', '>'),
                                '&lt;', '')

    -- select * from #Resultado_WhoisActive

    -- Verifica se n�o existe nenhum processo em Execu��o
    IF NOT EXISTS(SELECT TOP 1 * FROM #Resultado_WhoisActive)
        BEGIN
            INSERT INTO #Resultado_WhoisActive
            SELECT NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL
        END

    /**********************************************************
	--	Verifica se o limite de conex�es para o ALERT foi atingido
	**********************************************************/
    IF (@Qtd_Conexoes > @Conexoes_SQLServer_Parametro)
        BEGIN
            -- START - ALERT
            /**********************************************************
		--	CREATE EMAIL - ALERT
		**********************************************************/

            -----------------------------------------------------------
            --	ALERT - CONEX�ES - HEADER
            -----------------------------------------------------------
            SET @ALERTConexaoSQLServerHeader =
                    '<font color=black bold=true size=5>'
            SET @ALERTConexaoSQLServerHeader =
                        @ALERTConexaoSQLServerHeader +
                        '<BR /> TOP 25 - Conex�es Abertas no SQL Server <BR />'
            SET @ALERTConexaoSQLServerHeader =
                    @ALERTConexaoSQLServerHeader + '</font>'

            -----------------------------------------------------------
            --	ALERT - CONEX�ES - BODY
            -----------------------------------------------------------
            if object_id('tempdb..#ConexoesAbertas') is not null
                drop table #ConexoesAbertas

            SELECT TOP 25 IDENTITY(INT, 1, 1)  AS     id,
                          replace(
                                  replace(ec.client_net_address, '<', ''),
                                  '>',
                                  '')                 client_net_address,
                          case
                              when es.[program_name] = ''
                                  then 'Sem nome na string de conex�o'
                              else [program_name] end [program_name],
                          es.[host_name],
                          es.login_name, /*db_name(database_id)*/
                          ''                          Base,
                          COUNT(ec.session_id) AS     [connection count]
            into #ConexoesAbertas
            FROM sys.dm_exec_sessions AS es
                     INNER JOIN sys.dm_exec_connections AS ec
                                ON es.session_id = ec.session_id
            GROUP BY ec.client_net_address, es.[program_name],
                     es.[host_name],/*db_name(database_id),*/
                     es.login_name
            order by [connection count] desc

            SET @ALERTConexaoSQLServerTable = CAST((
                SELECT td = client_net_address + '</td>'
                                + '<td>' + [program_name] + '</td>'
                                + '<td>' + [host_name] + '</td>'
                                + '<td>' + login_name + '</td>'
                                + '<td>' + Base + '</td>'
                                + '<td>' + [connection count] + '</td>'

                FROM (
                         SELECT client_net_address,
                                [program_name],
                                [host_name],
                                login_name,
                                Base,
                                cast([connection count] as varchar) [connection count],
                                id
                         FROM #ConexoesAbertas
                     ) AS D
                ORDER BY id FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
                )

            -- Corrects Table Formatting
            SET @ALERTConexaoSQLServerTable = REPLACE(
                    REPLACE(REPLACE(@ALERTConexaoSQLServerTable,
                                    '&lt;', '<'), '&gt;', '>'), '<td>',
                    '<td align = center>')

            -- EMAIL Table Titles
            SET @ALERTConexaoSQLServerTable =
                        '<table cellspacing="2" cellpadding="5" border="3">'
                        + '<tr>
						<th bgcolor=#0B0B61 width="50"><font color=white>IP</font></th>
						<th bgcolor=#0B0B61 width="50"><font color=white>Aplicacao</font></th>
						<th bgcolor=#0B0B61 width="50"><font color=white>Hostname</font></th>
						<th bgcolor=#0B0B61 width="50"><font color=white>Login</font></th>
						<th bgcolor=#0B0B61 width="50"><font color=white>Database</font></th>
						<th bgcolor=#0B0B61 width="10"><font color=white>Qtd. Conex�es</font></th>
					</tr>'
                        + REPLACE(REPLACE(@ALERTConexaoSQLServerTable,
                                          '&lt;', '<'), '&gt;', '>')
                        + '</table>'

            -----------------------------------------------------------
            --	ALERT - HEADER
            -----------------------------------------------------------
            SET @ALERTConexaoProcessosExecHeader =
                    '<font color=black bold=true size=5>'
            SET @ALERTConexaoProcessosExecHeader =
                        @ALERTConexaoProcessosExecHeader +
                        '<BR /> TOP 50 - Processos executando no Database <BR />'
            SET @ALERTConexaoProcessosExecHeader =
                    @ALERTConexaoProcessosExecHeader + '</font>'

            -----------------------------------------------------------
            --	ALERT - BODY
            -----------------------------------------------------------
            SET @ALERTConexaoProcessosExecTable = CAST((
                SELECT td = [Duração] + '</td>'
                                + '<td>' + [database_name] + '</td>'
                                + '<td>' + [login_name] + '</td>'
                                + '<td>' + [host_name] + '</td>'
                                + '<td>' + [start_time] + '</td>'
                                + '<td>' + [status] + '</td>'
                                + '<td>' + [session_id] + '</td>'
                                + '<td>' + [blocking_session_id] +
                            '</td>'
                                + '<td>' + [Wait] + '</td>'
                                + '<td>' + [open_tran_count] + '</td>'
                                + '<td>' + [CPU] + '</td>'
                                + '<td>' + [reads] + '</td>'
                                + '<td>' + [writes] + '</td>'
                                + '<td>' + [sql_command] + '</td>'

                FROM (
                         -- EMAIL Table Details
                         SELECT TOP 50 ISNULL([dd hh:mm:ss.mss], '-') AS [Duração],
                                       ISNULL([database_name], '-')   AS [database_name],
                                       ISNULL([login_name], '-')      AS [login_name],
                                       ISNULL([host_name], '-')       AS [host_name],
                                       ISNULL(
                                               CONVERT(VARCHAR(20), [start_time], 120),
                                               '-')                   AS [start_time],
                                       ISNULL([status], '-')          AS [status],
                                       ISNULL(
                                               CAST([session_id] AS VARCHAR),
                                               '-')                   AS [session_id],
                                       ISNULL(
                                               CAST([blocking_session_id] AS VARCHAR),
                                               '-')                   AS [blocking_session_id],
                                       ISNULL([wait_info], '-')       AS [Wait],
                                       ISNULL(
                                               CAST([open_tran_count] AS VARCHAR),
                                               '-')                   AS [open_tran_count],
                                       ISNULL([CPU], '-')             AS [CPU],
                                       ISNULL([reads], '-')           AS [reads],
                                       ISNULL([writes], '-')          AS [writes],
                                       ISNULL(
                                               SUBSTRING([sql_command], 1, 300),
                                               '-')                   AS [sql_command]
                         FROM #Resultado_WhoisActive
                         ORDER BY [start_time]
                     ) AS D
                ORDER BY [start_time] FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
                )

            -- Corrects Table Formatting
            SET @ALERTConexaoProcessosExecTable = REPLACE(
                    REPLACE(REPLACE(@ALERTConexaoProcessosExecTable,
                                    '&lt;', '<'), '&gt;', '>'), '<td>',
                    '<td align = center>')

            -- EMAIL Table Titles
            SET @ALERTConexaoProcessosExecTable =
                        '<table cellspacing="2" cellpadding="5" border="3">'
                        + '<tr>
							<th bgcolor=#0B0B61 width="140"><font color=white>[dd hh:mm:ss.mss]</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Database</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Login</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Host Name</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Hora In�cio</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Status</font></th>
							<th bgcolor=#0B0B61 width="30"><font color=white>ID Session</font></th>
							<th bgcolor=#0B0B61 width="60"><font color=white>ID Session Bloqueando</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Wait</font></th>
							<th bgcolor=#0B0B61 width="60"><font color=white>transactions Abertas</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>CPU</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Reads</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Writes</font></th>
							<th bgcolor=#0B0B61 width="300"><font color=white>Query</font></th>
						</tr>'
                        + REPLACE(REPLACE(
                                          @ALERTConexaoProcessosExecTable,
                                          '&lt;', '<'), '&gt;', '>')
                        + '</table>'

            -----------------------------------------------------------
            -- Insert a blank page in EMAIL
            -----------------------------------------------------------
            SET @EmptyBodyEmail = ''
            SET @EmptyBodyEmail =
                        '<table cellpadding="5" cellspacing="5" border="0">' +
                        '<tr>
						<th width="500">               </th>
					</tr>'
                        + REPLACE(REPLACE(ISNULL(@EmptyBodyEmail, ''),
                                          '&lt;', '<'), '&gt;', '>')
                        + '</table>'


            /**********************************************************
		--	Set EMAIL Variables
		**********************************************************/
            SELECT @Importance = 'High',
                   @Subject =
                   'ALERT: Existem ' + cast(@Qtd_Conexoes as varchar) +
                   ' Conex�es Abertas no SQL Server no Servidor: ' +
                   @@SERVERNAME,
                   @EmailBody =
                   @ALERTConexaoSQLServerHeader + @EmptyBodyEmail +
                   @ALERTConexaoSQLServerTable + @EmptyBodyEmail +
                   @ALERTConexaoProcessosExecHeader + @EmptyBodyEmail +
                   @ALERTConexaoProcessosExecTable + @EmptyBodyEmail


            /**********************************************************
		--	SEND EMAIL - ALERT
		**********************************************************/
            EXEC [msdb].[dbo].[sp_send_dbmail]
                 @profile_name = @ProfileEmail,
                 @recipients = @EmailDestination,
                 @subject = @Subject,
                 @body = @EmailBody,
                 @body_format = 'HTML',
                 @importance = @Importance

            /**********************************************************
		-- Inserts a Record in the ALERT Control Table -> Fl_Tipo = 1 : ALERT
		**********************************************************/
            INSERT INTO [dbo].[ALERT] ([Id_ALERT_Parametro],
                                       [Ds_Mensagem], [Fl_Tipo])
            SELECT @Id_ALERT_Parametro, @Subject, 1
        END -- END - ALERT
    ELSE
        BEGIN
            -- START - CLEAR
            IF @Fl_Tipo = 1
                BEGIN
                    /**********************************************************
			--	CREATE EMAIL - CLEAR
			**********************************************************/

                    -----------------------------------------------------------
                    --	CLEAR - HEADER
                    -----------------------------------------------------------
                    SET @ALERTConexaoSQLServerHeader =
                            '<font color=black bold=true size=5>'
                    SET @ALERTConexaoSQLServerHeader =
                                @ALERTConexaoSQLServerHeader +
                                '<BR /> Conex�es abertas no SQL Server <BR />'
                    SET @ALERTConexaoSQLServerHeader =
                            @ALERTConexaoSQLServerHeader + '</font>'

                    -----------------------------------------------------------
                    --	CLEAR - BODY
                    -----------------------------------------------------------

                    if object_id(
                            'tempdb..#ConexoesAbertas_Clear') is not null
                        drop table #ConexoesAbertas_Clear

                    SELECT top 25 IDENTITY(INT, 1, 1)  AS     id,
                                  replace(
                                          replace(ec.client_net_address, '<', ''),
                                          '>',
                                          '')                 client_net_address,
                                  case
                                      when es.[program_name] = ''
                                          then 'Sem nome na string de conex�o'
                                      else [program_name] end [program_name],
                                  es.[host_name],
                                  es.login_name, /*db_name(database_id)*/
                                  ''                          Base,
                                  COUNT(ec.session_id) AS     [connection count]
                    into #ConexoesAbertas_Clear
                    FROM sys.dm_exec_sessions AS es
                             INNER JOIN sys.dm_exec_connections AS ec
                                        ON es.session_id = ec.session_id
                    GROUP BY ec.client_net_address, es.[program_name],
                             es.[host_name],/*db_name(database_id),*/
                             es.login_name
                    order by [connection count] desc

                    SET @ALERTConexaoSQLServerTable = CAST((
                        SELECT td = client_net_address + '</td>'
                                        + '<td>' + [program_name] +
                                    '</td>'
                                        + '<td>' + [host_name] + '</td>'
                                        + '<td>' + login_name + '</td>'
                                        + '<td>' + Base + '</td>'
                                        + '<td>' + [connection count] +
                                    '</td>'

                        FROM (
                                 SELECT client_net_address,
                                        [program_name],
                                        [host_name],
                                        login_name,
                                        Base,
                                        cast([connection count] as varchar) [connection count],
                                        id
                                 FROM #ConexoesAbertas_Clear
                             ) AS D
                        ORDER BY id FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
                        )

                    -- Corrects Table Formatting
                    SET @ALERTConexaoSQLServerTable = REPLACE(
                            REPLACE(REPLACE(
                                            @ALERTConexaoSQLServerTable,
                                            '&lt;', '<'), '&gt;', '>'),
                            '<td>', '<td align = center>')

                    -- EMAIL Table Titles
                    SET @ALERTConexaoSQLServerTable =
                                '<table cellspacing="2" cellpadding="5" border="3">'
                                + '<tr>
							<th bgcolor=#0B0B61 width="50"><font color=white>IP</font></th>
							<th bgcolor=#0B0B61 width="50"><font color=white>Aplicacao</font></th>
							<th bgcolor=#0B0B61 width="50"><font color=white>Hostname</font></th>
							<th bgcolor=#0B0B61 width="50"><font color=white>Login</font></th>
							<th bgcolor=#0B0B61 width="50"><font color=white>Database</font></th>
							<th bgcolor=#0B0B61 width="10"><font color=white>Qtd. Conex�es</font></th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  @ALERTConexaoSQLServerTable,
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'

                    -----------------------------------------------------------
                    --	ALERT - HEADER
                    -----------------------------------------------------------
                    SET @ALERTConexaoProcessosExecHeader =
                            '<font color=black bold=true size=5>'
                    SET @ALERTConexaoProcessosExecHeader =
                                @ALERTConexaoProcessosExecHeader +
                                '<BR /> TOP 50 - Processos executando no Database <BR />'
                    SET @ALERTConexaoProcessosExecHeader =
                                @ALERTConexaoProcessosExecHeader +
                                '</font>'

                    -----------------------------------------------------------
                    --	ALERT - BODY
                    -----------------------------------------------------------
                    SET @ALERTConexaoProcessosExecTable = CAST((
                        SELECT td = [Duração] + '</td>'
                                        + '<td>' + [database_name] +
                                    '</td>'
                                        + '<td>' + [login_name] +
                                    '</td>'
                                        + '<td>' + [host_name] + '</td>'
                                        + '<td>' + [start_time] +
                                    '</td>'
                                        + '<td>' + [status] + '</td>'
                                        + '<td>' + [session_id] +
                                    '</td>'
                                        + '<td>' +
                                    [blocking_session_id] + '</td>'
                                        + '<td>' + [Wait] + '</td>'
                                        + '<td>' + [open_tran_count] +
                                    '</td>'
                                        + '<td>' + [CPU] + '</td>'
                                        + '<td>' + [reads] + '</td>'
                                        + '<td>' + [writes] + '</td>'
                                        + '<td>' + [sql_command] +
                                    '</td>'

                        FROM (
                                 -- EMAIL Table Details
                                 SELECT TOP 50 ISNULL([dd hh:mm:ss.mss], '-') AS [Duração],
                                               ISNULL([database_name], '-')   AS [database_name],
                                               ISNULL([login_name], '-')      AS [login_name],
                                               ISNULL([host_name], '-')       AS [host_name],
                                               ISNULL(
                                                       CONVERT(VARCHAR(20), [start_time], 120),
                                                       '-')                   AS [start_time],
                                               ISNULL([status], '-')          AS [status],
                                               ISNULL(
                                                       CAST([session_id] AS VARCHAR),
                                                       '-')                   AS [session_id],
                                               ISNULL(
                                                       CAST([blocking_session_id] AS VARCHAR),
                                                       '-')                   AS [blocking_session_id],
                                               ISNULL([wait_info], '-')       AS [Wait],
                                               ISNULL(
                                                       CAST([open_tran_count] AS VARCHAR),
                                                       '-')                   AS [open_tran_count],
                                               ISNULL([CPU], '-')             AS [CPU],
                                               ISNULL([reads], '-')           AS [reads],
                                               ISNULL([writes], '-')          AS [writes],
                                               ISNULL(
                                                       SUBSTRING([sql_command], 1, 300),
                                                       '-')                   AS [sql_command]
                                 FROM #Resultado_WhoisActive
                                 ORDER BY [start_time]
                             ) AS D
                        ORDER BY [start_time] FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
                        )

                    -- Corrects Table Formatting
                    SET @ALERTConexaoProcessosExecTable = REPLACE(
                            REPLACE(REPLACE(
                                            @ALERTConexaoProcessosExecTable,
                                            '&lt;', '<'), '&gt;', '>'),
                            '<td>', '<td align = center>')

                    -- EMAIL Table Titles
                    SET @ALERTConexaoProcessosExecTable =
                                '<table cellspacing="2" cellpadding="5" border="3">'
                                + '<tr>
							<th bgcolor=#0B0B61 width="140"><font color=white>[dd hh:mm:ss.mss]</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Database</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Login</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Host Name</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Hora In�cio</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Status</font></th>
							<th bgcolor=#0B0B61 width="30"><font color=white>ID Session</font></th>
							<th bgcolor=#0B0B61 width="60"><font color=white>ID Session Bloqueando</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Wait</font></th>
							<th bgcolor=#0B0B61 width="60"><font color=white>transactions Abertas</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>CPU</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Reads</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Writes</font></th>
							<th bgcolor=#0B0B61 width="300"><font color=white>Query</font></th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  @ALERTConexaoProcessosExecTable,
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'


                    -----------------------------------------------------------
                    -- Insert a blank page in EMAIL
                    -----------------------------------------------------------
                    SET @EmptyBodyEmail = ''
                    SET @EmptyBodyEmail =
                                '<table cellpadding="5" cellspacing="5" border="0">' +
                                '<tr>
							<th width="500">               </th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  ISNULL(@EmptyBodyEmail, ''),
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'

                    /**********************************************************
			--	Set EMAIL Variables
			**********************************************************/
                    SELECT @Importance = 'High',
                           @Subject = 'CLEAR: Existem ' +
                                      cast(@Qtd_Conexoes as varchar) +
                                      ' Conex�es Abertas no SQL Server no Servidor: ' +
                                      @@SERVERNAME,
                           @EmailBody = @ALERTConexaoSQLServerHeader +
                                        @EmptyBodyEmail +
                                        @ALERTConexaoSQLServerTable +
                                        @EmptyBodyEmail +
                                        @ALERTConexaoProcessosExecHeader +
                                        @EmptyBodyEmail +
                                        @ALERTConexaoProcessosExecTable +
                                        @EmptyBodyEmail

                    /**********************************************************
			--	ENVIA O EMAIL - CLEAR
			**********************************************************/
                    EXEC [msdb].[dbo].[sp_send_dbmail]
                         @profile_name = @ProfileEmail,
                         @recipients = @EmailDestination,
                         @subject = @Subject,
                         @body = @EmailBody,
                         @body_format = 'HTML',
                         @importance = @Importance

                    /**********************************************************
			-- Inserts a Record in the ALERT Control Table -> Fl_Tipo = 0 : CLEAR
			**********************************************************/
                    INSERT INTO [dbo].[ALERT] ([Id_ALERT_Parametro],
                                               [Ds_Mensagem],
                                               [Fl_Tipo])
                    SELECT @Id_ALERT_Parametro, @Subject, 0
                END
        END -- END - CLEAR
END

GO
IF (OBJECT_ID('[dbo].[stpALERT_Erro_Banco_Dados]') IS NOT NULL)
    DROP PROCEDURE [dbo].[stpALERT_Erro_Banco_Dados]
GO

/**********************************************************
--	ALERT: ERRO Database
**********************************************************/

CREATE PROCEDURE [dbo].[stpALERT_Erro_Banco_Dados]
AS
BEGIN
    SET NOCOUNT ON

    -- Declares the variables
    DECLARE
        @Subject                     VARCHAR(500), @Fl_Tipo TINYINT, @Importance AS VARCHAR(6), @EmailBody VARCHAR(MAX), @EmptyBodyEmail VARCHAR(MAX),
        @ALERTPaginaCorrompidaHeader VARCHAR(MAX), @ALERTPaginaCorrompidaTable VARCHAR(MAX), @EmailDestination VARCHAR(500),
        @ALERTStatusDatabasesHeader  VARCHAR(MAX), @ALERTStatusDatabasesTable VARCHAR(MAX), @ProfileEmail VARCHAR(200)

    -- P�gina Corrompida
    DECLARE
        @Id_ALERT_Parametro INT = (SELECT Id_ALERT_Parametro
                                   FROM [Traces].[dbo].ALERT_Parametro (NOLOCK)
                                   WHERE Nm_ALERT = 'P�gina Corrompida')

    /**********************************************************
	--	ALERT: PAGINA CORROMPIDA
	**********************************************************/
    IF (OBJECT_ID('temp..#temp_Corrupcao_Pagina') IS NOT NULL)
        DROP TABLE #temp_Corrupcao_Pagina

    SELECT SP.*
    INTO #temp_Corrupcao_Pagina
    FROM [msdb].[dbo].[suspect_pages] SP
             LEFT JOIN [dbo].[Historico_Suspect_Pages] HSP
                       ON SP.database_id = HSP.database_id AND
                          SP.file_id = HSP.file_id
                           AND SP.[page_id] = HSP.[page_id]
                           AND CAST(SP.last_update_date AS DATE) =
                               CAST(HSP.Dt_Corrupcao AS DATE)
    WHERE HSP.[page_id] IS NULL

    -----------------------------------------------------------
    -- Retrieves ALERT parameters
    -----------------------------------------------------------
    -- Status Database
    SELECT @Id_ALERT_Parametro = 8 -- SELECT * FROM [Traces].[dbo].ALERT_Parametro

    SELECT @EmailDestination = Ds_Email,
           @ProfileEmail = Ds_Profile_Email
    FROM [dbo].[ALERT_Parametro]
    WHERE [Id_ALERT_Parametro] = @Id_ALERT_Parametro
    -- P�gina Corrompida

    /**********************************************************
	-- Verifica se existe alguma P�gina Corrompida
	**********************************************************/
    IF EXISTS(SELECT TOP 1 page_id FROM #temp_Corrupcao_Pagina)
        BEGIN
            -- START - ALERT
            /**********************************************************
		--	CREATE EMAIL - ALERT
		**********************************************************/

            -----------------------------------------------------------
            --	ALERT - HEADER
            -----------------------------------------------------------
            SET @ALERTPaginaCorrompidaHeader =
                    '<font color=black bold=true size=5>'
            SET @ALERTPaginaCorrompidaHeader =
                        @ALERTPaginaCorrompidaHeader +
                        '<BR /> P�ginas Corrompidas <BR />'
            SET @ALERTPaginaCorrompidaHeader =
                    @ALERTPaginaCorrompidaHeader + '</font>'

            -----------------------------------------------------------
            --	ALERT - BODY
            -----------------------------------------------------------
            SET @ALERTPaginaCorrompidaTable = CAST((
                SELECT td = Nm_Database + '</td>'
                                + '<td>' + file_id + '</td>'
                                + '<td>' + page_id + '</td>'
                                + '<td>' + event_type + '</td>'
                                + '<td>' + error_count + '</td>'
                                + '<td>' + last_update_date + '</td>'

                FROM (
                         -- EMAIL Table Details
                         SELECT B.name                       AS Nm_Database,
                                CAST(file_id AS VARCHAR)     AS file_id,
                                CAST(page_id AS VARCHAR)     AS page_id,
                                CAST(event_type AS VARCHAR)  AS event_type,
                                CAST(error_count AS VARCHAR) AS error_count,
                                CONVERT(VARCHAR(20), last_update_date,
                                                     120)    AS last_update_date
                         FROM #temp_Corrupcao_Pagina A
                                  JOIN [sys].[databases] B
                                       ON B.[database_id] = A.[database_id]
                     ) AS D FOR XML PATH ('tr'), TYPE) AS VARCHAR(MAX)
                )

            -- Corrects Table Formatting
            SET @ALERTPaginaCorrompidaTable = REPLACE(
                    REPLACE(REPLACE(@ALERTPaginaCorrompidaTable,
                                    '&lt;', '<'), '&gt;', '>'), '<td>',
                    '<td align = center>')

            -- EMAIL Table Titles
            SET @ALERTPaginaCorrompidaTable =
                        '<table cellspacing="2" cellpadding="5" border="3">'
                        + '<tr>
						<th bgcolor=#0B0B61 width="300"><font color=white>Nome Database</font></th>
						<th bgcolor=#0B0B61 width="150"><font color=white>File_Id</font></th>
						<th bgcolor=#0B0B61 width="150"><font color=white>Page_Id</font></th>
						<th bgcolor=#0B0B61 width="150"><font color=white>Event_Type</font></th>
						<th bgcolor=#0B0B61 width="150"><font color=white>Error_Count</font></th>
						<th bgcolor=#0B0B61 width="180"><font color=white>Last_Update_Date</font></th>
					</tr>'
                        + REPLACE(REPLACE(@ALERTPaginaCorrompidaTable,
                                          '&lt;', '<'), '&gt;', '>')
                        + '</table>'

            -----------------------------------------------------------
            -- Insert a blank page in EMAIL
            -----------------------------------------------------------
            SET @EmptyBodyEmail = ''
            SET @EmptyBodyEmail =
                        '<table cellpadding="5" cellspacing="5" border="0">' +
                        '<tr>
						<th width="500">               </th>
					</tr>'
                        + REPLACE(REPLACE(ISNULL(@EmptyBodyEmail, ''),
                                          '&lt;', '<'), '&gt;', '>')
                        + '</table>'

            /**********************************************************
		--	Set EMAIL Variables
		**********************************************************/
            SELECT @Importance = 'High',
                   @Subject =
                   'ALERT: Existe alguma P�gina Corrompida no Database no Servidor: ' +
                   @@SERVERNAME,
                   @EmailBody =
                   @ALERTPaginaCorrompidaHeader + @EmptyBodyEmail +
                   @ALERTPaginaCorrompidaTable + @EmptyBodyEmail

            /**********************************************************
		--	SEND EMAIL - ALERT
		**********************************************************/
            EXEC [msdb].[dbo].[sp_send_dbmail]
                 @profile_name = @ProfileEmail,
                 @recipients = @EmailDestination,
                 @subject = @Subject,
                 @body = @EmailBody,
                 @body_format = 'HTML',
                 @importance = @Importance

            /**********************************************************
		    -- Inserts a Record in the ALERT Control Table
		    **********************************************************/
            INSERT INTO [dbo].[Historico_Suspect_Pages]
            SELECT [database_id],
                   [file_id],
                   [page_id],
                   [event_type],
                   [last_update_date]
            FROM #temp_Corrupcao_Pagina

            /**********************************************************
		    -- Inserts a Record in the ALERT Control Table -> Fl_Tipo = 1 : ALERT
		    **********************************************************/
            INSERT INTO [dbo].[ALERT] ([Id_ALERT_Parametro],
                                       [Ds_Mensagem], [Fl_Tipo])
            SELECT @Id_ALERT_Parametro, @Subject, 1
        END
    -- END - ALERT


    /**********************************************************
	--	ALERT: DATABASE INDISPONIVEL
	**********************************************************/
    -- Status Database
    SELECT @Id_ALERT_Parametro = (SELECT Id_ALERT_Parametro
                                  FROM [Traces].[dbo].ALERT_Parametro (NOLOCK)
                                  WHERE Nm_ALERT = 'Status Database')

    -----------------------------------------------------------
    -- Retrieves ALERT parameters
    -----------------------------------------------------------
    SELECT @EmailDestination = Ds_Email
    FROM [dbo].[ALERT_Parametro]
    WHERE [Id_ALERT_Parametro] = @Id_ALERT_Parametro
    -- Status Database

    -- Verifica o �ltimo Tipo do ALERT registrado -> 0: CLEAR / 1: ALERT
    SELECT @Fl_Tipo = [Fl_Tipo]
    FROM [dbo].[ALERT]
    WHERE [Id_ALERT] = (SELECT MAX(Id_ALERT)
                        FROM [dbo].[ALERT]
                        WHERE [Id_ALERT_Parametro] = @Id_ALERT_Parametro)

    /**********************************************************
	-- Verifica se alguma Database n�o est� ONLINE
	**********************************************************/
    IF EXISTS(
            SELECT NULL
            FROM [sys].[databases]
            WHERE [state_desc] NOT IN ('ONLINE', 'RESTORING')
        )
        BEGIN
            -- START - ALERT
            IF ISNULL(@Fl_Tipo, 0) = 0 -- Envia o ALERT apenas uma vez
                BEGIN
                    /**********************************************************
			        --	CREATE EMAIL - ALERT
			        **********************************************************/

                    -----------------------------------------------------------
                    --	ALERT - HEADER
                    -----------------------------------------------------------
                    SET @ALERTStatusDatabasesHeader =
                            '<font color=black bold=true size=5>'
                    SET @ALERTStatusDatabasesHeader =
                                @ALERTStatusDatabasesHeader +
                                '<BR /> Status das Databases <BR />'
                    SET @ALERTStatusDatabasesHeader =
                            @ALERTStatusDatabasesHeader + '</font>'

                    -----------------------------------------------------------
                    --	ALERT - BODY
                    -----------------------------------------------------------
                    SET @ALERTStatusDatabasesTable = CAST((
                        SELECT td = [name] + '</td>'
                                        + '<td>' + [state_desc] +
                                    '</td>'

                        FROM (
                                 -- EMAIL Table Details
                                 SELECT [name], [state_desc]
                                 FROM [sys].[databases]
                                 WHERE [state_desc] NOT IN ('ONLINE', 'RESTORING')
                             ) AS D
                        ORDER BY [name] FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
                        )

                    -- Corrects Table Formatting
                    SET @ALERTStatusDatabasesTable = REPLACE(
                            REPLACE(REPLACE(@ALERTStatusDatabasesTable,
                                            '&lt;', '<'), '&gt;', '>'),
                            '<td>', '<td align = center>')

                    -- EMAIL Table Titles
                    SET @ALERTStatusDatabasesTable =
                                '<table cellspacing="2" cellpadding="5" border="3">'
                                + '<tr>
							<th bgcolor=#0B0B61 width="200"><font color=white>Database</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Status</font></th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  @ALERTStatusDatabasesTable,
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'

                    -----------------------------------------------------------
                    -- Insert a blank page in EMAIL
                    -----------------------------------------------------------
                    SET @EmptyBodyEmail = ''
                    SET @EmptyBodyEmail =
                                '<table cellpadding="5" cellspacing="5" border="0">' +
                                '<tr>
							<th width="500">               </th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  ISNULL(@EmptyBodyEmail, ''),
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'

                    /**********************************************************
			        --	Set EMAIL Variables
			        **********************************************************/
                    SELECT @Importance = 'High',
                           @Subject =
                           'ALERT: Existe alguma Database que n�o est� ONLINE no Servidor: ' +
                           @@SERVERNAME,
                           @EmailBody = @ALERTStatusDatabasesHeader +
                                        @EmptyBodyEmail +
                                        @ALERTStatusDatabasesTable +
                                        @EmptyBodyEmail

                    /**********************************************************
			        --	SEND EMAIL - ALERT
			        **********************************************************/
                    EXEC [msdb].[dbo].[sp_send_dbmail]
                         @profile_name = @ProfileEmail,
                         @recipients = @EmailDestination,
                         @subject = @Subject,
                         @body = @EmailBody,
                         @body_format = 'HTML',
                         @importance = @Importance

                    /**********************************************************
			        -- Inserts a Record in the ALERT Control Table -> Fl_Tipo = 1 : ALERT
			        **********************************************************/
                    INSERT INTO [dbo].[ALERT] ([Id_ALERT_Parametro],
                                               [Ds_Mensagem],
                                               [Fl_Tipo])
                    SELECT @Id_ALERT_Parametro, @Subject, 1
                END
        END -- END - ALERT
    ELSE
        BEGIN
            -- START - CLEAR
            IF ISNULL(@Fl_Tipo, 0) = 1
                BEGIN
                    /**********************************************************
			        --	CREATE EMAIL - CLEAR
			        **********************************************************/

                    -----------------------------------------------------------
                    --	CLEAR - HEADER
                    -----------------------------------------------------------
                    SET @ALERTStatusDatabasesHeader =
                            '<font color=black bold=true size=5>'
                    SET @ALERTStatusDatabasesHeader =
                                @ALERTStatusDatabasesHeader +
                                '<BR /> Status das Databases <BR />'
                    SET @ALERTStatusDatabasesHeader =
                            @ALERTStatusDatabasesHeader + '</font>'

                    -----------------------------------------------------------
                    --	CLEAR - BODY
                    -----------------------------------------------------------
                    SET @ALERTStatusDatabasesTable = CAST((
                        SELECT td = [name] + '</td>'
                                        + '<td>' + [state_desc] +
                                    '</td>'

                        FROM (
                                 -- EMAIL Table Details
                                 SELECT [name], [state_desc]
                                 FROM [sys].[databases]
                             ) AS D
                        ORDER BY [name] FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
                        )

                    -- Corrects Table Formatting
                    SET @ALERTStatusDatabasesTable = REPLACE(
                            REPLACE(REPLACE(@ALERTStatusDatabasesTable,
                                            '&lt;', '<'), '&gt;', '>'),
                            '<td>', '<td align = center>')

                    -- EMAIL Table Titles
                    SET @ALERTStatusDatabasesTable =
                                '<table cellspacing="2" cellpadding="5" border="3">'
                                + '<tr>
							<th bgcolor=#0B0B61 width="200"><font color=white>Database</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Status</font></th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  @ALERTStatusDatabasesTable,
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'

                    -----------------------------------------------------------
                    -- Insert a blank page in EMAIL
                    -----------------------------------------------------------
                    SET @EmptyBodyEmail = ''
                    SET @EmptyBodyEmail =
                                '<table cellpadding="5" cellspacing="5" border="0">' +
                                '<tr>
							<th width="500">               </th>
						</tr>'
                                + REPLACE(REPLACE(
                                                  ISNULL(@EmptyBodyEmail, ''),
                                                  '&lt;', '<'), '&gt;',
                                          '>')
                                + '</table>'

                    /**********************************************************
			        --	Set EMAIL Variables
			        **********************************************************/
                    SELECT @Importance = 'High',
                           @Subject =
                           'CLEAR: N�o existe mais alguma Database que n�o est� ONLINE no Servidor: ' +
                           @@SERVERNAME,
                           @EmailBody = @ALERTStatusDatabasesHeader +
                                        @EmptyBodyEmail +
                                        @ALERTStatusDatabasesTable +
                                        @EmptyBodyEmail

                    /**********************************************************
			        --	ENVIA O EMAIL - CLEAR
			        **********************************************************/
                    EXEC [msdb].[dbo].[sp_send_dbmail]
                         @profile_name = @ProfileEmail,
                         @recipients = @EmailDestination,
                         @subject = @Subject,
                         @body = @EmailBody,
                         @body_format = 'HTML',
                         @importance = @Importance

                    /**********************************************************
			        -- Inserts a Record in the ALERT Control Table -> Fl_Tipo = 0 : CLEAR
			        **********************************************************/
                    INSERT INTO [dbo].[ALERT] ([Id_ALERT_Parametro],
                                               [Ds_Mensagem],
                                               [Fl_Tipo])
                    SELECT @Id_ALERT_Parametro, @Subject, 0
                END
        END -- END - CLEAR
END


GO
IF (OBJECT_ID('[dbo].[stpALERT_Queries_Delayed]') IS NOT NULL)
    DROP PROCEDURE [dbo].[stpALERT_Queries_Delayed]
GO

/**********************************************************
--	ALERT: QUERIES Delayed
**********************************************************/

CREATE PROCEDURE [dbo].[stpALERT_Queries_Delayed]
AS
BEGIN
    SET NOCOUNT ON

    -- Queries Delayed
    DECLARE
        @Id_ALERT_Parametro INT = (SELECT Id_ALERT_Parametro
                                   FROM [Traces].[dbo].ALERT_Parametro (NOLOCK)
                                   WHERE Nm_ALERT = 'Queries Delayed')

    -----------------------------------------------------------
    -- Retrieves ALERT parameters
    -----------------------------------------------------------
    DECLARE
        @Queries_Delayed_Parametro INT, @EmailDestination VARCHAR(500), @ProfileEmail VARCHAR(200)

    SELECT @Queries_Delayed_Parametro = Vl_Parametro, -- Quantidade
           @EmailDestination = Ds_Email,
           @ProfileEmail = Ds_Profile_Email
    FROM [dbo].[ALERT_Parametro]
    WHERE [Id_ALERT_Parametro] = @Id_ALERT_Parametro
    -- Queries Delayed

    -- Create a tabela com as queries Delayed
    IF (OBJECT_ID('tempdb..#Queries_Delayed_Temp') IS NOT NULL)
        DROP TABLE #Queries_Delayed_Temp

    SELECT [StartTime],
           [DataBaseName],
           [Duration],
           [Reads],
           [Writes],
           [CPU],
           [TextData]
    INTO #Queries_Delayed_Temp
    FROM [dbo].[Traces]
    WHERE [StartTime] >= DATEADD(mi, -5, GETDATE())
    ORDER BY [Duration] DESC

    -- Declara a variavel e retorna a quantidade de Queries Lentas
    DECLARE
        @Quantidade_Queries_Delayed INT = (SELECT COUNT(*)
                                           FROM #Queries_Delayed_Temp)

    /**********************************************************
	--	Verifica se existem mais de 100 Queries Lentas nos �ltimos 5 minutos
	**********************************************************/
    IF (@Quantidade_Queries_Delayed > @Queries_Delayed_Parametro)
        BEGIN
            -- Create a tabela que ira armazenar os dados dos processos
            IF (OBJECT_ID('tempdb..#Resultado_WhoisActive') IS NOT NULL)
                DROP TABLE #Resultado_WhoisActive

            CREATE TABLE #Resultado_WhoisActive
            (
                [dd hh:mm:ss.mss]     VARCHAR(20),
                [database_name]       NVARCHAR(128),
                [login_name]          NVARCHAR(128),
                [host_name]           NVARCHAR(128),
                [start_time]          DATETIME,
                [status]              VARCHAR(30),
                [session_id]          INT,
                [blocking_session_id] INT,
                [wait_info]           VARCHAR(MAX),
                [open_tran_count]     INT,
                [CPU]                 VARCHAR(MAX),
                [reads]               VARCHAR(MAX),
                [writes]              VARCHAR(MAX),
                [sql_command]         XML
            )

            -----------------------------------------------------------
            --	ALERT - DADOS - WHOISACTIVE
            -----------------------------------------------------------
            -- Retorna todos os processos que est�o sendo executados no momento
            EXEC [dbo].[sp_whoisactive]
                 @get_outer_command = 1,
                 @output_column_list = '[dd hh:mm:ss.mss][database_name][login_name][host_name][start_time][status][session_id][blocking_session_id][wait_info][open_tran_count][CPU][reads][writes][sql_command]',
                 @destination_table = '#Resultado_WhoisActive'

            -- Altera a coluna que possui o comando SQL
            ALTER TABLE #Resultado_WhoisActive
                ALTER COLUMN [sql_command] VARCHAR(MAX)

            UPDATE #Resultado_WhoisActive
            SET [sql_command] = REPLACE(REPLACE(REPLACE(REPLACE(
                                                                CAST([sql_command] AS VARCHAR(1000)),
                                                                '<?query --',
                                                                ''),
                                                        '--?>', ''),
                                                '&gt;', '>'), '&lt;',
                                        '')

            -- select * from #Resultado_WhoisActive

            -- Verifica se n�o existe nenhum processo em Execu��o
            IF NOT EXISTS(SELECT TOP 1 * FROM #Resultado_WhoisActive)
                BEGIN
                    INSERT INTO #Resultado_WhoisActive
                    SELECT NULL,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           NULL,
                           NULL
                END

            /**********************************************************
		--	CREATE EMAIL - ALERT
		**********************************************************/
            -- Declares the variables
            DECLARE
                @Importance AS              VARCHAR(6), @EmailBody VARCHAR(MAX), @ALERTQueriesLentasHeader VARCHAR(MAX),
                @ResultadoWhoisactiveHeader VARCHAR(MAX), @ResultadoWhoisactiveTable VARCHAR(MAX),
                @ALERTQueriesLentasTable    VARCHAR(MAX), @EmptyBodyEmail VARCHAR(MAX), @Subject VARCHAR(500)

            -----------------------------------------------------------
            --	ALERT - HEADER - WHOISACTIVE
            -----------------------------------------------------------
            SET @ResultadoWhoisactiveHeader =
                    '<font color=black bold=true size=5>'
            SET @ResultadoWhoisactiveHeader =
                        @ResultadoWhoisactiveHeader +
                        '<BR /> TOP 50 - Processos executando no Database <BR />'
            SET @ResultadoWhoisactiveHeader =
                    @ResultadoWhoisactiveHeader + '</font>'

            -----------------------------------------------------------
            --	ALERT - BODY - WHOISACTIVE
            -----------------------------------------------------------
            SET @ResultadoWhoisactiveTable = CAST((
                SELECT td = [Duração] + '</td>'
                                + '<td>' + [database_name] + '</td>'
                                + '<td>' + [login_name] + '</td>'
                                + '<td>' + [host_name] + '</td>'
                                + '<td>' + [start_time] + '</td>'
                                + '<td>' + [status] + '</td>'
                                + '<td>' + [session_id] + '</td>'
                                + '<td>' + [blocking_session_id] +
                            '</td>'
                                + '<td>' + [Wait] + '</td>'
                                + '<td>' + [open_tran_count] + '</td>'
                                + '<td>' + [CPU] + '</td>'
                                + '<td>' + [reads] + '</td>'
                                + '<td>' + [writes] + '</td>'
                                + '<td>' + [sql_command] + '</td>'

                FROM (
                         -- EMAIL Table Details
                         SELECT TOP 50 ISNULL([dd hh:mm:ss.mss], '-') AS [Duração],
                                       ISNULL([database_name], '-')   AS [database_name],
                                       ISNULL([login_name], '-')      AS [login_name],
                                       ISNULL([host_name], '-')       AS [host_name],
                                       ISNULL(
                                               CONVERT(VARCHAR(20), [start_time], 120),
                                               '-')                   AS [start_time],
                                       ISNULL([status], '-')          AS [status],
                                       ISNULL(
                                               CAST([session_id] AS VARCHAR),
                                               '-')                   AS [session_id],
                                       ISNULL(
                                               CAST([blocking_session_id] AS VARCHAR),
                                               '-')                   AS [blocking_session_id],
                                       ISNULL([wait_info], '-')       AS [Wait],
                                       ISNULL(
                                               CAST([open_tran_count] AS VARCHAR),
                                               '-')                   AS [open_tran_count],
                                       ISNULL([CPU], '-')             AS [CPU],
                                       ISNULL([reads], '-')           AS [reads],
                                       ISNULL([writes], '-')          AS [writes],
                                       ISNULL(
                                               SUBSTRING([sql_command], 1, 300),
                                               '-')                   AS [sql_command]
                         FROM #Resultado_WhoisActive
                     ) AS D
                ORDER BY [start_time] FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
                )

            -- Corrects Table Formatting
            SET @ResultadoWhoisactiveTable = REPLACE(
                    REPLACE(REPLACE(@ResultadoWhoisactiveTable, '&lt;',
                                    '<'), '&gt;', '>'), '<td>',
                    '<td align = center>')

            -- EMAIL Table Titles
            SET @ResultadoWhoisactiveTable =
                        '<table cellspacing="2" cellpadding="5" border="3">'
                        + '<tr>
							<th bgcolor=#0B0B61 width="140"><font color=white>[dd hh:mm:ss.mss]</font></th>
							<th bgcolor=#0B0B61 width="100"><font color=white>Database</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Login</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Host Name</font></th>
							<th bgcolor=#0B0B61 width="200"><font color=white>Hora In�cio</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Status</font></th>
							<th bgcolor=#0B0B61 width="30"><font color=white>ID Session</font></th>
							<th bgcolor=#0B0B61 width="60"><font color=white>ID Session Bloqueando</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Wait</font></th>
							<th bgcolor=#0B0B61 width="60"><font color=white>transactions Abertas</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>CPU</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Reads</font></th>
							<th bgcolor=#0B0B61 width="120"><font color=white>Writes</font></th>
							<th bgcolor=#0B0B61 width="1000"><font color=white>Query</font></th>
						</tr>'
                        + REPLACE(REPLACE(@ResultadoWhoisactiveTable,
                                          '&lt;', '<'), '&gt;', '>')
                        + '</table>'

            -----------------------------------------------------------
            --	ALERT - HEADER
            -----------------------------------------------------------
            SET @ALERTQueriesLentasHeader =
                    '<font color=black bold=true size=5>'
            SET @ALERTQueriesLentasHeader =
                        @ALERTQueriesLentasHeader +
                        '<BR /> TOP 50 - Queries Delayed <BR />'
            SET @ALERTQueriesLentasHeader =
                    @ALERTQueriesLentasHeader + '</font>'

            -----------------------------------------------------------
            --	ALERT - BODY
            -----------------------------------------------------------
            SET @ALERTQueriesLentasTable = CAST((
                SELECT td = [StartTime] + '</td>'
                                + '<td>' + [DataBaseName] + '</td>'
                                + '<td>' + [Duration] + '</td>'
                                + '<td>' + [Reads] + '</td>'
                                + '<td>' + [Writes] + '</td>'
                                + '<td>' + [CPU] + '</td>'
                                + '<td>' + [TextData] + '</td>'
                FROM (
                         -- EMAIL Table Details
                         SELECT TOP 50 CONVERT(VARCHAR(20), [StartTime], 120) AS [StartTime],
                                       [DataBaseName],
                                       CAST([Duration] AS VARCHAR)            AS [Duration],
                                       CAST([Reads] AS VARCHAR)               AS [Reads],
                                       CAST([Writes] AS VARCHAR)              AS [Writes],
                                       CAST([CPU] AS VARCHAR)                 AS [CPU],
                                       SUBSTRING([TextData], 1, 150)          AS [TextData]
                         FROM #Queries_Delayed_Temp
                         ORDER BY [Duration] DESC
                     ) AS D
                ORDER BY CAST([Duration] AS NUMERIC(15, 2)) DESC FOR XML PATH ( 'tr' )) AS VARCHAR(MAX)
                )

            -- Corrects Table Formatting
            SET @ALERTQueriesLentasTable = REPLACE(
                    REPLACE(REPLACE(@ALERTQueriesLentasTable, '&lt;',
                                    '<'), '&gt;', '>'), '<td>',
                    '<td align = center>')

            -- EMAIL Table Titles
            SET @ALERTQueriesLentasTable =
                        '<table cellspacing="2" cellpadding="5" border="3">'
                        + '<tr>
						<th bgcolor=#0B0B61 width="100"><font color=white>StartTime</font></th>
						<th bgcolor=#0B0B61 width="50"><font color=white>DataBaseName</font></th>
						<th bgcolor=#0B0B61 width="70"><font color=white>Duration</font></th>
						<th bgcolor=#0B0B61 width="70"><font color=white>Reads</font></th>
						<th bgcolor=#0B0B61 width="70"><font color=white>Writes</font></th>
						<th bgcolor=#0B0B61 width="70"><font color=white>CPU</font></th>
						<th bgcolor=#0B0B61 width="300"><font color=white>TextData (150 caracteres iniciais)</font></th>
					</tr>'
                        + REPLACE(REPLACE(@ALERTQueriesLentasTable,
                                          '&lt;', '<'), '&gt;', '>')
                        + '</table>'

            -----------------------------------------------------------
            -- Insert a blank page in EMAIL
            -----------------------------------------------------------
            SET @EmptyBodyEmail = ''
            SET @EmptyBodyEmail =
                        '<table cellpadding="5" cellspacing="5" border="0">' +
                        '<tr>
						<th width="500">               </th>
					</tr>'
                        + REPLACE(REPLACE(ISNULL(@EmptyBodyEmail, ''),
                                          '&lt;', '<'), '&gt;', '>')
                        + '</table>'

            -----------------------------------------------------------
            -- Insert a blank page in EMAIL
            -----------------------------------------------------------
            SET @EmptyBodyEmail = ''
            SET @EmptyBodyEmail =
                        '<table cellpadding="5" cellspacing="5" border="0">' +
                        '<tr>
						<th width="500">               </th>
					</tr>'
                        + REPLACE(REPLACE(ISNULL(@EmptyBodyEmail, ''),
                                          '&lt;', '<'), '&gt;', '>')
                        + '</table>'

            /**********************************************************
		--	SEND EMAIL - ALERT
		**********************************************************/
            SELECT @Importance = 'High',
                   @Subject = 'ALERT: Existem ' + CAST(
                           @Quantidade_Queries_Delayed AS VARCHAR) +
                              ' queries Delayed nos �ltimos 5 minutos no Servidor: ' +
                              @@SERVERNAME,
                   @EmailBody =
                   @ResultadoWhoisactiveHeader + @EmptyBodyEmail +
                   @ResultadoWhoisactiveTable + @EmptyBodyEmail +
                   @ALERTQueriesLentasHeader + @EmptyBodyEmail +
                   @ALERTQueriesLentasTable + @EmptyBodyEmail


            EXEC [msdb].[dbo].[sp_send_dbmail]
                 @profile_name = @ProfileEmail,
                 @recipients = @EmailDestination,
                 @subject = @Subject,
                 @body = @EmailBody,
                 @body_format = 'HTML',
                 @importance = @Importance
        END
END

GO
IF (OBJECT_ID('[dbo].[stpALERT_Job_Falha]') IS NOT NULL)
    DROP PROCEDURE [dbo].[stpALERT_Job_Falha]
GO

/**********************************************************
--	ALERT: JOB FALHA
**********************************************************/

CREATE PROCEDURE [dbo].[stpALERT_Job_Falha]
AS
BEGIN
    SET NOCOUNT ON

    IF (OBJECT_ID('tempdb..#Result_History_Jobs') IS NOT NULL)
        DROP TABLE #Result_History_Jobs

    CREATE TABLE #Result_History_Jobs
    (
        [Cod]               INT IDENTITY (1,1),
        [Instance_Id]       INT,
        [Job_Id]            VARCHAR(255),
        [Job_Name]          VARCHAR(255),
        [Step_Id]           INT,
        [Step_Name]         VARCHAR(255),
        [SQl_Message_Id]    INT,
        [Sql_Severity]      INT,
        [SQl_Message]       VARCHAR(4490),
        [Run_Status]        INT,
        [Run_Date]          VARCHAR(20),
        [Run_Time]          VARCHAR(20),
        [Run_Duration]      INT,
        [Operator_Emailed]  VARCHAR(100),
        [Operator_NetSent]  VARCHAR(100),
        [Operator_Paged]    VARCHAR(100),
        [Retries_Attempted] INT,
        [Nm_Server]         VARCHAR(100)
    )

    -----------------------------------------------------------
    -- Retrieves ALERT parameters
    -----------------------------------------------------------
    DECLARE
        @JobFailed_Parametro INT, @EmailDestination VARCHAR(500), @ProfileEmail VARCHAR(200)

    -- Job Falha
    DECLARE
        @Id_ALERT_Parametro INT = (SELECT Id_ALERT_Parametro
                                   FROM [Traces].[dbo].ALERT_Parametro (NOLOCK)
                                   WHERE Nm_ALERT = 'Job Falha')

    SELECT @JobFailed_Parametro = Vl_Parametro, -- Horas
           @EmailDestination = Ds_Email,
           @ProfileEmail = Ds_Profile_Email
    FROM [dbo].[ALERT_Parametro]
    WHERE [Id_ALERT_Parametro] = @Id_ALERT_Parametro
    -- Jobs Failed

    -- Declares the variables
    DECLARE
        @Dt_Inicial VARCHAR(8), @Dt_Referencia DATETIME

    SELECT @Dt_Referencia = GETDATE()

    SELECT @Dt_Inicial = CONVERT(VARCHAR(8),
            (DATEADD(HOUR, -@JobFailed_Parametro, @Dt_Referencia)), 112)

    INSERT INTO #Result_History_Jobs
        EXEC [msdb].[dbo].[sp_help_jobhistory]
             @mode = 'FULL',
             @start_run_date = @Dt_Inicial

    -- Busca os dados dos JOBS que Falharam
    IF (OBJECT_ID('tempdb..#ALERT_Job_Falharam') IS NOT NULL)
        DROP TABLE #ALERT_Job_Falharam

    SELECT TOP 50 [Nm_Server]                          AS [Server],
                  [Job_Name],
                  CASE
                      WHEN [Run_Status] = 0 THEN 'Failed'
                      WHEN [Run_Status] = 1 THEN 'Succeeded'
                      WHEN [Run_Status] = 2 THEN 'Retry (step only)'
                      WHEN [Run_Status] = 3 THEN 'Cancelled'
                      WHEN [Run_Status] = 4 THEN 'In-progress message'
                      WHEN [Run_Status] = 5 THEN 'Unknown'
                      END                              AS [Status],
                  CAST([Run_Date] + ' ' +
                       RIGHT('00' + SUBSTRING([Run_Time],
                                              (LEN([Run_Time]) - 5), 2),
                             2) + ':' +
                       RIGHT('00' + SUBSTRING([Run_Time],
                                              (LEN([Run_Time]) - 3), 2),
                             2) + ':' +
                       RIGHT('00' + SUBSTRING([Run_Time],
                                              (LEN([Run_Time]) - 1), 2),
                             2) AS VARCHAR
                      )                                AS [Dt_Execucao],
                  RIGHT('00' +
                        SUBSTRING(CAST([Run_Duration] AS VARCHAR),
                                  (LEN([Run_Duration]) - 5), 2), 2) +
                  ':' +
                  RIGHT('00' +
                        SUBSTRING(CAST([Run_Duration] AS VARCHAR),
                                  (LEN([Run_Duration]) - 3), 2), 2) +
                  ':' +
                  RIGHT('00' +
                        SUBSTRING(CAST([Run_Duration] AS VARCHAR),
                                  (LEN([Run_Duration]) - 1), 2),
                        2)                             AS [Run_Duration],
                  CAST([SQl_Message] AS VARCHAR(3990)) AS [SQL_Message]
    INTO #ALERT_Job_Falharam
    FROM #Result_History_Jobs
    WHERE
      -- [Step_Id] = 0 AND condi��o para o retry
        [Run_Status] <> 1
      AND CAST(
                      [Run_Date] + ' ' + RIGHT(
                              '00' + SUBSTRING([Run_Time],
                                               (LEN([Run_Time]) - 5),
                                               2), 2) + ':' +
                      RIGHT('00' + SUBSTRING([Run_Time],
                                             (LEN([Run_Time]) - 3), 2),
                            2) + ':' +
                      RIGHT('00' + SUBSTRING([Run_Time],
                                             (LEN([Run_Time]) - 1), 2),
                            2) AS DATETIME
              ) >= DATEADD(HOUR, -@JobFailed_Parametro, @Dt_Referencia)
      AND CAST([Run_Date] + ' ' + RIGHT(
                '00' + SUBSTRING([Run_Time], (LEN([Run_Time]) - 5), 2),
                2) + ':' +
               RIGHT('00' + SUBSTRING([Run_Time], (LEN([Run_Time]) - 3),
                                      2), 2) + ':' +
               RIGHT('00' + SUBSTRING([Run_Time], (LEN([Run_Time]) - 1),
                                      2), 2) AS DATETIME
              ) < @Dt_Referencia
    ORDER BY [Dt_Execucao] DESC

    /**********************************************************
	--	Verifica se algum JOB Falhou
	**********************************************************/
    IF EXISTS(SELECT * FROM #ALERT_Job_Falharam)
        BEGIN
            /**********************************************************
		--	CREATE EMAIL - ALERT
		**********************************************************/
            -- Declares the variables
            DECLARE
                @EmailBody      VARCHAR(MAX), @ALERTLogHeader VARCHAR(MAX), @ALERTJobFailed VARCHAR(MAX),
                @EmptyBodyEmail VARCHAR(MAX), @Importance AS VARCHAR(6), @Subject VARCHAR(500)

            -----------------------------------------------------------
            --	ALERT - HEADER
            -----------------------------------------------------------
            SET @ALERTLogHeader = '<font color=black bold=true size=5>'
            SET @ALERTLogHeader = @ALERTLogHeader +
                                  '<BR /> TOP 50 - Jobs que Falharam nas �ltimas ' +
                                  CAST((@JobFailed_Parametro) AS VARCHAR) +
                                  ' Horas <BR />'
            SET @ALERTLogHeader = @ALERTLogHeader + '</font>'

            -----------------------------------------------------------
            --	ALERT - BODY
            -----------------------------------------------------------
            SET @ALERTJobFailed = CAST((
                SELECT td = [Job_Name] + '</td>'
                                + '<td>' + [Status] + '</td>'
                                + '<td>' + [Dt_Execucao] + '</td>'
                                + '<td>' + [Run_Duration] + '</td>'
                                + '<td>' + [SQL_Message] + '</td>'
                FROM (
                         -- EMAIL Table Details
                         SELECT [Job_Name],
                                [Status],
                                [Dt_Execucao],
                                [Run_Duration],
                                [SQL_Message]
                         FROM #ALERT_Job_Falharam
                     ) AS D
                ORDER BY [Dt_Execucao] DESC FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
                )

            -- Corrects Table Formatting
            SET @ALERTJobFailed = REPLACE(
                    REPLACE(REPLACE(@ALERTJobFailed, '&lt;', '<'),
                            '&gt;', '>'), '<td>', '<td align = center>')

            -- EMAIL Table Titles
            SET @ALERTJobFailed =
                        '<table cellspacing="2" cellpadding="5" border="3">'
                        + '<tr>
						<th bgcolor=#0B0B61 width="300"><font color=white>Nome do JOB</font></th>
						<th bgcolor=#0B0B61 width="150"><font color=white>Status</font></th>
						<th bgcolor=#0B0B61 width="150"><font color=white>Data da Execu��o</font></th>
						<th bgcolor=#0B0B61 width="100"><font color=white>Duração</font></th>
						<th bgcolor=#0B0B61 width="400"><font color=white>Mensagem Erro</font></th>
					</tr>'
                        +
                        REPLACE(REPLACE(@ALERTJobFailed, '&lt;', '<'),
                                '&gt;', '>')
                        + '</table>'

            -----------------------------------------------------------
            -- Insert a blank page in EMAIL
            -----------------------------------------------------------
            SET @EmptyBodyEmail = ''
            SET @EmptyBodyEmail =
                        '<table cellpadding="5" cellspacing="5" border="0">' +
                        '<tr>
						<th width="500">               </th>
					</tr>'
                        + REPLACE(REPLACE(ISNULL(@EmptyBodyEmail, ''),
                                          '&lt;', '<'), '&gt;', '>')
                        + '</table>'

            /**********************************************************
		--	Set EMAIL Variables
		**********************************************************/
            SELECT @Importance = 'High',
                   @Subject = 'ALERT: Jobs que Falharam nas �ltimas ' +
                              CAST((@JobFailed_Parametro) AS VARCHAR) +
                              ' Horas no Servidor: ' + @@SERVERNAME,
                   @EmailBody = @ALERTLogHeader + @EmptyBodyEmail +
                                @ALERTJobFailed + @EmptyBodyEmail


            /**********************************************************
		--	SEND EMAIL - ALERT
		**********************************************************/
            EXEC [msdb].[dbo].[sp_send_dbmail]
                 @profile_name = @ProfileEmail,
                 @recipients = @EmailDestination,
                 @subject = @Subject,
                 @body = @EmailBody,
                 @body_format = 'HTML',
                 @importance = @Importance

            /**********************************************************
		-- Inserts a Record in the ALERT Control Table -> Fl_Tipo = 1 : ALERT
		**********************************************************/
            INSERT INTO [dbo].[ALERT] ([Id_ALERT_Parametro],
                                       [Ds_Mensagem], [Fl_Tipo])
            SELECT @Id_ALERT_Parametro, @Subject, 1
        END
END

GO
IF (OBJECT_ID('[dbo].[stpALERT_SQL_Server_Reiniciado]') IS NOT NULL)
    DROP PROCEDURE [dbo].[stpALERT_SQL_Server_Reiniciado]
GO

/**********************************************************
--	ALERT: SQL Server Reiniciado
**********************************************************/

CREATE PROCEDURE [dbo].[stpALERT_SQL_Server_Reiniciado]
AS
BEGIN
    SET NOCOUNT ON

    -- SQL Server Reiniciado
    DECLARE
        @Id_ALERT_Parametro INT = (SELECT Id_ALERT_Parametro
                                   FROM [Traces].[dbo].ALERT_Parametro (NOLOCK)
                                   WHERE Nm_ALERT = 'SQL Server Reiniciado')

    -----------------------------------------------------------
    -- Retrieves ALERT parameters
    -----------------------------------------------------------
    DECLARE
        @SQL_Reiniciado_Parametro INT, @EmailDestination VARCHAR(500), @ProfileEmail VARCHAR(200)

    SELECT @SQL_Reiniciado_Parametro = Vl_Parametro, -- Minutos
           @EmailDestination = Ds_Email,
           @ProfileEmail = Ds_Profile_Email
    FROM [dbo].[ALERT_Parametro]
    WHERE [Id_ALERT_Parametro] = @Id_ALERT_Parametro
    -- SQL Server Reiniciado

    -- Verifica se o SQL Server foi Reiniciado
    IF (OBJECT_ID('tempdb..#ALERT_SQL_Reiniciado') IS NOT NULL)
        DROP TABLE #ALERT_SQL_Reiniciado

    SELECT [create_date]
    INTO #ALERT_SQL_Reiniciado
    FROM [sys].[databases] WITH (NOLOCK)
    WHERE [database_id] = 2 -- Verifica a Database "TempDb"
      AND [create_date] >=
          DATEADD(MINUTE, -@SQL_Reiniciado_Parametro, GETDATE())

    /**********************************************************
	--	Verifica se o SQL foi Reiniciado
	**********************************************************/
    IF EXISTS(SELECT * FROM #ALERT_SQL_Reiniciado)
        BEGIN
            /**********************************************************
		--	CREATE EMAIL - ALERT
		**********************************************************/
            -- Declares the variables
            DECLARE
                @EmailBody      VARCHAR(MAX), @ALERTLogHeader VARCHAR(MAX), @ALERTSQLReiniciado VARCHAR(MAX),
                @EmptyBodyEmail VARCHAR(MAX), @Importance AS VARCHAR(6), @Subject VARCHAR(500)

            -----------------------------------------------------------
            --	ALERT - HEADER
            -----------------------------------------------------------
            SET @ALERTLogHeader =
                    '<font color=black bold=true size= 5>'
            SET @ALERTLogHeader = @ALERTLogHeader +
                                  '<BR /> SQL Server Reiniciado nos �ltimos ' +
                                  CAST(
                                          (@SQL_Reiniciado_Parametro) AS VARCHAR) +
                                  ' Minutos <BR />'
            SET @ALERTLogHeader = @ALERTLogHeader + '</font>'

            -----------------------------------------------------------
            --	ALERT - BODY
            -----------------------------------------------------------
            SET @ALERTSQLReiniciado = CAST((
                SELECT td = [Create_Date] + '</td>'
                FROM (
                         -- EMAIL Table Details
                         SELECT CONVERT(VARCHAR(20), [create_date],
                                                     120) AS [Create_Date]
                         FROM #ALERT_SQL_Reiniciado
                     ) AS D FOR XML PATH ('tr'), TYPE) AS VARCHAR(MAX)
                )

            -- Corrects Table Formatting
            SET @ALERTSQLReiniciado = REPLACE(
                    REPLACE(REPLACE(@ALERTSQLReiniciado, '&lt;', '<'),
                            '&gt;', '>'), '<td>', '<td align = center>')

            -- EMAIL Table Titles
            SET @ALERTSQLReiniciado =
                        '<table cellspacing="2" cellpadding="5" border="3">'
                        + '<tr>
						<th width="400" bgcolor=#0B0B61><font color=white>Hor�rio Restart</font></th>
					 </tr>'
                        + REPLACE(REPLACE(@ALERTSQLReiniciado, '&lt;',
                                          '<'), '&gt;', '>')
                        + '</table>'

            -----------------------------------------------------------
            -- Insert a blank page in EMAIL
            -----------------------------------------------------------
            SET @EmptyBodyEmail = ''
            SET @EmptyBodyEmail =
                        '<table cellpadding="5" cellspacing="5" border="0">' +
                        '<tr>
						<th width="500">               </th>
					</tr>'
                        + REPLACE(REPLACE(ISNULL(@EmptyBodyEmail, ''),
                                          '&lt;', '<'), '&gt;', '>')
                        + '</table>'

            /**********************************************************
		    --	Set EMAIL Variables
		    **********************************************************/
            SELECT @Importance = 'High',
                   @Subject =
                   'ALERT: SQL Server Reiniciado nos �ltimos ' +
                   CAST((@SQL_Reiniciado_Parametro) AS VARCHAR) +
                   ' Minutos no Servidor: ' + @@SERVERNAME,
                   @EmailBody = @ALERTLogHeader + @EmptyBodyEmail +
                                @ALERTSQLReiniciado + @EmptyBodyEmail


            /**********************************************************
		    --	SEND EMAIL - ALERT
		    **********************************************************/
            EXEC [msdb].[dbo].[sp_send_dbmail]
                 @profile_name = @ProfileEmail,
                 @recipients = @EmailDestination,
                 @subject = @Subject,
                 @body = @EmailBody,
                 @body_format = 'HTML',
                 @importance = @Importance

            /**********************************************************
		    -- Inserts a Record in the ALERT Control Table -> Fl_Tipo = 1 : ALERT
		    **********************************************************/
            INSERT INTO [dbo].[ALERT] ([Id_ALERT_Parametro],
                                       [Ds_Mensagem], [Fl_Tipo])
            SELECT @Id_ALERT_Parametro, @Subject, 1
        END
END

GO
IF (OBJECT_ID('[dbo].[stpALERT_Database_Createda]') IS NOT NULL)
    DROP PROCEDURE [dbo].[stpALERT_Database_Createda]
GO

/**********************************************************
--	ALERT: Database Createda
**********************************************************/

CREATE PROCEDURE [dbo].[stpALERT_Database_Createda]
AS
BEGIN
    SET NOCOUNT ON

    -- Database Createda
    DECLARE
        @Id_ALERT_Parametro INT = (SELECT Id_ALERT_Parametro
                                   FROM [Traces].[dbo].ALERT_Parametro (NOLOCK)
                                   WHERE Nm_ALERT = 'Database Createda')

    -----------------------------------------------------------
    -- Retrieves ALERT parameters
    -----------------------------------------------------------
    DECLARE
        @Database_Createda_Parametro INT, @EmailDestination VARCHAR(500), @ProfileEmail VARCHAR(200)

    SELECT @Database_Createda_Parametro = Vl_Parametro, -- Horas
           @EmailDestination = Ds_Email,
           @ProfileEmail = Ds_Profile_Email
    FROM [dbo].[ALERT_Parametro]
    WHERE [Id_ALERT_Parametro] = @Id_ALERT_Parametro
    -- Database Createda

    -- Verifica se alguma base foi Createda no dia anterior
    IF (OBJECT_ID('tempdb..#ALERT_Base_Createda') IS NOT NULL)
        DROP TABLE #ALERT_Base_Createda

    SELECT [name], [recovery_model_desc], [create_date]
    INTO #ALERT_Base_Createda
    FROM [sys].[databases] WITH (NOLOCK)
    WHERE [database_id] <> 2 -- Desconsidera a Database "TempDb"
      AND [create_date] >=
          DATEADD(HOUR, -@Database_Createda_Parametro, GETDATE())

    /**********************************************************
	--	Verifica se alguam base foi Createda
	**********************************************************/
    IF EXISTS(SELECT * FROM #ALERT_Base_Createda)
        BEGIN
            /**********************************************************
		    --	CREATE EMAIL - ALERT
		    **********************************************************/
            -- Declares the variables
            DECLARE
                @EmailBody      VARCHAR(MAX), @ALERTLogHeader VARCHAR(MAX), @ALERTBaseCreateda VARCHAR(MAX),
                @EmptyBodyEmail VARCHAR(MAX), @Importance AS VARCHAR(6), @Subject VARCHAR(500)

            -----------------------------------------------------------
            --	ALERT - HEADER
            -----------------------------------------------------------
            SET @ALERTLogHeader =
                    '<font color=black bold=true size= 5>'
            SET @ALERTLogHeader = @ALERTLogHeader +
                                  '<BR /> Database Createda nas �ltimas ' +
                                  CAST(
                                          (@Database_Createda_Parametro) AS VARCHAR) +
                                  ' Horas <BR />'
            SET @ALERTLogHeader = @ALERTLogHeader + '</font>'

            -----------------------------------------------------------
            --	ALERT - BODY
            -----------------------------------------------------------
            SET @ALERTBaseCreateda = CAST((
                SELECT td =
                               [name] + '</td>'
                               + '<td>' + [recovery_model_desc] +
                               '</td>'
                               + '<td>' + [Create_Date] + '</td>'
                FROM (
                         -- EMAIL Table Details
                         SELECT [name],
                                [recovery_model_desc],
                                CONVERT(VARCHAR(20), [create_date],
                                                     120) AS [Create_Date]
                         FROM #ALERT_Base_Createda
                     ) AS D FOR XML PATH ('tr'), TYPE) AS VARCHAR(MAX)
                )

            -- Corrects Table Formatting
            SET @ALERTBaseCreateda = REPLACE(
                    REPLACE(REPLACE(@ALERTBaseCreateda, '&lt;', '<'),
                            '&gt;', '>'), '<td>', '<td align = center>')

            -- EMAIL Table Titles
            SET @ALERTBaseCreateda =
                        '<table cellspacing="2" cellpadding="5" border="3">'
                        + '<tr>
						<th width="300" bgcolor=#0B0B61><font color=white>Nome</font></th>
						<th width="300" bgcolor=#0B0B61><font color=white>Recovery Model</font></th>
						<th width="300" bgcolor=#0B0B61><font color=white>Data Create��o</font></th>
					 </tr>'
                        +
                        REPLACE(
                                REPLACE(@ALERTBaseCreateda, '&lt;', '<'),
                                '&gt;', '>')
                        + '</table>'

            -----------------------------------------------------------
            -- Insert a blank page in EMAIL
            -----------------------------------------------------------
            SET @EmptyBodyEmail = ''
            SET @EmptyBodyEmail =
                        '<table cellpadding="5" cellspacing="5" border="0">' +
                        '<tr>
						<th width="500">               </th>
					</tr>'
                        + REPLACE(REPLACE(ISNULL(@EmptyBodyEmail, ''),
                                          '&lt;', '<'), '&gt;', '>')
                        + '</table>'

            /**********************************************************
		    --	Set EMAIL Variables
		    **********************************************************/
            SELECT @Importance = 'High',
                   @Subject = 'ALERT: Database Createda nas �ltimas ' +
                              CAST(
                                      (@Database_Createda_Parametro) AS VARCHAR) +
                              ' Horas no Servidor: ' + @@SERVERNAME,
                   @EmailBody = @ALERTLogHeader + @EmptyBodyEmail +
                                @ALERTBaseCreateda + @EmptyBodyEmail

            /**********************************************************
		    --	SEND EMAIL - ALERT
		    **********************************************************/
            EXEC [msdb].[dbo].[sp_send_dbmail]
                 @profile_name = @ProfileEmail,
                 @recipients = @EmailDestination,
                 @subject = @Subject,
                 @body = @EmailBody,
                 @body_format = 'HTML',
                 @importance = @Importance

            /**********************************************************
		    -- Inserts a Record in the ALERT Control Table -> Fl_Tipo = 1 : ALERT
		    **********************************************************/
            INSERT INTO [dbo].[ALERT] ([Id_ALERT_Parametro],
                                       [Ds_Mensagem], [Fl_Tipo])
            SELECT @Id_ALERT_Parametro, @Subject, 1
        END
END

GO
IF (OBJECT_ID('[dbo].[stpALERT_Database_Sem_Backup]') IS NOT NULL)
    DROP PROCEDURE [dbo].[stpALERT_Database_Sem_Backup]
GO

/**********************************************************
--	ALERT: DATABASE SEM BACKUP
**********************************************************/

CREATE PROCEDURE [dbo].[stpALERT_Database_Sem_Backup]
AS
BEGIN
    SET NOCOUNT ON

    -- Databases sem Backup
    DECLARE
        @Id_ALERT_Parametro INT = (SELECT Id_ALERT_Parametro
                                   FROM [Traces].[dbo].ALERT_Parametro (NOLOCK)
                                   WHERE Nm_ALERT = 'Database sem Backup')

    -- Declares the variables
    DECLARE
        @Qtd_Databases_Total INT, @Qtd_Databases_Restore INT

    -----------------------------------------------------------
    -- Retrieves ALERT parameters
    -----------------------------------------------------------
    DECLARE
        @Database_Sem_Backup_Parametro INT, @EmailDestination VARCHAR(500), @ProfileEmail VARCHAR(200)

    SELECT @Database_Sem_Backup_Parametro = Vl_Parametro, -- Horas
           @EmailDestination = Ds_Email,
           @ProfileEmail = Ds_Profile_Email
    FROM [dbo].[ALERT_Parametro]
    WHERE [Id_ALERT_Parametro] = @Id_ALERT_Parametro
    -- Databases sem Backup

    -- Verifica a Quantidade Total de Databases
    IF (OBJECT_ID('tempdb..#ALERT_backup_databases_todas') IS NOT NULL)
        DROP TABLE #ALERT_backup_databases_todas

    SELECT [name] AS [Nm_Database]
    INTO #ALERT_backup_databases_todas
    FROM [sys].[databases]
    WHERE [name] NOT IN ('tempdb', 'ReportServerTempDB')
      AND state_desc <> 'OFFLINE'

    SELECT @Qtd_Databases_Total = COUNT(*)
    FROM #ALERT_backup_databases_todas

    -- Verifica a Quantidade de Databases que tiveram Backup nas ultimas 14 horas
    IF (OBJECT_ID(
            'tempdb..#ALERT_backup_databases_com_backup') IS NOT NULL)
        DROP TABLE #ALERT_backup_databases_com_backup

    SELECT DISTINCT [database_name] AS [Nm_Database]
    INTO #ALERT_backup_databases_com_backup
    FROM [msdb].[dbo].[backupset] B
             JOIN [msdb].[dbo].[backupmediafamily] BF
                  ON B.[media_set_id] = BF.[media_set_id]
    WHERE [backup_start_date] >=
          DATEADD(hh, -@Database_Sem_Backup_Parametro, GETDATE())
      AND [type] IN ('D', 'I')

    SELECT @Qtd_Databases_Restore = COUNT(*)
    FROM #ALERT_backup_databases_com_backup

    /**********************************************************
	--	Verifica se menos de 70 % das databases tiveram Backup
	**********************************************************/
    if (@Qtd_Databases_Restore < @Qtd_Databases_Total * 0.7)
        BEGIN
            -- Databases que n�o tiveram Backup
            IF (OBJECT_ID(
                    'tempdb..#ALERT_backup_databases_sem_backup') IS NOT NULL)
                DROP TABLE #ALERT_backup_databases_sem_backup

            SELECT A.[Nm_Database]
            INTO #ALERT_backup_databases_sem_backup
            FROM #ALERT_backup_databases_todas A WITH (NOLOCK)
                     LEFT JOIN #ALERT_backup_databases_com_backup B WITH (NOLOCK)
                               ON A.[Nm_Database] = B.[Nm_Database]
            WHERE B.[Nm_Database] IS NULL

            /**********************************************************
		    --	CREATE EMAIL - ALERT
		    **********************************************************/
            -- Declares the variables
            DECLARE
                @EmailBody     VARCHAR(MAX), @ALERTLogHeader VARCHAR(MAX), @ALERTLogTable VARCHAR(MAX), @EmptyBodyEmail VARCHAR(MAX),
                @Importance AS VARCHAR(6), @Subject VARCHAR(500)

            -----------------------------------------------------------
            --	ALERT - HEADER
            -----------------------------------------------------------
            SET @ALERTLogHeader = '<font color=black bold=true size=5>'
            SET @ALERTLogHeader = @ALERTLogHeader +
                                  '<BR /> Databases sem Backup nas �ltimas ' +
                                  CAST(
                                          (@Database_Sem_Backup_Parametro) AS VARCHAR) +
                                  ' Horas <BR />'
            SET @ALERTLogHeader = @ALERTLogHeader + '</font>'

            -----------------------------------------------------------
            --	ALERT - BODY
            -----------------------------------------------------------
            SET @ALERTLogTable = CAST((
                SELECT td = [Nm_Database] + '</td>'

                FROM (
                         -- EMAIL Table Details
                         SELECT [Nm_Database]
                         FROM #ALERT_backup_databases_sem_backup
                     ) AS D
                ORDER BY [Nm_Database] DESC FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
                )

            -- Corrects Table Formatting
            SET @ALERTLogTable = REPLACE(
                    REPLACE(REPLACE(@ALERTLogTable, '&lt;', '<'),
                            '&gt;', '>'), '<td>', '<td align = center>')

            -- EMAIL Table Titles
            SET @ALERTLogTable =
                        '<table cellspacing="2" cellpadding="5" border="3">'
                        + '<tr>
						<th width="200" bgcolor=#0B0B61><font color=white>Database</font></th>
					</tr>'
                        + REPLACE(REPLACE(@ALERTLogTable, '&lt;', '<'),
                                  '&gt;', '>')
                        + '</table>'

            -----------------------------------------------------------
            -- Insert a blank page in EMAIL
            -----------------------------------------------------------
            SET @EmptyBodyEmail = ''
            SET @EmptyBodyEmail =
                        '<table cellpadding="5" cellspacing="5" border="0">' +
                        '<tr>
						<th width="500">               </th>
					</tr>'
                        + REPLACE(REPLACE(ISNULL(@EmptyBodyEmail, ''),
                                          '&lt;', '<'), '&gt;', '>')
                        + '</table>'

            /**********************************************************
		    --	Set EMAIL Variables
		    **********************************************************/
            SELECT @Importance = 'High',
                   @Subject =
                   'ALERT: Existem Databases sem Backup nas �ltimas ' +
                   CAST((@Database_Sem_Backup_Parametro) AS VARCHAR) +
                   ' Horas no Servidor: ' + @@SERVERNAME,
                   @EmailBody = @ALERTLogHeader + @EmptyBodyEmail +
                                @ALERTLogTable + @EmptyBodyEmail

            /**********************************************************
		    --	SEND EMAIL - ALERT
		    **********************************************************/
            EXEC [msdb].[dbo].[sp_send_dbmail]
                 @profile_name = @ProfileEmail,
                 @recipients = @EmailDestination,
                 @subject = @Subject,
                 @body = @EmailBody,
                 @body_format = 'HTML',
                 @importance = @Importance

            /**********************************************************
		    -- Inserts a Record in the ALERT Control Table -> Fl_Tipo = 1 : ALERT
		    **********************************************************/
            INSERT INTO [dbo].[ALERT] ([Id_ALERT_Parametro],
                                       [Ds_Mensagem], [Fl_Tipo])
            SELECT @Id_ALERT_Parametro, @Subject, 1
        END
END

GO
IF (OBJECT_ID('[dbo].[stpCHECKDB_Databases]') IS NOT NULL)
    DROP PROCEDURE [dbo].[stpCHECKDB_Databases]
GO

/**********************************************************
--	ALERT: CHECKDB DATABASES
**********************************************************/

CREATE PROCEDURE [dbo].[stpCHECKDB_Databases]
AS
BEGIN
    SET NOCOUNT ON

    -- Declara a tabela que ir� armazenar o nome das Databases
    DECLARE
        @Databases TABLE
                   (
                       [Id_Database] INT IDENTITY (1, 1),
                       [Nm_Database] VARCHAR(50)
                   )

    -- Declares the variables
    DECLARE
        @Total INT, @Loop INT, @Nm_Database VARCHAR(50)

    -- Busca o nome das Databases
    INSERT INTO @Databases([Nm_Database])
    SELECT [name]
    FROM [master].[sys].[databases]
    WHERE [name] NOT IN ('tempdb') -- Colocar o nome da Database aqui, caso deseje desconsiderar alguma
      AND [state_desc] = 'ONLINE'

    -- Quantidade Total de Databases (utilizado no Loop abaixo)
    SELECT @Total = MAX([Id_Database])
    FROM @Databases

    SET @Loop = 1

    -- Realiza o CHECKDB para cada Database
    WHILE (@Loop <= @Total)
    BEGIN
        SELECT @Nm_Database = [Nm_Database]
        FROM @Databases
        WHERE [Id_Database] = @Loop

        DBCC CHECKDB (@Nm_Database) WITH NO_INFOMSGS
        SET @Loop = @Loop + 1
    END
END

GO
IF (OBJECT_ID('[dbo].[stpALERT_CheckDB]') IS NOT NULL)
    DROP PROCEDURE [dbo].[stpALERT_CheckDB]
GO

/**********************************************************
--	ALERT: Database CORROMPIDO
**********************************************************/

CREATE PROCEDURE [dbo].[stpALERT_CheckDB]
AS
BEGIN
    SET NOCOUNT ON

    SET DATEFORMAT MDY

    IF (OBJECT_ID('tempdb..#TempLog') IS NOT NULL)
        DROP TABLE #TempLog

    CREATE TABLE #TempLog
    (
        [LogDate]     DATETIME,
        [ProcessInfo] NVARCHAR(50),
        [Text]        NVARCHAR(MAX)
    )

    IF (OBJECT_ID('tempdb..#logF') IS NOT NULL)
        DROP TABLE #logF

    CREATE TABLE #logF
    (
        ArchiveNumber INT,
        LogDate       DATETIME,
        LogSize       INT
    )

    -- Seleciona o n�mero de arquivos.
    INSERT INTO #logF
        EXEC sp_enumerrorlogs

    DELETE
    FROM #logF
    WHERE LogDate < GETDATE() - 2

    DECLARE
        @TSQL NVARCHAR(2000), @lC INT

    SELECT @lC = MIN(ArchiveNumber) FROM #logF

    --Loop para realizar a leitura de todo o log
    WHILE @lC IS NOT NULL
    BEGIN
        INSERT INTO #TempLog
            EXEC sp_readerrorlog @lC

        SELECT @lC = MIN(ArchiveNumber)
        FROM #logF
        WHERE ArchiveNumber > @lC
    END

    IF OBJECT_ID('_Result_Corrupcao') IS NOT NULL
        DROP TABLE _Result_Corrupcao

    SELECT LogDate,
           SUBSTRING(Text, 15,
                     CHARINDEX(')', Text, 15) - 15) AS Nm_Database,
           SUBSTRING(Text, charindex('found', Text),
                     (charindex('Elapsed time', Text) -
                      charindex('found', Text)))    AS Erros,
           Text
    INTO _Result_Corrupcao
    FROM #TempLog
    WHERE LogDate >= GETDATE() - 1
      and Text like '%DBCC CHECKDB (%'
      and Text not like '%IDR%'
      and substring(Text, charindex('found', Text),
                    charindex('Elapsed time', Text) -
                    charindex('found', Text)) <>
          'found 0 errors and repaired 0 errors.'

    -- Declares the variables
    DECLARE
        @Subject                    VARCHAR(500), @Importance AS VARCHAR(6), @EmailBody VARCHAR(MAX), @EmptyBodyEmail VARCHAR(MAX),
        @ALERTBancoCorrompidoHeader VARCHAR(MAX), @ALERTBancoCorrompidoTable VARCHAR(MAX), @EmailDestination VARCHAR(500), @ProfileEmail VARCHAR(200)

    -----------------------------------------------------------
    -- Retrieves ALERT parameters
    -----------------------------------------------------------
    -- Database Corrompido
    DECLARE
        @Id_ALERT_Parametro INT = (SELECT Id_ALERT_Parametro
                                   FROM [Traces].[dbo].ALERT_Parametro (NOLOCK)
                                   WHERE Nm_ALERT = 'Database Corrompido')

    SELECT @EmailDestination = Ds_Email,
           @ProfileEmail = Ds_Profile_Email
    FROM [dbo].[ALERT_Parametro]
    WHERE [Id_ALERT_Parametro] = @Id_ALERT_Parametro
    -- Database Corrompido

    /**********************************************************
	-- Verifica se existe algum Database Corrompido
	**********************************************************/
    IF EXISTS(SELECT NULL FROM [Traces].[dbo].[_Result_Corrupcao])
        BEGIN
            -- START - ALERT
            /**********************************************************
		    --	CREATE EMAIL - ALERT
		    **********************************************************/

            -----------------------------------------------------------
            --	ALERT - HEADER
            -----------------------------------------------------------
            SET @ALERTBancoCorrompidoHeader =
                    '<font color=black bold=true size=5>'
            SET @ALERTBancoCorrompidoHeader =
                        @ALERTBancoCorrompidoHeader +
                        '<BR /> Database Corrompido <BR />'
            SET @ALERTBancoCorrompidoHeader =
                    @ALERTBancoCorrompidoHeader + '</font>'

            -----------------------------------------------------------
            --	ALERT - BODY
            -----------------------------------------------------------
            SET @ALERTBancoCorrompidoTable = CAST((
                SELECT td = [LogDate] + '</td>'
                                + '<td>' + [Nm_Database] + '</td>'
                                + '<td>' + [Erros] + '</td>'
                                + '<td>' + [Text] + '</td>'

                FROM (
                         -- EMAIL Table Details
                         SELECT CONVERT(VARCHAR(20), [LogDate], 120) AS [LogDate],
                                [Nm_Database],
                                [Erros],
                                [Text]
                         FROM [Traces].[dbo].[_Result_Corrupcao]
                     ) AS D FOR XML PATH ('tr'), TYPE) AS VARCHAR(MAX)
                )

            -- Corrects Table Formatting
            SET @ALERTBancoCorrompidoTable = REPLACE(
                    REPLACE(REPLACE(@ALERTBancoCorrompidoTable, '&lt;',
                                    '<'), '&gt;', '>'), '<td>',
                    '<td align = center>')

            -- EMAIL Table Titles
            SET @ALERTBancoCorrompidoTable =
                        '<table cellspacing="2" cellpadding="5" border="3">'
                        + '<tr>
						<th bgcolor=#0B0B61 width="100"><font color=white>Data Log</font></th>
						<th bgcolor=#0B0B61 width="180"><font color=white>Nome Database</font></th>
						<th bgcolor=#0B0B61 width="200"><font color=white>Erros</font></th>
						<th bgcolor=#0B0B61 width="300"><font color=white>Descri��o</font></th>
					</tr>'
                        + REPLACE(REPLACE(@ALERTBancoCorrompidoTable,
                                          '&lt;', '<'), '&gt;', '>')
                        + '</table>'

            -----------------------------------------------------------
            -- Insert a blank page in EMAIL
            -----------------------------------------------------------
            SET @EmptyBodyEmail = ''
            SET @EmptyBodyEmail =
                        '<table cellpadding="5" cellspacing="5" border="0">' +
                        '<tr>
						<th width="500">               </th>
					</tr>'
                        + REPLACE(REPLACE(ISNULL(@EmptyBodyEmail, ''),
                                          '&lt;', '<'), '&gt;', '>')
                        + '</table>'

            /**********************************************************
		    --	Set EMAIL Variables
		    **********************************************************/
            SELECT @Importance = 'High',
                   @Subject =
                   'ALERT: Existe algum Database Corrompido no Servidor: ' +
                   @@SERVERNAME + '. Verifique com urg�ncia!',
                   @EmailBody =
                   @ALERTBancoCorrompidoHeader + @EmptyBodyEmail +
                   @ALERTBancoCorrompidoTable + @EmptyBodyEmail

            /**********************************************************
		    --	SEND EMAIL - ALERT
		    **********************************************************/
            EXEC [msdb].[dbo].[sp_send_dbmail]
                 @profile_name = @ProfileEmail,
                 @recipients = @EmailDestination,
                 @subject = @Subject,
                 @body = @EmailBody,
                 @body_format = 'HTML',
                 @importance = @Importance

            /**********************************************************
		    -- Inserts a Record in the ALERT Control Table -> Fl_Tipo = 1 : ALERT
		    **********************************************************/
            INSERT INTO [dbo].[ALERT] ([Id_ALERT_Parametro],
                                       [Ds_Mensagem], [Fl_Tipo])
            SELECT @Id_ALERT_Parametro, @Subject, 1
        END -- END - ALERT

    IF (OBJECT_ID('_Result_Corrupcao') IS NOT NULL)
        DROP TABLE _Result_Corrupcao
END

GO
IF (OBJECT_ID('[dbo].[stpEnvia_Email_Processos_Execucao]') IS NOT NULL)
    DROP PROCEDURE [dbo].[stpEnvia_Email_Processos_Execucao]
GO

/**********************************************************
--	PROCEDURE ENVIA EMAIL WHOISACTIVE DBA
**********************************************************/

CREATE PROCEDURE [dbo].[stpEnvia_Email_Processos_Execucao]
AS
BEGIN
    SET NOCOUNT ON

    -- Declares the variables
    DECLARE
        @Subject                    VARCHAR(500), @Importance AS VARCHAR(6), @EmailBody VARCHAR(MAX), @EmptyBodyEmail VARCHAR(MAX),
        @ResultadoWhoisactiveHeader VARCHAR(MAX), @ResultadoWhoisactiveTable VARCHAR(MAX), @EmailDestination VARCHAR(500), @ProfileEmail VARCHAR(200)

    -- Create a tabela que ira armazenar os dados dos processos
    IF (OBJECT_ID('TempDb..#Resultado_WhoisActive') IS NOT NULL)
        DROP TABLE #Resultado_WhoisActive

    CREATE TABLE #Resultado_WhoisActive
    (
        [dd hh:mm:ss.mss]     VARCHAR(20),
        [database_name]       NVARCHAR(128),
        [login_name]          NVARCHAR(128),
        [host_name]           NVARCHAR(128),
        [start_time]          DATETIME,
        [status]              VARCHAR(30),
        [session_id]          INT,
        [blocking_session_id] INT,
        [wait_info]           VARCHAR(MAX),
        [open_tran_count]     INT,
        [CPU]                 VARCHAR(MAX),
        [reads]               VARCHAR(MAX),
        [writes]              VARCHAR(MAX),
        [sql_command]         XML
    )

    -- Retorna todos os processos que est�o sendo executados no momento
    EXEC [dbo].[sp_whoisactive]
         @get_outer_command = 1,
         @output_column_list = '[dd hh:mm:ss.mss][database_name][login_name][host_name][start_time][status][session_id][blocking_session_id][wait_info][open_tran_count][CPU][reads][writes][sql_command]',
         @destination_table = '#Resultado_WhoisActive'

    -- Altera a coluna que possui o comando SQL
    ALTER TABLE #Resultado_WhoisActive
        ALTER COLUMN [sql_command] VARCHAR(MAX)

    UPDATE #Resultado_WhoisActive
    SET [sql_command] = REPLACE(REPLACE(REPLACE(REPLACE(
                                                        CAST([sql_command] AS VARCHAR(1000)),
                                                        '<?query --',
                                                        ''), '--?>',
                                                ''), '&gt;', '>'),
                                '&lt;', '')

    -- select * from #Resultado_WhoisActive

    -- Verifica se n�o existe nenhum processo em Execu��o
    IF NOT EXISTS(SELECT TOP 1 * FROM #Resultado_WhoisActive)
        BEGIN
            INSERT INTO #Resultado_WhoisActive
            SELECT NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL,
                   NULL
        END

    -----------------------------------------------------------
    -- Retrieves ALERT parameters
    -----------------------------------------------------------
    -- Processos em Execu��o
    DECLARE
        @Id_ALERT_Parametro INT = (SELECT Id_ALERT_Parametro
                                   FROM [Traces].[dbo].ALERT_Parametro (NOLOCK)
                                   WHERE Nm_ALERT = 'Processos em Execu��o')

    SELECT @EmailDestination = Ds_Email,
           @ProfileEmail = Ds_Profile_Email
    FROM [dbo].[ALERT_Parametro]
    WHERE [Id_ALERT_Parametro] = @Id_ALERT_Parametro
    -- Processos em Execu��o

    /**********************************************************
	--	CREATE EMAIL
	**********************************************************/

    -----------------------------------------------------------
    --	HEADER
    -----------------------------------------------------------
    SET @ResultadoWhoisactiveHeader =
            '<font color=black bold=true size=5>'
    SET @ResultadoWhoisactiveHeader = @ResultadoWhoisactiveHeader +
                                      '<BR /> Processos em Execu��o no Database <BR />'
    SET @ResultadoWhoisactiveHeader =
            @ResultadoWhoisactiveHeader + '</font>'

    -----------------------------------------------------------
    --	BODY
    -----------------------------------------------------------
    SET @ResultadoWhoisactiveTable = CAST((
        SELECT td = [Duração] + '</td>'
                        + '<td>' + [database_name] + '</td>'
                        + '<td>' + [login_name] + '</td>'
                        + '<td>' + [host_name] + '</td>'
                        + '<td>' + [start_time] + '</td>'
                        + '<td>' + [status] + '</td>'
                        + '<td>' + [session_id] + '</td>'
                        + '<td>' + [blocking_session_id] + '</td>'
                        + '<td>' + [Wait] + '</td>'
                        + '<td>' + [open_tran_count] + '</td>'
                        + '<td>' + [CPU] + '</td>'
                        + '<td>' + [reads] + '</td>'
                        + '<td>' + [writes] + '</td>'
                        + '<td>' + [sql_command] + '</td>'

        FROM (
                 -- EMAIL Table Details
                 SELECT ISNULL([dd hh:mm:ss.mss], '-') AS [Duração],
                        ISNULL([database_name], '-')   AS [database_name],
                        ISNULL([login_name], '-')      AS [login_name],
                        ISNULL([host_name], '-')       AS [host_name],
                        ISNULL(CONVERT(VARCHAR(20), [start_time], 120),
                               '-')                    AS [start_time],
                        ISNULL([status], '-')          AS [status],
                        ISNULL(CAST([session_id] AS VARCHAR),
                               '-')                    AS [session_id],
                        ISNULL(CAST([blocking_session_id] AS VARCHAR),
                               '-')                    AS [blocking_session_id],
                        ISNULL([wait_info], '-')       AS [Wait],
                        ISNULL(CAST([open_tran_count] AS VARCHAR),
                               '-')                    AS [open_tran_count],
                        ISNULL([CPU], '-')             AS [CPU],
                        ISNULL([reads], '-')           AS [reads],
                        ISNULL([writes], '-')          AS [writes],
                        ISNULL(SUBSTRING([sql_command], 1, 300),
                               '-')                    AS [sql_command]
                 FROM #Resultado_WhoisActive
             ) AS D
        ORDER BY [start_time] FOR XML PATH ( 'tr' ), TYPE) AS VARCHAR(MAX)
        )

    -- Corrects Table Formatting
    SET @ResultadoWhoisactiveTable = REPLACE(
            REPLACE(REPLACE(@ResultadoWhoisactiveTable, '&lt;', '<'),
                    '&gt;', '>'), '<td>', '<td align = center>')

    -- EMAIL Table Titles
    SET @ResultadoWhoisactiveTable =
                '<table cellspacing="2" cellpadding="5" border="3">'
                + '<tr>
					<th bgcolor=#0B0B61 width="140"><font color=white>[dd hh:mm:ss.mss]</font></th>
					<th bgcolor=#0B0B61 width="100"><font color=white>Database</font></th>
					<th bgcolor=#0B0B61 width="120"><font color=white>Login</font></th>
					<th bgcolor=#0B0B61 width="120"><font color=white>Host Name</font></th>
					<th bgcolor=#0B0B61 width="200"><font color=white>Hora In�cio</font></th>
					<th bgcolor=#0B0B61 width="120"><font color=white>Status</font></th>
					<th bgcolor=#0B0B61 width="30"><font color=white>ID Session</font></th>
					<th bgcolor=#0B0B61 width="60"><font color=white>ID Session Bloqueando</font></th>
					<th bgcolor=#0B0B61 width="120"><font color=white>Wait</font></th>
					<th bgcolor=#0B0B61 width="60"><font color=white>transactions Abertas</font></th>
					<th bgcolor=#0B0B61 width="120"><font color=white>CPU</font></th>
					<th bgcolor=#0B0B61 width="120"><font color=white>Reads</font></th>
					<th bgcolor=#0B0B61 width="120"><font color=white>Writes</font></th>
					<th bgcolor=#0B0B61 width="1000"><font color=white>Query</font></th>
				</tr>'
                + REPLACE(REPLACE(@ResultadoWhoisactiveTable, '&lt;',
                                  '<'), '&gt;', '>')
                + '</table>'

    -----------------------------------------------------------
    -- Insert a blank page in EMAIL
    -----------------------------------------------------------
    SET @EmptyBodyEmail = ''
    SET @EmptyBodyEmail =
                '<table cellpadding="5" cellspacing="5" border="0">' +
                '<tr>
					<th width="500">               </th>
				</tr>'
                + REPLACE(REPLACE(ISNULL(@EmptyBodyEmail, ''), '&lt;',
                                  '<'), '&gt;', '>')
                + '</table>'

    /**********************************************************
	--	Arrow Variations of Email
	**********************************************************/
    SELECT @Importance = 'High',
           @Subject =
           'Processos em execu��o no Servidor: ' + @@SERVERNAME,
           @EmailBody = @ResultadoWhoisactiveHeader + @EmptyBodyEmail +
                        @ResultadoWhoisactiveTable + @EmptyBodyEmail

    /**********************************************************
	--	SEND EMAIL
	**********************************************************/
    EXEC [msdb].[dbo].[sp_send_dbmail]
         @profile_name = @ProfileEmail,
         @recipients = @EmailDestination,
         @subject = @Subject,
         @body = @EmailBody,
         @body_format = 'HTML',
         @importance = @Importance
END