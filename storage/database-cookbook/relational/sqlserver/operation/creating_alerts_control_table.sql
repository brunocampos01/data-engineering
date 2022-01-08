/******************************************************************************
--	Instructions for using the script
*******************************************************************************/
--	1) F5 to run script
--	2) Switch e-mail "brunocampos01@gmail.com" that should receive the Alerts
--  and replace the profile 'MSSQLServer'

DECLARE
    @Ds_Email VARCHAR(MAX)
DECLARE
    @Ds_Profile_Email VARCHAR(MAX)

SELECT @Ds_Email = 'brunocampos01@gmail.com'
SELECT @Ds_Profile_Email = 'MSSQLServer'

--	2) F5 to run script

/********************************************************************************************
--	Database that will be used to store Alerts data. If necessary, change to the desired name.
*********************************************************************************************/
USE [Traces]

-------------------------------------------------------------------------------
--	Create tables
-------------------------------------------------------------------------------
USE [Traces]

IF (OBJECT_ID('[dbo].[Alerta]') IS NOT NULL)
    DROP TABLE [dbo].[Alerta]

CREATE TABLE [dbo].[Alerta]
(
    [Id_Alerta]           INT IDENTITY PRIMARY KEY,
    [Id_Alerta_Parametro] INT NOT NULL,
    [Ds_Mensagem]         VARCHAR(2000),
    [Fl_Tipo]             TINYINT, -- 0: CLEAR / 1: ALERTA
    [Dt_Alerta]           DATETIME DEFAULT (GETDATE())
)

IF (OBJECT_ID('[dbo].[Alerta_Parametro]') IS NOT NULL)
    DROP TABLE [dbo].[Alerta_Parametro]

CREATE TABLE [dbo].[Alerta_Parametro]
(
    [Id_Alerta_Parametro] INT          NOT NULL IDENTITY (1,1) PRIMARY KEY,
    [Nm_Alerta]           VARCHAR(100) NOT NULL,
    [Nm_Procedure]        VARCHAR(100) NOT NULL,
    [Fl_Clear]            BIT          NOT NULL,
    [Vl_Parametro]        INT          NULL,
    [Ds_Metrica]          VARCHAR(50)  NULL,
    [Ds_Profile_Email]    VARCHAR(200) NULL,
    [Ds_Email]            VARCHAR(500) NULL
) ON [PRIMARY]

ALTER TABLE [dbo].[Alerta]
    ADD CONSTRAINT FK01_Alerta
        FOREIGN KEY ([Id_Alerta_Parametro])
            REFERENCES [dbo].[Alerta_Parametro] ([Id_Alerta_Parametro])

-------------------------------------------------------------------------------
--	Inserts the data into the Parameter table
-------------------------------------------------------------------------------
INSERT INTO [dbo].[Alerta_Parametro]([Nm_Alerta], [Nm_Procedure],
                                     [Fl_Clear], [Vl_Parametro],
                                     [Ds_Metrica], [Ds_Profile_Email],
                                     [Ds_Email])
VALUES ('CheckList Database Version', '1.0.0', 0, NULL, NULL,
        @Ds_Profile_Email, @Ds_Email),
       ('Alert Version', '1.0.0', 0, NULL, NULL, NULL, NULL),
       ('Blocked Process', 'stpAlert_Blocked Process', 1, 2,
        'Minutes', @Ds_Profile_Email, @Ds_Email),
       ('File Log Full', 'stpAlert_File_Log_Full', 1, 85,
        'Percentage', @Ds_Profile_Email, @Ds_Email),
       ('Disk Space', 'stpAlert_Disk Space', 1, 80, 'Percenage',
        @Ds_Profile_Email, @Ds_Email),
       ('CPU Usage', 'stpAlert_CPU usage', 1, 85, 'Percentage',
        @Ds_Profile_Email, @Ds_Email),
        --('MaxSize Arquivo SQL', 'stpAlerta_MaxSize_Arquivo_SQL', 1, 15, 'Tamanho (MB)', @Ds_Profile_Email, @Ds_Email),
       ('Tempdb Utilizacao Arquivo MDF',
        'stpAlerta_Tempdb_Utilizacao_Arquivo_MDF', 1, 70, 'Percentage',
        @Ds_Profile_Email, @Ds_Email),
       ('Conexion SQL Server', 'stpAlert_Conexion_SQLServer', 1, 2000,
        'Amount', @Ds_Profile_Email, @Ds_Email),
       ('Status Database', 'stpAlert_Error_Data Bases', 1, NULL, NULL,
        @Ds_Profile_Email, @Ds_Email),
       ('Pg Corrupted', 'stpAlert_Error_DB', 0, NULL,
        NULL, @Ds_Profile_Email, @Ds_Email),
       ('Delayed Queries', 'stpAlert_Delayed Queries', 0, 100,
        'Amount', @Ds_Profile_Email, @Ds_Email),
       ('Trace Delayed Queries ', 'stpCreate_Trace', 0, 3, 'Seconds',
        @Ds_Profile_Email, @Ds_Email),
       ('Job Fail', 'stpAlert_Job_Fail', 0, 24, 'Hours',
        @Ds_Profile_Email, @Ds_Email),
       ('SQL Server Restarted', 'stpAlert_SQL_Server_Restarted', 0,
        20, 'Minutes', @Ds_Profile_Email, @Ds_Email),
       ('Database Created', 'stpAlert_Database_Created', 0, 24, 'Hour',
        @Ds_Profile_Email, @Ds_Email),
       ('Database without Backup', 'stpAlert_Database_without_Backup',
        0, 24,
        'Horas', @Ds_Profile_Email, @Ds_Email),
       ('Corrupted Database', 'stpAlert_CheckDB', 0, NULL, NULL,
        @Ds_Profile_Email, @Ds_Email),
       ('Processes in Execution',
        'stpEnvia_Email_Processes in Execution', 0,
        NULL, NULL, @Ds_Profile_Email, @Ds_Email)

-- Checks the result of the table with the parameters of the Alerts
select *
from [dbo].Alerta_Parametro

-- select * from [dbo].Alerta
