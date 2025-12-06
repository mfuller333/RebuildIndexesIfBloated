SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

IF SCHEMA_ID(N'DBA') IS NULL
    EXEC('CREATE SCHEMA DBA AUTHORIZATION dbo');
GO

IF OBJECT_ID('DBA.usp_UpdateStatisticsIfChanged','P') IS NULL
    EXEC('CREATE PROCEDURE DBA.usp_UpdateStatisticsIfChanged AS RETURN 0;');
GO

-- Ensure log table exists in this utility DB
IF OBJECT_ID('DBA.UpdateStatsLog','U') IS NULL
BEGIN
    CREATE TABLE DBA.UpdateStatsLog
    (
        log_id               BIGINT IDENTITY(1,1) PRIMARY KEY,
        run_utc              DATETIME2(3)   NOT NULL CONSTRAINT DF_USL_run DEFAULT (SYSUTCDATETIME()),
        database_name        SYSNAME        NOT NULL,
        schema_name          SYSNAME        NOT NULL,
        table_name           SYSNAME        NOT NULL,
        stats_name           SYSNAME        NOT NULL,
        stats_id             INT            NOT NULL,
        [rows]               BIGINT         NULL,
        modification_counter BIGINT         NULL,
        change_pct           DECIMAL(9,4)   NULL,
        last_updated         DATETIME2(3)   NULL,
        rows_sampled         BIGINT         NULL,
        sample_mode          VARCHAR(12)    NOT NULL,
        [action]             VARCHAR(20)    NOT NULL,
        cmd                  NVARCHAR(MAX)  NOT NULL,
        [status]             VARCHAR(20)    NOT NULL,
        error_message        NVARCHAR(4000) NULL,
        error_number         INT            NULL,
        error_severity       INT            NULL,
        error_state          INT            NULL,
        error_line           INT            NULL,
        error_proc           NVARCHAR(128)  NULL,
        run_id               UNIQUEIDENTIFIER NULL
    );
END;
GO

IF COL_LENGTH('DBA.UpdateStatsLog','run_id') IS NULL
    ALTER TABLE DBA.UpdateStatsLog ADD run_id UNIQUEIDENTIFIER NULL;
GO

IF NOT EXISTS (
        SELECT 1
        FROM sys.indexes
        WHERE object_id = OBJECT_ID('DBA.UpdateStatsLog')
          AND name = N'IX_UpdateStatsLog_run'
)
BEGIN
    CREATE INDEX IX_UpdateStatsLog_run
        ON DBA.UpdateStatsLog(run_id);
END;
GO

/********************************************************************************************************************/
/****** Name:        DBA.usp_UpdateStatisticsIfChanged                                                           ****/
/****** Purpose:     From a single utility DB, update stats in target DB(s) based on change %, or ALL_CHANGES.   ****/
/******              Allows FULLSCAN, SAMPLED <n>% or default sampling. Logs centrally to DBA.UpdateStatsLog.    ****/
/******                                                                                                          ****/
/****** Input:       @Help                  = 1 prints help and examples; 0 runs normally                        ****/
/******              @TargetDatabases       = CSV of DBs | 'ALL_USER_DBS' with optional '-DbName' exclusions     ****/
/******              @ChangeThresholdPercent= when @ChangeScope IS NULL, update when change% >= this             ****/
/******              @ChangeScope           = 'ALL_CHANGES' (case-sensitive) to update all stats with changes    ****/
/******              @SampleMode            = 'FULLSCAN' | 'DEFAULT' | 'SAMPLED'                                 ****/
/******              @SamplePercent         = 1..100 when @SampleMode = 'SAMPLED' (rounded to INT for 2014)      ****/
/******              @WhatIf                = 1 dry-run; 0 execute                                               ****/
/******                                                                                                          ****/
/****** Output:      Rows logged to DBA.UpdateStatsLog with action, status, change%, command, and details.       ****/
/******              Progress messages printed as stats are analyzed and updated.                                ****/
/****** Created by:  Mike Fuller                                                                                 ****/
/****** Date Updated: 12/06/2025                                                                                 ****/
/****** Version:     2.1.1 (fixed cross-DB execution)                                                            ****/
/******                                                                                               ¯\_(ツ)_/¯ ****/
/********************************************************************************************************************/
ALTER PROCEDURE [DBA].[usp_UpdateStatisticsIfChanged]
      @Help                    BIT           = 0,
      @TargetDatabases         NVARCHAR(MAX) = NULL,   -- CSV | 'ALL_USER_DBS' (exact case) | with '-DbName' excludes
      @ChangeThresholdPercent  DECIMAL(6,2)  = NULL,   -- used when @ChangeScope IS NULL
      @ChangeScope             VARCHAR(20)   = NULL,   -- 'ALL_CHANGES' (exact case) or NULL
      @SampleMode              VARCHAR(12)   = 'DEFAULT', -- 'FULLSCAN' | 'DEFAULT' | 'SAMPLED'
      @SamplePercent           DECIMAL(6,2)  = NULL,   -- 1..100 when @SampleMode='SAMPLED'
      @WhatIf                  BIT           = 1
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    -- Help
    IF @Help = 1
    BEGIN
        SELECT param_name, sql_type, default_value, description, example
        FROM (VALUES
            (N'@TargetDatabases',        N'NVARCHAR(MAX)',  N'NULL',
                N'CSV or ALL_USER_DBS (exact case). Use ''-DbName'' to exclude.',
                N'@TargetDatabases = N''ALL_USER_DBS,-DBA,-UtilityDb'''),
            (N'@ChangeThresholdPercent', N'DECIMAL(6,2)',   N'NULL',
                N'Update when change% >= this (dm_db_stats_properties).',
                N'@ChangeThresholdPercent = 20.0'),
            (N'@ChangeScope',            N'VARCHAR(20)',    N'NULL',
                N'ALL_CHANGES to update any stats with changes; else uses threshold.',
                N'@ChangeScope = ''ALL_CHANGES'''),
            (N'@SampleMode',             N'VARCHAR(12)',    N'''DEFAULT''',
                N'FULLSCAN, DEFAULT, or SAMPLED.',
                N'@SampleMode = ''SAMPLED'''),
            (N'@SamplePercent',          N'DECIMAL(6,2)',   N'NULL',
                N'When @SampleMode = SAMPLED: 1..100 (rounded to whole % on SQL 2014).',
                N'@SamplePercent = 10.0'),
            (N'@WhatIf',                 N'BIT',            N'1',
                N'1=dry-run (log/print only); 0=execute UPDATE STATISTICS.',
                N'@WhatIf = 0')
        ) d(param_name, sql_type, default_value, description, example);

        SELECT example_label, example_command
        FROM (VALUES
            (N'All user DBs except DBA, ALL_CHANGES, SAMPLED 10% (dry run)',
             N'EXEC DBA.usp_UpdateStatisticsIfChanged @TargetDatabases = N''ALL_USER_DBS,-DBA'', @ChangeScope = ''ALL_CHANGES'', @SampleMode = ''SAMPLED'', @SamplePercent = 10, @WhatIf = 1;'),
            (N'One DB, threshold 20%, DEFAULT (execute)',
             N'EXEC DBA.usp_UpdateStatisticsIfChanged @TargetDatabases = N''YourDb'', @ChangeThresholdPercent = 20.0, @SampleMode = ''DEFAULT'', @WhatIf = 0;')
        ) x(example_label, example_command);
        RETURN;
    END;

    -- Validation
    IF @TargetDatabases IS NULL OR LTRIM(RTRIM(@TargetDatabases)) = N''
    BEGIN
        RAISERROR('@TargetDatabases is required.',16,1);
        RETURN;
    END;

    IF @SampleMode COLLATE Latin1_General_CS_AS NOT IN (N'FULLSCAN', N'DEFAULT', N'SAMPLED')
    BEGIN
        RAISERROR('@SampleMode must be FULLSCAN, DEFAULT, or SAMPLED (case-sensitive).',16,1);
        RETURN;
    END;

    DECLARE @SamplePercentInt INT = NULL;
    IF @SampleMode COLLATE Latin1_General_CS_AS = N'SAMPLED'
    BEGIN
        IF @SamplePercent IS NULL OR @SamplePercent <= 0 OR @SamplePercent > 100
        BEGIN
            RAISERROR('When @SampleMode = SAMPLED, @SamplePercent must be > 0 and <= 100.',16,1);
            RETURN;
        END;
        SET @SamplePercentInt = CONVERT(INT, ROUND(@SamplePercent, 0));
        IF @SamplePercentInt = 0 SET @SamplePercentInt = 1;
    END;

    IF @ChangeScope IS NOT NULL
       AND @ChangeScope COLLATE Latin1_General_CS_AS <> N'ALL_CHANGES'
    BEGIN
        RAISERROR('@ChangeScope must be ALL_CHANGES or NULL.',16,1);
        RETURN;
    END;

    IF @ChangeScope IS NULL
       AND ( @ChangeThresholdPercent IS NULL
             OR @ChangeThresholdPercent < 0
             OR @ChangeThresholdPercent > 100 )
    BEGIN
        RAISERROR('When @ChangeScope is NULL, @ChangeThresholdPercent must be between 0 and 100.',16,1);
        RETURN;
    END;


    -- Build target DB list (@TargetDatabases with ALL_USER_DBS and -DbName excludes)
    IF OBJECT_ID('tempdb..#targets') IS NOT NULL DROP TABLE #targets;
    IF OBJECT_ID('tempdb..#include') IS NOT NULL DROP TABLE #include;
    IF OBJECT_ID('tempdb..#exclude') IS NOT NULL DROP TABLE #exclude;

    CREATE TABLE #targets (db_name SYSNAME NOT NULL PRIMARY KEY);
    CREATE TABLE #include (db_name SYSNAME NOT NULL PRIMARY KEY);
    CREATE TABLE #exclude (db_name SYSNAME NOT NULL PRIMARY KEY);

    DECLARE @list NVARCHAR(MAX) = @TargetDatabases + N',';
    DECLARE @pos INT, @tok NVARCHAR(4000);
    DECLARE @HasAll bit = 0;

    WHILE LEN(@list) > 0
    BEGIN
        SET @pos = CHARINDEX(N',', @list);
        SET @tok = LTRIM(RTRIM(SUBSTRING(@list,1,@pos-1)));
        SET @list = SUBSTRING(@list, @pos+1, 2147483647);

        IF @tok = N'' CONTINUE;

        IF @tok COLLATE Latin1_General_CS_AS = N'ALL_USER_DBS'
        BEGIN
            SET @HasAll = 1;
            CONTINUE;
        END;

        IF LEFT(@tok,1) = N'-'
        BEGIN
            SET @tok = SUBSTRING(@tok,2,4000);
            IF LEN(@tok) > 0 AND NOT EXISTS (SELECT 1 FROM #exclude WHERE db_name=@tok)
                INSERT #exclude(db_name) VALUES(@tok);
        END
        ELSE
        BEGIN
            IF LEN(@tok) > 0 AND NOT EXISTS (SELECT 1 FROM #include WHERE db_name=@tok)
                INSERT #include(db_name) VALUES(@tok);
        END;
    END;

    IF @HasAll = 1
    BEGIN
        INSERT #targets(db_name)
        SELECT d.name
        FROM sys.databases d
        WHERE d.name NOT IN (N'master',N'model',N'msdb',N'tempdb',N'distribution')
          AND d.state = 0
          AND d.is_read_only = 0;
    END;

    INSERT #targets(db_name)
    SELECT i.db_name
    FROM #include AS i
    JOIN sys.databases d
      ON d.name = i.db_name COLLATE DATABASE_DEFAULT
    WHERE d.name NOT IN (N'master',N'model',N'msdb',N'tempdb',N'distribution')
      AND d.state = 0
      AND d.is_read_only = 0
      AND NOT EXISTS (SELECT 1 FROM #targets t WHERE t.db_name = i.db_name COLLATE DATABASE_DEFAULT);

    DELETE t
      FROM #targets t
      JOIN #exclude e
        ON e.db_name = t.db_name COLLATE DATABASE_DEFAULT;

    IF NOT EXISTS (SELECT 1 FROM #targets)
    BEGIN
        RAISERROR('No valid target databases resolved from @TargetDatabases.',16,1);
        RETURN;
    END;

    -------------------------------------------------------------------------
    -- Run ID and global candidates table
    -------------------------------------------------------------------------
    DECLARE @RunId UNIQUEIDENTIFIER = NEWID();
    DECLARE @msg NVARCHAR(4000);

    SET @msg = N'Starting stats run ' + CONVERT(NVARCHAR(36), @RunId)
             + N'; WhatIf=' + CONVERT(NVARCHAR(1), @WhatIf)
             + N'; ChangeScope=' + ISNULL(@ChangeScope,N'NULL')
             + N'; Threshold=' + ISNULL(CONVERT(NVARCHAR(32),@ChangeThresholdPercent),N'NULL')
             + N'; SampleMode=' + @SampleMode;
    RAISERROR(@msg,10,1) WITH NOWAIT;

    IF OBJECT_ID('tempdb..#candidates') IS NOT NULL DROP TABLE #candidates;

    CREATE TABLE #candidates
    (
        database_name        SYSNAME       NOT NULL,
        schema_name          SYSNAME       NOT NULL,
        table_name           SYSNAME       NOT NULL,
        stats_name           SYSNAME       NOT NULL,
        stats_id             INT           NOT NULL,
        [rows]               BIGINT        NULL,
        modification_counter BIGINT        NULL,
        change_pct           DECIMAL(9,4)  NULL,
        last_updated         DATETIME2(3)  NULL,
        rows_sampled         BIGINT        NULL,
        cmd                  NVARCHAR(MAX) NOT NULL
    );


    -- Collect candidates from each DB using [db].sys.sp_executesql + INSERT EXEC
    DECLARE @db SYSNAME;
    DECLARE @DynamicExec NVARCHAR(300);
    DECLARE @SQL         NVARCHAR(MAX);
    DECLARE @cnt INT;

    DECLARE cur_db CURSOR LOCAL FAST_FORWARD FOR
        SELECT db_name FROM #targets ORDER BY db_name;

    OPEN cur_db;
    FETCH NEXT FROM cur_db INTO @db;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name=@db AND state=0 AND is_read_only=0)
        BEGIN
            RAISERROR('Skipping database "%s": not ONLINE and read-write.',10,1,@db) WITH NOWAIT;
            FETCH NEXT FROM cur_db INTO @db;
            CONTINUE;
        END;

        SET @DynamicExec = QUOTENAME(@db) + N'.sys.sp_executesql';

        SET @msg = N'Analyzing database ' + QUOTENAME(@db) + N' for stats changes...';
        RAISERROR(@msg,10,1) WITH NOWAIT;

        SET @SQL = N'SET NOCOUNT ON;

        SELECT
            DB_NAME() AS database_name,
            s.name    AS schema_name,
            t.name    AS table_name,
            st.name   AS stats_name,
            st.stats_id,
            sp.[rows],
            sp.modification_counter,
            CAST(
                CASE
                    WHEN sp.[rows] IS NULL THEN NULL
                    WHEN sp.[rows] = 0 THEN CASE WHEN sp.modification_counter > 0 THEN 100.0 ELSE 0.0 END
                    ELSE (100.0 * CONVERT(DECIMAL(18,6), sp.modification_counter)) / NULLIF(CONVERT(DECIMAL(18,6), sp.[rows]),0)
                END
                AS DECIMAL(9,4)
            ) AS change_pct,
            sp.last_updated,
            sp.rows_sampled,
            N''UPDATE STATISTICS '' +
                QUOTENAME(s.name) + N''.'' + QUOTENAME(t.name) + N'' '' + QUOTENAME(st.name) +
                CASE 
                    WHEN @pSampleMode = N''FULLSCAN'' THEN N'' WITH FULLSCAN''
                    WHEN @pSampleMode = N''SAMPLED''  THEN N'' WITH SAMPLE '' + CONVERT(NVARCHAR(12), @pSamplePercent) + N'' PERCENT''
                    ELSE N''''
                END AS cmd
        FROM sys.stats AS st
        JOIN sys.tables AS t
            ON t.object_id = st.object_id
        JOIN sys.schemas AS s
            ON s.schema_id = t.schema_id
        CROSS APPLY sys.dm_db_stats_properties(st.object_id, st.stats_id) AS sp
        LEFT JOIN sys.indexes AS i
            ON i.object_id = st.object_id
           AND i.index_id  = st.stats_id
        WHERE t.is_ms_shipped = 0
          AND t.is_memory_optimized = 0
          AND (i.index_id IS NULL OR i.is_hypothetical = 0)
          AND sp.modification_counter IS NOT NULL
          AND (
                (@pChangeScope = N''ALL_CHANGES'' AND sp.modification_counter > 0)
                OR
                (@pChangeScope IS NULL AND (
                      (sp.[rows] = 0 AND sp.modification_counter > 0)
                   OR ((100.0 * CONVERT(DECIMAL(18,6), sp.modification_counter))
                        / NULLIF(CONVERT(DECIMAL(18,6), sp.[rows]),0)) >= @pChangeThresholdPercent)
                )
              );';

        INSERT INTO #candidates
        (
            database_name, schema_name, table_name, stats_name, stats_id,
            [rows], modification_counter, change_pct, last_updated, rows_sampled, cmd
        )
        EXEC @DynamicExec
             @SQL,
             N'@pChangeThresholdPercent DECIMAL(6,2),
               @pChangeScope            NVARCHAR(20),
               @pSampleMode            NVARCHAR(12),
               @pSamplePercent         INT',
             @pChangeThresholdPercent = @ChangeThresholdPercent,
             @pChangeScope            = @ChangeScope,
             @pSampleMode             = @SampleMode,
             @pSamplePercent          = @SamplePercentInt;

        SELECT @cnt = COUNT(*)
        FROM #candidates
        WHERE database_name = @db;

        SET @msg = N'Completed analysis of ' + QUOTENAME(@db)
                 + N'; total candidates so far for this DB: ' + CONVERT(NVARCHAR(20), @cnt) + N'.';
        RAISERROR(@msg,10,1) WITH NOWAIT;

        FETCH NEXT FROM cur_db INTO @db;
    END;

    CLOSE cur_db;
    DEALLOCATE cur_db;

    IF NOT EXISTS (SELECT 1 FROM #candidates)
    BEGIN
        RAISERROR('No statistics met the criteria in any target database.',10,1);
        RETURN;
    END;

    -- Log run into DBA.UpdateStatsLog and build #todo for execution
    IF OBJECT_ID('tempdb..#todo') IS NOT NULL DROP TABLE #todo;

    CREATE TABLE #todo
    (
        log_id        BIGINT      NOT NULL PRIMARY KEY,
        database_name SYSNAME     NOT NULL,
        schema_name   SYSNAME     NOT NULL,
        table_name    SYSNAME     NOT NULL,
        stats_name    SYSNAME     NOT NULL,
        cmd           NVARCHAR(MAX) NOT NULL
    );

    INSERT DBA.UpdateStatsLog
    (
        database_name, schema_name, table_name, stats_name, stats_id,
        [rows], modification_counter, change_pct, last_updated, rows_sampled,
        sample_mode, [action], cmd, [status], run_id
    )
    OUTPUT inserted.log_id,
           inserted.database_name,
           inserted.schema_name,
           inserted.table_name,
           inserted.stats_name,
           inserted.cmd
      INTO #todo (log_id, database_name, schema_name, table_name, stats_name, cmd)
    SELECT
        c.database_name,
        c.schema_name,
        c.table_name,
        c.stats_name,
        c.stats_id,
        c.[rows],
        c.modification_counter,
        c.change_pct,
        c.last_updated,
        c.rows_sampled,
        @SampleMode                               AS sample_mode,
        CASE WHEN @WhatIf = 1 THEN 'DRYRUN' ELSE 'UPDATE' END AS [action],
        c.cmd,
        CASE WHEN @WhatIf = 1 THEN 'SKIPPED' ELSE 'PENDING' END AS [status],
        @RunId                                    AS run_id
    FROM #candidates AS c;

    IF NOT EXISTS (SELECT 1 FROM #todo)
    BEGIN
        RAISERROR('No statistics were logged for this run.',10,1);
        RETURN;
    END;

    SET @msg = N'Logged ' + CONVERT(NVARCHAR(20),(SELECT COUNT(*) FROM #todo))
             + N' statistics entries for run ' + CONVERT(NVARCHAR(36),@RunId) + N'.';
    RAISERROR(@msg,10,1) WITH NOWAIT;

    -- WhatIf: just show a summary and the first few commands
    IF @WhatIf = 1
    BEGIN
        RAISERROR('WhatIf = 1; no UPDATE STATISTICS will be executed.',10,1) WITH NOWAIT;

        SELECT
            database_name,
            schema_name,
            table_name,
            stats_name,
            [rows],
            modification_counter,
            change_pct,
            last_updated,
            rows_sampled
        FROM DBA.UpdateStatsLog
        WHERE run_id = @RunId
        ORDER BY database_name, schema_name, table_name, stats_name;

        RETURN;
    END;


    -- Execute UPDATE STATISTICS commands with progress output
    DECLARE
        @log_id_exec     BIGINT,
        @db_exec         SYSNAME,
        @schema_exec     SYSNAME,
        @table_exec      SYSNAME,
        @stats_exec      SYSNAME,
        @cmd_exec        NVARCHAR(MAX),
        @DynamicExec_exec NVARCHAR(300);

    DECLARE cur_exec CURSOR LOCAL FAST_FORWARD FOR
        SELECT log_id, database_name, schema_name, table_name, stats_name, cmd
        FROM #todo
        ORDER BY database_name, schema_name, table_name, stats_name;

    OPEN cur_exec;
    FETCH NEXT FROM cur_exec
        INTO @log_id_exec, @db_exec, @schema_exec, @table_exec, @stats_exec, @cmd_exec;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @msg = N'Updating stats ' + QUOTENAME(@schema_exec) + N'.' + QUOTENAME(@table_exec)
                 + N'.' + QUOTENAME(@stats_exec)
                 + N' in database ' + QUOTENAME(@db_exec) + N'...';
        RAISERROR(@msg,10,1) WITH NOWAIT;

        SET @DynamicExec_exec = QUOTENAME(@db_exec) + N'.sys.sp_executesql';

        BEGIN TRY
            EXEC @DynamicExec_exec @cmd_exec;

            UPDATE DBA.UpdateStatsLog
               SET [status] = 'SUCCESS'
             WHERE log_id = @log_id_exec;

            SET @msg = N'SUCCESS updating stats ' + QUOTENAME(@schema_exec) + N'.' + QUOTENAME(@table_exec)
                     + N'.' + QUOTENAME(@stats_exec)
                     + N' in database ' + QUOTENAME(@db_exec) + N'.';
            RAISERROR(@msg,10,1) WITH NOWAIT;
        END TRY
        BEGIN CATCH
            UPDATE DBA.UpdateStatsLog
               SET [status]       = 'FAILED',
                   error_message  = ERROR_MESSAGE(),
                   error_number   = ERROR_NUMBER(),
                   error_severity = ERROR_SEVERITY(),
                   error_state    = ERROR_STATE(),
                   error_line     = ERROR_LINE(),
                   error_proc     = ERROR_PROCEDURE()
             WHERE log_id = @log_id_exec;

            SET @msg = N'FAILED updating stats ' + QUOTENAME(@schema_exec) + N'.' + QUOTENAME(@table_exec)
                     + N'.' + QUOTENAME(@stats_exec)
                     + N' in database ' + QUOTENAME(@db_exec)
                     + N': ' + CONVERT(NVARCHAR(4000), ERROR_MESSAGE());
            RAISERROR(@msg,10,1) WITH NOWAIT;
        END CATCH;

        FETCH NEXT FROM cur_exec
            INTO @log_id_exec, @db_exec, @schema_exec, @table_exec, @stats_exec, @cmd_exec;
    END;

    CLOSE cur_exec;
    DEALLOCATE cur_exec;

    SET @msg = N'Completed stats run ' + CONVERT(NVARCHAR(36),@RunId) + N'.';
    RAISERROR(@msg,10,1) WITH NOWAIT;
END;
