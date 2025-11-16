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

/********************************************************************************************************************/
/****** Name:        DBA.usp_UpdateStatisticsIfChanged                                                           ****/
/****** Purpose:     From a *single* utility DB, update stats in target DB(s) based on change %, or ALL_CHANGES. ****/
/******              Allows FULLSCAN, SAMPLED <n>% or default sampling. Central logging to DBA.UpdateStatsLog.   ****/
/******                                                                                                          ****/
/****** Input:       @Help                  = 1 prints help and examples; 0 runs normally                        ****/
/******              @TargetDatabases       = CSV of DBs | 'ALL_USER_DBS' with optional '-DbName' exclusions     ****/
/******              @ChangeThresholdPercent= when @ChangeScope IS NULL, update when change% >= this             ****/
/******              @ChangeScope           = 'ALL_CHANGES' (case-sensitive) to update all stats with changes    ****/
/******              @SampleMode            = 'FULLSCAN' | 'DEFAULT' | 'SAMPLED' (case-sensitive)                ****/
/******              @SamplePercent         = 1..100 when @SampleMode = 'SAMPLED' (rounded to int on SQL 2014)   ****/
/******              @LogDatabase           = NULL logs to this utility DB; else central log DB name             ****/
/******              @WhatIf                = 1 dry-run; 0 execute                                               ****/
/******                                                                                                          ****/
/****** Output:      Rows logged to [DBA].[UpdateStatsLog] with action, status, change%, command, and details.   ****/
/****** Created by:  Mike Fuller                                                                                 ****/
/****** Date Updated: 11/16/2025                                                                                 ****/
/****** Version:     1.3                                                                                         ****/
/********************************************************************************************************************/
ALTER PROCEDURE [DBA].[usp_UpdateStatisticsIfChanged]
      @Help                    BIT           = 0,
      @TargetDatabases         NVARCHAR(MAX) = NULL,   -- CSV | 'ALL_USER_DBS' (exact case) | with '-DbName' excludes
      @ChangeThresholdPercent  DECIMAL(6,2)  = NULL,   -- used when @ChangeScope IS NULL
      @ChangeScope             VARCHAR(20)   = NULL,   -- 'ALL_CHANGES' (exact case) or NULL
      @SampleMode              VARCHAR(12)   = 'DEFAULT', -- 'FULLSCAN' | 'DEFAULT' | 'SAMPLED' (exact case)
      @SamplePercent           DECIMAL(6,2)  = NULL,   -- 1..100 when @SampleMode='SAMPLED' (rounded to INT)
      @LogDatabase             SYSNAME       = NULL,   -- defaults to this utility DB if NULL
      @WhatIf                  BIT           = 1
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    /* ----- Help ----- */
    IF @Help = 1
    BEGIN
        SELECT param_name, sql_type, default_value, description, example
        FROM (VALUES
            (N'@TargetDatabases',        N'NVARCHAR(MAX)',  N'NULL',
                N'CSV or **ALL_USER_DBS** (exact case). Use ''-DbName'' to exclude.',
                N'@TargetDatabases = N''ALL_USER_DBS,-DBA,-UtilityDb'''),
            (N'@ChangeThresholdPercent', N'DECIMAL(6,2)',   N'NULL',
                N'Update when change% >= this (dm_db_stats_properties).',
                N'@ChangeThresholdPercent = 20.0'),
            (N'@ChangeScope',            N'VARCHAR(20)',    N'NULL',
                N'**ALL_CHANGES** to update any stats with changes; else uses threshold.',
                N'@ChangeScope = ''ALL_CHANGES'''),
            (N'@SampleMode',             N'VARCHAR(12)',    N'''DEFAULT''',
                N'**FULLSCAN**, **DEFAULT**, or **SAMPLED**.',
                N'@SampleMode = ''SAMPLED'''),
            (N'@SamplePercent',          N'DECIMAL(6,2)',   N'NULL',
                N'When @SampleMode=''SAMPLED'': 1..100 (rounded to whole % on SQL Server 2014).',
                N'@SamplePercent = 12.5'),
            (N'@LogDatabase',            N'SYSNAME',        N'NULL',
                N'Central log DB; default = this utility DB.',
                N'@LogDatabase = N''UtilityDb'''),
            (N'@WhatIf',                 N'BIT',            N'1',
                N'1=dry-run (log/return only); 0=execute.',
                N'@WhatIf = 0')
        ) d(param_name, sql_type, default_value, description, example);

        SELECT example_label, example_command FROM (VALUES
            (N'All user DBs except DBA, ALL_CHANGES, SAMPLED 20% (dry run)',
             N'EXEC DBA.usp_UpdateStatisticsIfChanged @TargetDatabases = N''ALL_USER_DBS,-DBA'', @ChangeScope = ''ALL_CHANGES'', @SampleMode = ''SAMPLED'', @SamplePercent = 20, @WhatIf = 1;'),
            (N'Just one DB, threshold 20%, DEFAULT (execute)',
             N'EXEC DBA.usp_UpdateStatisticsIfChanged @TargetDatabases = N''DBA'', @ChangeThresholdPercent = 20, @SampleMode = ''DEFAULT'', @WhatIf = 0;'),
            (N'Two DBs, FULLSCAN (dry run)',
             N'EXEC DBA.usp_UpdateStatisticsIfChanged @TargetDatabases = N''Orders,Inventory'', @SampleMode = ''FULLSCAN'', @WhatIf = 1;')
        ) x(example_label, example_command);
        RETURN;
    END

    /* ----- Validation (exact-case tokens) ----- */
    IF @TargetDatabases IS NULL OR LTRIM(RTRIM(@TargetDatabases)) = N''
    BEGIN RAISERROR('@TargetDatabases is required.',16,1); RETURN; END

    IF @SampleMode COLLATE Latin1_General_CS_AS NOT IN (N'FULLSCAN', N'DEFAULT', N'SAMPLED')
    BEGIN RAISERROR('@SampleMode must be ''FULLSCAN'', ''DEFAULT'', or ''SAMPLED'' (case-sensitive).',16,1); RETURN; END

    DECLARE @SamplePercentInt INT = NULL;
    IF @SampleMode COLLATE Latin1_General_CS_AS = N'SAMPLED'
    BEGIN
        IF @SamplePercent IS NULL OR @SamplePercent <= 0 OR @SamplePercent > 100
        BEGIN RAISERROR('When @SampleMode = ''SAMPLED'', @SamplePercent must be > 0 and <= 100.',16,1); RETURN; END
        SET @SamplePercentInt = CONVERT(INT, ROUND(@SamplePercent, 0));  -- SQL 2014 needs whole %
        IF @SamplePercentInt = 0 SET @SamplePercentInt = 1;
    END

    IF @ChangeScope IS NOT NULL
       AND @ChangeScope COLLATE Latin1_General_CS_AS <> N'ALL_CHANGES'
    BEGIN RAISERROR('@ChangeScope must be ''ALL_CHANGES'' or NULL.',16,1); RETURN; END

    IF @ChangeScope IS NULL
       AND ( @ChangeThresholdPercent IS NULL OR @ChangeThresholdPercent < 0 OR @ChangeThresholdPercent > 100 )
    BEGIN RAISERROR('When @ChangeScope is NULL, @ChangeThresholdPercent must be between 0 and 100.',16,1); RETURN; END

    /* ----- Resolve log DB & ensure table exists ----- */
    DECLARE @HomeDb SYSNAME = DB_NAME();                          -- this utility DB
    DECLARE @LogDb  SYSNAME = ISNULL(@LogDatabase, @HomeDb);
    DECLARE @qLogDb NVARCHAR(258) = QUOTENAME(@LogDb COLLATE DATABASE_DEFAULT);
    DECLARE @LogTable NVARCHAR(512) = @qLogDb + N'.[DBA].[UpdateStatsLog]';

    DECLARE @ddl NVARCHAR(MAX) = N'';
    SET @ddl += N'USE ' + @qLogDb + N';' + CHAR(10);
    SET @ddl += N'IF SCHEMA_ID(N''DBA'') IS NULL EXEC(''CREATE SCHEMA DBA AUTHORIZATION dbo'');' + CHAR(10);
    SET @ddl += N'IF OBJECT_ID(N''[DBA].[UpdateStatsLog]'',''U'') IS NULL
    BEGIN
        CREATE TABLE [DBA].[UpdateStatsLog]
        (
            log_id                 BIGINT IDENTITY(1,1) PRIMARY KEY,
            run_utc                DATETIME2(3)   NOT NULL CONSTRAINT DF_USL_run DEFAULT (SYSUTCDATETIME()),
            database_name          SYSNAME        NOT NULL,
            schema_name            SYSNAME        NOT NULL,
            table_name             SYSNAME        NOT NULL,
            stats_name             SYSNAME        NOT NULL,
            stats_id               INT            NOT NULL,
            [rows]                 BIGINT         NULL,
            modification_counter   BIGINT         NULL,
            change_pct             DECIMAL(9,4)   NULL,
            last_updated           DATETIME2(3)   NULL,
            rows_sampled           BIGINT         NULL,
            sample_mode            VARCHAR(12)    NOT NULL,
            [action]               VARCHAR(20)    NOT NULL,
            cmd                    NVARCHAR(MAX)  NOT NULL,
            [status]               VARCHAR(20)    NOT NULL,
            error_message          NVARCHAR(4000) NULL,
            error_number           INT            NULL,
            error_severity         INT            NULL,
            error_state            INT            NULL,
            error_line             INT            NULL,
            error_proc             NVARCHAR(128)  NULL,
            run_id                 UNIQUEIDENTIFIER NULL
        );
    END
    ELSE
    BEGIN
        IF COL_LENGTH(N''[DBA].[UpdateStatsLog]'', N''run_id'') IS NULL
            ALTER TABLE [DBA].[UpdateStatsLog] ADD run_id UNIQUEIDENTIFIER NULL;
    END
    IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N''[DBA].[UpdateStatsLog]'') AND name = N''IX_UpdateStatsLog_run'')
        CREATE INDEX IX_UpdateStatsLog_run ON [DBA].[UpdateStatsLog](run_id);';

    BEGIN TRY
        EXEC (@ddl);
    END TRY
    BEGIN CATCH
	    DECLARE @Err AS NVARCHAR(255) = ERROR_MESSAGE();
        RAISERROR('Failed to prepare [DBA].[UpdateStatsLog] in %s: %s', 16, 1, @LogDb, @Err); RETURN;
    END CATCH

    /* ----- Build target DB list from @TargetDatabases (supports ALL_USER_DBS and -DbName) ----- */
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
        END

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
        END
    END

    IF @HasAll = 1
    BEGIN
        INSERT #targets(db_name)
        SELECT d.name
        FROM sys.databases d
        WHERE d.name NOT IN (N'master',N'model',N'msdb',N'tempdb',N'distribution')
          AND d.state = 0
          AND d.is_read_only = 0;
    END

    INSERT #targets(db_name)
    SELECT i.db_name
    FROM #include AS i
    JOIN sys.databases d
      ON d.name = i.db_name COLLATE DATABASE_DEFAULT
    WHERE d.name NOT IN (N'master',N'model',N'msdb',N'tempdb',N'distribution')
      AND d.state = 0
      AND d.is_read_only = 0
      AND NOT EXISTS (SELECT 1 FROM #targets t WHERE t.db_name = i.db_name COLLATE DATABASE_DEFAULT);

    -- Apply excludes
    DELETE t
      FROM #targets t
      JOIN #exclude e
        ON e.db_name = t.db_name COLLATE DATABASE_DEFAULT;

    IF NOT EXISTS (SELECT 1 FROM #targets)
    BEGIN RAISERROR('No valid target databases resolved from @TargetDatabases.',16,1); RETURN; END

    /* ----- Execute per target DB ----- */
    DECLARE @db SYSNAME, @qDb NVARCHAR(258), @sql NVARCHAR(MAX);
    DECLARE @RunId UNIQUEIDENTIFIER = NEWID();  -- tag this run
    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR SELECT db_name FROM #targets ORDER BY db_name;
    OPEN cur; FETCH NEXT FROM cur INTO @db;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name=@db AND state=0 AND is_read_only=0)
        BEGIN
            RAISERROR('Skipping database "%s": not ONLINE and read-write.',10,1,@db) WITH NOWAIT;
            FETCH NEXT FROM cur INTO @db; CONTINUE;
        END

        SET @qDb = QUOTENAME(@db COLLATE DATABASE_DEFAULT);
        SET @sql = N'';

        -- Read candidates in the correct DB, then emit per-object commands that start with USE <DB>;
        SET @sql += N'USE ' + @qDb + N';' + CHAR(10);
        SET @sql += N'SET NOCOUNT ON; SET XACT_ABORT ON;' + CHAR(10);

        SET @sql += N'IF OBJECT_ID(''tempdb..#candidates'') IS NOT NULL DROP TABLE #candidates;' + CHAR(10);
        SET @sql += N'CREATE TABLE #candidates
                    (
                        schema_name           SYSNAME       NOT NULL,
                        table_name            SYSNAME       NOT NULL,
                        stats_name            SYSNAME       NOT NULL,
                        stats_id              INT           NOT NULL,
                        [rows]                BIGINT        NULL,
                        modification_counter  BIGINT        NULL,
                        change_pct            DECIMAL(9,4)  NULL,
                        last_updated          DATETIME2(3)  NULL,
                        rows_sampled          BIGINT        NULL,
                        cmd                   NVARCHAR(MAX) NOT NULL
                    );' + CHAR(10);

        SET @sql += N'INSERT #candidates
                    (
                        schema_name, table_name, stats_name, stats_id, [rows],
                        modification_counter, change_pct, last_updated, rows_sampled, cmd
                    )
                    SELECT
                        s.name  AS schema_name,
                        t.name  AS table_name,
                        st.name AS stats_name,
                        st.stats_id,
                        sp.[rows],
                        sp.modification_counter,
                        CAST(CASE
                                WHEN sp.[rows] IS NULL THEN NULL
                                WHEN sp.[rows] = 0 THEN CASE WHEN sp.modification_counter > 0 THEN 100.0 ELSE 0.0 END
                                ELSE (100.0 * CONVERT(DECIMAL(18,6), sp.modification_counter)) / NULLIF(CONVERT(DECIMAL(18,6), sp.[rows]),0)
                             END AS DECIMAL(9,4)) AS change_pct,
                        sp.last_updated,
                        sp.rows_sampled,
                        N''USE '' + QUOTENAME(DB_NAME()) + N''; '' +
                        N''UPDATE STATISTICS '' + QUOTENAME(s.name) + N''.'' + QUOTENAME(t.name) + N'' '' + QUOTENAME(st.name) +
                        CASE 
                            WHEN @pSampleMode COLLATE Latin1_General_CS_AS = N''FULLSCAN'' THEN N'' WITH FULLSCAN''
                            WHEN @pSampleMode COLLATE Latin1_General_CS_AS = N''SAMPLED''  THEN N'' WITH SAMPLE '' + CONVERT(NVARCHAR(12), @pSamplePercent) + N'' PERCENT''
                            ELSE N''''
                        END
                    FROM sys.stats     AS st
                    JOIN sys.tables    AS t  ON t.object_id = st.object_id
                    JOIN sys.schemas   AS s  ON s.schema_id = t.schema_id
                    LEFT JOIN sys.indexes AS i
                           ON i.object_id = st.object_id
                          AND i.index_id  = st.stats_id
                    CROSS APPLY sys.dm_db_stats_properties(st.object_id, st.stats_id) AS sp
                    WHERE t.is_ms_shipped = 0
                      AND t.is_memory_optimized = 0
                      AND (i.index_id IS NULL OR i.is_hypothetical = 0)
                      AND sp.modification_counter IS NOT NULL
                      AND (
                            (@pChangeScope COLLATE Latin1_General_CS_AS = N''ALL_CHANGES'' AND sp.modification_counter > 0)
                            OR
                            (@pChangeScope IS NULL AND (
                                  (sp.[rows] = 0 AND sp.modification_counter > 0)
                               OR ((100.0 * CONVERT(DECIMAL(18,6), sp.modification_counter)) / NULLIF(CONVERT(DECIMAL(18,6), sp.[rows]),0)) >= @pChangeThresholdPercent)
                            )
                          );' + CHAR(10);

        SET @sql += N'IF NOT EXISTS (SELECT 1 FROM #candidates)
                    BEGIN
                        PRINT ''No statistics met the criteria for '' + DB_NAME() + ''.'';
                        RETURN;
                    END;' + CHAR(10);

        SET @sql += N'IF OBJECT_ID(''tempdb..#todo'') IS NOT NULL DROP TABLE #todo;
                    CREATE TABLE #todo (log_id BIGINT PRIMARY KEY, cmd NVARCHAR(MAX) NOT NULL);' + CHAR(10);

        SET @sql += N'INSERT INTO ' + @LogTable + N'
                    (
                        database_name, schema_name, table_name, stats_name, stats_id,
                        [rows], modification_counter, change_pct, last_updated, rows_sampled,
                        sample_mode, [action], cmd, [status], run_id
                    )
                    SELECT
                        DB_NAME()                 AS database_name,
                        c.schema_name,
                        c.table_name,
                        c.stats_name,
                        c.stats_id,
                        c.[rows],
                        c.modification_counter,
                        c.change_pct,
                        c.last_updated,
                        c.rows_sampled,
                        @pSampleMode              AS sample_mode,
                        CASE WHEN @pWhatIf = 1 THEN ''DRYRUN'' ELSE ''UPDATE'' END AS [action],
                        c.cmd,
                        CASE WHEN @pWhatIf = 1 THEN ''SKIPPED'' ELSE ''PENDING'' END AS [status],
                        @pRunId                   AS run_id
                    FROM #candidates AS c;' + CHAR(10);

        SET @sql += N'IF OBJECT_ID(''tempdb..#exec'') IS NOT NULL DROP TABLE #exec;
                    CREATE TABLE #exec
                    (
                        rn       INT IDENTITY(1,1) PRIMARY KEY,
                        log_id   BIGINT        NOT NULL,
                        cmd      NVARCHAR(MAX) NOT NULL
                    );' + CHAR(10);

        SET @sql += N'INSERT #exec (log_id, cmd)
                    SELECT l.log_id, l.cmd
                    FROM ' + @LogTable + N' AS l
                    WHERE l.run_id = @pRunId
                    ORDER BY l.log_id;' + CHAR(10);

        SET @sql += N'DECLARE @i INT = 1, @imax INT = (SELECT COUNT(*) FROM #exec);
                    DECLARE @cmd NVARCHAR(MAX), @log_id BIGINT;
                    WHILE @i <= @imax
                    BEGIN
                        SELECT @cmd = cmd, @log_id = log_id FROM #exec WHERE rn = @i;
                        IF @pWhatIf = 0
                        BEGIN
                            BEGIN TRY
                                EXEC sys.sp_executesql @cmd;  -- starts with USE <DB>; then UPDATE STATISTICS
                                UPDATE ' + @LogTable + N' SET [status] = ''SUCCESS'' WHERE log_id = @log_id;
                            END TRY
                            BEGIN CATCH
                                UPDATE ' + @LogTable + N'
                                   SET [status]       = ''FAILED'',
                                       error_message  = ERROR_MESSAGE(),
                                       error_number   = ERROR_NUMBER(),
                                       error_severity = ERROR_SEVERITY(),
                                       error_state    = ERROR_STATE(),
                                       error_line     = ERROR_LINE(),
                                       error_proc     = ERROR_PROCEDURE()
                                 WHERE log_id = @log_id;
                            END CATCH;
                        END
                        ELSE
                        BEGIN
                            -- DRY RUN: leave status as SKIPPED
                        END
                        SET @i += 1;
                    END;' + CHAR(10);

        SET @sql += N'SELECT
                        l.[action],
                        l.[status],
                        (l.schema_name + N''.'' + l.table_name) AS [object_name],
                        l.stats_name,
                        COUNT(*) AS stats_updated
                    FROM ' + @LogTable + N' AS l
                    WHERE l.run_id = @pRunId
                    GROUP BY l.[action], l.[status], (l.schema_name + N''.'' + l.table_name), l.stats_name
                    ORDER BY stats_updated DESC, [object_name], stats_name;' + CHAR(10);

        EXEC sys.sp_executesql
            @sql,
            N'@pChangeThresholdPercent   DECIMAL(6,2),
              @pChangeScope              VARCHAR(20),
              @pSampleMode               VARCHAR(12),
              @pSamplePercent            INT,
              @pWhatIf                   BIT,
              @pRunId                    UNIQUEIDENTIFIER',
              @pChangeThresholdPercent = @ChangeThresholdPercent,
              @pChangeScope            = @ChangeScope,
              @pSampleMode             = @SampleMode,
              @pSamplePercent          = @SamplePercentInt,
              @pWhatIf                 = @WhatIf,
              @pRunId                  = @RunId;

        FETCH NEXT FROM cur INTO @db;
    END

    CLOSE cur; DEALLOCATE cur;
END