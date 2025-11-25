SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

IF SCHEMA_ID(N'DBA') IS NULL
    EXEC('CREATE SCHEMA DBA AUTHORIZATION dbo');
GO

IF OBJECT_ID('DBA.usp_RebuildIndexesIfBloated','P') IS NULL
    EXEC('CREATE PROCEDURE DBA.usp_RebuildIndexesIfBloated AS RETURN 0;');
GO
/*****************************************************************************************************************/
/****** Name:        DBA.usp_RebuildIndexesIfBloated                                                        ******/
/****** Purpose:     Rebuild only leaf-level ROWSTORE index partitions that are bloated by page density.    ******/
/******              Skips tiny indexes. Partition-aware. Optional ONLINE, MAXDOP, SORT_IN_TEMPDB,          ******/
/******              compression override, resumable, and low-priority waits.                               ******/
/******                                                                                                     ******/
/****** Input:       @Help                       = 1 prints help and examples; 0 runs normally              ******/
/******              @TargetDatabases            = CSV of DBs, or 'ALL_USER_DBS', with optional             ******/
/******                                              *exclusions* prefixed by '-' (e.g. '-DW,-ReportServer')******/
/******                                              System DBs (master, model, msdb, tempdb) and           ******/
/******                                              'distribution' are always excluded.                    ******/
/******              @MinPageDensityPct          = rebuild when avg page density < this (default 70.0)      ******/
/******              @MinPageCount               = skip partitions < this many pages (default 1000)         ******/
/******              @UseExistingFillFactor      = 1 keep per-index fill factor; 0 use @FillFactor          ******/
/******              @FillFactor                 = fill factor when above = 0 (1 to 100)                    ******/
/******              @Online                     = 1 ONLINE = ON when supported; 0 OFF                      ******/
/******              @MaxDOP                     = NULL omit; else MAXDOP value                             ******/
/******              @SortInTempdb               = 1 SORT_IN_TEMPDB = ON; 0 OFF                             ******/
/******              @UseCompressionFromSource   = 1 keep DATA_COMPRESSION from source; 0 override          ******/
/******              @ForceCompression           = NONE | ROW | PAGE when above = 0                         ******/
/******              @SampleMode                 = 'SAMPLED' | 'DETAILED'                                   ******/
/******              @CaptureTrendingSignals     = 1 ensures trending metrics; auto-SAMPLED ? DETAILED      ******/
/******              @LogDatabase                = NULL log in each target DB; else central log DB name     ******/
/******              @WaitAtLowPriorityMinutes   = minutes for WAIT_AT_LOW_PRIORITY (ONLINE only)           ******/
/******              @AbortAfterWait             = NONE | SELF | BLOCKERS (requires above)                  ******/
/******              @Resumable                  = 1 use RESUMABLE (ONLINE only; SQL 2019+); 0 off          ******/
/******              @MaxDurationMinutes         = MAX_DURATION minutes for resumable (optional)            ******/
/******              @DelayMsBetweenCommands     = optional delay in ms between rebuild commands            ******/
/******              @WhatIf                     = 1 dry-run; logs/prints only; 0 execute rebuilds          ******/
/******                                                                                                     ******/
/****** Output:      1) Messages: STARTING db, per-table candidate updates, COMPLETED db.                   ******/
/******              2) Result set per DB with candidate tables (#scan_out emitted via SELECT).             ******/
/******              3) Rows logged to [DBA].[IndexBloatRebuildLog] with action/status/details.             ******/
/****** Created by:  Mike Fuller                                                                            ******/
/****** Date Updated: 11/16/2025                                                                            ******/
/****** Version:     1.9.1                                                                                  ******/
/*****************************************************************************************************************/
ALTER PROCEDURE [DBA].[usp_RebuildIndexesIfBloated]
      @Help                       BIT          = 0,
      @TargetDatabases            NVARCHAR(MAX),            -- REQUIRED: CSV | 'ALL_USER_DBS' | negatives '-DbName'
      @MinPageDensityPct          DECIMAL(5,2) = 70.0,
      @MinPageCount               INT          = 1000,
      @UseExistingFillFactor      BIT          = 1,
      @FillFactor                 TINYINT      = NULL,
      @Online                     BIT          = 1,
      @MaxDOP                     INT          = NULL,
      @SortInTempdb               BIT          = 1,
      @UseCompressionFromSource   BIT          = 1,
      @ForceCompression           NVARCHAR(20) = NULL,
      @SampleMode                 VARCHAR(16)  = 'SAMPLED',  -- SAMPLED | DETAILED 
      @CaptureTrendingSignals     BIT          = 0,          -- if 1 and SampleMode=SAMPLED, auto-upshift to DETAILED
      @LogDatabase                SYSNAME      = NULL,
      @WaitAtLowPriorityMinutes   INT          = NULL,
      @AbortAfterWait             NVARCHAR(20) = NULL,       -- NONE | SELF | BLOCKERS
      @Resumable                  BIT          = 0,
      @MaxDurationMinutes         INT          = NULL,
      @DelayMsBetweenCommands     INT          = NULL,
      @WhatIf                     BIT          = 1
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    -- Help 
    IF @Help = 1
    BEGIN
        SELECT
            param_name, 
            sql_type, 
            default_value, 
            description, 
            example
        FROM (VALUES
              (N'@TargetDatabases',          N'NVARCHAR(MAX)',  N'(required)',     N'CSV list or **ALL_USER_DBS** (exact case). Supports exclusions via -DbName. System DBs and distribution are always excluded.', N'@TargetDatabases = N''ALL_USER_DBS,-DW,-ReportServer''')
            , (N'@MinPageDensityPct',        N'DECIMAL(5,2)',   N'70.0',           N'Rebuild when avg page density for a leaf partition is below this percent.',                                   N'65.0')
            , (N'@MinPageCount',             N'INT',            N'1000',           N'Skip tiny partitions below this page count.',                                                                 N'500')
            , (N'@UseExistingFillFactor',    N'BIT',            N'1',              N'Keep each index''s current fill factor. If 0, use @FillFactor.',                                              N'1')
            , (N'@FillFactor',               N'TINYINT',        N'NULL',           N'Fill factor when @UseExistingFillFactor = 0. Valid 1 to 100.',                                                N'90')
            , (N'@Online',                   N'BIT',            N'1',              N'Use ONLINE = ON when supported.',                                                                              N'1')
            , (N'@MaxDOP',                   N'INT',            N'NULL',           N'MAXDOP for rebuilds. If NULL, server default is used.',                                                       N'4')
            , (N'@SortInTempdb',             N'BIT',            N'1',              N'Use SORT_IN_TEMPDB.',                                                                                          N'1')
            , (N'@UseCompressionFromSource', N'BIT',            N'1',              N'Preserve DATA_COMPRESSION of each partition when supported.',                                                  N'1')
            , (N'@ForceCompression',         N'NVARCHAR(20)',   N'NULL',           N'Override compression for rowstore: NONE, ROW, or PAGE (when not preserving).',                                 N'N''ROW''')
            , (N'@SampleMode',               N'VARCHAR(16)',    N'''SAMPLED''',    N'dm_db_index_physical_stats mode: SAMPLED or DETAILED.',                                                        N'''DETAILED''')
            , (N'@CaptureTrendingSignals',   N'BIT',            N'0',              N'If 1 and SampleMode=SAMPLED, auto-upshift to DETAILED to capture row/ghost/forwarded metrics.',               N'1')
            , (N'@LogDatabase',              N'SYSNAME',        N'NULL',           N'Central log DB. If NULL, logs in each target DB.',                                                             N'N''UtilityDb''')
            , (N'@WaitAtLowPriorityMinutes', N'INT',            N'NULL',           N'Optional WAIT_AT_LOW_PRIORITY MAX_DURATION (ONLINE only).',                                                    N'5')
            , (N'@AbortAfterWait',           N'NVARCHAR(20)',   N'NULL',           N'ABORT_AFTER_WAIT: NONE, SELF, or BLOCKERS (requires minutes).',                                                N'N''BLOCKERS''')
            , (N'@Resumable',                N'BIT',            N'0',              N'RESUMABLE = ON for online rebuilds when supported (SQL 2019+).',                                               N'1')
            , (N'@MaxDurationMinutes',       N'INT',            N'NULL',           N'Resumable MAX_DURATION minutes.',                                                                              N'60')
            , (N'@DelayMsBetweenCommands',   N'INT',            N'NULL',           N'Optional delay between commands in milliseconds.',                                                             N'5000')
            , (N'@WhatIf',                   N'BIT',            N'1',              N'Dry run: log/print only.',                                                                                     N'0')
        ) d(param_name, sql_type, default_value, description, example)
        ORDER BY param_name;

        -- Effective behavior on this server 
        SELECT
            server_version_major = CAST(PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128)),4) AS INT),
            server_version_build = CAST(PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128)),2) AS INT), 
            edition              = CAST(SERVERPROPERTY('Edition') AS NVARCHAR(128)),
            supports_online      = CASE WHEN CAST(SERVERPROPERTY('Edition') AS NVARCHAR(128)) LIKE '%Enterprise%'
                                          OR CAST(SERVERPROPERTY('Edition') AS NVARCHAR(128)) LIKE '%Developer%'
                                          OR CAST(SERVERPROPERTY('Edition') AS NVARCHAR(128)) LIKE '%Evaluation%'
                                        THEN 1 ELSE 0 END, 
            supports_compression = CASE 
                                      WHEN CAST(PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128)),4) AS INT) > 13 THEN 1
                                      WHEN CAST(PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128)),4) AS INT) = 13
                                       AND CAST(PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128)),2) AS INT) >= 4000 THEN 1
                                      WHEN CAST(SERVERPROPERTY('Edition') AS NVARCHAR(128)) LIKE '%Enterprise%'
                                        OR CAST(SERVERPROPERTY('Edition') AS NVARCHAR(128)) LIKE '%Developer%'
                                        OR CAST(SERVERPROPERTY('Edition') AS NVARCHAR(128)) LIKE '%Evaluation%' THEN 1
                                      ELSE 0
                                   END,
            supports_resumable_rebuild = CASE 
                                            WHEN CAST(PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128)),4) AS INT) >= 15
                                                 AND (CAST(SERVERPROPERTY('Edition') AS NVARCHAR(128)) LIKE '%Enterprise%'
                                                   OR CAST(SERVERPROPERTY('Edition') AS NVARCHAR(128)) LIKE '%Developer%'
                                                   OR CAST(SERVERPROPERTY('Edition') AS NVARCHAR(128)) LIKE '%Evaluation%')
                                            THEN 1 ELSE 0
                                         END;

        PRINT 'Logged metrics for trending (captured per candidate partition):';
        SELECT 'avg_row_bytes','Average record size in bytes','If steady shape but rising, density likely degrades' UNION ALL
        SELECT 'record_count','Estimated rows','Context for density and growth' UNION ALL
        SELECT 'ghost_record_count','Deleted not yet cleaned','Delete churn + low density == bloat risk' UNION ALL
        SELECT 'forwarded_record_count','Heap pointer hops','Heap churn hurts scans/space' UNION ALL
        SELECT 'au_total_pages','Pages reserved in AUs','Capacity reserved' UNION ALL
        SELECT 'au_used_pages','Used pages in AUs','Active portion' UNION ALL
        SELECT 'au_data_pages','Row-data pages','Rowstore footprint' UNION ALL
        SELECT 'allocated_unused_pages','total - used','Allocated but unused headroom';
        
        -- Examples 
        SELECT example_label, example_command FROM
        (
            VALUES
              (N'All user DBs (dry run)', 
               N'EXEC DBA.usp_RebuildIndexesIfBloated @TargetDatabases = N''ALL_USER_DBS'', @WhatIf = 1;'),
              (N'All user DBs except DW & SSRS (execute, central log)', 
               N'EXEC DBA.usp_RebuildIndexesIfBloated @TargetDatabases = N''ALL_USER_DBS,-DW,-ReportServer,-ReportServerTempDB'', @LogDatabase = N''UtilityDb'', @WhatIf = 0;'),
              (N'Specific list (execute, resumable online)', 
               N'EXEC DBA.usp_RebuildIndexesIfBloated @TargetDatabases = N''Orders,Inventory,Finance'', @Online=1, @Resumable=1, @MaxDOP=4, @WhatIf = 0;'),
              (N'Single database via @TargetDatabases', 
               N'EXEC DBA.usp_RebuildIndexesIfBloated @TargetDatabases = N''SalesDb'', @WhatIf = 0;')
        ) X(example_label, example_command);
        RETURN;
    END

    -- Server/Edition capability detection 
    DECLARE @verMajor int = CAST(PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)),4) AS int);
    DECLARE @verBuild int = CAST(PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)),2) AS int);
    DECLARE @edition  nvarchar(128)= CAST(SERVERPROPERTY('Edition') AS nvarchar(128)) COLLATE DATABASE_DEFAULT;

    DECLARE @isEntDevEval bit = CASE WHEN @edition LIKE '%Enterprise%' OR @edition LIKE '%Developer%' OR @edition LIKE '%Evaluation%' THEN 1 ELSE 0 END;
    DECLARE @supportsOnline bit = @isEntDevEval; -- on-prem editions
    DECLARE @supportsCompression bit = CASE WHEN @verMajor > 13 OR (@verMajor = 13 AND @verBuild >= 4000) OR @isEntDevEval = 1 THEN 1 ELSE 0 END; 
    DECLARE @supportsResumableRebuild bit = CASE WHEN @verMajor >= 15 AND @supportsOnline = 1 THEN 1 ELSE 0 END; -- SQL 2019+ & ONLINE

    -- Effective sample mode with seatbelt 
    DECLARE @EffectiveSampleMode VARCHAR(16) = @SampleMode;
    IF @CaptureTrendingSignals = 1 AND UPPER(@EffectiveSampleMode) = 'SAMPLED'
        SET @EffectiveSampleMode = 'DETAILED';

    -- validation 
    IF @TargetDatabases IS NULL OR LTRIM(RTRIM(@TargetDatabases)) = N''
    BEGIN RAISERROR('@TargetDatabases is required.',16,1); RETURN; END

    IF @MinPageDensityPct IS NULL OR @MinPageDensityPct <= 0 OR @MinPageDensityPct >= 100
    BEGIN RAISERROR('@MinPageDensityPct must be between 0 and 100 (exclusive).',16,1); RETURN; END

    IF @MinPageCount IS NULL OR @MinPageCount <= 0
    BEGIN RAISERROR('@MinPageCount must be a positive integer.',16,1); RETURN; END

    IF @UseExistingFillFactor = 0 AND ( @FillFactor IS NULL OR @FillFactor NOT BETWEEN 1 AND 100 )
    BEGIN RAISERROR('When @UseExistingFillFactor = 0, @FillFactor must be 1-100.',16,1); RETURN; END

    IF @MaxDOP IS NOT NULL AND @MaxDOP NOT BETWEEN 0 AND 32767
    BEGIN RAISERROR('@MaxDOP must be between 0 and 32767.',16,1); RETURN; END

    IF @DelayMsBetweenCommands IS NOT NULL AND @DelayMsBetweenCommands < 0
    BEGIN RAISERROR('@DelayMsBetweenCommands must be >= 0.',16,1); RETURN; END

    IF UPPER(ISNULL(@SampleMode,'')) COLLATE DATABASE_DEFAULT NOT IN (N'SAMPLED' COLLATE DATABASE_DEFAULT,N'DETAILED' COLLATE DATABASE_DEFAULT)
    BEGIN RAISERROR('@SampleMode must be SAMPLED or DETAILED.',16,1); RETURN; END

    IF @WaitAtLowPriorityMinutes IS NOT NULL AND @WaitAtLowPriorityMinutes <= 0
    BEGIN RAISERROR('@WaitAtLowPriorityMinutes must be a positive integer when provided.',16,1); RETURN; END

    IF @WaitAtLowPriorityMinutes IS NOT NULL AND UPPER(ISNULL(@AbortAfterWait,'')) COLLATE DATABASE_DEFAULT NOT IN (N'NONE' COLLATE DATABASE_DEFAULT,N'SELF' COLLATE DATABASE_DEFAULT,N'BLOCKERS' COLLATE DATABASE_DEFAULT)
    BEGIN RAISERROR('@AbortAfterWait must be NONE, SELF, or BLOCKERS when @WaitAtLowPriorityMinutes is set.',16,1); RETURN; END

    IF @Resumable = 1 AND @Online = 0
    BEGIN RAISERROR('RESUMABLE requires @Online = 1.',16,1); RETURN; END

    DECLARE @IncludeOnlineOption bit = CASE WHEN @Online = 1 AND @supportsOnline = 1 THEN 1 ELSE 0 END; 
    IF @IncludeOnlineOption = 0
    BEGIN
        SET @Online = 0; 
        SET @WaitAtLowPriorityMinutes = NULL;
        SET @AbortAfterWait = NULL;
    END

    IF @supportsResumableRebuild = 0
    BEGIN
        SET @Resumable = 0;
        SET @MaxDurationMinutes = NULL;
    END

    DECLARE @IncludeDataCompressionOption bit = CASE WHEN @supportsCompression = 1 THEN 1 ELSE 0 END; 
    IF @IncludeDataCompressionOption = 0
    BEGIN
        SET @UseCompressionFromSource = 0;
        SET @ForceCompression = NULL;
    END
    ELSE IF @UseCompressionFromSource = 0 AND UPPER(ISNULL(@ForceCompression,'')) NOT IN ('NONE','ROW','PAGE')
    BEGIN
        RAISERROR('Invalid @ForceCompression for rowstore. Use NONE, ROW, or PAGE.',16,1); RETURN;
    END

    -- Parse targets
    IF OBJECT_ID('tempdb..#includes') IS NOT NULL DROP TABLE #includes;
    IF OBJECT_ID('tempdb..#excludes') IS NOT NULL DROP TABLE #excludes;
    IF OBJECT_ID('tempdb..#targets')  IS NOT NULL DROP TABLE #targets;
    CREATE TABLE #includes (name SYSNAME NOT NULL PRIMARY KEY);
    CREATE TABLE #excludes (name SYSNAME NOT NULL PRIMARY KEY);
    CREATE TABLE #targets  (db_name SYSNAME NOT NULL PRIMARY KEY);

    DECLARE @list NVARCHAR(MAX) = @TargetDatabases + N',';
    DECLARE @pos INT, @tok NVARCHAR(4000), @AllUsers BIT = 0;

    WHILE LEN(@list) > 0
    BEGIN
        SET @pos = CHARINDEX(N',' , @list);
        SET @tok = LTRIM(RTRIM(SUBSTRING(@list, 1, @pos - 1)));
        SET @list = SUBSTRING(@list, @pos + 1, 2147483647);

        IF @tok = N'' CONTINUE;

        IF @tok COLLATE Latin1_General_CS_AS = N'ALL_USER_DBS'
        BEGIN
            SET @AllUsers = 1;
            CONTINUE;
        END

        IF LEFT(@tok,1) = N'-'
        BEGIN
            SET @tok = LTRIM(RTRIM(SUBSTRING(@tok,2,4000)));
            IF LEN(@tok) > 0 AND NOT EXISTS (SELECT 1 FROM #excludes WHERE name = @tok)
                INSERT #excludes(name) VALUES(@tok);
            CONTINUE;
        END

        IF NOT EXISTS (SELECT 1 FROM #includes WHERE name = @tok)
            INSERT #includes(name) VALUES(@tok);
    END

    -- ALL_USER_DBS
    IF @AllUsers = 1
    BEGIN
        INSERT #targets(db_name)
        SELECT d.name
        FROM sys.databases AS d
        WHERE d.name NOT IN (N'master', N'model', N'msdb', N'tempdb', N'distribution') --skipping replication as well 
          AND d.state = 0
          AND d.is_read_only = 0;
    END

    -- Add explicit includes (single DB or CSV)
    INSERT #targets(db_name)
    SELECT 
        i.name
    FROM #includes AS i
    JOIN sys.databases AS d
      ON d.name = i.name COLLATE DATABASE_DEFAULT
    WHERE d.name NOT IN (N'master', N'model', N'msdb', N'tempdb', N'distribution')
      AND d.state = 0
      AND d.is_read_only = 0
      AND NOT EXISTS (SELECT 1 FROM #targets WHERE db_name = i.name);

    -- Apply explicit excludes
    DELETE t
    FROM #targets AS t
    JOIN #excludes AS x
      ON t.db_name = x.name COLLATE DATABASE_DEFAULT;

    IF NOT EXISTS (SELECT 1 FROM #targets)
    BEGIN
        RAISERROR('No valid target databases resolved after parsing @TargetDatabases.',16,1);
        RETURN;
    END

    --Iterate per target DB 
    DECLARE @db SYSNAME;
    DECLARE cur CURSOR LOCAL FAST_FORWARD FOR SELECT db_name FROM #targets ORDER BY db_name;
    OPEN cur;
    FETCH NEXT FROM cur INTO @db;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = @db AND state = 0 AND is_read_only = 0)
        BEGIN
            RAISERROR('Skipping database "%s": not ONLINE and read-write.',10,1,@db) WITH NOWAIT;
            FETCH NEXT FROM cur INTO @db;
            CONTINUE;
        END

        -- STARTING message for this DB
        RAISERROR(N'===== STARTING database: [%s] =====', 10, 1, @db) WITH NOWAIT;

        DECLARE @qDb NVARCHAR(258) = QUOTENAME(@db COLLATE DATABASE_DEFAULT);
        DECLARE @LogDb SYSNAME = ISNULL(@LogDatabase, @db);
        DECLARE @qLogDb NVARCHAR(258) = QUOTENAME(@LogDb COLLATE DATABASE_DEFAULT);

        -- Ensure log table exists for this target's chosen log DB
        DECLARE @ddl NVARCHAR(MAX) =
        N'USE ' + @qLogDb + N';
          IF SCHEMA_ID(N''DBA'') IS NULL EXEC(''CREATE SCHEMA DBA AUTHORIZATION dbo'');
          IF OBJECT_ID(N''[DBA].[IndexBloatRebuildLog]'', N''U'') IS NULL
          BEGIN
              CREATE TABLE [DBA].[IndexBloatRebuildLog]
              (
                  log_id               BIGINT IDENTITY(1,1) PRIMARY KEY,
                  run_utc              DATETIME2(3)   NOT NULL CONSTRAINT DF_IBRL_run DEFAULT (SYSUTCDATETIME()),
                  database_name        SYSNAME        NOT NULL,
                  schema_name          SYSNAME        NOT NULL,
                  table_name           SYSNAME        NOT NULL,
                  index_name           SYSNAME        NOT NULL,
                  index_id             INT            NOT NULL,
                  partition_number     INT            NOT NULL,
                  page_count           BIGINT         NOT NULL,
                  page_density_pct     DECIMAL(6,2)   NOT NULL,
                  fragmentation_pct    DECIMAL(6,2)   NOT NULL,
                  chosen_fill_factor   INT            NULL,
                  online_on            BIT            NOT NULL,
                  maxdop_used          INT            NULL,
                  avg_row_bytes        DECIMAL(18,2)  NULL,
                  record_count         BIGINT         NULL,
                  ghost_record_count   BIGINT         NULL,
                  forwarded_record_count BIGINT       NULL,
                  au_total_pages       BIGINT         NULL,
                  au_used_pages        BIGINT         NULL,
                  au_data_pages        BIGINT         NULL,
                  [action]             VARCHAR(20)    NOT NULL,
                  cmd                  NVARCHAR(MAX)  NOT NULL,
                  [status]             VARCHAR(20)    NOT NULL,
                  error_message        NVARCHAR(4000) NULL,
                  error_number         INT            NULL,
                  error_severity       INT            NULL,
                  error_state          INT            NULL,
                  error_line           INT            NULL,
                  error_proc           NVARCHAR(128)  NULL
              );
          END
          -- Upgrade: ensure columns
          IF COL_LENGTH(N''[DBA].[IndexBloatRebuildLog]'', N''avg_row_bytes'') IS NULL
               ALTER TABLE [DBA].[IndexBloatRebuildLog] ADD avg_row_bytes DECIMAL(18,2) NULL;
          IF COL_LENGTH(N''[DBA].[IndexBloatRebuildLog]'', N''record_count'') IS NULL
               ALTER TABLE [DBA].[IndexBloatRebuildLog] ADD record_count BIGINT NULL;
          IF COL_LENGTH(N''[DBA].[IndexBloatRebuildLog]'', N''ghost_record_count'') IS NULL
               ALTER TABLE [DBA].[IndexBloatRebuildLog] ADD ghost_record_count BIGINT NULL;
          IF COL_LENGTH(N''[DBA].[IndexBloatRebuildLog]'', N''forwarded_record_count'') IS NULL
               ALTER TABLE [DBA].[IndexBloatRebuildLog] ADD forwarded_record_count BIGINT NULL;
          IF COL_LENGTH(N''[DBA].[IndexBloatRebuildLog]'', N''au_total_pages'') IS NULL
               ALTER TABLE [DBA].[IndexBloatRebuildLog] ADD au_total_pages BIGINT NULL;
          IF COL_LENGTH(N''[DBA].[IndexBloatRebuildLog]'', N''au_used_pages'') IS NULL
               ALTER TABLE [DBA].[IndexBloatRebuildLog] ADD au_used_pages BIGINT NULL;
          IF COL_LENGTH(N''[DBA].[IndexBloatRebuildLog]'', N''au_data_pages'') IS NULL
               ALTER TABLE [DBA].[IndexBloatRebuildLog] ADD au_data_pages BIGINT NULL;
          IF COL_LENGTH(N''[DBA].[IndexBloatRebuildLog]'', N''allocated_unused_pages'') IS NOT NULL
             AND ISNULL(COLUMNPROPERTY(OBJECT_ID(N''[DBA].[IndexBloatRebuildLog]''), N''allocated_unused_pages'', ''IsComputed''),0) = 0
          BEGIN
              ALTER TABLE [DBA].[IndexBloatRebuildLog] DROP COLUMN allocated_unused_pages;
          END
          IF COL_LENGTH(N''[DBA].[IndexBloatRebuildLog]'', N''allocated_unused_pages'') IS NULL
          BEGIN
              EXEC(N''ALTER TABLE [DBA].[IndexBloatRebuildLog] ADD allocated_unused_pages AS (au_total_pages - au_used_pages);'');
          END';

        BEGIN TRY
            EXEC (@ddl);
        END TRY
        BEGIN CATCH
            DECLARE @Err NVARCHAR(255) = ERROR_MESSAGE();
            RAISERROR('Failed to prepare logging table in %s: %s', 16, 1, @LogDb, @Err); 
            RAISERROR(N'===== COMPLETED database (with errors): [%s] =====', 10, 1, @db) WITH NOWAIT;
            FETCH NEXT FROM cur INTO @db;
            CONTINUE;
        END CATCH

        -- Core per-DB logic with progress
		DECLARE @sql NVARCHAR(MAX) =
		N'USE ' + @qDb + N';
		SET NOCOUNT ON;
		SET XACT_ABORT ON;
		SET DEADLOCK_PRIORITY LOW;

		-- One message variable for the whole batch
		DECLARE @msg NVARCHAR(4000);

		-- Candidate collection
		IF OBJECT_ID(''tempdb..#candidates'') IS NOT NULL DROP TABLE #candidates;
		CREATE TABLE #candidates
		(
			schema_name         SYSNAME       NOT NULL,
			table_name          SYSNAME       NOT NULL,
			index_name          SYSNAME       NOT NULL,
			index_id            INT           NOT NULL,
			partition_number    INT           NOT NULL,
			page_count          BIGINT        NOT NULL,
			page_density_pct    DECIMAL(6,2)  NOT NULL,
			fragmentation_pct   DECIMAL(6,2)  NOT NULL,
			avg_row_bytes       DECIMAL(18,2) NOT NULL,
			record_count        BIGINT        NOT NULL,
			ghost_record_count  BIGINT        NOT NULL,
			fwd_record_count    BIGINT        NOT NULL,
			au_total_pages      BIGINT        NOT NULL,
			au_used_pages       BIGINT        NOT NULL,
			au_data_pages       BIGINT        NOT NULL,
			compression_desc    NVARCHAR(60)  NOT NULL,
			chosen_fill_factor  INT           NULL,
			is_partitioned      BIT           NOT NULL,
			is_filtered         BIT           NOT NULL,
			has_included_lob    BIT           NOT NULL,
			has_key_blocker     BIT           NOT NULL,
			resumable_supported BIT           NOT NULL,
			cmd                 NVARCHAR(MAX) NOT NULL
		);

		DECLARE @mode VARCHAR(16) = CASE WHEN UPPER(@pSampleMode COLLATE DATABASE_DEFAULT) = N''DETAILED'' THEN N''DETAILED'' ELSE N''SAMPLED'' END;

		INSERT INTO #candidates
		(
			schema_name,  
            table_name, 
            index_name, 
            index_id, 
            partition_number, 
            page_count, 
            page_density_pct,
			fragmentation_pct, 
            avg_row_bytes, 
            record_count, 
            ghost_record_count, 
            fwd_record_count,
			au_total_pages, 
            au_used_pages, 
            au_data_pages,
            compression_desc, 
            chosen_fill_factor,
			is_partitioned, 
            is_filtered, 
            has_included_lob, 
            has_key_blocker, 
            resumable_supported, 
            cmd
		)
		SELECT
            s.name, 
            t.name, 
            i.name, 
            i.index_id,
            ps.partition_number, 
            ps.page_count,
            ps.avg_page_space_used_in_percent, 
            ps.avg_fragmentation_in_percent,
            COALESCE(CAST(ps.avg_record_size_in_bytes AS DECIMAL(18,2)), 0),
            COALESCE(ps.record_count, 0), 
            COALESCE(ps.ghost_record_count, 0), 
            COALESCE(ps.forwarded_record_count, 0),
            COALESCE(SUM(au.total_pages),0), 
            COALESCE(SUM(au.used_pages),0), 
            COALESCE(SUM(au.data_pages),0),
            p.data_compression_desc,
            CASE WHEN @pUseExistingFillFactor = 1 THEN NULLIF(i.fill_factor, 0) ELSE @pFillFactor END,
            CASE WHEN psch.data_space_id IS NULL THEN 0 ELSE 1 END,
            i.has_filter,
            blockers.has_included_lob,
            blockers.has_key_blocker,
            rs.resumable_supported,
            (
            N''ALTER INDEX '' + QUOTENAME(i.name) +
            N'' ON '' + QUOTENAME(s.name) + N''.'' + QUOTENAME(t.name) +
            CASE WHEN psch.data_space_id IS NULL THEN N'' REBUILD '' ELSE N'' REBUILD PARTITION = '' + CONVERT(VARCHAR(12), ps.partition_number) + N'' '' END +
            N''WITH (SORT_IN_TEMPDB = '' +
            CASE WHEN @pOnline = 1 AND @pResumable = 1 AND rs.resumable_supported = 1 THEN N''OFF'' ELSE CASE WHEN @pSortInTempdb = 1 THEN N''ON'' ELSE N''OFF'' END END +
            CASE WHEN (CASE WHEN @pUseExistingFillFactor = 1 THEN NULLIF(i.fill_factor,0) ELSE @pFillFactor END) IS NOT NULL
                    THEN N'', FILLFACTOR = '' + CONVERT(VARCHAR(4), (CASE WHEN @pUseExistingFillFactor = 1 THEN NULLIF(i.fill_factor,0) ELSE @pFillFactor END))
                    ELSE N'''' END +
            CASE WHEN @pIncludeOnlineOption = 1 AND @pOnline = 1
                    THEN N'', ONLINE = ON'' + CASE WHEN @pWaitAtLowPriorityMinutes IS NOT NULL
                                                THEN N'' (WAIT_AT_LOW_PRIORITY (MAX_DURATION = '' + CONVERT(VARCHAR(4), @pWaitAtLowPriorityMinutes) + N'' MINUTES, ABORT_AFTER_WAIT = '' + (@pAbortAfterWait COLLATE DATABASE_DEFAULT) + N''))''
                                                ELSE N'''' END
                    ELSE N'''' END +
            CASE WHEN @pMaxDOP IS NOT NULL THEN N'', MAXDOP = '' + CONVERT(VARCHAR(5), @pMaxDOP) ELSE N'''' END +
            CASE WHEN @pIncludeDataCompressionOption = 1 
                    THEN N'', DATA_COMPRESSION = '' + CASE WHEN @pUseCompressionFromSource = 1 THEN p.data_compression_desc COLLATE DATABASE_DEFAULT ELSE (@pForceCompression COLLATE DATABASE_DEFAULT) END
                    ELSE N'''' END +
            CASE WHEN @pOnline = 1 AND @pResumable = 1 AND rs.resumable_supported = 1 THEN N'', RESUMABLE = ON'' ELSE N'''' END +
            CASE WHEN @pOnline = 1 AND @pResumable = 1 AND rs.resumable_supported = 1 AND @pMaxDurationMinutes IS NOT NULL
                    THEN N'', MAX_DURATION = '' + CONVERT(VARCHAR(4), @pMaxDurationMinutes) + N'' MINUTES'' ELSE N'''' END +
            N'')'' +
            CASE WHEN @pOnline = 1 AND @pResumable = 1 AND rs.resumable_supported = 0 THEN N'' /* downgraded: filtered and/or included LOB and/or computed/rowversion key */'' ELSE N'''' END
            )
		FROM sys.indexes AS i
		JOIN sys.tables  AS t 
            ON t.object_id = i.object_id
		JOIN sys.schemas AS s 
            ON s.schema_id = t.schema_id
		JOIN sys.partitions AS p 
            ON p.object_id = i.object_id 
            AND p.index_id = i.index_id
		JOIN sys.data_spaces AS ds 
            ON ds.data_space_id = i.data_space_id
		LEFT JOIN sys.partition_schemes AS psch 
            ON psch.data_space_id = ds.data_space_id
		JOIN sys.allocation_units AS au 
            ON au.container_id = p.hobt_id 
            AND au.type IN (1, 3)
		CROSS APPLY
		(
			SELECT
				has_included_lob = CASE WHEN EXISTS
				(
					SELECT 1 
                    FROM sys.index_columns ic
					JOIN sys.columns c 
                        ON c.object_id = ic.object_id 
                        AND c.column_id = ic.column_id
					WHERE ic.object_id = i.object_id 
                        AND ic.index_id = i.index_id
						AND ic.is_included_column = 1
						AND (c.max_length = -1 OR c.system_type_id IN (34,35,99,241))
				) THEN 1 ELSE 0 END,
				has_key_blocker = CASE WHEN EXISTS
				(
					SELECT 1 
                    FROM sys.index_columns ic
					JOIN sys.columns c 
                        ON c.object_id = ic.object_id 
                        AND c.column_id = ic.column_id
					WHERE ic.object_id = i.object_id 
                        AND ic.index_id = i.index_id
						AND ic.key_ordinal > 0 AND (c.is_computed = 1 OR c.system_type_id = 189)
				) THEN 1 ELSE 0 END
		) AS blockers
		CROSS APPLY
		(
			SELECT resumable_supported = CASE WHEN i.has_filter = 0 AND blockers.has_included_lob = 0 AND blockers.has_key_blocker = 0 THEN 1 ELSE 0 END
		) AS rs
		CROSS APPLY sys.dm_db_index_physical_stats(DB_ID(), i.object_id, i.index_id, p.partition_number, @mode) AS ps
		WHERE
			i.index_id > 0
			AND i.type IN (1,2)
			AND i.is_hypothetical = 0
			AND i.is_disabled = 0
			AND ps.index_level = 0
			AND ps.page_count >= @pMinPageCount
			AND ps.avg_page_space_used_in_percent < @pMinPageDensityPct
			AND t.is_ms_shipped = 0
			AND t.is_memory_optimized = 0
			AND (@pIncludeDataCompressionOption = 1 OR p.data_compression = 0)
		GROUP BY
			s.name, 
            t.name, 
            i.name, 
            i.index_id, 
            ps.partition_number, 
            ps.page_count,
			ps.avg_page_space_used_in_percent, 
            ps.avg_fragmentation_in_percent,
			ps.avg_record_size_in_bytes, 
            ps.record_count, 
            ps.ghost_record_count, 
            ps.forwarded_record_count,
			p.data_compression_desc, 
            i.fill_factor, 
            psch.data_space_id, 
            i.has_filter,
			blockers.has_included_lob, 
            blockers.has_key_blocker, 
            rs.resumable_supported
		OPTION (RECOMPILE);

		-- Progress: summarize & print per-table
		DECLARE @cand_count INT = (SELECT COUNT(*) FROM #candidates);
		IF @cand_count = 0
		BEGIN
			SET @msg = N''No bloated rowstore index partitions met the thresholds for '' + QUOTENAME(DB_NAME()) + N''.'';
			;RAISERROR(@msg, 10, 1) WITH NOWAIT;
		END
		ELSE
		BEGIN
			IF OBJECT_ID(''tempdb..#scan_out'') IS NOT NULL DROP TABLE #scan_out;
			SELECT 
				database_name = DB_NAME(),
				schema_name,
				table_name,
				candidate_partitions = COUNT(*),
				min_density_pct = MIN(page_density_pct),
				avg_density_pct = CAST(AVG(page_density_pct) AS DECIMAL(6,2)),
				max_pages = MAX(page_count),
				total_pages = SUM(page_count)
			INTO #scan_out
			FROM #candidates
			GROUP BY 
                schema_name, 
                table_name;

			DECLARE @tbl_count INT = (SELECT COUNT(*) FROM #scan_out);
			SET @msg = N''Found '' + CONVERT(NVARCHAR(20), @cand_count)
						+ N'' candidate partition(s) across '' + CONVERT(NVARCHAR(20), @tbl_count)
						+ N'' table(s) in '' + QUOTENAME(DB_NAME()) + N''.'';
			;RAISERROR(@msg, 10, 1) WITH NOWAIT;

			-- Per-table live updates (reuse @msg — do NOT redeclare)
			DECLARE @s SYSNAME, @t SYSNAME, @minD DECIMAL(6,2), @totP BIGINT, @parts INT;
			DECLARE c_tbl CURSOR LOCAL FAST_FORWARD FOR
				SELECT 
                    schema_name, 
                    table_name, 
                    min_density_pct, 
                    total_pages, 
                    candidate_partitions
				FROM #scan_out
				ORDER BY 
                    min_density_pct ASC, 
                    total_pages DESC;
			OPEN c_tbl;
			FETCH NEXT FROM c_tbl INTO @s, @t, @minD, @totP, @parts;
			WHILE @@FETCH_STATUS = 0
			BEGIN
				SET @msg = N''  -> Candidate: '' + QUOTENAME(@s) + N''.'' + QUOTENAME(@t)
						 + N'' | min_density='' + CONVERT(NVARCHAR(32), CONVERT(DECIMAL(6,2), @minD)) + N''%%''
						 + N'' | total_pages='' + CONVERT(NVARCHAR(32), @totP)
						 + N'' | partitions='' + CONVERT(NVARCHAR(32), @parts);
				;RAISERROR(@msg, 10, 1) WITH NOWAIT;

				FETCH NEXT FROM c_tbl INTO @s, @t, @minD, @totP, @parts;
			END
			CLOSE c_tbl; DEALLOCATE c_tbl;

			-- Emit result set so caller can capture candidate tables
			SELECT * FROM #scan_out ORDER BY min_density_pct ASC, total_pages DESC;
		END

		-- Optional resumable note
		IF @pResumable = 1
		BEGIN
			DECLARE @downgraded INT = (SELECT COUNT(*) FROM #candidates WHERE resumable_supported = 0);
			IF @downgraded > 0
			BEGIN
				SET @msg = N''Note: '' + CONVERT(NVARCHAR(20), @downgraded) 
							+ N'' candidate(s) downgraded from RESUMABLE due to filtered and/or included LOB and/or computed/rowversion key.'';
				;RAISERROR(@msg, 10, 1) WITH NOWAIT;
			END
		END

		-- Logging + execution (unchanged except reuse @msg and prefix ; before RAISERROR)
		IF OBJECT_ID(''tempdb..#todo'') IS NOT NULL DROP TABLE #todo;
		CREATE TABLE #todo (log_id BIGINT PRIMARY KEY, cmd NVARCHAR(MAX) NOT NULL);

		;WITH to_log AS
		(
			SELECT
                DB_NAME() AS database_name,
                c.schema_name, 
                c.table_name, 
                c.index_name, 
                c.index_id, 
                c.partition_number,
                c.page_count, 
                c.page_density_pct, 
                c.fragmentation_pct,
                c.avg_row_bytes, 
                c.record_count, 
                c.ghost_record_count, 
                c.fwd_record_count,
                c.au_total_pages, 
                c.au_used_pages, 
                c.au_data_pages,
                c.chosen_fill_factor,
                @pOnline AS online_on,
                @pMaxDOP AS maxdop_used,
                CASE WHEN @pWhatIf = 1 THEN ''DRYRUN'' ELSE ''REBUILD'' END AS [action],
                c.cmd AS cmd,
                CASE WHEN @pWhatIf = 1 THEN ''SKIPPED'' ELSE ''PENDING'' END AS [status]
			FROM #candidates AS c
		)
		INSERT INTO ' + @qLogDb + N'.[DBA].[IndexBloatRebuildLog]
		(
			database_name, 
            schema_name, 
            table_name, 
            index_name, 
            index_id, 
            partition_number,
			page_count, 
            page_density_pct, 
            fragmentation_pct,
			avg_row_bytes, 
            record_count, 
            ghost_record_count, 
            forwarded_record_count,
			au_total_pages, 
            au_used_pages, 
            au_data_pages,
			chosen_fill_factor, 
            online_on, 
            maxdop_used, 
            [action], 
            cmd, 
            [status]
		)
		OUTPUT inserted.log_id, inserted.cmd INTO #todo(log_id, cmd)
		SELECT
			database_name, 
            schema_name,
            table_name, 
            index_name, 
            index_id, 
            partition_number,
			page_count, 
            page_density_pct, 
            fragmentation_pct,
			avg_row_bytes, 
            record_count, 
            ghost_record_count, 
            fwd_record_count,
			au_total_pages, 
            au_used_pages, 
            au_data_pages,
			chosen_fill_factor, 
            online_on, 
            maxdop_used, 
            [action], 
            cmd, 
            [status]
		FROM to_log;

		IF NOT EXISTS (SELECT 1 FROM #todo)
		BEGIN
			RETURN;
		END

		IF @pWhatIf = 1
		BEGIN
			SELECT
				l.log_id,
				l.schema_name COLLATE DATABASE_DEFAULT AS schema_name,
				l.table_name COLLATE DATABASE_DEFAULT AS table_name,
				l.index_name COLLATE DATABASE_DEFAULT AS index_name,
				l.partition_number,
				l.page_density_pct,
				l.page_count,
				l.avg_row_bytes,
				l.record_count,
				l.ghost_record_count,
				l.forwarded_record_count,
				l.au_total_pages,
				l.au_used_pages,
				l.au_data_pages,
				allocated_unused_pages = (l.au_total_pages - l.au_used_pages),
				l.cmd
			FROM ' + @qLogDb + N'.[DBA].[IndexBloatRebuildLog] AS l
			WHERE l.log_id IN (SELECT log_id FROM #todo)
			ORDER BY l.page_density_pct ASC, l.page_count DESC;
			RETURN;
		END

		IF OBJECT_ID(''tempdb..#exec'') IS NOT NULL DROP TABLE #exec;
		CREATE TABLE #exec
		(
			rn               INT IDENTITY(1,1) PRIMARY KEY,
			log_id           BIGINT        NOT NULL,
			cmd              NVARCHAR(MAX) NOT NULL,
			schema_name      SYSNAME       NOT NULL,
			table_name       SYSNAME       NOT NULL,
			index_name       SYSNAME       NOT NULL,
			partition_number INT           NOT NULL,
			page_count       BIGINT        NOT NULL
		);

		INSERT #exec (log_id, cmd, schema_name, table_name, index_name, partition_number, page_count)
		SELECT 
            t.log_id, 
            t.cmd,
		    l.schema_name COLLATE DATABASE_DEFAULT,
			l.table_name COLLATE DATABASE_DEFAULT,
			l.index_name COLLATE DATABASE_DEFAULT,
			l.partition_number, 
            l.page_count
		FROM #todo AS t
		JOIN ' + @qLogDb + N'.[DBA].[IndexBloatRebuildLog] AS l
			ON l.log_id = t.log_id
		ORDER BY 
            l.page_density_pct ASC, 
            l.page_count DESC;

		DECLARE @i INT = 1, @imax INT = (SELECT COUNT(*) FROM #exec);
		DECLARE @cmd NVARCHAR(MAX), @log_id BIGINT, @schema SYSNAME, @table SYSNAME, @index SYSNAME, @part INT, @pages BIGINT;

		DECLARE @delay NVARCHAR(16) = NULL;
		IF @pDelayMsBetweenCommands IS NOT NULL
		BEGIN
			SET @delay = RIGHT(''00'' + CONVERT(VARCHAR(2), (@pDelayMsBetweenCommands/3600000) % 24),2) + '':'' 
					   + RIGHT(''00'' + CONVERT(VARCHAR(2), (@pDelayMsBetweenCommands/60000) % 60),2) + '':'' 
					   + RIGHT(''00'' + CONVERT(VARCHAR(2), (@pDelayMsBetweenCommands/1000) % 60),2) + ''.'' 
					   + RIGHT(''000'' + CONVERT(VARCHAR(3), @pDelayMsBetweenCommands % 1000),3);
		END

		WHILE @i <= @imax
		BEGIN
			SELECT @cmd = cmd, @log_id = log_id, @schema = schema_name, @table = table_name, @index = index_name, @part = partition_number, @pages = page_count
			FROM #exec WHERE rn = @i;

			SET @msg = N''Rebuilding '' + QUOTENAME(@schema) + N''.'' + QUOTENAME(@table) + N''.'' + QUOTENAME(@index)
						+ N'' (partition '' + CONVERT(NVARCHAR(12), @part) + N'', pages = '' + CONVERT(NVARCHAR(20), @pages) + N'')'';
			;RAISERROR(@msg, 10, 1) WITH NOWAIT;

			BEGIN TRY
				EXEC sys.sp_executesql @cmd;

				UPDATE ' + @qLogDb + N'.[DBA].[IndexBloatRebuildLog]
					SET [status] = ''SUCCESS''
					WHERE log_id = @log_id;

				SET @msg = N''SUCCESS  '' + QUOTENAME(@schema) + N''.'' + QUOTENAME(@table) + N''.'' + QUOTENAME(@index)
							+ N'' (partition '' + CONVERT(NVARCHAR(12), @part) + N'')'';
				;RAISERROR(@msg, 10, 1) WITH NOWAIT;
			END TRY
			BEGIN CATCH
				UPDATE ' + @qLogDb + N'.[DBA].[IndexBloatRebuildLog]
					SET [status] = ''FAILED'',
						error_message = ERROR_MESSAGE(),
						error_number = ERROR_NUMBER(),
						error_severity = ERROR_SEVERITY(),
						error_state = ERROR_STATE(),
						error_line = ERROR_LINE(),
						error_proc = ERROR_PROCEDURE()
					WHERE log_id = @log_id;

				SET @msg = N''FAILED   '' + QUOTENAME(@schema) + N''.'' + QUOTENAME(@table) + N''.'' + QUOTENAME(@index)
							+ N'' (partition '' + CONVERT(NVARCHAR(12), @part) + N''): '' + CONVERT(NVARCHAR(4000), ERROR_MESSAGE());
				;RAISERROR(@msg, 10, 1) WITH NOWAIT;
			END CATCH;

			IF @delay IS NOT NULL WAITFOR DELAY @delay;
			SET @i += 1;
		END

		SELECT 
			l.[action],
			l.[status],
			(l.schema_name COLLATE DATABASE_DEFAULT + N''.'' + l.table_name COLLATE DATABASE_DEFAULT) AS [object_name],
			l.index_name COLLATE DATABASE_DEFAULT AS index_name,
			COUNT(*) AS partitions_affected,
			SUM(l.page_count) AS total_pages,
			MIN(l.page_density_pct) AS min_density,
			AVG(l.page_density_pct) AS avg_density
		FROM ' + @qLogDb + N'.[DBA].[IndexBloatRebuildLog] AS l
		WHERE l.log_id IN (SELECT log_id FROM #todo)
		GROUP BY 
            l.[action], 
            l.[status], 
            (l.schema_name COLLATE DATABASE_DEFAULT + N''.'' + l.table_name COLLATE DATABASE_DEFAULT), 
            l.index_name COLLATE DATABASE_DEFAULT
		ORDER BY total_pages DESC, [object_name], index_name;';


        EXEC sys.sp_executesql
            @sql,
            N'@pMinPageDensityPct DECIMAL(5,2),
              @pMinPageCount INT,
              @pUseExistingFillFactor BIT,
              @pFillFactor TINYINT,
              @pOnline BIT,
              @pMaxDOP INT,
              @pSortInTempdb BIT,
              @pUseCompressionFromSource BIT,
              @pForceCompression NVARCHAR(20),
              @pSampleMode VARCHAR(16),
              @pWhatIf BIT,
              @pWaitAtLowPriorityMinutes INT,
              @pAbortAfterWait NVARCHAR(20),
              @pResumable BIT,
              @pMaxDurationMinutes INT,
              @pDelayMsBetweenCommands INT,
              @pIncludeDataCompressionOption BIT,
              @pIncludeOnlineOption BIT',
              @pMinPageDensityPct            = @MinPageDensityPct,
              @pMinPageCount                 = @MinPageCount,
              @pUseExistingFillFactor        = @UseExistingFillFactor,
              @pFillFactor                   = @FillFactor,
              @pOnline                       = @Online,
              @pMaxDOP                       = @MaxDOP,
              @pSortInTempdb                 = @SortInTempdb,
              @pUseCompressionFromSource     = @UseCompressionFromSource,
              @pForceCompression             = @ForceCompression,
              @pSampleMode                   = @EffectiveSampleMode,  
              @pWhatIf                       = @WhatIf,
              @pWaitAtLowPriorityMinutes     = @WaitAtLowPriorityMinutes,
              @pAbortAfterWait               = @AbortAfterWait,
              @pResumable                    = @Resumable,
              @pMaxDurationMinutes           = @MaxDurationMinutes,
              @pDelayMsBetweenCommands       = @DelayMsBetweenCommands,
              @pIncludeDataCompressionOption = @IncludeDataCompressionOption, 
              @pIncludeOnlineOption          = @IncludeOnlineOption;

        -- COMPLETED message for this DB
        RAISERROR(N'===== COMPLETED database: [%s] =====', 10, 1, @db) WITH NOWAIT;

        FETCH NEXT FROM cur INTO @db;
    END

    CLOSE cur; DEALLOCATE cur;
END
