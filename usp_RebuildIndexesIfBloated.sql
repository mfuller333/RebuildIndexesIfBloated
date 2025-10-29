SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'DBA')
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
/******              @DatabaseName               = target database to analyze/rebuild (REQUIRED)            ******/
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
/******              @LogDatabase                = NULL log in target DB; else central log DB name          ******/
/******              @WaitAtLowPriorityMinutes   = minutes for WAIT_AT_LOW_PRIORITY (ONLINE only)           ******/
/******              @AbortAfterWait             = NONE | SELF | BLOCKERS (requires above)                  ******/
/******              @Resumable                  = 1 use RESUMABLE (ONLINE only; SQL 2019+); 0 off          ******/
/******              @MaxDurationMinutes         = MAX_DURATION minutes for resumable (optional)            ******/
/******              @DelayMsBetweenCommands     = optional delay in ms between rebuild commands            ******/
/******              @WhatIf                     = 1 dry-run; logs/prints only; 0 execute rebuilds          ******/
/******                                                                                                     ******/
/****** Output:      Rows logged to [DBA].[IndexBloatRebuildLog] with action, status, and details.          ******/
/******              Also logs: avg_row_bytes, record_count, ghost_record_count,                            ******/
/******              forwarded_record_count, au_total_pages, au_used_pages, au_data_pages,                  ******/
/******              and computed allocated_unused_pages for trending.                                      ******/
/****** Created by: Mike Fuller                                                                             ******/
/****** Date Updated: 10/29/2025                                                                            ******/
/****** Version:     1.6 (2014-2022)                                                                        ******/
/******                                                                                          ¯\_(ツ)_/¯ ******/
/*****************************************************************************************************************/
ALTER PROCEDURE [DBA].[usp_RebuildIndexesIfBloated]
      @Help                       BIT          = 0,
      @DatabaseName               SYSNAME      = NULL,
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
      @CaptureTrendingSignals     BIT          = 0,          -- if 1 and SampleMode=SAMPLED, auto-upshift to DETAILED takes more time
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

    -- Help output: @Help = 1 shows parameter docs and examples then exits 
    IF @Help = 1
    BEGIN
        SELECT
            param_name,
            sql_type,
            default_value,
            description,
            example
        FROM
        (
            VALUES
              (N'@DatabaseName',             N'SYSNAME',        N'(required)',                    N'Target database to analyze and rebuild. Must be ONLINE and writable.',                                                                       '@DatabaseName = N''YourDb'''),
              (N'@MinPageDensityPct',        N'DECIMAL(5,2)',   N'70.0',                          N'Rebuild when avg page density for a leaf partition is below this percent.',                                                                      N'65.0'),
              (N'@MinPageCount',             N'INT',            N'1000',                          N'Skip tiny partitions below this page count.',                                                                                                    N'500'),
              (N'@UseExistingFillFactor',    N'BIT',            N'1',                             N'Keep each index''s current fill factor. If 0, use @FillFactor.',                                                                                 N'1'),
              (N'@FillFactor',               N'TINYINT',        N'NULL',                          N'Fill factor when @UseExistingFillFactor = 0. Valid 1 to 100.',                                                                                   N'90'),
              (N'@Online',                   N'BIT',            N'1',                             N'Use ONLINE = ON when supported. Disabled automatically on unsupported editions.',                                                                N'1'),
              (N'@MaxDOP',                   N'INT',            N'NULL',                          N'MAXDOP for rebuilds. If NULL, server default is used. (0 uses all schedulers.)',                                                                 N'4'),
              (N'@SortInTempdb',             N'BIT',            N'1',                             N'Use SORT_IN_TEMPDB to push sort work into tempdb.',                                                                                              N'1'),
              (N'@UseCompressionFromSource', N'BIT',            N'1',                             N'Preserve existing DATA_COMPRESSION setting of each partition (if supported).',                                                                   N'1'),
              (N'@ForceCompression',         N'NVARCHAR(20)',   N'NULL',                          N'Override compression for rowstore: NONE, ROW, or PAGE. Ignored if @UseCompressionFromSource = 1.',                                               N'N''ROW'''),
              (N'@SampleMode',               N'VARCHAR(16)',    N'''SAMPLED''',                   N'Mode for dm_db_index_physical_stats: SAMPLED or DETAILED. LIMITED is not supported.',                                                            N'''SAMPLED'''),
              (N'@CaptureTrendingSignals',   N'BIT',            N'1',                             N'If 1 and @SampleMode = SAMPLED, auto-use DETAILED so avg_row_bytes/ghosts/forwarded are reliable.',                                              N'1'),
              (N'@LogDatabase',              N'SYSNAME',        N'NULL',                          N'Central logging DB. If NULL, logs to the target database.',                                                                                      N'N''UtilityDb'''),
              (N'@WaitAtLowPriorityMinutes', N'INT',            N'NULL',                          N'When ONLINE = ON, optional WAIT_AT_LOW_PRIORITY MAX_DURATION minutes (SQL 2014+).',                                                              N'5'),
              (N'@AbortAfterWait',           N'NVARCHAR(20)',   N'NULL',                          N'WAIT_AT_LOW_PRIORITY ABORT_AFTER_WAIT: NONE, SELF, or BLOCKERS. Requires @WaitAtLowPriorityMinutes.',                                            N'N''BLOCKERS'''),
              (N'@Resumable',                N'BIT',            N'0',                             N'Use RESUMABLE = ON for online rebuilds when supported. Filtered indexes and indexes that include LOB types are skipped when @Resumable = 1.',    N'1'),
              (N'@MaxDurationMinutes',       N'INT',            N'NULL',                          N'Resumable MAX_DURATION minutes. Optional.',                                                                                                      N'60'),
              (N'@DelayMsBetweenCommands',   N'INT',            N'NULL',                          N'Optional WAITFOR delay between rebuild commands in milliseconds.',                                                                               N'5000'),
              (N'@WhatIf',                   N'BIT',            N'1',                             N'Dry run. If 1, log and print commands without executing. If 0, execute rebuilds.',                                                               N'0')
        ) d(param_name, sql_type, default_value, description, example)
        ORDER BY param_name;

        -- Effective behavior on this server (accurate for 2014�2022)
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
        SELECT
            metric         = 'avg_row_bytes',
            meaning        = 'Average record size in bytes from dm_db_index_physical_stats',
            why_it_matters = 'If data shape is steady but this rises, page density likely degrades'
        UNION ALL SELECT 'record_count','Estimated rows from dm_db_index_physical_stats','Context for density and growth'
        UNION ALL SELECT 'ghost_record_count','Deleted rows not yet cleaned','Delete churn + low density is a bloat red flag'
        UNION ALL SELECT 'forwarded_record_count','Heap pointer hops due to updates','Heap churn that hurts scans/space'
        UNION ALL SELECT 'au_total_pages','Pages reserved in allocation units (in-row/overflow)','Capacity reserved by the partition'
        UNION ALL SELECT 'au_used_pages','Pages currently used in those AUs','How much of the reservation is active'
        UNION ALL SELECT 'au_data_pages','Pages that hold row data','Baseline for row storage footprint'
        UNION ALL SELECT 'allocated_unused_pages','Computed as au_total_pages - au_used_pages','Allocated but unused headroom to watch';

        /* Examples */
        SELECT
            example_label,
            example_command
        FROM
        (
            VALUES
              (N'Dry run on one DB',      N'EXEC DBA.usp_RebuildIndexesIfBloated @DatabaseName = N''YourDb'', @WhatIf = 1;')
            , (N'Execute, Online, DOP 4', N'EXEC DBA.usp_RebuildIndexesIfBloated @DatabaseName = N''YourDb'', @Online = 1, @MaxDOP = 4, @WhatIf = 0;')
            , (N'Force ROW compression',  N'EXEC DBA.usp_RebuildIndexesIfBloated @DatabaseName = N''YourDb'', @UseCompressionFromSource = 0, @ForceCompression = N''ROW'', @WhatIf = 0;')
            , (N'Strict fill factor 90',  N'EXEC DBA.usp_RebuildIndexesIfBloated @DatabaseName = N''YourDb'', @UseExistingFillFactor = 0, @FillFactor = 90, @WhatIf = 0;')
            , (N'Low priority wait',      N'EXEC DBA.usp_RebuildIndexesIfBloated @DatabaseName = N''YourDb'', @WaitAtLowPriorityMinutes = 5, @AbortAfterWait = N''BLOCKERS'', @WhatIf = 0;')
            , (N'Help screen',            N'EXEC DBA.usp_RebuildIndexesIfBloated @Help = 1;')
            , (N'Trending query (paste in Log DB)', N'SELECT TOP (5) run_utc, page_density_pct, avg_row_bytes, ghost_record_count, au_total_pages, au_used_pages, (au_total_pages - au_used_pages) AS allocated_unused_pages FROM DBA.IndexBloatRebuildLog WHERE database_name = N''YourDb'' AND schema_name = N''dbo'' AND table_name = N''YourTable'' AND [action] = ''DRYRUN'' ORDER BY run_utc DESC;')
        ) x(example_label, example_command);

        RETURN;
    END

    -- Server/Edition capability detection (2014�2022 safe)
    DECLARE @verMajor int          = CAST(PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)),4) AS int);
    DECLARE @verBuild int          = CAST(PARSENAME(CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)),2) AS int); -- build (e.g., 4000 = 2016 SP1)
    DECLARE @edition  nvarchar(128)= CAST(SERVERPROPERTY('Edition') AS nvarchar(128));

    DECLARE @isEntDevEval bit = CASE WHEN @edition LIKE '%Enterprise%' OR @edition LIKE '%Developer%' OR @edition LIKE '%Evaluation%' THEN 1 ELSE 0 END;
    DECLARE @supportsOnline bit = @isEntDevEval; -- on-prem editions
    DECLARE @supportsCompression bit = CASE WHEN @verMajor > 13 OR (@verMajor = 13 AND @verBuild >= 4000) OR @isEntDevEval = 1 THEN 1 ELSE 0 END; 
    DECLARE @supportsResumableRebuild bit = CASE WHEN @verMajor >= 15 AND @supportsOnline = 1 THEN 1 ELSE 0 END; -- SQL 2019+ & ONLINE

    -- Effective sample mode with "seatbelt" for trending signals
    DECLARE @effectiveSampleMode VARCHAR(16) = @SampleMode;
    IF @CaptureTrendingSignals = 1 AND UPPER(@effectiveSampleMode) = 'SAMPLED'
        SET @effectiveSampleMode = 'DETAILED';

    IF @DatabaseName IS NULL
    BEGIN
        RAISERROR('@DatabaseName is required.',16,1);
        RETURN;
    END;

    IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = @DatabaseName)
    BEGIN
        RAISERROR('Database "%s" not found.',16,1,@DatabaseName);
        RETURN;
    END;

    IF EXISTS (SELECT 1 FROM sys.databases WHERE name = @DatabaseName AND state_desc <> 'ONLINE')
    BEGIN
        RAISERROR('Database "%s" is not ONLINE.',16,1,@DatabaseName);
        RETURN;
    END;

    IF EXISTS (SELECT 1 FROM sys.databases WHERE name = @DatabaseName AND is_read_only = 1)
    BEGIN
        RAISERROR('Database "%s" is READ_ONLY.',16,1,@DatabaseName);
        RETURN;
    END;

    IF @MinPageDensityPct IS NULL OR @MinPageDensityPct <= 0 OR @MinPageDensityPct >= 100
    BEGIN
        RAISERROR('@MinPageDensityPct must be between 0 and 100 (exclusive).',16,1); RETURN;
    END;

    IF @MinPageCount IS NULL OR @MinPageCount <= 0
    BEGIN
        RAISERROR('@MinPageCount must be a positive integer.',16,1); RETURN;
    END;

    IF @UseExistingFillFactor = 0 AND ( @FillFactor IS NULL OR @FillFactor NOT BETWEEN 1 AND 100 )
    BEGIN
        RAISERROR('When @UseExistingFillFactor = 0, @FillFactor must be 1-100.',16,1);
        RETURN;
    END;

    IF @MaxDOP IS NOT NULL AND @MaxDOP NOT BETWEEN 0 AND 32767
    BEGIN
        RAISERROR('@MaxDOP must be between 0 and 32767.',16,1); RETURN;
    END;

    IF @DelayMsBetweenCommands IS NOT NULL AND @DelayMsBetweenCommands < 0
    BEGIN
        RAISERROR('@DelayMsBetweenCommands must be >= 0.',16,1); RETURN;
    END;

    IF UPPER(ISNULL(@SampleMode,'')) NOT IN ('SAMPLED','DETAILED')
    BEGIN
        RAISERROR('@SampleMode must be SAMPLED or DETAILED.',16,1);
        RETURN;
    END

    IF @WaitAtLowPriorityMinutes IS NOT NULL AND @WaitAtLowPriorityMinutes <= 0
    BEGIN
        RAISERROR('@WaitAtLowPriorityMinutes must be a positive integer when provided.',16,1); RETURN;
    END;

    IF @WaitAtLowPriorityMinutes IS NOT NULL AND UPPER(ISNULL(@AbortAfterWait,'')) NOT IN ('NONE','SELF','BLOCKERS')
    BEGIN
        RAISERROR('@AbortAfterWait must be NONE, SELF, or BLOCKERS when @WaitAtLowPriorityMinutes is set.',16,1);
        RETURN;
    END;

    IF @Resumable = 1 AND @Online = 0
    BEGIN
        RAISERROR('RESUMABLE requires @Online = 1.',16,1);
        RETURN;
    END;

    -- ONLINE / WAIT_AT_LOW_PRIORITY only if ONLINE is supported on this edition
    DECLARE @includeOnlineOption bit = CASE WHEN @Online = 1 AND @supportsOnline = 1 THEN 1 ELSE 0 END; 

    IF @includeOnlineOption = 0
    BEGIN
        SET @Online = 0; -- be explicit
        SET @WaitAtLowPriorityMinutes = NULL;
        SET @AbortAfterWait = NULL;
    END

    -- Resumable REBUILD is SQL 2019+ only, and requires ONLINE
    IF @supportsResumableRebuild = 0
    BEGIN
        SET @Resumable = 0;
        SET @MaxDurationMinutes = NULL;
    END

    -- Compression on unsupported editions: never emit the option; also skip compressed partitions later
    DECLARE @includeDataCompressionOption bit = CASE WHEN @supportsCompression = 1 THEN 1 ELSE 0 END; 
    IF @includeDataCompressionOption = 0
    BEGIN
        SET @UseCompressionFromSource = 0;
        SET @ForceCompression = NULL; -- will omit DATA_COMPRESSION entirely
    END
    ELSE
    BEGIN
        IF @UseCompressionFromSource = 0 AND UPPER(ISNULL(@ForceCompression,'')) NOT IN ('NONE','ROW','PAGE')
        BEGIN
            RAISERROR('Invalid @ForceCompression for rowstore. Use NONE, ROW, or PAGE.',16,1);
            RETURN;
        END;
    END

    DECLARE @LogDb  SYSNAME       = ISNULL(@LogDatabase, @DatabaseName);
    DECLARE @qLogDb NVARCHAR(258) = QUOTENAME(@LogDb);

    DECLARE @ddl NVARCHAR(MAX) =
    N'USE ' + @qLogDb + N';
    IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N''DBA'')
        EXEC(''CREATE SCHEMA DBA AUTHORIZATION dbo'');
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
    -- Upgrade path: add columns if missing
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

    -- allocated_unused_pages exists but is not computed, convert it 
    IF COL_LENGTH(N''[DBA].[IndexBloatRebuildLog]'', N''allocated_unused_pages'') IS NOT NULL
       AND ISNULL(COLUMNPROPERTY(OBJECT_ID(N''[DBA].[IndexBloatRebuildLog]''),
                                  N''allocated_unused_pages'', ''IsComputed''),0) = 0
    BEGIN
        ALTER TABLE [DBA].[IndexBloatRebuildLog] DROP COLUMN allocated_unused_pages;
    END

    -- Add computed column after base cols exist, using dynamic SQL to avoid compile-time name resolution
    IF COL_LENGTH(N''[DBA].[IndexBloatRebuildLog]'', N''allocated_unused_pages'') IS NULL
    BEGIN
        EXEC(N''ALTER TABLE [DBA].[IndexBloatRebuildLog]
              ADD allocated_unused_pages AS (au_total_pages - au_used_pages);'');
    END';

    BEGIN TRY
        EXEC (@ddl);
    END TRY
    BEGIN CATCH
        DECLARE @em nvarchar(4000) = ERROR_MESSAGE();
        RAISERROR('Failed to prepare logging table in %s: %s',16,1,@LogDb,@em);
        RETURN;
    END CATCH

    DECLARE @qDb NVARCHAR(258) = QUOTENAME(@DatabaseName);

    DECLARE @sql NVARCHAR(MAX) =
    N'USE ' + @qDb + N';
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    SET DEADLOCK_PRIORITY LOW;

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

    DECLARE @mode VARCHAR(16) =
        CASE UPPER(@pSampleMode)
            WHEN ''DETAILED'' THEN ''DETAILED''
            ELSE ''SAMPLED''
        END;

    -- Build candidate list 
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
          s.name AS schema_name,
          t.name AS table_name,
          i.name AS index_name,
          i.index_id,
          ps.partition_number,
          ps.page_count,
          ps.avg_page_space_used_in_percent AS page_density_pct,
          ps.avg_fragmentation_in_percent AS fragmentation_pct,
          COALESCE(CAST(ps.avg_record_size_in_bytes AS DECIMAL(18,2)), 0) AS avg_row_bytes,
          COALESCE(ps.record_count, 0) AS record_count,
          COALESCE(ps.ghost_record_count, 0) AS ghost_record_count,
          COALESCE(ps.forwarded_record_count, 0) AS fwd_record_count,
          COALESCE(SUM(au.total_pages),0) AS au_total_pages,
          COALESCE(SUM(au.used_pages),0) AS au_used_pages,
          COALESCE(SUM(au.data_pages),0) AS au_data_pages,
          p.data_compression_desc AS compression_desc,
          CASE WHEN @pUseExistingFillFactor = 1 THEN NULLIF(i.fill_factor,0) ELSE @pFillFactor END AS chosen_fill_factor,
          CASE WHEN psch.data_space_id IS NULL THEN 0 ELSE 1 END AS is_partitioned,
          i.has_filter AS is_filtered,
          blockers.has_included_lob AS has_included_lob,
          blockers.has_key_blocker AS has_key_blocker,
          rs.resumable_supported AS resumable_supported,
         (
            N''ALTER INDEX '' + QUOTENAME(i.name) +
            N'' ON '' + QUOTENAME(s.name) + N''.'' + QUOTENAME(t.name) +
            CASE WHEN psch.data_space_id IS NULL
                 THEN N'' REBUILD ''
                 ELSE N'' REBUILD PARTITION = '' + CONVERT(VARCHAR(12), ps.partition_number) + N'' ''
            END +
            N''WITH (SORT_IN_TEMPDB = '' +
            CASE
                WHEN @pOnline = 1 AND @pResumable = 1 AND rs.resumable_supported = 1 THEN N''OFF''  -- resumable requires SORT_IN_TEMPDB=OFF
                ELSE CASE WHEN @pSortInTempdb = 1 THEN N''ON'' ELSE N''OFF'' END
            END +
            CASE
                WHEN (CASE WHEN @pUseExistingFillFactor = 1 THEN NULLIF(i.fill_factor,0) ELSE @pFillFactor END) IS NOT NULL
                    THEN N'', FILLFACTOR = '' + CONVERT(VARCHAR(4), (CASE WHEN @pUseExistingFillFactor = 1 THEN NULLIF(i.fill_factor,0) ELSE @pFillFactor END))
                ELSE N'''' END +
            CASE 
                WHEN @pIncludeOnlineOption = 1 AND @pOnline = 1 THEN
                    N'', ONLINE = ON'' +
                    CASE WHEN @pWaitAtLowPriorityMinutes IS NOT NULL
                         THEN N'' (WAIT_AT_LOW_PRIORITY (MAX_DURATION = '' 
                              + CONVERT(VARCHAR(4), @pWaitAtLowPriorityMinutes) + N'' MINUTES, ABORT_AFTER_WAIT = '' + @pAbortAfterWait + N''))''
                         ELSE N'''' END
                ELSE N'''' 
            END +
            CASE WHEN @pMaxDOP IS NOT NULL THEN N'', MAXDOP = '' + CONVERT(VARCHAR(5), @pMaxDOP) ELSE N'''' END +
            CASE 
                WHEN @pIncludeDataCompressionOption = 1 
                     THEN N'', DATA_COMPRESSION = '' + CASE WHEN @pUseCompressionFromSource = 1 THEN p.data_compression_desc ELSE @pForceCompression END
                ELSE N'''' 
            END +
            CASE WHEN @pOnline = 1 AND @pResumable = 1 AND rs.resumable_supported = 1 THEN N'', RESUMABLE = ON'' ELSE N'''' END +
            CASE WHEN @pOnline = 1 AND @pResumable = 1 AND rs.resumable_supported = 1 AND @pMaxDurationMinutes IS NOT NULL
                 THEN N'', MAX_DURATION = '' + CONVERT(VARCHAR(4), @pMaxDurationMinutes) + N'' MINUTES''
                 ELSE N'''' END +
            N'')'' +
            CASE WHEN @pOnline = 1 AND @pResumable = 1 AND rs.resumable_supported = 0
                 THEN N'' /* downgraded: filtered and/or included LOB and/or computed/rowversion key */''
                 ELSE N'''' END
          ) AS cmd
    FROM sys.indexes AS i
    JOIN sys.tables  AS t
      ON t.object_id = i.object_id
    JOIN sys.schemas AS s
      ON s.schema_id = t.schema_id
    JOIN sys.partitions AS p
      ON p.object_id = i.object_id
     AND p.index_id   = i.index_id
    JOIN sys.data_spaces AS ds
      ON ds.data_space_id = i.data_space_id
    LEFT JOIN sys.partition_schemes AS psch
      ON psch.data_space_id = ds.data_space_id
    JOIN sys.allocation_units AS au
      ON au.container_id = p.hobt_id
     AND au.type IN (1,3)  -- 1=IN_ROW_DATA, 3=ROW_OVERFLOW_DATA  (omit 2=LOB_DATA by design)
    -- blockers for resumable operations
    CROSS APPLY
    (
        SELECT
            has_included_lob =
                CASE WHEN EXISTS
                (
                    SELECT 1
                    FROM sys.index_columns ic
                    JOIN sys.columns c
                      ON c.object_id = ic.object_id
                     AND c.column_id = ic.column_id
                    WHERE ic.object_id = i.object_id
                      AND ic.index_id = i.index_id
                      AND ic.is_included_column = 1
                      AND (c.max_length = -1 OR c.system_type_id IN (34,35,99,241)) -- image,text,ntext,xml; MAX types
                ) THEN 1 ELSE 0 END,
            has_key_blocker =
                CASE WHEN EXISTS
                (
                    SELECT 1
                    FROM sys.index_columns ic
                    JOIN sys.columns c
                      ON c.object_id = ic.object_id
                     AND c.column_id = ic.column_id
                    WHERE ic.object_id = i.object_id
                      AND ic.index_id = i.index_id
                      AND ic.key_ordinal > 0
                      AND (c.is_computed = 1 OR c.system_type_id = 189) -- timestamp/rowversion
                ) THEN 1 ELSE 0 END
    ) AS blockers
    -- per-index resumable support
    CROSS APPLY
    (
        SELECT resumable_supported =
            CASE WHEN i.has_filter = 0 AND blockers.has_included_lob = 0 AND blockers.has_key_blocker = 0
                 THEN 1 ELSE 0 END
    ) AS rs
    CROSS APPLY sys.dm_db_index_physical_stats
    (
        DB_ID(),
        i.object_id,
        i.index_id,
        p.partition_number,
        @mode
    ) AS ps
    WHERE
        i.index_id > 0
        AND i.type IN (1,2)       -- clustered/nonclustered rowstore
        AND i.is_hypothetical = 0
        AND i.is_disabled = 0
        AND ps.index_level = 0    -- leaf level only
        AND ps.page_count >= @pMinPageCount
        AND ps.avg_page_space_used_in_percent < @pMinPageDensityPct
        AND t.is_ms_shipped = 0
        AND t.is_memory_optimized = 0
        AND (@pIncludeDataCompressionOption = 1 OR p.data_compression = 0)
    GROUP BY
        s.name, t.name, i.name, i.index_id,
        ps.partition_number, ps.page_count,
        ps.avg_page_space_used_in_percent, ps.avg_fragmentation_in_percent,
        ps.avg_record_size_in_bytes, ps.record_count, ps.ghost_record_count, ps.forwarded_record_count,
        p.data_compression_desc,
        i.fill_factor,
        psch.data_space_id, 
        CASE WHEN psch.data_space_id IS NULL THEN 0 ELSE 1 END,
        i.has_filter, blockers.has_included_lob, blockers.has_key_blocker,
        rs.resumable_supported
    OPTION (RECOMPILE);

    -- quick heads-up message
    IF @pResumable = 1
    BEGIN
        DECLARE @downgraded INT =
            (SELECT COUNT(*) FROM #candidates WHERE resumable_supported = 0);
        IF @downgraded > 0
            RAISERROR(''Note: %d candidate(s) downgraded from RESUMABLE due to filtered and/or included LOB and/or computed/rowversion key.'',
                       10, 1, @downgraded) WITH NOWAIT;
    END

    IF OBJECT_ID(''tempdb..#todo'') IS NOT NULL DROP TABLE #todo;
    CREATE TABLE #todo (log_id BIGINT PRIMARY KEY, cmd NVARCHAR(MAX) NOT NULL);

    -- Log decisions 
    ;WITH to_log AS
    (
        SELECT
              DB_NAME()            AS database_name,
              c.schema_name        AS schema_name,
              c.table_name         AS table_name,
              c.index_name         AS index_name,
              c.index_id           AS index_id,
              c.partition_number   AS partition_number,
              c.page_count         AS page_count,
              c.page_density_pct   AS page_density_pct,
              c.fragmentation_pct  AS fragmentation_pct,
              c.avg_row_bytes      AS avg_row_bytes,
              c.record_count       AS record_count,
              c.ghost_record_count AS ghost_record_count,
              c.fwd_record_count   AS forwarded_record_count,
              c.au_total_pages     AS au_total_pages,
              c.au_used_pages      AS au_used_pages,
              c.au_data_pages      AS au_data_pages,
              c.chosen_fill_factor AS chosen_fill_factor,
              @pOnline             AS online_on,
              @pMaxDOP             AS maxdop_used,
              CASE WHEN @pWhatIf = 1 THEN ''DRYRUN'' ELSE ''REBUILD'' END AS [action],
              c.cmd                AS cmd,
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
    FROM to_log;

    IF NOT EXISTS (SELECT 1 FROM #todo)
    BEGIN
        PRINT ''No bloated rowstore index partitions met the thresholds for '' + DB_NAME() + ''.'';
        RETURN;
    END

    IF @pWhatIf = 1
    BEGIN
        SELECT
            l.log_id,
            l.schema_name,
            l.table_name,
            l.index_name,
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
    SELECT t.log_id, t.cmd, l.schema_name, l.table_name, l.index_name, l.partition_number, l.page_count
    FROM #todo AS t
    JOIN ' + @qLogDb + N'.[DBA].[IndexBloatRebuildLog] AS l ON l.log_id = t.log_id
    ORDER BY l.page_density_pct ASC, l.page_count DESC;

    DECLARE @i INT = 1, @imax INT = (SELECT COUNT(*) FROM #exec);
    DECLARE
        @cmd NVARCHAR(MAX),
        @log_id BIGINT,
        @schema SYSNAME,
        @table SYSNAME,
        @index SYSNAME,
        @part  INT,
        @pages BIGINT;

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
        SELECT
            @cmd   = cmd,
            @log_id= log_id,
            @schema= schema_name,
            @table = table_name,
            @index = index_name,
            @part  = partition_number,
            @pages = page_count
        FROM #exec
        WHERE rn = @i;

        -- BEFORE: announce rebuild target
        DECLARE @msg NVARCHAR(4000);
        SET @msg = N''Rebuilding '' + QUOTENAME(@schema) + N''.'' + QUOTENAME(@table) + N''.'' + QUOTENAME(@index)
                 + N'' (partition '' + CONVERT(NVARCHAR(12), @part) + N'', pages = '' + CONVERT(NVARCHAR(20), @pages) + N'')'';
        RAISERROR(@msg, 10, 1) WITH NOWAIT;

        BEGIN TRY
            EXEC sys.sp_executesql @cmd;

            UPDATE ' + @qLogDb + N'.[DBA].[IndexBloatRebuildLog]
               SET [status] = ''SUCCESS''
             WHERE log_id = @log_id;

            -- AFTER success
            SET @msg = N''SUCCESS  '' + QUOTENAME(@schema) + N''.'' + QUOTENAME(@table) + N''.'' + QUOTENAME(@index)
                     + N'' (partition '' + CONVERT(NVARCHAR(12), @part) + N'')'';
            RAISERROR(@msg, 10, 1) WITH NOWAIT;
        END TRY
        BEGIN CATCH
            UPDATE ' + @qLogDb + N'.[DBA].[IndexBloatRebuildLog]
               SET [status]       = ''FAILED'',
                   error_message  = ERROR_MESSAGE(),
                   error_number   = ERROR_NUMBER(),
                   error_severity = ERROR_SEVERITY(),
                   error_state    = ERROR_STATE(),
                   error_line     = ERROR_LINE(),
                   error_proc     = ERROR_PROCEDURE()
             WHERE log_id = @log_id;

            -- AFTER failure with details
            SET @msg = N''FAILED   '' + QUOTENAME(@schema) + N''.'' + QUOTENAME(@table) + N''.'' + QUOTENAME(@index)
                     + N'' (partition '' + CONVERT(NVARCHAR(12), @part) + N''): '' + CONVERT(NVARCHAR(4000), ERROR_MESSAGE());
            RAISERROR(@msg, 10, 1) WITH NOWAIT;
        END CATCH;

        IF @delay IS NOT NULL
            WAITFOR DELAY @delay;

        SET @i += 1;
    END

    -- Per-object rollup: table and total pages 
    SELECT 
        l.[action],
        l.[status],
        l.schema_name + N''.'' + l.table_name AS [object_name],
        l.index_name,
        COUNT(*) AS partitions_affected,
        SUM(l.page_count) AS total_pages,
        MIN(l.page_density_pct) AS min_density,
        AVG(l.page_density_pct) AS avg_density
    FROM ' + @qLogDb + N'.[DBA].[IndexBloatRebuildLog] AS l
    WHERE l.log_id IN (SELECT log_id FROM #todo)
    GROUP BY l.[action], l.[status], l.schema_name + N''.'' + l.table_name, l.index_name
    ORDER BY total_pages DESC, object_name, index_name;
    ';

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
END

