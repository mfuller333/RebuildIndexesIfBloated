---
# AdaptiveDBCare — Multi-Database Index & Statistics Optimization Release


This release of AdaptiveDBCare introduces coordinated, cross-database index-rebuild and statistics-update capabilities, enhanced telemetry, safer sampling, richer logging, and improved operator visibility. It elevates AdaptiveDBCare from single-DB routines into a unified, intelligent maintenance engine for large SQL Server estates.
# DBA.usp_RebuildIndexesIfBloated (v**1.9.1**, 2025‑11‑16)


---
Rebuild **only** what’s bloated. This procedure finds leaf‑level **rowstore** index partitions whose **avg_page_space_used_in_percent** is below a threshold, then rebuilds just those partitions—optionally **ONLINE**, **RESUMABLE**, respecting (or overriding) **FILLFACTOR** and **DATA_COMPRESSION**—and logs every decision.

> **Works on SQL Server 2014–2022.** ONLINE/RESUMABLE features auto‑downgrade based on edition/version (Enterprise/Developer/Evaluation for ONLINE; SQL 2019+ for RESUMABLE). Defaults to safe **WhatIf** mode.

---

## What’s new in 1.9.1

* **Multi‑DB orchestration** via `@TargetDatabases`: CSV list, or the token **ALL_USER_DBS**, with **exclusions** using `-DbName` (e.g., `ALL_USER_DBS,-DW,-ReportServer`).
* **Help switch** `@Help = 1`: prints a parameter cheatsheet, server capability discovery, and runnable examples.
* **Seatbelt for sampling**: `@CaptureTrendingSignals = 1` auto‑upsifts `SAMPLED` → `DETAILED` to collect reliable row/ghost/forwarded metrics.
* **Smarter logging bootstrap**: ensures/updates `[DBA].[IndexBloatRebuildLog]` (adds trending columns and a **computed** `allocated_unused_pages = au_total_pages - au_used_pages`).
* **Resumable auto‑downgrade** per candidate when filtered/includes LOB/computed/rowversion keys would block it—still rebuilds online, just not resumable.
* **Optional pacing** via `@DelayMsBetweenCommands` between rebuilds.
* **Progress UX**: live “STARTING/COMPLETED” messages per DB, per‑table candidate summaries, a per‑DB candidates result set, and an end‑of‑run object summary.

---

## Why this exists

* **Page density pays the rent**—especially on SSD/NVMe. Logical fragmentation doesn’t show up on your invoice.
* **Surgical partitions**: touch only the slices that need it; stop hammering every index every time.
* **Receipts or it didn’t happen**: every candidate, command, and result is logged for audit and trending.

---

## Features

* Targets **rowstore** only (clustered & nonclustered), **leaf level**.
* Leaves tiny partitions alone via `@MinPageCount`.
* **Partition‑aware**: rebuilds specific partitions only.
* **ONLINE = ON** with optional `WAIT_AT_LOW_PRIORITY (MAX_DURATION, ABORT_AFTER_WAIT)`.
* **RESUMABLE** (SQL 2019+, ONLINE only); per‑candidate auto‑downgrade when unsupported.
* **Compression**: preserve from source or force `NONE | ROW | PAGE`; auto‑disabled if not supported on the server.
* **Fill factor**: preserve per index or set a global `@FillFactor`.
* **MaxDOP**: honor server default when NULL; you can explicitly set `0` (unlimited) or a specific value.
* **SORT_IN_TEMPDB**: ON by default; automatically **OFF** when RESUMABLE is used (engine limitation).
* **Full logging** to `[DBA].[IndexBloatRebuildLog]` in a **target DB** or a central log DB.
* Captures trending signals: `avg_row_bytes`, `record_count`, `ghost_record_count`, `forwarded_record_count`, AU pages (`au_total_pages`, `au_used_pages`, `au_data_pages`), and computed `allocated_unused_pages`.
* Defaults to **WhatIf** (dry run).

---

## Compatibility

* **SQL Server**: 2014–2022
* **ONLINE**: Enterprise, Developer, Evaluation
* **RESUMABLE**: SQL Server 2019+ **and** ONLINE
* **Compression**: auto‑detects support; preserves or overrides accordingly

The procedure **self‑detects** edition/version and quietly downgrades options it can’t safely use.

---

## Installation

1. Ensure the `DBA` schema exists (the proc will create it if needed).
2. Deploy the procedure once—typically into a Utility/DBA database for central use.
3. First execution will ensure `[DBA].[IndexBloatRebuildLog]` exists **in either the target DB** or a central **Log DB** you choose via `@LogDatabase`. Upgrades add trending columns and the computed `allocated_unused_pages`.

> Deploy once; call it for any user database(s). Choose to log in the target or a central Utility DB.

---

## Parameters

| Parameter                   | Type          |      Default | Notes                                                                                                                                                                          |
| --------------------------- | ------------- | -----------: | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `@Help`                     | BIT           |            0 | `1` prints help, capabilities, and examples; returns without scanning.                                                                                                         |
| `@TargetDatabases`          | NVARCHAR(MAX) | **required** | CSV list or **ALL_USER_DBS** (exact case), with optional **exclusions** using `-DbName`. System DBs (`master`,`model`,`msdb`,`tempdb`) and `distribution` are always excluded. |
| `@MinPageDensityPct`        | DECIMAL(5,2)  |         70.0 | Rebuild when **leaf** partition density is below this percent.                                                                                                                 |
| `@MinPageCount`             | INT           |         1000 | Skip partitions smaller than this page count.                                                                                                                                  |
| `@UseExistingFillFactor`    | BIT           |            1 | Keep each index’s fill factor.                                                                                                                                                 |
| `@FillFactor`               | TINYINT       |         NULL | Used only when `@UseExistingFillFactor = 0`. (1–100)                                                                                                                           |
| `@Online`                   | BIT           |            1 | ONLINE when edition supports it; otherwise auto‑disabled.                                                                                                                      |
| `@MaxDOP`                   | INT           |         NULL | If NULL, uses server default. You may pass `0` (unlimited) or a specific DOP.                                                                                                  |
| `@SortInTempdb`             | BIT           |            1 | `RESUMABLE=ON` forces this OFF automatically.                                                                                                                                  |
| `@UseCompressionFromSource` | BIT           |            1 | Preserve `DATA_COMPRESSION` per partition when supported.                                                                                                                      |
| `@ForceCompression`         | NVARCHAR(20)  |         NULL | `NONE` `ROW` or `PAGE` when not preserving.                                                                                                                                    |
| `@SampleMode`               | VARCHAR(16)   |    `SAMPLED` | `SAMPLED` or `DETAILED`.                                                                                                                                                       |
| `@CaptureTrendingSignals`   | BIT           |            0 | If `1` and `SAMPLED`, auto‑upsifts to `DETAILED`.                                                                                                                              |
| `@LogDatabase`              | SYSNAME       |         NULL | If set, logs to this DB instead of the target DB.                                                                                                                              |
| `@WaitAtLowPriorityMinutes` | INT           |         NULL | With ONLINE, enables `WAIT_AT_LOW_PRIORITY (MAX_DURATION)`.                                                                                                                    |
| `@AbortAfterWait`           | NVARCHAR(20)  |         NULL | `NONE` `SELF` or `BLOCKERS` (requires minutes).                                                                                                                                |
| `@Resumable`                | BIT           |            0 | RESUMABLE rebuilds (SQL 2019+, ONLINE required).                                                                                                                               |
| `@MaxDurationMinutes`       | INT           |         NULL | RESUMABLE `MAX_DURATION`.                                                                                                                                                      |
| `@DelayMsBetweenCommands`   | INT           |         NULL | Optional wait between rebuild commands (ms).                                                                                                                                   |
| `@WhatIf`                   | BIT           |            1 | Dry run by default.                                                                                                                                                            |

**Important safeguards**

* `@SampleMode` accepts only `SAMPLED` or `DETAILED`.
* `RESUMABLE` requires `@Online = 1` and SQL Server 2019+; **per‑candidate** it is skipped for **filtered** indexes, **included LOB** columns, or **computed/rowversion key** columns.
* If ONLINE isn’t supported on the edition, all ONLINE options are disabled.
* If compression isn’t supported, compression options are disabled and only **uncompressed** partitions are considered.

---

## Quick start

### Help / cheatsheet

```sql
EXEC DBA.usp_RebuildIndexesIfBloated @Help = 1;
```

### Dry run across all user DBs

```sql
EXEC DBA.usp_RebuildIndexesIfBloated
    @TargetDatabases = N'ALL_USER_DBS',
    @WhatIf          = 1;
```

### All user DBs except DW & SSRS, log centrally, execute

```sql
EXEC DBA.usp_RebuildIndexesIfBloated
    @TargetDatabases = N'ALL_USER_DBS,-DW,-ReportServer,-ReportServerTempDB',
    @LogDatabase     = N'UtilityDb',
    @WhatIf          = 0;
```

### One database, ONLINE with DOP 4

```sql
EXEC DBA.usp_RebuildIndexesIfBloated
    @TargetDatabases = N'YourDb',
    @Online          = 1,
    @MaxDOP          = 4,
    @WhatIf          = 0;
```

### Force ROW compression

```sql
EXEC DBA.usp_RebuildIndexesIfBloated
    @TargetDatabases          = N'YourDb',
    @UseCompressionFromSource = 0,
    @ForceCompression         = N'ROW',
    @WhatIf                   = 0;
```

### Low‑priority wait

```sql
EXEC DBA.usp_RebuildIndexesIfBloated
    @TargetDatabases          = N'YourDb',
    @Online                   = 1,
    @WaitAtLowPriorityMinutes = 5,
    @AbortAfterWait           = N'BLOCKERS',
    @WhatIf                   = 0;
```

### Resumable with MAX_DURATION (SQL 2019+)

```sql
EXEC DBA.usp_RebuildIndexesIfBloated
    @TargetDatabases   = N'YourDb',
    @Online            = 1,
    @Resumable         = 1,
    @MaxDurationMinutes= 60,
    @WhatIf            = 0;
```

---

## Output & logging

* **Messages**: per‑DB `STARTING` / table‑level candidate updates / per‑DB `COMPLETED`.
* **Result sets**:

  * Per DB: a **candidates summary** (`#scan_out`) with `schema_name`, `table_name`, candidate partitions, min/avg density, and page totals.
  * In **WhatIf** mode: a **detailed list** of planned actions (one per candidate partition) including density, AU pages, `allocated_unused_pages`, and the command text.
  * In **execute** mode: a **summary** by object and index with action, status, partitions affected, and page totals.
* **Log table**: `[DBA].[IndexBloatRebuildLog]` gets one row per candidate/action with:

  * Identity: `database_name, schema_name, table_name, index_name, index_id, partition_number`
  * Metrics: `page_count, page_density_pct, fragmentation_pct, avg_row_bytes, record_count, ghost_record_count, forwarded_record_count`
  * AU pages: `au_total_pages, au_used_pages, au_data_pages`, and computed `allocated_unused_pages`
  * Options: `chosen_fill_factor, online_on, maxdop_used`
  * Action & status: `DRYRUN/REBUILD` and `SKIPPED/PENDING/SUCCESS/FAILED`
  * `cmd` text and full error metadata on failures

**Trending tip**

```sql
SELECT TOP (5)
    run_utc, page_density_pct, avg_row_bytes,
    ghost_record_count, au_total_pages, au_used_pages,
    (au_total_pages - au_used_pages) AS allocated_unused_pages
FROM DBA.IndexBloatRebuildLog
WHERE database_name = N'YourDb'
  AND schema_name   = N'dbo'
  AND table_name    = N'YourTable'
  AND [action]      = 'DRYRUN'
ORDER BY run_utc DESC;
```

---

## Operational notes

* **tempdb/version store**: ONLINE + `SORT_IN_TEMPDB = ON` pushes work to tempdb and version store; RESUMABLE implicitly forces `SORT_IN_TEMPDB = OFF`.
* **Transaction log**: rebuilds are fully logged; ensure primaries/secondaries can keep up.
* **Extent ordering**: if you require strictly ordered allocation, set `@MaxDOP = 1`.
* **Sampling**: `SAMPLED` is fast; turn on `@CaptureTrendingSignals = 1` to auto‑use `DETAILED` for reliable row‑size/churn metrics.
* **Execution order**: worst density first, breaking ties by size—because triage.

---

## Known limitations

* Rowstore only; columnstore is out of scope.
* RESUMABLE is skipped for filtered indexes, indexes with **included LOB**, and **computed/rowversion** key columns.
* Memory‑optimized tables are skipped.

---

## Versioning

* **Version**: 1.9.1
* **Last updated**: 2025‑11‑16

See the procedure header for exact parameter list and capability checks.

---

## Contributing

Issues and PRs welcome. Please include:

* Exact SQL Server version and edition
* Execution parameters used
* Snippets from `[DBA].[IndexBloatRebuildLog]` for failing cases
* If relevant, `sys.dm_db_index_physical_stats` output for the affected index

---

## License

MIT. Do good things. Rebuild responsibly.

---

## Credit

Created by **Mike Fuller**.

---

# DBA.usp_UpdateStatisticsIfChanged (v**1.3**, 2025‑11‑16)

From a **single** utility DB, update statistics in one or many target databases **only when they’ve changed**—either by a **threshold %** or using `ALL_CHANGES`. Supports `FULLSCAN`, `DEFAULT`, or `SAMPLED <n>%`. All actions are centrally logged.

> **Works on SQL Server 2014–2022.** Designed to be deployed once in a Utility/DBA DB and run against many user DBs. Defaults to **WhatIf**.

---

## Why this exists

* Stop carpet‑bombing stats: touch the ones that moved.
* Keep the story straight: central log shows **what** you updated, **why**, **how**, and **with what sample**.

---

## Features

* Multi‑DB orchestration: CSV, **ALL_USER_DBS**, and `-DbName` exclusions; online, read‑write DBs only.
* Candidate detection via `sys.dm_db_stats_properties`:

  * `ALL_CHANGES`: any stats with `modification_counter > 0`
  * Threshold mode: `change% >= @ChangeThresholdPercent` (or `rows=0 AND modification>0`)
* Sampling modes: `FULLSCAN`, `DEFAULT`, or `SAMPLED n%`
  *Note: value is rounded to a whole percent for SQL Server 2014.*
* Builds per‑object `UPDATE STATISTICS` commands prefixed with `USE <DB>;` for safety.
* Central logging to `[DBA].[UpdateStatsLog]`, including a **run_id** to group a run.
* Defaults to **WhatIf** (log/return only).

---

## Parameters

| Parameter                 | Type          |      Default | Notes                                                                                          |
| ------------------------- | ------------- | -----------: | ---------------------------------------------------------------------------------------------- |
| `@Help`                   | BIT           |            0 | `1` prints help and examples; returns.                                                         |
| `@TargetDatabases`        | NVARCHAR(MAX) | **required** | CSV list or **ALL_USER_DBS** (exact case) with `-DbName` exclusions.                           |
| `@ChangeThresholdPercent` | DECIMAL(6,2)  |         NULL | Used when `@ChangeScope IS NULL`. Must be `0–100`.                                             |
| `@ChangeScope`            | VARCHAR(20)   |         NULL | Exact‑case token **ALL_CHANGES** to update *any* stats with changes; otherwise threshold mode. |
| `@SampleMode`             | VARCHAR(12)   |    `DEFAULT` | Exact‑case tokens: **FULLSCAN**, **DEFAULT**, **SAMPLED**.                                     |
| `@SamplePercent`          | DECIMAL(6,2)  |         NULL | Required when `@SampleMode='SAMPLED'`. Range `>0` to `<=100`; rounded to whole % on SQL 2014.  |
| `@LogDatabase`            | SYSNAME       |         NULL | Central log DB; defaults to **this** utility DB when NULL.                                     |
| `@WhatIf`                 | BIT           |            1 | `1` = DRYRUN (log/return only); `0` = execute.                                                 |

---

## Output & logging

* **Result set**: per run, a summary by object and stats name with `action`, `status`, and counts.
* **Log table**: `[DBA].[UpdateStatsLog]` gets one row per candidate/action with:

  * Identity: `database_name, schema_name, table_name, stats_name, stats_id`
  * Signals: `rows, modification_counter, change_pct, last_updated, rows_sampled`
  * Options & action: `sample_mode, [action] DRYRUN/UPDATE, cmd`
  * Status: `SKIPPED/PENDING/SUCCESS/FAILED`, plus full error metadata
  * `run_id` to correlate a given execution

---

## Quick start

### Help / cheatsheet

```sql
EXEC DBA.usp_UpdateStatisticsIfChanged @Help = 1;
```

### All user DBs except DBA, ALL_CHANGES, SAMPLED 20% (dry run)

```sql
EXEC DBA.usp_UpdateStatisticsIfChanged
    @TargetDatabases        = N'ALL_USER_DBS,-DBA',
    @ChangeScope            = 'ALL_CHANGES',
    @SampleMode             = 'SAMPLED',
    @SamplePercent          = 20,
    @WhatIf                 = 1;
```

### One DB, threshold 20%, DEFAULT (execute)

```sql
EXEC DBA.usp_UpdateStatisticsIfChanged
    @TargetDatabases        = N'DBA',
    @ChangeThresholdPercent = 20,
    @SampleMode             = 'DEFAULT',
    @WhatIf                 = 0;
```

### Two DBs, FULLSCAN (dry run), central log

```sql
EXEC DBA.usp_UpdateStatisticsIfChanged
    @TargetDatabases = N'Orders,Inventory',
    @SampleMode      = 'FULLSCAN',
    @LogDatabase     = N'UtilityDb',
    @WhatIf          = 1;
```

---

## Known limitations

* Relies on `sys.dm_db_stats_properties`; hypothetical indexes are excluded.
* Memory‑optimized tables are skipped.
* Sampling granularity on SQL 2014 rounds to whole percent (engine behavior).

---

## Versioning

* **Version**: 1.3
* **Last updated**: 2025‑11‑16

---

## License

MIT. Go forth and sample responsibly.

---

## Credit

Created by **Mike Fuller**.

---

---
