
# DBA.usp_RebuildIndexesIfBloated

Rebuild only what is bloated. This stored procedure targets leaf-level **rowstore** index partitions whose **avg_page_space_used_in_percent** falls below a threshold. It is partition aware, ONLINE aware, optionally resumable, and logs every decision.

> Works on SQL Server 2014 through 2022. Supports Enterprise, Developer, and Evaluation features where applicable. Defaults to a safe **WhatIf** mode.

---

## Why this exists

- Page density tells the truth on SSDs. Logical fragmentation does not pay your storage bill.
- Touch only the slices that need it. Stop flooring the gas on every index every time.
- Keep a paper trail. Every candidate and command is logged for audit and trending.

---

## Features

- Targets **rowstore** only (clustered and nonclustered)
- Leaves tiny partitions alone via `@MinPageCount`
- Partition aware: rebuilds specific partitions only
- Optional `ONLINE = ON` with `WAIT_AT_LOW_PRIORITY`
- Optional **resumable** rebuilds (SQL 2019+, ONLINE only)
- Compression handled: preserve from source or force `NONE | ROW | PAGE`
- Fill factor handled: preserve per index or force a global value
- Full logging to `[DBA].[IndexBloatRebuildLog]` in a target or central log DB
- Captures trending signals: `avg_row_bytes`, `record_count`, `ghost_record_count`, `forwarded_record_count`, AU pages, and a computed `allocated_unused_pages`
- Defaults to **WhatIf** so you can review before execution

---

## Compatibility

- **SQL Server**: 2014 to 2022  
- **ONLINE**: Enterprise, Developer, Evaluation  
- **Resumable**: SQL Server 2019+ and ONLINE  
- **Compression**: available broadly on newer versions and Enterprise family

The procedure self-detects edition and version, then quietly downgrades features if not supported.

---

## Installation

1. Create the `DBA` schema if needed and deploy the procedure in the database of your choice (or Utility database if you prefer central deployment).
2. First execution will ensure the log table exists in either the **target DB** or a central **Log DB** you specify via `@LogDatabase`.

> You can deploy once in a Utility and call it for any user database. It logs to a Utility or target DB as you prefer.

---

## Parameters

| Parameter | Type | Default | Notes |
|---|---|---:|---|
| `@DatabaseName` | SYSNAME | **required** | Target database to analyze and rebuild |
| `@MinPageDensityPct` | DECIMAL(5,2) | 70.0 | Rebuild when leaf partition density is below this percent |
| `@MinPageCount` | INT | 1000 | Skip partitions smaller than this page count |
| `@UseExistingFillFactor` | BIT | 1 | Keep index fill factor as is |
| `@FillFactor` | TINYINT | NULL | Used only when `@UseExistingFillFactor = 0` |
| `@Online` | BIT | 1 | ONLINE when supported by edition |
| `@MaxDOP` | INT | NULL | If null, use server default |
| `@SortInTempdb` | BIT | 1 | Sort in tempdb unless resumable requires OFF |
| `@UseCompressionFromSource` | BIT | 1 | Keep existing DATA_COMPRESSION |
| `@ForceCompression` | NVARCHAR(20) | NULL | `NONE` `ROW` or `PAGE` when not preserving |
| `@SampleMode` | VARCHAR(16) | `SAMPLED` | `SAMPLED` or `DETAILED` |
| `@CaptureTrendingSignals` | BIT | 0 | If 1 and `SAMPLED`, auto upshift to `DETAILED` for reliable metrics |
| `@LogDatabase` | SYSNAME | NULL | If set, logs to this DB instead of target DB |
| `@WaitAtLowPriorityMinutes` | INT | NULL | With ONLINE, enables `WAIT_AT_LOW_PRIORITY` |
| `@AbortAfterWait` | NVARCHAR(20) | NULL | `NONE` `SELF` or `BLOCKERS` with low-priority waits |
| `@Resumable` | BIT | 0 | Resumable rebuilds (SQL 2019+, ONLINE required) |
| `@MaxDurationMinutes` | INT | NULL | Resumable `MAX_DURATION` |
| `@DelayMsBetweenCommands` | INT | NULL | Optional wait between commands |
| `@WhatIf` | BIT | 1 | Dry run by default |

**Important safeguards**

- `@SampleMode` accepts only `SAMPLED` or `DETAILED`
- Resumable rebuilds are skipped for filtered indexes, included LOB columns, or computed/rowversion key columns
- If ONLINE is unsupported on the edition, ONLINE options are disabled

---

## Quick start

### Dry run on one database
```sql
EXEC DBA.usp_RebuildIndexesIfBloated
    @DatabaseName = N'YourDb',
    @WhatIf       = 1;
````

### Execute with ONLINE and DOP 4

```sql
EXEC DBA.usp_RebuildIndexesIfBloated
    @DatabaseName = N'YourDb',
    @Online       = 1,
    @MaxDOP       = 4,
    @WhatIf       = 0;
```

### Force ROW compression

```sql
EXEC DBA.usp_RebuildIndexesIfBloated
    @DatabaseName             = N'YourDb',
    @UseCompressionFromSource = 0,
    @ForceCompression         = N'ROW',
    @WhatIf                   = 0;
```

### Low priority wait

```sql
EXEC DBA.usp_RebuildIndexesIfBloated
    @DatabaseName             = N'YourDb',
    @Online                   = 1,
    @WaitAtLowPriorityMinutes = 5,
    @AbortAfterWait           = N'BLOCKERS',
    @WhatIf                   = 0;
```

### Resumable with MAX_DURATION

```sql
EXEC DBA.usp_RebuildIndexesIfBloated
    @DatabaseName       = N'YourDb',
    @Online             = 1,
    @Resumable          = 1,
    @MaxDurationMinutes = 60,
    @WhatIf             = 0;
```

---

## Logging

**What is logged**: one row per candidate or action in **[DBA].[IndexBloatRebuildLog]**, including identity (database_name, schema_name, table_name, index_name, index_id, partition_number), metrics (page_count, page_density_pct, fragmentation_pct, avg_row_bytes, record_count, ghost_record_count, forwarded_record_count), allocation unit pages (au_total_pages, au_used_pages, au_data_pages), computed `allocated_unused_pages` = `au_total_pages - au_used_pages`, chosen options (chosen_fill_factor, ONLINE flag, MaxDOP), action and status (DRYRUN or REBUILD; SKIPPED, PENDING, SUCCESS, FAILED), the executed `cmd` text, and full error metadata on failures.

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

* tempdb and version store: ONLINE with `SORT_IN_TEMPDB = ON` pushes work to tempdb and version store
* Transaction log: rebuilds are fully logged; make sure primary and secondaries keep up
* MaxDOP: if you need strictly ordered extent allocation, set `@MaxDOP = 1`
* Sampling: `SAMPLED` is fast; set `@CaptureTrendingSignals = 1` to auto use `DETAILED` for reliable row-size and churn metrics

---

## Known limitations

* Rowstore only; columnstore is out of scope
* Resumable rebuilds skip filtered indexes, indexes with included LOB, and indexes with computed or rowversion keys
* Memory optimized tables are skipped

---

## Versioning

* Version: **1.6** (2014 to 2022)
* Last updated: **2025-10-29**

See the header in the procedure for the exact parameter list and server capability checks.

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

```

```
