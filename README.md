# moonlink 🥮


```
┌───────────────────────────────────┐
│           Client APIs             │
├───────────────────────────────────┤
│         Table Management          │
├───────────────────┬───────────────┤
│   Write Pipeline  │  Read Pipeline│
├───────────────────┴───────────────┤
│         Storage Engine            │
└───────────────────────────────────┘
```

## Key components

### moonlink
a storage engine with in-memory arrow buffer on top of iceberg, support efficient streaming write (including updates/deletes) and up-to-date union read.

- moonlink_row: represent an input row from upstream system, converted into storage type
- TableHandler: Central coordinator that manages asynchronous operations for tables
- MooncakeTable: Core table implementation with memory and disk management, MemSlice + Snapshot
  - MemSlice: In-memory buffer for recent writes
  - Snapshots: Point-in-time views of table data for consistent reads

### moonlink_connectors
Connector for various inputs, currently only postgres
- sink: implement a sink for pg_replicate
- util: type conversion


## High-level workflow:

Write -> MemSlice ===Async==> Snapshot -> Read

## Write Path
- Append/ Delete/ Commit: Apply to MemSlice. And track changes in 'next_snapshot_task'.

- Flush: Start async 'flush' Operation to write parquet, then CommitFlush

## Read Path
- Snapshot: Every second, start async 'create_snapshot' to apply 'next_snapshot_task'.

- Read: Prepare one more file using in memory state. And return a list of all parquet files. 


## Concurrency Model

MoonLink employs an event-driven, asynchronous processing model:

- Event Loop: Processes operations in order within a single thread per table
- Async Tasks: Offloads I/O operations to separate tasks
- Mostly Lock-Free: Minimizes contention on critical paths
- Snapshot Isolation: Readers see consistent state without blocking writers


## UNDONE
- Write Iceberg (a snapshot should be equvalent to an iceberg snapshot and should write iceberg metadata)
  - Load from snapshot on restart
  - Merge/ Compact Iceberg
  - Advanced Iceberg Options
- Index & Delete of rows that are not in memslice ✅
  - Return deletion vector in read
  - On-Disk index
  - Row identifier other than int primary key
  - RefCount of temp parquet files & deletion vectors
- Handle large writes/ flush before commit
  - Handle streaming write in logical replicate
- Data types
  - Major types ✅
  - Decimal
  - Array
  - Postgres customized types
- Perf, a lot of unoptimized code
- Concurrent test
