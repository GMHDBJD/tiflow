# Offline incremental replay with a blackhole target

This opt-in mode measures DM's incremental processing work without an upstream
MySQL connection or downstream database execution. It uses normal DM master,
worker, source and task commands; there is no separate benchmark runtime.
**It discards all output and is not a replication/backup target.**

## Execution boundary

The normal parser, filters, routing, schema tracking, row transformations,
conflict handling, compactor (when enabled), worker scheduling, batching and SQL
construction remain in use. DML and DDL use the normal DBConn/BaseConn paths.
The blackhole is a write-only `database/sql` connector: begin/execute/commit pass
through DM's normal retry and metrics code, but the SQL driver performs no network
or disk I/O. SQL strings and parameters are not retained by the sink.

Checkpoint handling is **not replaced by a simplified memory implementation**.
Normal flush triggers, DML barriers, snapshots, table-info JSON serialization,
SQL construction and transaction execution remain in use. Table checkpoints are
written before the global checkpoint. Only the SQL driver discards the writes,
including checkpoint schema/table creation, updates and deletes.

This does not simulate MySQL driver protocol encoding, server execution, affected
rows, constraints, network latency or failures. Affected-row diagnostics are
inapplicable. Database metadata queries are not given fabricated responses: they
are replaced by explicit offline metadata or fail. DDL's downstream clock lookup
(for database-specific error recovery) is skipped. Source status reports the
configured archive end, not a live server position. Archive replay does not
report historical event timestamps as live replication lag.

## Input contract

* Immutable MySQL ROW/FULL binlogs with checksums and their original Rotate events.
* An explicit start position and exclusive end position, both transaction boundaries.
  The start must not split a transaction. The end is checked against the last
  transaction boundary; reaching EOF before it is an error, not success.
* Optional initial schema files containing only CREATE DATABASE, USE and CREATE
  TABLE statements, describing the source at the start position. Do not provide
  a mysqldump file containing INSERT, SET, LOCK or DROP statements. Seed rows are
  unnecessary because the target does not execute SQL. Schema-changing DDL in
  the binlog continues through the normal schema tracker.
* Explicit timezone, SQL mode and lower-case-table-names metadata. For SQL
  generation, downstream table structure and indexes mirror the source. This
  does not model a downstream with different indexes or additional columns.
* Files must be readable at identical absolute paths on every eligible worker.
  Files and directory contents must not be changed during a run. Treat the
  archive and schema paths as trusted input, not a sandbox.

The initial implementation requires the local source and blackhole target
**together**, incremental mode, no sharding, no relay, no GTID positioning, no
online DDL, no validator, and no strict downstream collation discovery.
Task `start-time` and in-place task updates are unsupported; recreate the task to
change inputs or settings. A real `target-database` is rejected, not ignored.
Old binaries do not understand these options; offline tasks cannot be downgraded.
Normal database-backed tasks keep their existing behavior.

## Configuration and commands

Place one scene's binlogs and its initial schema on the worker. For example:

```yaml
# source.yaml
source-id: local-source
enable-gtid: false
enable-relay: false
local-binlog:
  directory: /data/dm-benchmark/scene/binlog
  schema-files: [/data/dm-benchmark/scene/schema.sql]
  timezone: UTC
  sql-mode: STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION
  lower-case-table-names: 0
  end:
    binlog-name: mysql-bin.000005
    binlog-pos: 800000000  # replace with the scene's exact end position
```

```yaml
# task.yaml
name: offline-replay
task-mode: incremental
blackhole: true
online-ddl: false
mysql-instances:
  - source-id: local-source
    meta:
      binlog-name: mysql-bin.000001
      binlog-pos: 4
    syncer-config-name: replay
syncers:
  replay:
    worker-count: 4
    batch: 100
    checkpoint-flush-interval: 30
    safe-mode-duration: 0s
```

Start master and worker normally, then use the ordinary commands:

```sh
dmctl --master-addr 127.0.0.1:8261 operate-source create source.yaml
dmctl --master-addr 127.0.0.1:8261 check-task task.yaml
dmctl --master-addr 127.0.0.1:8261 start-task task.yaml
dmctl --master-addr 127.0.0.1:8261 query-status offline-replay
```

`check-task` validates the offline configuration on the master, not the contents
of worker-local files. Missing/invalid files fail on the worker. At the exact
end, DM drains the job queues and flushes checkpoints before reporting Finished.
`binlogType` is `archive`. There is no remote fallback on file/read errors.

Pause/resume in the **same Syncer instance** retains saved/flushed state and schema.
There is no persistent checkpoint: restarting the worker, failover or recreating
the task starts from configured meta. Use a recreated task for independent runs;
do not interpret a resume at EOF as a fresh benchmark.

## Measurements and evidence

Run scenes separately. Preserve the revision, source/task configs, input file
sizes and positions, schema, source generator settings, hardware/resource limits,
cache conditions, run interval, logs, profiles, metrics and failures. Keep
warm-up, repeated runs and pause/restart lifecycle tests separate from throughput
measurements. Local disk and the archive reader may still bottleneck.

The archive reader uses a 64 KiB file-read buffer and a default 10,240-event
queue, matching the current go-mysql remote client's read-buffer and event-cache
defaults. It still performs full event decoding and checksum verification, and
uses the normal StreamerController location/GTID bookkeeping. This is **not the
remote replication reader**: file framing uses ParseReader rather than MySQL
packet reception and Parse, and does not measure network/protocol overhead.
Do not interpret an archive-only rate as a production remote-reader limit.
The queue is bounded by event count, not bytes; wide rows or large events can
increase retained memory substantially. Record memory as well as throughput.

The completion log includes separate `dataSQL` and `checkpointSQL` counters:
submitted statements/parameters, commits and rollbacks. These are not source-row
or source-transaction counts. Legacy query-status `totalRows` also includes some
control jobs; use the workload manifest for source row counts. Driver timings are
blackhole execution timings, not a prediction of real downstream latency.

Use the same checkpoint interval, batch/worker settings, compaction and safe-mode
settings when comparing revisions. The sample disables initial safe mode to make
runs explicit; that is a benchmark choice, not the normal task default. A run
shorter than the checkpoint interval does not exercise periodic flush costs;
use longer runs when measuring those costs.
