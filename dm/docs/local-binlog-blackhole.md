# Configurable binlog replay and blackhole output

Input transport and output target are independent. A source can read immutable
local binlogs or use DM's native remote MySQL reader. A task can write to a real
TiDB target or explicitly discard database execution with `blackhole: true`.
Normal DM master, worker, source and task commands are used in every case.
**Blackhole discards all output and is not a replication/backup target.**

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
(for database-specific error recovery) is skipped. For local input, source status reports the
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

Local input, finite replay, and blackhole currently support incremental mode,
no sharding, no relay, no GTID positioning, no online DDL and no validator.
Local input also cannot discover strict downstream collation metadata.
An explicit filename/position start is required. In-place task updates are
unsupported; recreate the task to change input, replay or output settings.
Finite replay does not accept `start-time`. A blackhole task rejects an additional
`target-database` instead of silently discarding its configuration.
Old binaries do not understand these options. Normal live database-backed tasks
without these options retain their existing behavior.

For a **real target**, prepare the corresponding initial target schema and seed
rows before replaying UPDATE/DELETE workloads, just as for ordinary incremental
replication. Initial `schema-files` initialize DM's schema tracker; they do not
create or populate the real target. The real target's own schema/index discovery,
SQL execution and persistent checkpoints remain enabled.

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

`check-task` validates configuration and applicable checks on each real endpoint:
remote source version, binlog settings, replication privilege and start position;
real target privilege. It honors `ignore-checking-items`. Cross-database schema
compatibility checks are not performed for replay/blackhole: today's source
schema need not match the historical initial schema. Worker-local archive/schema
contents are checked on the worker. The check result states these limits.

At the exact transaction-boundary end, DM drains job queues and flushes
checkpoints before reporting Finished. It does not wait for remote EOF or a new
event beyond the endpoint. Splitting an event/transaction is an error. Local
`binlogType` is `archive`, with no remote fallback on file/read errors.

Pause/resume in the **same Syncer instance** retains saved/flushed state and schema.
With blackhole there is no persistent checkpoint: restarting the worker, failover
or recreating the task starts from configured meta. A real target retains normal
persistent checkpoints and restores checkpointed schema instead of overwriting
it with the original schema files. Use a recreated task for independent runs;
do not interpret a resume at EOF as a fresh benchmark.

## Native remote input

Keep the task above (including `blackhole: true`), but replace the source with:

```yaml
source-id: local-source  # must match mysql-instances[].source-id
flavor: mysql
enable-gtid: false
enable-relay: false
from:
  host: 127.0.0.1
  port: 3306
  user: dm_replay
  password: ""  # supply the test server's credentials securely
binlog-replay:
  schema-files: [/data/dm-benchmark/scene/schema.sql]
  end:
    binlog-name: mysql-bin.000005
    binlog-pos: 800000000
```

`binlog-replay` supplies finite-range/schema metadata; it does not select a reader
or an output. Do not specify it together with `local-binlog`, which already
contains this metadata. Without `local-binlog`, DM opens the real upstream
metadata connection and uses its existing remote StreamerController,
BinlogSyncer, MySQL packet reads, parser, checksum verification and event queue.
Timezone, SQL mode and table-name case settings are discovered from the upstream.
`binlogType` is `remote`. Supplying the same buffer/queue defaults to an archive
reader is not a substitute for this path.

The existing MySQL instance must still own/register the requested binlogs. Do not
merely copy archive files into an arbitrary live MySQL datadir or execute them
back into tables. Prefer the preserved generator instance and its original log
index. Stop workload generation during measurement and record the exact start
and end from the corpus manifest. The remote reader may prefetch beyond the end;
Syncer never applies events past it.

To use a real target with either input, omit `blackhole` and provide an ordinary
`target-database`. With no local input and no finite `binlog-replay`, blackhole
can also consume a live remote stream; provide schema-creating DDL in its stream
if no initial schema is supplied. Blackhole never guesses historical schema from
the upstream's current tables.

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

With blackhole, the completion log includes separate `dataSQL` and `checkpointSQL` counters:
submitted statements/parameters, commits and rollbacks. These are not source-row
or source-transaction counts. Legacy query-status `totalRows` also includes some
control jobs; use the workload manifest for source row counts. Driver timings are
blackhole execution timings, not a prediction of real downstream latency.

Use the same checkpoint interval, batch/worker settings, compaction and safe-mode
settings when comparing revisions. The sample disables initial safe mode to make
runs explicit; that is a benchmark choice, not the normal task default. A run
shorter than the checkpoint interval does not exercise periodic flush costs;
use longer runs when measuring those costs.

A local-versus-remote comparison must use the same logical workload, schema,
transaction size, DM binary/settings, full warmups and repeated runs. Record
MySQL's CPU/I/O, network placement and TLS as well as DM resources. A same-host
loopback TCP result exercises the native remote client but does **not** emulate
cloud network latency, TLS, a busy MySQL instance or a production capacity limit.
