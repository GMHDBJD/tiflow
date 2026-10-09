# Local binlog / blackhole integration test

This case starts ordinary DM master/worker processes and uses dmctl to create a
source, check/start a task, observe Finished at the exact end, and stop the task
and source. No upstream or downstream database is used.

`data/binlog.b64` is the unchanged file header and first two complete transactions
of a MySQL 8.0.46 ROW/FULL/CRC32 binlog produced by sysbench `oltp_write_only` on
synthetic tables (16 tables, `id`, `k`, `c CHAR(120)`, `pad CHAR(60)`). It contains
UPDATE, DELETE and INSERT row events. `schema.sql` is the initial schema.
`manifest.json` records positions and SHA-256. Base64 makes this small fixture
portable in source control; this is a lifecycle test, **not a performance corpus**.
The representative multi-GB benchmark archives must be supplied separately.

Run through the integration suite, or against already built release binaries:

```sh
python3 dm/tests/local_binlog_blackhole/check.py --bin-dir /path/to/bin \
  --work-dir /tmp/local-binlog-check --master-port 28261 \
  --worker-port 28262 --peer-port 28291
```

The release bin directory must contain `dm-master`, `dm-worker`, and `dmctl`
(or `dm-ctl`). `--test-binaries` uses the `.test` binaries and writes coverage.
All spawned processes are scoped to this case and cleaned up; logs, configurations
and every dmctl response remain in the work directory, including on failure.

To exercise the **native remote reader**, point the same test at a disposable
MySQL 8.0 server with ROW/FULL binlogs and replication privileges:

```sh
python3 dm/tests/local_binlog_blackhole/check.py --bin-dir /path/to/bin \
  --work-dir /tmp/remote-binlog-check --remote-host 127.0.0.1 \
  --mysql-port 3306 --mysql-user root \
  --mysql-command 'mysql --protocol=tcp -h127.0.0.1 -P3306 -uroot'
```

The administrative client command must connect to the same server. It can also
be `docker exec -i CONTAINER mysql -uroot`. `MYSQL_PWD` (or the environment variable
named by `--mysql-password-env`) supplies the DM source password; do not put
passwords in command-line arguments. Generated source/task files have mode 0600;
treat the work directory as private and redact credentials before sharing it.

The remote case creates a uniquely named fixture database without overwriting
existing databases, records positions around INSERT/UPDATE/DELETE and an ALTER
TABLE, and checks `binlogType: remote` and Finished at the exact transaction end.
It leaves the small fixture database for inspection; use disposable test servers.
No binlogs are reset or purged. This is still a lifecycle test, not a benchmark.
