#!/usr/bin/env python3
# Copyright 2026 PingCAP, Inc. Licensed under the Apache License, Version 2.0.
"""Normal cluster lifecycle test, with no upstream/downstream database dependency."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import shlex
import signal
import socket
import subprocess
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bin-dir", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--test-binaries", action="store_true")
    parser.add_argument("--master-port", type=int, default=8261)
    parser.add_argument("--worker-port", type=int, default=8262)
    parser.add_argument("--peer-port", type=int, default=8291)
    args = parser.parse_args()
    root = args.work_dir.resolve()
    root.mkdir(parents=True, exist_ok=False)
    bindir = args.bin_dir.resolve()
    fixture = Path(__file__).resolve().parent / "data"
    manifest = json.loads((fixture / "manifest.json").read_text())
    archive = root / "binlog"
    archive.mkdir()
    data = base64.b64decode((fixture / "binlog.b64").read_bytes())
    if hashlib.sha256(data).hexdigest() != manifest["sha256"]:
        raise RuntimeError("test fixture digest mismatch")
    (archive / manifest["filename"]).write_bytes(data)
    source = root / "source.yaml"
    source.write_text(f"""source-id: local-source
enable-gtid: false
enable-relay: false
local-binlog:
  directory: {json.dumps(str(archive))}
  schema-files: [{json.dumps(str(fixture / 'schema.sql'))}]
  timezone: UTC
  sql-mode: STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION
  lower-case-table-names: 0
  end:
    binlog-name: {manifest['filename']}
    binlog-pos: {manifest['end']}
""")
    task = root / "task.yaml"
    task.write_text(f"""name: offline-test
task-mode: incremental
blackhole: true
online-ddl: false
mysql-instances:
  - source-id: local-source
    meta:
      binlog-name: {manifest['filename']}
      binlog-pos: {manifest['start']}
    syncer-config-name: replay
syncers:
  replay:
    worker-count: 4
    batch: 100
    safe-mode-duration: 0s
""")
    processes = []
    sequence = 0

    def command(name, suffix):
        binary = bindir / (name + (".test" if args.test_binaries else ""))
        if name == "dmctl" and not binary.exists() and not args.test_binaries:
            binary = bindir / "dm-ctl"
        if args.test_binaries:
            return [str(binary), f"-test.coverprofile={root / (suffix + '.out')}", "DEVEL"]
        return [str(binary)]

    def wait_port(port):
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if any(p.poll() is not None for p, _ in processes):
                raise RuntimeError("cluster process exited; inspect logs")
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=1):
                    return
            except OSError:
                time.sleep(0.2)
        raise TimeoutError(f"cluster port {port} not ready")

    def launch(name, flags, port):
        log = (root / (name + ".console")).open("w")
        try:
            proc = subprocess.Popen(command(name, name) + flags, cwd=root,
                                    stdout=log, stderr=subprocess.STDOUT,
                                    start_new_session=True)
        except BaseException:
            log.close()
            raise
        processes.append((proc, log))
        wait_port(port)

    def ctl(*words):
        nonlocal sequence
        sequence += 1
        label = f"ctl-{sequence:03d}"
        cmd = command("dmctl", label) + ["--master-addr", f"127.0.0.1:{args.master_port}"]
        stdin = shlex.join(words) + "\n" if args.test_binaries else None
        if not args.test_binaries:
            cmd.extend(words)
        result = subprocess.run(cmd, input=stdin, text=True, cwd=root,
                                capture_output=True, timeout=40)
        (root / (label + ".stdout")).write_text(result.stdout)
        (root / (label + ".stderr")).write_text(result.stderr)
        if result.returncode:
            raise RuntimeError(f"{label} failed: {words}")
        value, _ = json.JSONDecoder().raw_decode(result.stdout[result.stdout.index("{"):])
        def check_results(obj):
            if isinstance(obj, dict):
                if obj.get("result") is False:
                    raise RuntimeError(f"{label}: {obj}")
                for child in obj.values():
                    check_results(child)
            elif isinstance(obj, list):
                for child in obj:
                    check_results(child)
        check_results(value)
        return value

    try:
        # Never accidentally query or stop an unrelated cluster on these ports.
        for port in (args.master_port, args.worker_port, args.peer_port):
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", port))
        peer = f"http://127.0.0.1:{args.peer_port}"
        launch("dm-master", ["--name", "offline-master", "--master-addr", f"127.0.0.1:{args.master_port}",
                            "--advertise-addr", f"127.0.0.1:{args.master_port}",
                            "--peer-urls", peer, "--advertise-peer-urls", peer,
                            "--initial-cluster", f"offline-master={peer}",
                            "--data-dir", str(root / "master-data"),
                            "--log-file", str(root / "master.log")], args.master_port)
        launch("dm-worker", ["--name", "offline-worker", "--worker-addr", f"127.0.0.1:{args.worker_port}",
                            "--advertise-addr", f"127.0.0.1:{args.worker_port}",
                            "--join", f"127.0.0.1:{args.master_port}",
                            "--log-file", str(root / "worker.log")], args.worker_port)
        ctl("operate-source", "create", str(source))
        ctl("check-task", str(task))
        ctl("start-task", str(task))
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            status = ctl("query-status", "offline-test")
            tasks = [task for src in status.get("sources", []) for task in src.get("subTaskStatus", [])]
            if tasks and all(task["stage"] == "Finished" for task in tasks):
                for task in tasks:
                    sync = task["sync"]
                    expected = f"({manifest['filename']}, {manifest['end']})"
                    if sync["syncerBinlog"] != expected or sync["binlogType"] != "archive":
                        raise RuntimeError(f"unexpected completion: {sync}")
                (root / "result.json").write_text(json.dumps(status, indent=2))
                break
            if any(task["stage"] == "Paused" for task in tasks):
                raise RuntimeError(f"task paused unexpectedly: {status}")
            time.sleep(0.5)
        else:
            raise TimeoutError("task did not finish")
        ctl("stop-task", "offline-test")
        ctl("operate-source", "stop", str(source))
        print(f"PASS: ordinary source/task lifecycle; evidence: {root}")
    finally:
        for proc, log in reversed(processes):
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGTERM)
                try:
                    proc.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    os.killpg(proc.pid, signal.SIGKILL)
                    proc.wait()
            log.close()


if __name__ == "__main__":
    main()
