#!/bin/bash
set -euo pipefail
cur=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
python3 "$cur/check.py" --test-binaries --bin-dir "$PWD/bin" \
	--work-dir "${TEST_DIR:-/tmp/dm_test}/${TEST_NAME:-local_binlog_blackhole}/$(date +%s%N)"
echo "[$(date)] <<<<<< test case ${TEST_NAME:-local_binlog_blackhole} success! >>>>>>"
