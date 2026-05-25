#!/bin/bash
# Retry wrapper for hive-init.
# HiveServer2 sometimes needs extra seconds after the healthcheck passes before
# it can handle DDL (especially STORED AS PARQUET which touches the warehouse FS).
set -euo pipefail

MAX_RETRIES=10
RETRY_DELAY=10

for i in $(seq 1 "$MAX_RETRIES"); do
    echo "[hive-init] attempt $i/$MAX_RETRIES …"
    if beeline -u 'jdbc:hive2://hive:10000' \
               --hiveconf hive.cli.print.header=false \
               -f /init/init.sql 2>&1; then
        echo "[hive-init] SUCCESS — datacoolie_test tables created."
        exit 0
    fi
    echo "[hive-init] beeline failed; retrying in ${RETRY_DELAY}s …"
    sleep "$RETRY_DELAY"
done

echo "[hive-init] ERROR: all $MAX_RETRIES attempts failed."
exit 1
