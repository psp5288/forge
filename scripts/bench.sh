#!/bin/sh
# Measured workload for the README. fsync-before-ack is the server default:
# SET is acknowledged only after F_FULLFSYNC on macOS.
# Requires redis-benchmark on PATH and a release forge-server already listening.
set -eu
host="${1:-127.0.0.1}"
port="${2:-6399}"
echo "warmup SET c=16 P=1 d=32 n=400"
redis-benchmark -h "$host" -p "$port" -t set -c 16 -P 1 -d 32 -n 400 -q
echo "SET c=16 P=1 d=32 n=3000"
redis-benchmark -h "$host" -p "$port" -t set -c 16 -P 1 -d 32 -n 3000
echo "GET c=16 P=1 d=32 n=8000"
redis-benchmark -h "$host" -p "$port" -t get -c 16 -P 1 -d 32 -n 8000
echo "SET c=16 P=16 d=32 n=20000"
redis-benchmark -h "$host" -p "$port" -t set -c 16 -P 16 -d 32 -n 20000
