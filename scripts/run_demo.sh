#!/usr/bin/env bash
# Runs the three demo scenarios against a RabbitMQ broker on localhost
# and writes the output to logs/. Requires RabbitMQ to already be running.
set -u
cd "$(dirname "$0")/.."
PY="${PYTHON:-python3}"
mkdir -p logs

reset() {
  sleep 1   # let the broker requeue anything a killed worker left unacked
  "$PY" scripts/purge_queue.py
  rm -f processed_events.txt processed_events.txt.lock
}

PIDS=()
start_worker() { "$PY" -u worker.py "$@" >> "$L" 2>&1 & PIDS+=($!); }
stop_all() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done; PIDS=(); wait 2>/dev/null; sleep 1; }

# ---------------------------------------------------------------- 1
echo "== Scenario 1: successful processing + two competing consumers"
reset; L=logs/1_success_competing_consumers.log; : > "$L"
start_worker --name worker-1
start_worker --name worker-2
sleep 2
"$PY" -u producer.py --count 6 >> "$L" 2>&1
sleep 8
stop_all

# ---------------------------------------------------------------- 2
echo "== Scenario 2: worker dies before ACK -> redelivery"
reset; L=logs/2_failure_and_redelivery.log; : > "$L"
start_worker --name worker-1 --crash-before-ack
sleep 2
start_worker --name worker-2
sleep 2
"$PY" -u producer.py --count 3 >> "$L" 2>&1
sleep 6
stop_all

# ---------------------------------------------------------------- 3
echo "== Scenario 3: duplicate event_id is detected"
reset; L=logs/3_duplicate_handling.log; : > "$L"
start_worker --name worker-1
start_worker --name worker-2
sleep 2
"$PY" -u producer.py --count 3 --duplicate >> "$L" 2>&1
sleep 6
stop_all

echo "Done. See logs/"
