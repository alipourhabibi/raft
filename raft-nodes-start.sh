#!/usr/bin/env bash

set -euo pipefail

LOG_FILE="raft-combined.log.out"

declare -A NODES=(
  [node1]=50051
  [node2]=50052
  [node3]=50053
)

PIDS=()

cleanup() {
  echo ""
  echo "Stopping nodes..."

  # stop processes
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
  done

  # wait for them to exit so logs flush
  wait "${PIDS[@]}" 2>/dev/null || true

  echo "Merging logs..."

  FILES=(raft-node*.log)

  # ensure combined file exists
  : > "$LOG_FILE"

  if [ -e "${FILES[0]}" ]; then
    cat "${FILES[@]}" | sort -k3,3 -k4,4 >> "$LOG_FILE"
  fi

  rm -f raft-node*.log

  echo "Combined log written to $LOG_FILE"
  echo "Total lines: $(wc -l < "$LOG_FILE")"
  echo ""
  tail -n 20 "$LOG_FILE" || true

  exit 0
}

trap cleanup INT TERM

rm -f raft-node*.log
: > "$LOG_FILE"

echo "Starting nodes..."

for name in "${!NODES[@]}"; do
  port="${NODES[$name]}"

  peers=()
  for other in "${!NODES[@]}"; do
    [[ "$other" == "$name" ]] && continue
    peers+=("${other}:localhost:${NODES[$other]}")
  done

  peers_str=$(IFS=','; echo "${peers[*]}")

  (
    PORT="$port" \
    ID="$name" \
    NODES="$peers_str" \
    stdbuf -oL -eL go run . 2>&1 | sed "s/^/${name} | /"
  ) > "raft-${name}.log" &

  PIDS+=($!)
done

echo "Nodes started: ${PIDS[*]}"
echo "Press Ctrl+C to stop"

wait || true
cleanup
