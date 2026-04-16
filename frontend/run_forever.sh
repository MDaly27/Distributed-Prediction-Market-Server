#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="/tmp/prediction-market-logs"
LOG_FILE="$LOG_DIR/frontend.log"
mkdir -p "$LOG_DIR"

while true; do
  printf '[%s] starting frontend\n' "$(date -Is)" >>"$LOG_FILE"
  if ! "$SCRIPT_DIR/run_component.sh" >>"$LOG_FILE" 2>&1; then
    printf '[%s] frontend exited, restarting in 5 seconds\n' "$(date -Is)" >>"$LOG_FILE"
    sleep 5
    continue
  fi
  printf '[%s] frontend exited cleanly, restarting in 5 seconds\n' "$(date -Is)" >>"$LOG_FILE"
  sleep 5
done
