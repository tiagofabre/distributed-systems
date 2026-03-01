#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

BASE_URL="${BASE_URL:-http://127.0.0.1:8201}"
RATE="${RATE:-10}"
DURATION="${DURATION:-30s}"
TARGETS="${TARGETS:-all}"
SAVE_RESULTS="${SAVE_RESULTS:-0}"
CONNECTIONS="${CONNECTIONS:-16}"
PHASE_DELAY="${PHASE_DELAY:-3}"

if ! command -v vegeta &>/dev/null; then
  echo "vegeta is not in PATH. Install it (e.g. brew install vegeta or see https://github.com/tsenart/vegeta)." >&2
  exit 1
fi

run_attack() {
  local name="$1"
  local target_file="$2"
  local targets_path="$SCRIPT_DIR/$target_file"
  if [[ ! -f "$targets_path" ]]; then
    echo "Target file not found: $targets_path" >&2
    return 1
  fi
  echo "--- $name (rate=$RATE, duration=$DURATION) ---" >&2
  if [[ "$SAVE_RESULTS" == "1" ]]; then
    local bin_file="$SCRIPT_DIR/results-$name.bin"
    sed "s|__BASE_URL__|$BASE_URL|g" "$targets_path" | vegeta attack -rate="$RATE" -duration="$DURATION" -connections="$CONNECTIONS" | tee "$bin_file" | vegeta report
    echo "Results saved to $bin_file" >&2
  else
    sed "s|__BASE_URL__|$BASE_URL|g" "$targets_path" | vegeta attack -rate="$RATE" -duration="$DURATION" -connections="$CONNECTIONS" | vegeta report
  fi
}

case "$TARGETS" in
  health)
    run_attack "health" "targets/health.txt"
    ;;
  put)
    run_attack "put" "targets/put.txt"
    ;;
  get)
    run_attack "get" "targets/get.txt"
    ;;
  all)
    run_attack "health" "targets/health.txt"
    echo "" >&2
    [[ "$PHASE_DELAY" -gt 0 ]] && echo "Waiting ${PHASE_DELAY}s for connections to close..." >&2 && sleep "$PHASE_DELAY"
    run_attack "put" "targets/put.txt"
    echo "" >&2
    [[ "$PHASE_DELAY" -gt 0 ]] && echo "Waiting ${PHASE_DELAY}s for connections to close..." >&2 && sleep "$PHASE_DELAY"
    run_attack "get" "targets/get.txt"
    ;;
  *)
    echo "TARGETS must be one of: health, get, put, all (got: $TARGETS)" >&2
    exit 1
    ;;
esac
