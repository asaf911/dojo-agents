#!/usr/bin/env bash
set -euo pipefail
export PATH="/usr/local/bin:/usr/bin:/bin"
PYTHON="${PYTHON:-/usr/bin/python3}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$ROOT/data/logs"
mkdir -p "$LOG_DIR"
LOG="$LOG_DIR/pipeline-$(date -u +%Y%m%d).log"
cd "$ROOT"
echo "=== $(date -Is) pipeline start pid=$$ ===" >>"$LOG"
"$PYTHON" -m pipeline.run_incremental "$@" 2>&1 | tee -a "$LOG"
exit "${PIPESTATUS[0]}"
