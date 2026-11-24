#!/usr/bin/env bash
set -euo pipefail
task_root=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
task_binary="$task_root/.build/linux/edudb"
if [ -x "$task_root/bin/edudb-linux" ]; then task_binary="$task_root/bin/edudb-linux"; fi
exec "$task_binary" serve --data "$task_root/data-v41" --web "$task_root/web" --port "${1:-8765}"
