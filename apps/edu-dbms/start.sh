#!/usr/bin/env bash
set -euo pipefail
task_root=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
candidates=("$task_root/bin/edudb-linux" "$task_root/.build/linux/edudb")
if [[ -n "${EDUDB_BINARY:-}" ]]; then candidates=("$EDUDB_BINARY"); fi
for candidate in "${candidates[@]}"; do
  [[ -x "$candidate" ]] || continue
  if output=$("$candidate" help 2>/dev/null) && [[ "${output%%$'\n'*}" == 'EduDB 4.1.0' ]]; then
    if [[ "${1:-}" == '--print-binary' ]]; then printf '%s\n' "$candidate"; exit 0; fi
    exec "$candidate" serve --data "${2:-$task_root/data-v41}" --web "$task_root/web" --port "${1:-8765}"
  fi
done
echo 'No runnable EduDB 4.1.0 binary found. Run bash build.sh or install the 4.1.0 Linux release.' >&2
exit 1
