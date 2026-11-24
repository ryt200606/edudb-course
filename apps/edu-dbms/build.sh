#!/usr/bin/env bash
set -euo pipefail
task_root=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
cmake -S "$task_root" -B "$task_root/.build/linux" -DCMAKE_BUILD_TYPE=Release
cmake --build "$task_root/.build/linux" --parallel 2
ctest --test-dir "$task_root/.build/linux" --output-on-failure
