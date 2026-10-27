#!/usr/bin/env bash
set -euo pipefail
module_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
cmake -S "$module_dir/src/main/c" -B "$module_dir/target/native" -DCMAKE_BUILD_TYPE=Release
cmake --build "$module_dir/target/native" --parallel 4
