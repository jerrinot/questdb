#!/usr/bin/env bash
set -euo pipefail
module_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
hints=()
# Homebrew's llvm@21 is keg-only: CMake finds it only through an explicit prefix.
if [[ "$(uname -s)" == Darwin && -z "${CMAKE_PREFIX_PATH:-}" && -z "${LLVM_DIR:-}" ]] && command -v brew >/dev/null; then
    if llvm_prefix=$(brew --prefix llvm@21 2>/dev/null) && [[ -d "$llvm_prefix/lib/cmake/llvm" ]]; then
        hints+=("-DCMAKE_PREFIX_PATH=$llvm_prefix")
    fi
fi
# The expansion guard keeps an empty array legal under "set -u" in macOS's bash 3.2.
cmake -S "$module_dir/src/main/c" -B "$module_dir/target/native" -DCMAKE_BUILD_TYPE=Release ${hints[@]+"${hints[@]}"}
cmake --build "$module_dir/target/native" --parallel 4
