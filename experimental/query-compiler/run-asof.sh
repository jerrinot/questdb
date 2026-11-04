#!/usr/bin/env bash
set -euo pipefail
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd -- "$repo_dir"
exec java --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
  -Dout=experimental/query-compiler/parallel-log.conf \
  --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.time.zone=ALL-UNNAMED --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
  -Xmx2g -cp "${ASOF_CLASSES:-experimental/query-compiler/target/classes}:experimental/query-compiler/target/classes:core/target/classes" \
  org.questdb.experimental.codegen.AsOfSelectionMatrix "$@"
