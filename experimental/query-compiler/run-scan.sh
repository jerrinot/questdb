#!/usr/bin/env bash
set -euo pipefail
repo_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
cd -- "$repo_dir"
core_classpath=${QUERY_CODEGEN_CORE_CLASSPATH:-core/target/classes}
exec java \
  -Dout=experimental/query-compiler/parallel-log.conf \
  --enable-native-access=ALL-UNNAMED \
  --sun-misc-unsafe-memory-access=allow \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/java.time.zone=ALL-UNNAMED \
  --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
  -cp "experimental/query-compiler/target/classes:$core_classpath" \
  org.questdb.experimental.codegen.ScanMatrix "$@"
