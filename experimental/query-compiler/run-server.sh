#!/usr/bin/env bash
set -euo pipefail
module_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd -- "$module_dir/../.." && pwd)

if [[ ! -f "$module_dir/target/classes/org/questdb/experimental/codegen/ResolvedQueryCompiler.class" || ! -f "$module_dir/target/native/libquery_codegen_llvm.so" ]]; then
    echo "Build with: mvn -Pquery-compiler-prototype -pl experimental/query-compiler -am -DskipTests -Djacoco.skip=true install" >&2
    exit 1
fi

# Ordinary ServerMain; server.conf decides whether the compiler is enabled.
exec java -ea \
    --enable-native-access=ALL-UNNAMED \
    --sun-misc-unsafe-memory-access=allow \
    --add-opens=java.base/java.lang=ALL-UNNAMED \
    --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
    --add-opens=java.base/java.nio=ALL-UNNAMED \
    --add-opens=java.base/java.time.zone=ALL-UNNAMED \
    --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
    -cp "$module_dir/target/classes:$repo_dir/core/target/classes" \
    io.questdb.ServerMain "$@"
