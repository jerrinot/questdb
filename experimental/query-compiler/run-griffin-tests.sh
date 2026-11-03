#!/usr/bin/env bash
set -euo pipefail
module_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repo_dir=$(cd -- "$module_dir/../.." && pwd)

if [[ $# -lt 2 || ! -f "$module_dir/target/test-classes/org/questdb/experimental/codegen/GriffinTestRunner.class" ]]; then
    echo "Build with: mvn -Pquery-compiler-prototype -pl experimental/query-compiler -am -DskipTests -Djacoco.skip=true install" >&2
    echo "Usage: $0 stock|llvm class[#method] ..." >&2
    exit 1
fi

mvn -q -f "$module_dir/pom.xml" dependency:build-classpath \
    -DincludeScope=test -Dmdep.outputFile="$module_dir/target/griffin-classpath.txt"
dependencies=$(< "$module_dir/target/griffin-classpath.txt")
# Core resources include filesystem fixtures; prefer the current checkout's compiled tests.
classpath="$module_dir/target/test-classes:$module_dir/target/classes:$repo_dir/core/target/test-classes:$repo_dir/core/target/classes:$dependencies"
cd -- "$repo_dir"
exec java -ea -Xmx2g --enable-native-access=ALL-UNNAMED --sun-misc-unsafe-memory-access=allow \
    --add-opens=java.base/java.lang=ALL-UNNAMED \
    --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
    --add-opens=java.base/java.nio=ALL-UNNAMED \
    --add-opens=java.base/java.time.zone=ALL-UNNAMED \
    --add-exports=java.base/jdk.internal.vm=ALL-UNNAMED \
    -cp "$classpath" org.questdb.experimental.codegen.GriffinTestRunner "$@"
