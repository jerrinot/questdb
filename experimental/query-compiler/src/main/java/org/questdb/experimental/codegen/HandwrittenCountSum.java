/*+*****************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2026 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

package org.questdb.experimental.codegen;

import io.questdb.cairo.ColumnType;
import io.questdb.cairo.map.Map;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Fixed-shape measurement reference. Its loop is handwritten C, independent of our typed IR. */
final class HandwrittenCountSum {
    private HandwrittenCountSum() { }

    static CompiledKernel compile(QuestMapProbe probe, boolean sharded, Path artifacts) throws IOException {
        long started = System.nanoTime();
        Files.createDirectories(artifacts);
        Path directory = Files.createTempDirectory(artifacts, "handwritten-").toAbsolutePath();
        Path source;
        try {
            Path classes = Path.of(HandwrittenCountSum.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            source = classes.getParent().getParent().resolve("src/main/c/handwritten_count_sum.c");
        } catch (Exception e) {
            throw new IOException("Cannot locate handwritten reference source", e);
        }
        Path input = directory.resolve("query.c"), output = directory.resolve("frontend.ll"), log = directory.resolve("clang.log");
        Files.writeString(input, "#define SHARDED " + (sharded ? 1 : 0)
                + "\n#define ORDERED " + (probe.ordered() ? 1 : 0)
                + "\n#define KEY_BYTES " + ColumnType.sizeOf(probe.keyType())
                + "\n#define ROW_SHIFT " + Map.BATCH_ROW_INDEX_SHIFT
                + "\n#define ROW_MASK " + Map.BATCH_ROW_INDEX_MASK
                + "\n#define OFFSET_MASK " + Map.BATCH_OFFSET_MASK + "ULL"
                + "\n#define HINTS " + (Boolean.getBoolean("query.codegen.disablePrefetchHints") ? 0 : 1)
                + "\n" + Files.readString(source));
        // -O2 selects frontend attributes without optnone; LLVM passes run only in our existing JIT.
        List<String> command = List.of("clang-21", "-std=c11", "-O2", "-Xclang", "-disable-llvm-passes",
                "-march=native", "-fwrapv", "-S", "-emit-llvm", "-Wall", "-Wextra", "-Werror",
                "-Wno-unused-function", input.toString(), "-o", output.toString());
        Files.writeString(directory.resolve("command.txt"), String.join("\n", command) + "\n");
        Process process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            if (!process.waitFor(30, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                process.waitFor();
                throw new IOException("Handwritten frontend timed out: " + log);
            }
        } catch (InterruptedException e) {
            process.destroyForcibly();
            Thread.currentThread().interrupt();
            throw new IOException("Handwritten frontend interrupted", e);
        }
        if (process.exitValue() != 0) throw new IOException("Handwritten frontend failed: " + Files.readString(log));
        return new LlvmBackend().compileSource(Files.readString(output), directory, started);
    }
}
