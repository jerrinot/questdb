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

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;

/** Conservative share of the last data cache reachable by this process. Missing metadata disables subdivision. */
final class CacheSizing {
    private static final List<Cache> CACHES = discover(0);
    private static final List<Cache> L2_CACHES = discover(2);
    // Admission hint only: a large allocation can still have a small hot working set.
    static final long L2_DATA_BYTES = L2_CACHES.stream().mapToLong(Cache::bytes).max().orElse(0);
    // Leave room for the scan and probe scratch when a worker owns a cache.
    // Allowed siblings share it; use the smallest budget on heterogeneous CPUs.
    static final long LOCAL_AGGREGATE_BYTES = L2_CACHES.stream()
            .mapToLong(cache -> cache.bytes / Math.max(2, cache.processors)).min().orElse(256 * 1024);

    static long perWorker(int parallelism) {
        long result = Long.MAX_VALUE;
        for (Cache cache : CACHES) result = Math.min(result, cache.bytes / Math.min(parallelism, cache.processors));
        return result == Long.MAX_VALUE ? 0 : result;
    }

    private static List<Cache> discover(int targetLevel) {
        if (System.getProperty("os.name").equals("Mac OS X")) return discoverDarwin(targetLevel);
        try {
            BitSet allowed = null;
            for (String line : Files.readAllLines(Path.of("/proc/self/status"))) {
                if (line.startsWith("Cpus_allowed_list:")) allowed = processors(line.substring(line.indexOf(':') + 1).trim());
            }
            if (allowed == null || allowed.isEmpty()) return List.of();
            var result = new ArrayList<Cache>();
            var seen = new HashSet<String>();
            for (int cpu = allowed.nextSetBit(0); cpu >= 0; cpu = allowed.nextSetBit(cpu + 1)) {
                Path last = null;
                int level = 0;
                try (var entries = Files.newDirectoryStream(Path.of("/sys/devices/system/cpu/cpu" + cpu + "/cache"), "index*")) {
                    for (Path entry : entries) {
                        String type = read(entry, "type");
                        int candidate = Integer.parseInt(read(entry, "level"));
                        if ((type.equals("Unified") || type.equals("Data")) && candidate > level
                                && (targetLevel == 0 || candidate == targetLevel)) {
                            last = entry;
                            level = candidate;
                        }
                    }
                }
                if (last == null) return List.of();
                String shared = read(last, "shared_cpu_list");
                if (seen.add(level + ":" + shared)) {
                    BitSet members = processors(shared);
                    members.and(allowed);
                    String size = read(last, "size");
                    long bytes = size.endsWith("K") ? Long.parseLong(size.substring(0, size.length() - 1)) * 1024
                            : size.endsWith("M") ? Long.parseLong(size.substring(0, size.length() - 1)) * 1024 * 1024
                            : Long.parseLong(size);
                    if (bytes <= 0 || members.isEmpty()) return List.of();
                    result.add(new Cache(bytes, members.cardinality()));
                }
            }
            return List.copyOf(result);
        } catch (IOException | IllegalArgumentException | SecurityException unavailable) {
            return List.of();
        }
    }

    /**
     * macOS has neither sysfs nor a CPU affinity mask. sysctl describes one cache
     * shape per performance level; a scheduler may place workers on any level.
     */
    private static List<Cache> discoverDarwin(int targetLevel) {
        try (Arena arena = Arena.ofConfined()) {
            Linker linker = Linker.nativeLinker();
            MethodHandle sysctl = linker.downcallHandle(linker.defaultLookup().find("sysctlbyname").orElseThrow(),
                    FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.ADDRESS,
                            ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
            long levels = sysctl(sysctl, arena, "hw.nperflevels");
            var result = new ArrayList<Cache>();
            // Intel hosts and older releases report no performance levels.
            for (int i = 0; i < Math.max(1, levels); i++) {
                String prefix = levels > 0 ? "hw.perflevel" + i + "." : "hw.";
                long bytes = 0, processors = 0;
                for (int level = targetLevel == 0 ? 3 : targetLevel; level >= 2 && bytes <= 0; level--) {
                    if (targetLevel != 0 && level != targetLevel) break;
                    bytes = sysctl(sysctl, arena, prefix + "l" + level + "cachesize");
                    // Without levels, hw.cacheconfig[n] counts the logical CPUs sharing level n.
                    processors = levels > 0 ? sysctl(sysctl, arena, prefix + "cpusperl" + level)
                            : sysctl(sysctl, arena, "hw.cacheconfig", level);
                }
                if (bytes <= 0 || processors <= 0 || processors > Integer.MAX_VALUE) return List.of();
                result.add(new Cache(bytes, (int) processors));
            }
            return List.copyOf(result);
        } catch (Throwable unavailable) {
            if (unavailable instanceof Error error && !(unavailable instanceof LinkageError)) throw error;
            return List.of();
        }
    }

    private static long sysctl(MethodHandle sysctl, Arena arena, String name) throws Throwable { return sysctl(sysctl, arena, name, 0); }

    /** Element of an integer sysctl; 0 when the name, element or integer width is absent. */
    private static long sysctl(MethodHandle sysctl, Arena arena, String name, int index) throws Throwable {
        MemorySegment value = arena.allocate(ValueLayout.JAVA_LONG, 16), size = arena.allocate(ValueLayout.JAVA_LONG);
        size.set(ValueLayout.JAVA_LONG, 0, value.byteSize());
        if ((int) sysctl.invokeExact(arena.allocateFrom(name), value, size, MemorySegment.NULL, 0L) != 0) return 0;
        long bytes = size.get(ValueLayout.JAVA_LONG, 0);
        // Scalars are 4 or 8 bytes wide; arrays such as hw.cacheconfig hold 8-byte elements.
        if (bytes == 4 && index == 0) return value.get(ValueLayout.JAVA_INT, 0);
        return bytes % 8 == 0 && index < bytes / 8 ? value.getAtIndex(ValueLayout.JAVA_LONG, index) : 0;
    }

    private static String read(Path directory, String name) throws IOException {
        return Files.readString(directory.resolve(name)).trim();
    }

    private static BitSet processors(String value) {
        var result = new BitSet();
        for (String item : value.split(",")) {
            int separator = item.indexOf('-');
            int first = Integer.parseInt(separator < 0 ? item : item.substring(0, separator));
            int last = separator < 0 ? first : Integer.parseInt(item.substring(separator + 1));
            if (first < 0 || last < first || last == Integer.MAX_VALUE) throw new IllegalArgumentException("CPU range");
            result.set(first, last + 1);
        }
        return result;
    }

    private record Cache(long bytes, int processors) { }
}
