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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;

/** Conservative share of the last data cache reachable by this process. Missing metadata disables subdivision. */
final class CacheSizing {
    private static final List<Cache> CACHES = discover();

    static long perWorker(int parallelism) {
        long result = Long.MAX_VALUE;
        for (Cache cache : CACHES) result = Math.min(result, cache.bytes / Math.min(parallelism, cache.processors));
        return result == Long.MAX_VALUE ? 0 : result;
    }

    private static List<Cache> discover() {
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
                        if ((type.equals("Unified") || type.equals("Data")) && candidate > level) {
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
