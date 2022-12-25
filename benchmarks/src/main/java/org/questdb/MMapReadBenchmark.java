/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
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

package org.questdb;

import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.TableReader;
import io.questdb.cairo.TableReaderRecordCursor;
import io.questdb.cairo.sql.Record;
import io.questdb.std.Files;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import io.questdb.std.str.Path;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.AsyncProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class MMapReadBenchmark {
    private static final String COLUMN_PATH = "default/l.d";
    private static final String DB_ROOT = "/var/folders/fz/7kqm2jvs3fl3rv2rl3_xjg6m0000gn/T/junit16093019351624921787/dbRoot";
    private static final long RECORD_COUNT = 200_000_000;
    private static final String TABLE_NAME = "sorttest";
    private TableReaderRecordCursor cursor;

    private long pointer;
    private TableReader reader;
    private Record record;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MMapReadBenchmark.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(5)
                .forks(1)
                .addProfiler(AsyncProfiler.class)
                .build();

        new Runner(opt).run();
    }

    @Setup
    public void setup() {
        try (Path path = new Path()) {
            path.of(DB_ROOT).concat(TABLE_NAME).concat(COLUMN_PATH).$();
            int fd = Files.openRO(path);
            try {
                pointer = Files.mmap(fd, RECORD_COUNT * Long.BYTES, 0, Files.MAP_RO, MemoryTag.NATIVE_DEFAULT);
            } finally {
                Files.close(fd);
            }
        }

        CairoConfiguration config = new DefaultCairoConfiguration(DB_ROOT);
        reader = new TableReader(config, TABLE_NAME);
        cursor = reader.getCursor();
        record = cursor.getRecord();
    }

    @TearDown
    public void tearDown() {
        Files.munmap(pointer, RECORD_COUNT * Long.BYTES, MemoryTag.NATIVE_DEFAULT);
        cursor.close();
        reader.close();
    }

    @Benchmark
    public long testBaseline() {
        long v = 0;
        for (long l = 0; l < RECORD_COUNT; l++) {
            v += Unsafe.getUnsafe().getLong(pointer + l * Long.BYTES);
        }
        return v;
    }

    @Benchmark
    public long testTableReader() {
        cursor.toTop();
        long v = 0;
        while (cursor.hasNext()) {
            v += record.getLong(0);
        }
        return v;
    }
}
