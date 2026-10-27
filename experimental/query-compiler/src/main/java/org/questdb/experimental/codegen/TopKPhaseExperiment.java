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

import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.std.DirectLongLongSortedList;
import io.questdb.std.MemoryTag;
import io.questdb.std.Misc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/** Diagnostic phase isolation over the finalized result of a real 500M-row SQL aggregation. */
final class TopKPhaseExperiment {
    static List<Long> run(RecordCursorFactory factory, SqlExecutionContextImpl context,
                          int warmups, int samples, String label, boolean measured,
                          Runnable start, Runnable stop) throws Exception {
        int limit = Integer.getInteger("query.codegen.topKLimit", 0);
        if (limit <= 0) throw new IllegalArgumentException("Phase isolation requires topKLimit");
        int order = System.getProperty("query.codegen.topKDirection", "desc").equals("asc") ? 0 : 1;
        while (factory != null && !(factory instanceof AsyncGroupByRecordCursorFactory)) factory = factory.getBaseFactory();
        if (!(factory instanceof AsyncGroupByRecordCursorFactory aggregate)) throw new IllegalArgumentException("Expected ordinary async GROUP BY");
        int column = Integer.getInteger("query.codegen.topKColumn", factory.getMetadata().getColumnCount()) - 1;
        var atom = aggregate.getAtom();
        try (var cursor = aggregate.getCursor(context);
             var list = DirectLongLongSortedList.getInstance(order, limit, MemoryTag.NATIVE_DEFAULT)) {
            long build = System.nanoTime();
            cursor.hasNext(); // Build the complete aggregation before timing this diagnostic phase.
            System.out.printf(Locale.ROOT, "TOPK SETUP %s groups=%d aggregation-ms=%.3f sharded=%s%n",
                    label, cursor.size(), (System.nanoTime() - build) / 1e6, atom.isSharded());
            double[] elapsed = new double[samples];
            List<Long> expected = null;
            for (int round = -warmups; round < samples; round++) {
                // The ordinary query clears these when closing. Isolating the phase keeps maps alive.
                list.clear();
                Misc.clear(atom.getOwnerLongTopKList());
                Misc.clearObjList(atom.getPerWorkerLongTopKLists());
                cursor.toTop();
                var before = ScanMatrix.Resources.read();
                if (measured && round >= 0) start.run();
                long began = System.nanoTime();
                double ms;
                try {
                    cursor.longTopK(list, column);
                    ms = (System.nanoTime() - began) / 1e6;
                } finally { if (measured && round >= 0) stop.run(); }
                var after = ScanMatrix.Resources.read();
                var values = new ArrayList<Long>();
                var rows = list.getCursor(); rows.toTop();
                while (rows.hasNext()) values.add(rows.value());
                if (expected != null && !expected.equals(values)) throw new AssertionError("Top-K phase changed results");
                expected = values;
                if (round >= 0) elapsed[round] = ms;
                System.out.printf(Locale.ROOT, "TOPK %s %s ms=%.3f input-groups=%d retained=%d major-faults=%d read-bytes=%d%n",
                        label, round < 0 ? "warm" : "sample", ms, cursor.size(), list.size(),
                        after.faults() - before.faults(), after.bytes() - before.bytes());
            }
            if (measured) {
                Arrays.sort(elapsed);
                System.out.printf(Locale.ROOT, "TOPK RESULT %s median=%.3f range=[%.3f,%.3f]%n",
                        label, elapsed[samples / 2], elapsed[0], elapsed[samples - 1]);
            }
            return expected;
        }
    }
}
