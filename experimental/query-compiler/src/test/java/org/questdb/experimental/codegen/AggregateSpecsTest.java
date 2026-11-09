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

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.sql.Function;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.columns.*;
import io.questdb.griffin.engine.functions.groupby.*;
import io.questdb.griffin.engine.groupby.FlyweightPackedMapValue;
import io.questdb.std.ObjList;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;

import static io.questdb.cairo.ColumnType.*;
import static org.junit.Assert.*;

public class AggregateSpecsTest {
    private static final ArrayColumnTypes INPUTS = new ArrayColumnTypes().add(DOUBLE).add(SHORT).add(LONG)
            .add(BOOLEAN).add(VARCHAR).add(TIMESTAMP).add(TIMESTAMP_NANO).add(CHAR).add(STRING);

    @Test public void validatesEveryOrdinaryStateSlotAndRejectsOverlap() throws Exception {
        var pair = new ObjList<Function>();
        pair.add(DoubleColumn.newInstance(0)); pair.add(LongColumn.newInstance(2));
        var regression = (GroupByFunction) new RegressionSlopeFunctionFactory().newInstance(0, pair, null, null, null);
        var momentArgs = new ObjList<Function>(); momentArgs.add(DoubleColumn.newInstance(0));
        var moment = (GroupByFunction) new VarSampleGroupByFunctionFactory().newInstance(0, momentArgs, null, null, null);
        for (var function : List.of(new CountLongConstGroupByFunction(), new AvgShortGroupByFunction(ShortColumn.newInstance(1)),
                new MinShortGroupByFunction(ShortColumn.newInstance(1)), new BitAndShortGroupByFunction(ShortColumn.newInstance(1)),
                new BoolAndGroupByFunction(BooleanColumn.newInstance(3)), new NSumDoubleGroupByFunction(DoubleColumn.newInstance(0)),
                new WeightedAvgDoubleGroupByFunction(DoubleColumn.newInstance(0), LongColumn.newInstance(2)),
                new CountDistinctLongGroupByFunction(LongColumn.newInstance(2), 16, 0.6),
                new MinVarcharGroupByFunction(new VarcharColumn(4)),
                new MinTimestampGroupByFunction(TimestampColumn.newInstance(5, TIMESTAMP), TIMESTAMP),
                new MaxTimestampGroupByFunction(TimestampColumn.newInstance(6, TIMESTAMP_NANO), TIMESTAMP_NANO), regression, moment)) {
            try (function) {
                var types = new ArrayColumnTypes().add(INT); // Nonzero, unaligned start in packed state.
                function.initValueTypes(types);
                var layout = new FlyweightPackedMapValue(types);
                var update = AggregateSpecs.bind(function, types, layout::getOffset, new HashSet<>(),
                        new ResolvedInputs(INPUTS), true, true);
                assertNotNull(function.getClass().getSimpleName(), update);
                assertEquals(layout.getOffset(1), update.offset());
                for (int slot = 1; slot < types.getColumnCount(); slot++) {
                    var wrong = new ArrayColumnTypes();
                    for (int i = 0; i < types.getColumnCount(); i++) wrong.add(i == slot
                            ? (types.getColumnType(i) == LONG ? DOUBLE : LONG) : types.getColumnType(i));
                    assertThrows(function.getClass().getSimpleName() + " slot " + slot, IllegalArgumentException.class,
                            () -> AggregateSpecs.bind(function, wrong, layout::getOffset, new HashSet<>(),
                                    new ResolvedInputs(INPUTS), true, true));
                    var assigned = new HashSet<Integer>(); assigned.add(slot);
                    assertThrows(IllegalArgumentException.class, () -> AggregateSpecs.bind(function, types,
                            layout::getOffset, assigned, new ResolvedInputs(INPUTS), true, true));
                }
                function.initValueIndex(-1);
                assertThrows(IllegalArgumentException.class, () -> AggregateSpecs.bind(function, types,
                        layout::getOffset, new HashSet<>(), new ResolvedInputs(INPUTS), true, true));
                function.initValueIndex(types.getColumnCount());
                assertThrows(IllegalArgumentException.class, () -> AggregateSpecs.bind(function, types,
                        layout::getOffset, new HashSet<>(), new ResolvedInputs(INPUTS), true, true));
            }
        }
    }

    @Test public void onlyTopKRequiresContiguousState() {
        try (var topK = new GroupedTopKAggregate(LongColumn.newInstance(2), 3, false);
             var average = new AvgDoubleGroupByFunction(DoubleColumn.newInstance(0))) {
            var topTypes = new ArrayColumnTypes(); topK.initValueTypes(topTypes);
            assertNotNull(AggregateSpecs.bind(topK, topTypes, i -> i * 8L + 4, new HashSet<>(), new ResolvedInputs(INPUTS), true, true));
            assertThrows(IllegalArgumentException.class, () -> AggregateSpecs.bind(topK, topTypes, i -> i * 16L,
                    new HashSet<>(), new ResolvedInputs(INPUTS), true, true));
            var avgTypes = new ArrayColumnTypes(); average.initValueTypes(avgTypes);
            var update = (AggregateUpdates.AverageUpdate) AggregateSpecs.bind(average, avgTypes, i -> i * 16L + 4,
                    new HashSet<>(), new ResolvedInputs(INPUTS), true, true);
            assertEquals(4, update.offset());
            assertEquals(20, update.countOffset());
        }
    }

    @Test public void preservesExactClassAndConsumerAdmission() {
        try (var subclass = new CountLongConstGroupByFunction() { };
             var distinct = new CountDistinctLongGroupByFunction(LongColumn.newInstance(2), 16, 0.6);
             var minimum = new MinVarcharGroupByFunction(new VarcharColumn(4));
             var stringCount = new CountStrGroupByFunction(new StrColumn(8));
             var charCount = new CountStrGroupByFunction(new CharColumn(7))) {
            assertNull(bind(subclass, true, true));
            assertSame(subclass, AggregateSpecs.outputFunction(subclass));
            assertNull(bind(distinct, false, true));
            assertNotNull(bind(distinct, true, true));
            assertNull(bind(minimum, true, false));
            assertNotNull(bind(minimum, true, true));
            assertNull(bind(stringCount, true, true));
            assertNotNull(bind(charCount, true, true));
        }
    }

    @Test public void finalizedReaderIsPartOfTheImplementationContract() {
        try (var average = new AvgShortGroupByFunction(ShortColumn.newInstance(1));
             var sum = new NSumDoubleGroupByFunction(DoubleColumn.newInstance(0));
             var bool = new BoolAndGroupByFunction(BooleanColumn.newInstance(3));
             var count = new CountLongConstGroupByFunction()) {
            for (var function : List.of(average, sum, bool, count)) {
                var update = bind(function, true, true);
                assertNotNull(update);
                Function reader = AggregateSpecs.outputFunction(function);
                if (function == average || function == sum) {
                    assertTrue(update.finalizesResult());
                    assertTrue(reader instanceof DoubleColumn);
                    assertEquals(function.getValueIndex(), ((DoubleColumn) reader).getColumnIndex());
                } else {
                    assertSame(function, reader);
                }
            }
            assertTrue(bind(bool, true, true).finalizesResult());
        }
    }

    private static AggregateUpdates.Update bind(GroupByFunction function, boolean distinct, boolean mutable) {
        var types = new ArrayColumnTypes().add(INT);
        function.initValueTypes(types);
        var layout = new FlyweightPackedMapValue(types);
        return AggregateSpecs.bind(function, types, layout::getOffset, new HashSet<>(), new ResolvedInputs(INPUTS), distinct, mutable);
    }
}
