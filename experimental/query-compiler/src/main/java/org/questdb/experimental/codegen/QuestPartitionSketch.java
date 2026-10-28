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

import io.questdb.griffin.engine.table.GroupByMapFragment;
import static org.questdb.experimental.codegen.Ir.*;

/** Execution metadata only: sample the map hash, then remix for independent HLL register/rank bits. */
final class QuestPartitionSketch {
    // HyperLogLogDenseRepresentation header: type byte and cached-cardinality long.
    static final int HEADER_BYTES = Byte.BYTES + Long.BYTES;
    static final int PRECISION = 7;
    static final int REGISTERS = 1 << PRECISION;
    static final int SAMPLE_RATE = 16;
    static final long BYTES = GroupByMapFragment.NUM_SHARDS * REGISTERS;
    // One deterministic distinct-key sample per register: hash priority and encoded key bytes.
    static final int KEY_SAMPLE_BYTES = 16;
    static final long VARIABLE_BYTES = BYTES + BYTES * KEY_SAMPLE_BYTES;

    static void add(Builder b, Expr sketch, Expr hash) {
        add(b, sketch, hash, null);
    }

    static void add(Builder b, Expr sketch, Expr hash, Expr keyBytes) {
        b.when(ne(sketch, nil()), enabled -> enabled.when(eq(binary(Op.AND,
                binary(Op.USHR, hash, i64(52)), i64(SAMPLE_RATE - 1)), i64(0)), sample -> {
            Var partition = sample.let(binary(Op.USHR, hash, i64(56)));
            // Hash.fmix64: the cheap map hash alone biases dense scalar domains.
            Var mixed = sample.let(hash);
            sample.assign(mixed, binary(Op.MUL, binary(Op.XOR, mixed, binary(Op.USHR, mixed, i64(33))), i64(0xff51afd7ed558ccdL)));
            sample.assign(mixed, binary(Op.MUL, binary(Op.XOR, mixed, binary(Op.USHR, mixed, i64(33))), i64(0xc4ceb9fe1a85ec53L)));
            sample.assign(mixed, binary(Op.XOR, mixed, binary(Op.USHR, mixed, i64(33))));
            Var register = sample.let(binary(Op.AND, binary(Op.USHR, mixed, i64(45)), i64(REGISTERS - 1)));
            Var index = sample.let(Ir.add(binary(Op.MUL, partition, i64(REGISTERS)), register));
            Var address = sample.let(new Offset(sketch, index));
            Var rank = sample.let(Ir.add(new TrailingZeros(Ir.add(binary(Op.AND, mixed,
                    i64((1L << 45) - 1)), i64(1L << 45))), i64(1)));
            Var previous = sample.let(new Load(MemoryType.I8, address));
            sample.store(MemoryType.I8, address, new Select(binary(Op.LT, previous, rank), rank, previous));
            if (keyBytes != null) {
                Var entry = sample.let(new Offset(sketch, Ir.add(i64(BYTES), binary(Op.MUL, index, i64(KEY_SAMPLE_BYTES)))));
                Var priority = sample.let(binary(Op.USHR, mixed, i64(1)));
                Var prior = sample.let(load(entry));
                // Repeated rows and merges select the same key; frequency cannot bias its width.
                sample.when(binary(Op.EITHER, binary(Op.LT, prior, priority),
                        binary(Op.BOTH, eq(prior, priority), binary(Op.LT, load(at(entry, 8)), keyBytes))), chosen -> {
                    chosen.store(entry, priority);
                    chosen.store(at(entry, 8), keyBytes);
                });
            }
        }));
    }
}
