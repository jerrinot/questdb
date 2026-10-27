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
import java.nio.file.Path;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/** Query-local choice at a serial chunk or joined parallel-frame boundary. */
final class AdaptiveKernel implements Kernel {
    static final int EXPLOIT_CALLS = 128;
    static final int SAMPLES = 5;
    static final long MIN_SAMPLE_ROWS = 4096;
    private final Kernel[] alternatives;
    private final LongSupplier clock;
    private final ThreadLocal<Policy> policies;

    AdaptiveKernel(Kernel fused, Kernel masked, LongSupplier clock) {
        this(new Kernel[]{fused, masked}, clock);
    }

    AdaptiveKernel(Kernel[] alternatives, LongSupplier clock) {
        if (alternatives.length < 2) throw new IllegalArgumentException("At least two alternatives required");
        this.alternatives = alternatives.clone();
        this.clock = clock;
        int count = alternatives.length;
        policies = ThreadLocal.withInitial(() -> new Policy(count));
    }

    static CompiledKernel compile(Backend backend, List<Ir.Program> alternatives, Path artifacts) throws IOException {
        var programs = new LinkedHashSet<>(alternatives);
        if (programs.isEmpty()) throw new IllegalArgumentException("No alternatives");
        if (programs.size() == 1) return backend.compile(programs.getFirst(), artifacts);
        var code = new ArrayList<CompiledKernel>();
        try {
            for (var program : programs) code.add(backend.compile(program, artifacts));
        } catch (IOException | RuntimeException | Error failure) {
            for (var compiled : code)
                try { compiled.close(); } catch (Throwable cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
        return new CompiledKernel(new AdaptiveKernel(code.toArray(Kernel[]::new), System::nanoTime), () -> {
            RuntimeException failure = null;
            for (var compiled : code) try { compiled.close(); } catch (RuntimeException cleanup) {
                if (failure == null) failure = cleanup; else failure.addSuppressed(cleanup);
            }
            if (failure != null) throw failure;
        }, code.getFirst().artifacts(), code.stream().mapToLong(CompiledKernel::compilationNanos).sum());
    }

    @Override
    public long run(long columns, long rows, long state) {
        Policy policy = policies.get();
        if (rows < MIN_SAMPLE_ROWS) return alternatives[policy.winner].run(columns, rows, state);
        if (policy.remaining > 0) {
            policy.remaining--;
            return alternatives[policy.winner].run(columns, rows, state);
        }
        int choice = policy.choice();
        if (policy.warmup > 0) {
            long result = alternatives[choice].run(columns, rows, state);
            if (result == 0) policy.warmed();
            return result;
        }
        long start = clock.getAsLong();
        long result = alternatives[choice].run(columns, rows, state);
        long elapsed = clock.getAsLong() - start;
        // Failed chunks are never replayed and do not train a successful-work model.
        if (result == 0) policy.observed(choice, elapsed, rows);
        return result;
    }

    /** The caller joins every use of the selected code before returning. */
    void runBatch(long rows, Consumer<Kernel> work) {
        Policy policy = policies.get();
        if (rows < MIN_SAMPLE_ROWS) { work.accept(alternatives[policy.winner]); return; }
        if (policy.remaining > 0) {
            policy.remaining--;
            work.accept(alternatives[policy.winner]);
            return;
        }
        int choice = policy.choice();
        if (policy.warmup > 0) {
            work.accept(alternatives[choice]);
            policy.warmed();
        } else {
            long start = clock.getAsLong();
            work.accept(alternatives[choice]);
            policy.observed(choice, clock.getAsLong() - start, rows);
        }
    }

    private static final class Policy {
        final double[][] costs;
        final int[] counts, active;
        int warmup, activeCount;
        int step, remaining, winner, order, challenger;

        Policy(int size) {
            costs = new double[size][SAMPLES];
            counts = new int[size];
            active = new int[size];
            for (int i = 0; i < size; i++) active[i] = i;
            activeCount = size;
            warmup = 2 * size;
        }

        int choice() {
            int position = step % (2 * activeCount);
            if (position >= activeCount) position = 2 * activeCount - 1 - position;
            return active[order == 0 ? position : activeCount - 1 - position];
        }

        void warmed() {
            warmup--;
            step = warmup == 0 ? 0 : step + 1;
        }

        void observed(int choice, long elapsed, long rows) {
            costs[choice][counts[choice]++] = (double) Math.max(1, elapsed) / rows;
            if (++step == activeCount * SAMPLES) decide();
        }

        void decide() {
            for (int i = 0; i < activeCount; i++) Arrays.sort(costs[active[i]]);
            int best = winner;
            for (int i = 0; i < activeCount; i++)
                if (costs[active[i]][SAMPLES / 2] < costs[best][SAMPLES / 2]) best = active[i];
            if (costs[best][SAMPLES / 2] < costs[winner][SAMPLES / 2] * .9) winner = best;
            Arrays.fill(counts, 0);
            step = 0;
            order ^= 1;
            remaining = EXPLOIT_CALLS;
            // After initial exploration, compare the winner against one rotating
            // challenger. Do not pay for every loser in every exploration period.
            while (challenger == winner) challenger = (challenger + 1) % costs.length;
            active[0] = winner; active[1] = challenger;
            challenger = (challenger + 1) % costs.length;
            activeCount = 2;
        }
    }
}
