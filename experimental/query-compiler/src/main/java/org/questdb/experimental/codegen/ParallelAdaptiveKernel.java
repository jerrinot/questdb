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

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/** Measures alternatives in shared periods; native calls never hold the policy lock. */
final class ParallelAdaptiveKernel {
    // Two sampled alternatives cost 2 * (2 settling + 5 measured) frames per lane.
    // 1024 exploitation frames keep repeated exploration below 2% of completed frames.
    private static final int EXPLOIT_CALLS = 1024;
    private final Kernel[] alternatives;
    private final LongSupplier clock;
    private final double[] costs;
    private final int parallelism;
    private int[] active;
    private int activeIndex;
    private int challenger;
    private int winner;
    private boolean reverse;
    private volatile Phase phase;

    ParallelAdaptiveKernel(Kernel[] alternatives, int parallelism, LongSupplier clock) {
        if (alternatives.length < 2 || parallelism < 1) throw new IllegalArgumentException("Invalid parallel alternatives");
        this.alternatives = alternatives.clone();
        this.parallelism = parallelism;
        this.clock = clock;
        costs = new double[alternatives.length];
        Arrays.fill(costs, Double.POSITIVE_INFINITY);
        active = new int[alternatives.length];
        for (int i = 0; i < active.length; i++) active[i] = i;
        phase = sample(0);
    }

    void runBatch(long rows, Consumer<Kernel> work) {
        Phase current = phase;
        if (rows < AdaptiveKernel.MIN_SAMPLE_ROWS) {
            work.accept(alternatives[current.choice]);
            return;
        }
        long start = current.costs == null ? 0 : clock.getAsLong();
        work.accept(alternatives[current.choice]);
        // Exceptions and unfinished batches do not train or advance the policy.
        complete(current, rows, current.costs == null ? 0 : clock.getAsLong() - start);
    }

    private synchronized void complete(Phase current, long rows, long elapsed) {
        // A slow call from the previous period must not contaminate its successor.
        if (current != phase) return;
        if (current.costs == null) {
            if (--current.remaining == 0) {
                while (challenger == winner) challenger = (challenger + 1) % alternatives.length;
                int next = challenger;
                challenger = (challenger + 1) % alternatives.length;
                active = reverse ? new int[]{next, winner} : new int[]{winner, next};
                reverse = !reverse;
                activeIndex = 0;
                phase = sample(active[0]);
            }
            return;
        }
        if (current.remaining > 0) {
            current.remaining--;
            return;
        }
        current.costs[current.count++] = (double) Math.max(1, elapsed) / rows;
        if (current.count != current.costs.length) return;
        Arrays.sort(current.costs);
        costs[current.choice] = current.costs[current.costs.length / 2];
        if (++activeIndex < active.length) {
            phase = sample(active[activeIndex]);
            return;
        }
        int best = winner;
        for (int candidate : active) if (costs[candidate] < costs[best]) best = candidate;
        if (costs[best] < costs[winner] * .9) winner = best;
        phase = new Phase(winner, Math.multiplyExact(EXPLOIT_CALLS, parallelism), null);
    }

    private Phase sample(int choice) {
        return new Phase(choice, Math.multiplyExact(2, parallelism),
                new double[Math.multiplyExact(AdaptiveKernel.SAMPLES, parallelism)]);
    }

    private static final class Phase {
        final int choice;
        final double[] costs;
        int count;
        int remaining;

        Phase(int choice, int remaining, double[] costs) {
            this.choice = choice;
            this.remaining = remaining;
            this.costs = costs;
        }
    }
}
