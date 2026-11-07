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

package io.questdb.griffin.engine.table;

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.CairoException;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.sql.Function;
import io.questdb.cairo.sql.PageFrameMemoryRecord;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.cairo.sql.SqlExecutionCircuitBreaker;
import io.questdb.cairo.sql.StatefulAtom;
import io.questdb.cairo.sql.SymbolTable;
import io.questdb.cairo.sql.SymbolTableSource;
import io.questdb.cairo.sql.async.UnorderedPageFrameSequence;
import io.questdb.griffin.HashJoinGroupByFunctions;
import io.questdb.griffin.HashJoinGroupByKeys;
import io.questdb.griffin.HashJoinGroupByMetadata;
import io.questdb.griffin.SqlException;
import io.questdb.griffin.SqlExecutionContext;
import io.questdb.griffin.engine.PerWorkerLockOwner;
import io.questdb.griffin.engine.PerWorkerLocks;
import io.questdb.griffin.engine.QueryCodeGenerator;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.functions.groupby.CountLongConstGroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByFunctionsUpdater;
import io.questdb.griffin.engine.groupby.SimpleMapValue;
import io.questdb.griffin.engine.join.CompiledJoin;
import io.questdb.griffin.engine.join.FrozenHashJoinBuild;
import io.questdb.griffin.engine.join.HashJoinGroupByRecord;
import io.questdb.griffin.engine.join.IntHashJoinBuild;
import io.questdb.griffin.engine.join.SymbolKeyTranslator;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.QuietCloseable;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/** Owns aggregate slots and coordinates the build and probe phases on one frame sequence. */
public final class AsyncHashJoinGroupByAtom implements StatefulAtom, PerWorkerLockOwner {
    boolean collectExecutionTimings;
    long buildNanos;
    long probeNanos;
    long mergeNanos;
    private final ObjList<HashJoinBuildContext> builds = new ObjList<>();
    private final FrozenHashJoinBuild[] nativeBuilds;
    private final boolean hasDependentJoins;
    private final int countValueIndex;
    private final AsyncFilterContext filterContext;
    private final HashJoinGroupByFunctions functions;
    private final PerWorkerLocks perWorkerLocks;
    private final HashJoinGroupByRowUpdater rowUpdater = HashJoinGroupByRowUpdater.newInstance();
    private final ObjList<Slot> slots = new ObjList<>();
    private boolean isFiltersInitialized;
    private boolean isFunctionsInitialized;
    private UnorderedPageFrameSequence<AsyncHashJoinGroupByAtom> frameSequence;
    private long pairsPerCheck;
    private GroupByShardingContext shardingContext;
    private CompiledJoin compiledJoin;

    AsyncHashJoinGroupByAtom(CairoEngine engine, ObjList<HashJoinInput> inputs,
                            HashJoinGroupByMetadata metadata, HashJoinGroupByFunctions functions,
                            AsyncFilterContext filterContext, boolean isOuter, int workerCount) {
        this.functions = functions;
        this.filterContext = filterContext;
        boolean countOnly = inputs.size() == 1 && !isOuter && isPairCount(functions);
        nativeBuilds = new FrozenHashJoinBuild[inputs.size()];
        boolean dependent = false;
        for (int b = 0; b < metadata.getBuildCount(); b++) {
            for (int k = 0; k < metadata.getBuild(b).getProbeKeyColumns().size(); k++) {
                dependent |= metadata.getBuild(b).getProbeKeyColumns().getQuick(k) >= metadata.getProbeColumnCount();
            }
        }
        hasDependentJoins = dependent;
        countValueIndex = countOnly ? functions.getGroupByFunctions(-1).getQuick(0).getValueIndex() : -1;
        perWorkerLocks = new PerWorkerLocks(engine.getConfiguration(), workerCount);
        try {
            for (int i = 0; i < inputs.size(); i++) {
                HashJoinInput input = inputs.getQuick(i);
                builds.add(new HashJoinBuildContext(engine, input.getFactory(), input.getFilters(), input.getOnFilter(),
                        input.getWorkerOnFilters(), metadata.getBuild(i), isOuter, countOnly, workerCount, perWorkerLocks));
            }
            if (functions.isKeyed()) {
                ObjList<GroupByFunctionsUpdater> workerUpdaters = new ObjList<>();
                for (int i = 0; i < workerCount; i++) {
                    workerUpdaters.add(functions.getUpdater(i));
                }
                shardingContext = new GroupByShardingContext(engine.getConfiguration(), functions.getKeyTypes(),
                        functions.getValueTypes(), functions.getUpdater(-1), workerUpdaters, perWorkerLocks, workerCount);
            }
            for (int i = -1; i < workerCount; i++) {
                Slot slot = new Slot(builds, metadata, i);
                slots.add(slot);
                if (!functions.isKeyed()) {
                    slot.value = new SimpleMapValue(functions.getValueTypes().getColumnCount());
                }
            }
        } catch (Throwable th) {
            Misc.free(this, th);
            throw th;
        }
    }

    @Override
    public void clear() {
        Throwable failure = null;
        if (isFunctionsInitialized) {
            isFunctionsInitialized = false;
            try {
                functions.cursorClosed();
            } catch (Throwable th) {
                failure = th;
            }
        }
        if (isFiltersInitialized) {
            isFiltersInitialized = false;
            Function ownerFilter = filterContext.getFilter(-1);
            failure = cursorClosed(failure, ownerFilter);
            for (int i = 0; i < slots.size() - 1; i++) {
                Function workerFilter = filterContext.getFilter(i);
                if (workerFilter != ownerFilter) {
                    failure = cursorClosed(failure, workerFilter);
                }
            }
        }
        for (int i = 0; i < slots.size(); i++) {
            try {
                slots.getQuick(i).clear(functions.getUpdater(i - 1));
            } catch (Throwable th) {
                failure = addFailure(failure, th);
            }
        }
        try {
            filterContext.clear();
        } catch (Throwable th) {
            failure = addFailure(failure, th);
        }
        if (shardingContext != null) {
            try {
                shardingContext.clear();
            } catch (Throwable th) {
                failure = addFailure(failure, th);
            }
        }
        if (compiledJoin != null) {
            try {
                compiledJoin.clearExecution();
            } catch (Throwable th) {
                failure = addFailure(failure, th);
            }
        }
        java.util.Arrays.fill(nativeBuilds, null);
        for (int i = builds.size() - 1; i >= 0; i--) {
            try {
                builds.getQuick(i).clear();
            } catch (Throwable th) {
                failure = addFailure(failure, th);
            }
        }
        CairoException.rethrowCleanupFailure(failure);
    }

    @Override
    public void close() {
        Throwable failure = null;
        try {
            clear();
        } catch (Throwable th) {
            failure = th;
        }
        failure = Misc.freeObjListBestEffort(failure, slots);
        slots.clear();
        failure = Misc.freeBestEffort(failure, compiledJoin);
        compiledJoin = null;
        failure = Misc.freeBestEffort(failure, shardingContext);
        shardingContext = null;
        failure = Misc.freeObjListBestEffort(failure, builds);
        builds.clear();
        frameSequence = null;
        CairoException.rethrowCleanupFailure(failure);
    }

    /**
     * Page frames of the open cursor's build input, zero until the cursor builds.
     */
    @TestOnly
    public int getBuildFrameCount() {
        return builds.getQuick(0).getBuildFrameCount();
    }

    /**
     * Native bytes that the slots' build key stagers hold: none once the frame tasks of a parallel
     * build are done, and none for the INT layout, which stages no key.
     */
    @TestOnly
    public long getBuildKeyStagerSizeInBytes() {
        return builds.getQuick(0).getBuildKeyStagerSizeInBytes();
    }

    /**
     * Hash partitions of the open cursor's build: one for a build on the owner.
     */
    @TestOnly
    public int getBuildPartitionCount() {
        return builds.getQuick(0).getBuildPartitionCount();
    }

    /**
     * The build published for the open cursor, or null when no cursor is open, or when the open
     * cursor has not been asked for a result yet; see {@link #build(SqlExecutionCircuitBreaker)}.
     */
    @TestOnly
    public FrozenHashJoinBuild getFrozenBuild() {
        return builds.getQuick(0).getFrozenBuild();
    }

    /**
     * True when the open cursor's build ran on the workers with its frames' rows written straight
     * into the frames' regions of the heap, as a build whose frames keep every row does; see
     * {@link IntHashJoinBuild#beginUnfilteredPartitioning}.
     */
    @TestOnly
    public boolean hasBuildFrameRegions() {
        return builds.getQuick(0).hasBuildFrameRegions();
    }

    /**
     * True when the open cursor's build ran on the workers, in rounds of the frame sequence's tasks.
     */
    @TestOnly
    public boolean isBuiltInRounds() {
        return builds.getQuick(0).isBuiltInRounds();
    }

    /**
     * True when the open cursor's build holds each key's row count in place of the key's rows: the
     * query only counts the joined pairs and the build repeats a key. The reducers then add a
     * key's count per probe row and visit no pair; see {@link FrozenHashJoinBuild#countKeyRows()}.
     * A build of one row per key keeps its rows, whose lookup finds the count already; see
     * {@link #isCountedByMatch()}.
     */
    public boolean isCountedByKey() {
        return builds.getQuick(0).isCountedByKey();
    }

    /**
     * True when the query only counts the joined pairs and the open cursor's build holds one row per
     * key, as an empty build does too: each probe row then adds one pair when the build holds its
     * key, and the reducers pass no pair through the row updater.
     */
    public boolean isCountedByMatch() {
        return builds.getQuick(0).isCountedByMatch();
    }

    /**
     * True when the open cursor's build counted its key rows on the workers, in a round of the frame
     * sequence's tasks, each over a range of hash partitions, rather than in one pass on the owner.
     * A build of more than one partition takes the round unless its frames are too small for it;
     * see {@link IntHashJoinBuild#getKeyRowCountTaskCount()}.
     */
    @TestOnly
    public boolean isCountedInRounds() {
        return builds.getQuick(0).isCountedInRounds();
    }

    /**
     * True when the open cursor's probes read a copy of the build's payload columns rather than the
     * columns where they live; see {@link #prepareProbe(long, SqlExecutionCircuitBreaker)}.
     */
    @TestOnly
    public boolean isPayloadCopied() {
        return builds.getQuick(0).isPayloadCopied();
    }

    /**
     * True when the open cursor's translation cache of the key-th SYMBOL key pair was filled from
     * the build dictionary rather than opened unresolved; see {@link SymbolKeyTranslator}. False
     * until the cursor builds, and for a factory without that many SYMBOL key pairs.
     */
    @TestOnly
    public boolean isSymbolKeyCacheFilledFromBuild(int key) {
        return builds.getQuick(0).isSymbolKeyCacheFilledFromBuild(key);
    }

    int getProbeKeyColumn() {
        return builds.getQuick(0).getProbeKeyColumn();
    }

    /**
     * True when the open cursor's build has more than one hash partition, so that the reducers run
     * the loops over the slots' partitioned probes.
     */
    boolean isBuildPartitioned() {
        return builds.getQuick(0).isBuildPartitioned();
    }

    boolean isBuildUnique() {
        return builds.getQuick(0).isBuildUnique();
    }

    /**
     * True when the query only counts the joined pairs of an INNER join, as EXPLAIN reports. Each
     * execution then takes a key's pairs from the key's row count, or, over a build of one row per
     * key, from the lookup alone; see {@link #isCountedByKey()} and {@link #isCountedByMatch()}.
     */
    boolean isCountOnly() {
        return builds.getQuick(0).isCountOnly();
    }

    /**
     * True when the probe stages its key through a {@link RecordSink} rather than reading an INT.
     */
    boolean isKeyStaged() {
        return builds.getQuick(0).isKeyStaged();
    }

    boolean isOuter() {
        return builds.getQuick(0).isOuter();
    }

    /**
     * True for the INT layout's lone SYMBOL pair, whose probe key translates before every lookup.
     */
    boolean isSymbolKey() {
        return builds.getQuick(0).isSymbolKey();
    }

    /**
     * True when some execution may copy the build's payload columns, as EXPLAIN reports.
     */
    boolean isPayloadCopyable() {
        return builds.getQuick(0).isPayloadCopyable();
    }

    boolean isProbeNeeded() {
        for (int i = 0; i < builds.size(); i++) {
            if (!builds.getQuick(i).isProbeNeeded()) {
                return false;
            }
        }
        return true;
    }

    public static boolean isParquetInProbeReach(long probeRows, long buildRows, long buildKeyCount, long parquetFrameCount) {
        return HashJoinBuildContext.isParquetInProbeReach(probeRows, buildRows, buildKeyCount, parquetFrameCount);
    }

    static boolean isPayloadCopyWorthIt(long buildRows, long parquetBuildRows, boolean isParquetReached,
                                       boolean isParquetOverReaderCache, int copyRowSize, long probeRows,
                                       long copyMaxSize, double copyMinProbeRatio) {
        return HashJoinBuildContext.isPayloadCopyWorthIt(buildRows, parquetBuildRows, isParquetReached,
                isParquetOverReaderCache, copyRowSize, probeRows, copyMaxSize, copyMinProbeRatio);
    }

    void build(SqlExecutionCircuitBreaker breaker) {
        for (int i = 0; i < builds.size(); i++) {
            builds.getQuick(i).build(breaker);
            nativeBuilds[i] = builds.getQuick(i).getFrozenBuild();
        }
        if (isMultiJoin()) {
            for (int i = 0; i < slots.size(); i++) {
                slots.getQuick(i).bindJoins(builds);
            }
        }
    }

    void prepareProbe(long probeRows, SqlExecutionCircuitBreaker breaker) {
        long knownProbeRows = filterContext.getFilter(-1) == null ? probeRows : -1;
        for (int i = 0, n = builds.size(); i < n; i++) {
            builds.getQuick(i).prepareLookup(knownProbeRows, breaker);
        }
        if (compiledJoin != null) {
            // Frame counts are exact only before a residual probe filter.
            compiledJoin.prepareBuilds(nativeBuilds, knownProbeRows,
                    frameSequence.getMemoryTracker(), breaker);
            CompiledJoin.Updater owner = getSlot(-1).compiledUpdater;
            if (owner != null && hasDependentJoins) {
                owner.prepareDependencies(nativeBuilds, knownProbeRows,
                        frameSequence.getMemoryTracker(), breaker);
            }
        }
        for (int i = 0; i < builds.size(); i++) {
            builds.getQuick(i).maybeCopyPayload(probeRows, breaker);
        }
        if (compiledJoin != null) {
            compiledJoin.preparePayloads(nativeBuilds, knownProbeRows, frameSequence.getMemoryTracker(), breaker);
        }
    }

    boolean hasDependentJoins() {
        return hasDependentJoins;
    }

    boolean isMultiJoin() {
        return builds.size() > 1;
    }

    int getProbeKeyColumn(int index) {
        return builds.getQuick(index).getProbeKeyColumn();
    }

    public HashJoinGroupByFunctions getFunctions() {
        return functions;
    }

    public CharSequence getCompiledJoinName() {
        return compiledJoin != null ? compiledJoin.getName() : null;
    }

    @TestOnly
    public void collectExecutionTimings() {
        collectExecutionTimings = true;
    }

    @TestOnly
    public long[] getExecutionTimings() {
        return new long[]{buildNanos, probeNanos, mergeNanos};
    }

    @TestOnly
    public long getCompiledJoinNativeRows() {
        long rows = 0;
        for (int i = 0; i < slots.size(); i++) {
            CompiledJoin.Updater updater = slots.getQuick(i).compiledUpdater;
            if (updater != null) {
                rows += updater.getNativeRows();
            }
        }
        return rows;
    }

    @TestOnly
    public long getCompiledJoinMapMisses() {
        long misses = 0;
        for (int i = 0; i < slots.size(); i++) {
            CompiledJoin.Updater updater = slots.getQuick(i).compiledUpdater;
            if (updater != null) {
                misses += updater.getNativeMapMisses();
            }
        }
        return misses;
    }

    void compileJoin(@Nullable QueryCodeGenerator compiler, HashJoinGroupByMetadata metadata) {
        if (compiler == null || builds.getQuick(0).isOuter()
                || builds.getQuick(0).isCountOnly()) {
            return;
        }
        ObjList<CompiledJoin.Build> sources = new ObjList<>();
        for (int i = 0; i < builds.size(); i++) {
            HashJoinGroupByMetadata.Build build = metadata.getBuild(i);
            HashJoinGroupByKeys keys = build.getKeys();
            int[] columns = new int[keys.size()], types = new int[keys.size()];
            for (int k = 0; k < keys.size(); k++) {
                // The native key encoding must match the ordinary RecordSink exactly.
                if (keys.getProbeType(k) != keys.getBuildType(k)) return;
                columns[k] = build.getProbeKeyColumns().getQuick(k);
                types[k] = keys.getType(k);
            }
            sources.add(new CompiledJoin.Build(columns, types, build.getBuildColumns().size()));
        }
        compiledJoin = compiler.compileJoin(metadata.getJoinedMetadata(), metadata.getProbeColumnCount(),
                functions.getValueTypes(), functions.getGroupByFunctions(-1), sources, getSlot(-1).value,
                functions.getFilter(-1), functions.getKeyTypes(), functions.getKeyColumns(), functions.getKeyFunctions(-1),
                functions.isKeyed() ? getFragment(-1).getMap() : null);
        if (compiledJoin != null) {
            for (int i = 0; i < slots.size(); i++) {
                Slot slot = slots.getQuick(i);
                slot.compiledUpdater = compiledJoin.newUpdater(() -> {
                    if (!frameSequence.isActive()) {
                        throw frameSequence.buildInterruptionException();
                    }
                    if (!frameSequence.isUninterruptible()) {
                        int state = slot.compiledBreaker.getState(frameSequence.getStartTime(),
                                frameSequence.getCircuitBreaker().getFd());
                        if (state != SqlExecutionCircuitBreaker.STATE_OK) {
                            frameSequence.cancel(state);
                            throw frameSequence.buildInterruptionException();
                        }
                    }
                });
                for (int b = 0; b < builds.size(); b++) {
                    int symbol = 0;
                    for (int k = 0; k < sources.getQuick(b).keyTypes().length; k++) {
                        if (sources.getQuick(b).keyTypes()[k] == io.questdb.cairo.ColumnType.SYMBOL) {
                            slot.compiledUpdater.bindSymbol(b, k, slot.joins[b].getSymbolKeyView(symbol++));
                        }
                    }
                }
            }
        }
    }

    boolean aggregateCompiled(Slot slot, int slotId, GroupByMapFragment fragment, Map map,
                              PageFrameMemoryRecord record, long rowCount, long rowIdsAddress, SqlExecutionCircuitBreaker breaker) {
        if (slot.compiledUpdater == null) {
            return false;
        }
        slot.compiledBreaker = breaker;
        try {
            final boolean completed;
            if (fragment != null) {
                completed = slot.compiledUpdater.updateKeyedMulti(record, rowCount, rowIdsAddress, nativeBuilds,
                        map, fragment.isNotSharded() ? null : fragment.getShards(), functions.getUpdater(slotId));
                if (completed) {
                    shardingContext.maybeEnableSharding(fragment, 0);
                }
            } else {
                completed = slot.compiledUpdater.updateMulti(record, rowCount, rowIdsAddress, nativeBuilds, slot.value);
            }
            return completed;
        } finally {
            slot.compiledBreaker = null;
        }
    }

    /**
     * The class of this factory's row updater, which no other factory shares.
     */
    @TestOnly
    public Class<?> getRowUpdaterClass() {
        return rowUpdater.getClass();
    }

    @Override
    @TestOnly
    public PerWorkerLocks getPerWorkerLocks() {
        return perWorkerLocks;
    }

    private static Throwable addFailure(Throwable failure, Throwable th) {
        if (failure == null) {
            return th;
        }
        if (failure != th) {
            failure.addSuppressed(th);
        }
        return failure;
    }

    private static Throwable cursorClosed(Throwable failure, Function function) {
        if (function != null) {
            try {
                function.cursorClosed();
            } catch (Throwable th) {
                return addFailure(failure, th);
            }
        }
        return failure;
    }

    /**
     * True when the functions only count the joined pairs: no grouping key, no post-join filter and
     * count(*) as the lone aggregate, which reads no column of either input. The number of pairs
     * is then all the query takes from the join.
     */
    private static boolean isPairCount(HashJoinGroupByFunctions functions) {
        final ObjList<GroupByFunction> groupByFunctions = functions.getGroupByFunctions(-1);
        return !functions.isKeyed()
                && functions.getFilter(-1) == null
                && groupByFunctions.size() == 1
                && groupByFunctions.getQuick(0).getClass() == CountLongConstGroupByFunction.class;
    }

    /**
     * Adds a frame's joined pairs to the slot's count(*), as the row updater's update per pair does.
     */
    void addPairCount(Slot slot, long pairCount) {
        if (pairCount > 0) {
            slot.value.addLong(countValueIndex, pairCount);
            slot.value.setNew(false);
        }
    }

    AsyncFilterContext getFilterContext() {
        return filterContext;
    }

    GroupByMapFragment getFragment(int slot) {
        return shardingContext != null ? shardingContext.getFragment(slot) : null;
    }

    long getPairsPerCheck() {
        return pairsPerCheck;
    }

    HashJoinGroupByRowUpdater getRowUpdater() {
        return rowUpdater;
    }

    Slot getSlot(int slot) {
        return slots.getQuick(slot + 1);
    }

    /**
     * Binds the sequence that owns this atom, which a parallel build dispatches its rounds through.
     */
    void bindFrameSequence(UnorderedPageFrameSequence<AsyncHashJoinGroupByAtom> frameSequence) {
        this.frameSequence = frameSequence;
        for (int i = 0; i < builds.size(); i++) {
            builds.getQuick(i).bindFrameSequence(frameSequence);
        }
    }

    int maybeAcquire(int workerId, boolean isOwner, SqlExecutionCircuitBreaker breaker) {
        return workerId == -1 && isOwner ? -1 : perWorkerLocks.acquireSlot(workerId, breaker);
    }

    GroupByShardingContext getShardingContext() {
        return shardingContext;
    }

    public boolean isSharded() {
        return shardingContext != null && shardingContext.isSharded();
    }

    SimpleMapValue mergeScalar() {
        SimpleMapValue dest = getSlot(-1).value;
        GroupByFunctionsUpdater updater = functions.getUpdater(-1);
        for (int i = 1; i < slots.size(); i++) {
            SimpleMapValue src = slots.getQuick(i).value;
            if (!src.isNew()) {
                if (dest.isNew()) {
                    dest.copy(src);
                } else {
                    updater.merge(dest, src);
                }
                dest.setNew(false);
            }
        }
        return dest;
    }

    void release(int slot) {
        perWorkerLocks.releaseSlot(slot);
    }

    /**
     * Opens the build scan and initializes the filters and the functions, and reads no row of
     * either input: {@link #build(SqlExecutionCircuitBreaker)} does the work that grows with the
     * build, when the cursor is first asked for a result. Everything that can throw a
     * SqlException runs here, so that it surfaces from getCursor().
     */
    @Override
    public void init(SymbolTableSource symbolTableSource, SqlExecutionContext executionContext) throws SqlException {
        try {
            for (int i = 0; i < builds.size(); i++) {
                builds.getQuick(i).init(i == 0 ? symbolTableSource : slots.getQuick(0), symbolTableSource, executionContext);
            }
            // Join fanout is not bounded by a frame, so reducers check once per page frame of matched pairs.
            pairsPerCheck = Math.max(1, executionContext.getPageFrameMaxRows());
            if (shardingContext != null) {
                shardingContext.setMemoryTracker(executionContext.getMemoryTracker());
                shardingContext.reopen();
            }
            for (int i = 0; i < slots.size(); i++) {
                Slot slot = slots.getQuick(i);
                if (!functions.isKeyed()) {
                    functions.getUpdater(i - 1).updateEmpty(slot.value);
                    slot.value.setNew(true);
                }
            }
            isFiltersInitialized = true;
            filterContext.initFilters(symbolTableSource, executionContext);
            isFunctionsInitialized = true;
            // A slot resolves a function's symbol tables through the two open scans, so the
            // functions need no frozen build and parents can resolve output symbols at once.
            functions.init(slots, executionContext);
        } catch (Throwable th) {
            // The sequence closes the frame cursor when init throws. Release functions
            // while their borrowed symbol sources are still alive, then the build cursor.
            try {
                clear();
            } catch (Throwable cleanup) {
                th.addSuppressed(cleanup);
            }
            throw th;
        }
    }

    static final class Slot implements QuietCloseable, SymbolTableSource {
        final HashJoinBuildContext.Slot join;
        final HashJoinBuildContext.Slot[] joins;
        final HashJoinGroupByRecord[] joinedRecords;
        final FrozenHashJoinBuild.Probe[] probes;
        private final int probeColumnCount;
        private final int[] payloadCounts;
        SimpleMapValue value;
        CompiledJoin.Updater compiledUpdater;
        SqlExecutionCircuitBreaker compiledBreaker;

        Slot(ObjList<HashJoinBuildContext> builds, HashJoinGroupByMetadata metadata, int slot) {
            probeColumnCount = metadata.getProbeColumnCount();
            joins = new HashJoinBuildContext.Slot[builds.size()];
            joinedRecords = new HashJoinGroupByRecord[builds.size()];
            probes = builds.size() > 1 ? new FrozenHashJoinBuild.Probe[builds.size()] : null;
            payloadCounts = new int[builds.size()];
            for (int i = 0; i < builds.size(); i++) {
                joins[i] = builds.getQuick(i).getSlot(slot);
                joinedRecords[i] = joins[i].joinedRecord;
                payloadCounts[i] = metadata.getBuild(i).getBuildColumns().size();
            }
            join = joins[0];
        }

        void find(int index, int column, PageFrameMemoryRecord record) {
            if (probes[index] instanceof FrozenHashJoinBuild.IntProbe probe) {
                int key = (index == 0 ? record : joinedRecords[index - 1]).getInt(column);
                SymbolKeyTranslator.View translator = joins[index].symbolKeyView;
                probe.findUnchecked(translator == null ? key : translator.translate(key));
            } else {
                ((FrozenHashJoinBuild.RecordProbe) probes[index]).findUnchecked(joins[index].keyRecord);
            }
        }

        void bindJoins(ObjList<HashJoinBuildContext> builds) {
            for (int i = 0; i < joins.length; i++) {
                probes[i] = builds.getQuick(i).isKeyStaged()
                        ? (builds.getQuick(i).isBuildPartitioned() ? joins[i].partitionedRecordProbe : joins[i].recordProbe)
                        : (builds.getQuick(i).isBuildPartitioned() ? joins[i].partitionedIntProbe : joins[i].intProbe);
                if (i > 0) {
                    joins[i].bindProbeRecord(joinedRecords[i - 1]);
                    joinedRecords[i].of(joinedRecords[i - 1], joinedRecords[i - 1], probes[i]);
                }
                joinedRecords[i].setHasMatch(true);
            }
        }

        @Override
        public void close() {
            Throwable failure = Misc.freeBestEffort(null, compiledUpdater);
            compiledUpdater = null;
            failure = Misc.freeBestEffort(failure, value);
            value = null;
            CairoException.rethrowCleanupFailure(failure);
        }

        @Override
        public SymbolTable getSymbolTable(int column) {
            if (column < probeColumnCount) {
                return join.getSymbolTable(column);
            }
            int payload = column - probeColumnCount;
            int prefix = probeColumnCount;
            for (int i = 0; i < joins.length; i++) {
                if (payload < payloadCounts[i]) {
                    return joins[i].getSymbolTable(prefix + payload);
                }
                payload -= payloadCounts[i];
                prefix += payloadCounts[i];
            }
            throw new IndexOutOfBoundsException();
        }

        @Override
        public SymbolTable newSymbolTable(int column) {
            if (column < probeColumnCount) {
                return join.newSymbolTable(column);
            }
            int payload = column - probeColumnCount;
            int prefix = probeColumnCount;
            for (int i = 0; i < joins.length; i++) {
                if (payload < payloadCounts[i]) {
                    return joins[i].newSymbolTable(prefix + payload);
                }
                payload -= payloadCounts[i];
                prefix += payloadCounts[i];
            }
            throw new IndexOutOfBoundsException();
        }

        void clear(GroupByFunctionsUpdater updater) {
            if (compiledUpdater != null) {
                compiledUpdater.clearExecution();
            }
            for (int i = 0; i < joinedRecords.length; i++) joinedRecords[i].clear();
            if (probes != null) {
                java.util.Arrays.fill(probes, null);
            }
            if (value != null) {
                updater.updateEmpty(value);
                value.setNew(true);
            }
        }
    }
}
