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

package io.questdb.griffin;

import io.questdb.cairo.ArrayColumnTypes;
import io.questdb.cairo.CairoConfiguration;
import io.questdb.cairo.ColumnType;
import io.questdb.cairo.ColumnTypes;
import io.questdb.cairo.GenericRecordMetadata;
import io.questdb.cairo.IndexType;
import io.questdb.cairo.ListColumnFilter;
import io.questdb.cairo.RecordSink;
import io.questdb.cairo.RecordSinkFactory;
import io.questdb.cairo.TableColumnMetadata;
import io.questdb.cairo.sql.RecordMetadata;
import io.questdb.griffin.engine.join.HashJoinGroupByRecord;
import io.questdb.griffin.engine.join.JoinRecordMetadata;
import io.questdb.griffin.model.ExpressionNode;
import io.questdb.griffin.model.QueryColumn;
import io.questdb.std.BitSet;
import io.questdb.std.BytecodeAssembler;
import io.questdb.std.IntList;
import io.questdb.std.Misc;
import io.questdb.std.ObjList;
import io.questdb.std.str.StringSink;
import java.io.Closeable;
import org.jetbrains.annotations.Nullable;

/** Binds the common probe and each pruned build payload to one joined expression namespace. */
public final class HashJoinGroupByMetadata implements Closeable {
    private final ObjList<Build> builds = new ObjList<>();
    private final ObjList<QueryColumn> columns = new ObjList<>();
    private final JoinRecordMetadata joinedMetadata;
    private final int probeColumnCount;
    private final ExpressionNode buildOnFilter;
    private ExpressionNode postJoinFilter;

    public HashJoinGroupByMetadata(CairoConfiguration configuration, BytecodeAssembler asm,
                                   HashJoinGroupByCandidate candidate, RecordMetadata probeMetadata,
                                   IntList probeColumns, RecordMetadata buildMetadata, IntList buildColumns) throws SqlException {
        this(configuration, asm, candidate, inputMetadatas(candidate, probeMetadata, buildMetadata),
                inputColumns(candidate, probeColumns, buildColumns));
    }

    HashJoinGroupByMetadata(CairoConfiguration configuration, BytecodeAssembler asm,
                           HashJoinGroupByCandidate candidate, RecordMetadata[] inputs, IntList[] baseColumns) throws SqlException {
        final int probe = candidate.getProbeIndex();
        final RecordMetadata probeMetadata = inputs[probe];
        probeColumnCount = probeMetadata.getColumnCount();
        int totalColumns = probeColumnCount;
        for (int i = 0; i < candidate.getBuildCount(); i++) totalColumns += candidate.getRequiredBuildColumns(i).size();
        joinedMetadata = new JoinRecordMetadata(configuration, totalColumns);
        try {
            for (int i = 0; i < probeColumnCount; i++) {
                joinedMetadata.add(candidate.getProbeModel().getName(), copyColumn(probeMetadata, i, probeMetadata.isSymbolTableStatic(i)));
            }
            int[] offsets = new int[inputs.length];
            java.util.Arrays.fill(offsets, -1);
            offsets[probe] = 0;
            for (int i = 0; i < candidate.getBuildCount(); i++) {
                int source = candidate.getBuildIndex(i);
                HashJoinGroupByKeys keys = candidate.getKeys(i);
                IntList probeKeys = new IntList();
                for (int k = 0; k < keys.size(); k++) {
                    int owner = keys.getProbeSource(k) < 0 ? probe : keys.getProbeSource(k);
                    int column = -1;
                    if (owner == probe) column = requireColumn(baseColumns[probe], keys.getProbeColumn(k));
                    else for (int b = 0; b < i; b++) {
                        if (candidate.getBuildIndex(b) == owner) {
                            column = offsets[owner] + requireColumn(candidate.getRequiredBuildColumns(b), keys.getProbeColumn(k));
                            break;
                        }
                    }
                    if (column < 0) throw SqlException.$(0, "hash join key source is not available");
                    probeKeys.add(column);
                }
                Build build = new Build(configuration, asm, candidate, i, joinedMetadata, probeKeys,
                        inputs[source], baseColumns[source]);
                builds.add(build);
                offsets[source] = joinedMetadata.getColumnCount();
                for (int column = 0; column < build.payloadMetadata.getColumnCount(); column++) {
                    joinedMetadata.add(candidate.getBuildModel(i).getName(), build.payloadMetadata.getColumnMetadata(column));
                }
            }
            RecordMetadata resolved = candidate.getResolvedMetadata();
            IntList resolvedToJoined = new IntList();
            for (int i = 0; i < resolved.getColumnCount(); i++) {
                int source = candidate.getColumnSource(i);
                int base = candidate.getBaseColumnIndex(i);
                int physical = indexOf(baseColumns[source], base);
                if (physical >= 0 && inputs[source].getColumnType(physical) != resolved.getColumnType(i)) {
                    throw SqlException.$(0, "hash join input mapping type mismatch [column=").put(base).put(']');
                }
                int column = physical;
                if (source != probe) {
                    column = -1;
                    for (int b = 0; b < builds.size(); b++) {
                        if (candidate.getBuildIndex(b) == source) {
                            int payload = indexOf(candidate.getRequiredBuildColumns(b), base);
                            column = payload < 0 ? -1 : offsets[source] + payload;
                            break;
                        }
                    }
                }
                resolvedToJoined.add(column);
            }
            IntList resolvedToBuild = new IntList();
            int firstBuild = candidate.getBuildIndex(0);
            for (int i = 0; i < resolved.getColumnCount(); i++) {
                resolvedToBuild.add(candidate.getColumnSource(i) == firstBuild
                        ? indexOf(baseColumns[firstBuild], candidate.getBaseColumnIndex(i)) : -1);
            }
            buildOnFilter = remap(candidate.getResolvedBuildOnFilter(), resolved, resolvedToBuild, inputs[firstBuild]);
            for (int i = 0; i < candidate.getResolvedColumns().size(); i++) {
                QueryColumn column = candidate.getResolvedColumns().getQuick(i);
                columns.add(QueryColumn.FACTORY.newInstance().of(column.getAlias(),
                        remap(column.getAst(), resolved, resolvedToJoined, joinedMetadata)));
            }
            for (int i = 0; i < candidate.getResolvedPostJoinFilters().size(); i++) {
                ExpressionNode filter = remap(candidate.getResolvedPostJoinFilters().getQuick(i), resolved, resolvedToJoined, joinedMetadata);
                if (postJoinFilter == null) {
                    postJoinFilter = filter;
                } else {
                    ExpressionNode and = ExpressionNode.FACTORY.newInstance().of(ExpressionNode.OPERATION, "and", 0, filter.position);
                    and.paramCount = 2;
                    and.lhs = postJoinFilter;
                    and.rhs = filter;
                    postJoinFilter = and;
                }
            }
        } catch (Throwable th) {
            Misc.free(joinedMetadata, th);
            throw th;
        }
    }

    public Build getBuild(int index) {
        return builds.getQuick(index);
    }

    public int getBuildCount() {
        return builds.size();
    }

    public boolean hasStaticSymbolTables() {
        for (int i = 0; i < builds.size(); i++) {
            if (!builds.getQuick(i).hasStaticSymbolTables()) {
                return false;
            }
        }
        return true;
    }

    public String getCondition() {
        StringSink sink = new StringSink();
        for (int i = 0; i < builds.size(); i++) {
            if (i > 0) {
                sink.put(" and ");
            }
            sink.put(builds.getQuick(i).getCondition());
        }
        return sink.toString();
    }

    private static RecordMetadata[] inputMetadatas(HashJoinGroupByCandidate candidate, RecordMetadata probe, RecordMetadata build) {
        RecordMetadata[] inputs = new RecordMetadata[2];
        inputs[candidate.getProbeIndex()] = probe;
        inputs[candidate.getBuildIndex(0)] = build;
        return inputs;
    }

    private static IntList[] inputColumns(HashJoinGroupByCandidate candidate, IntList probe, IntList build) {
        IntList[] inputs = new IntList[2];
        inputs[candidate.getProbeIndex()] = probe;
        inputs[candidate.getBuildIndex(0)] = build;
        return inputs;
    }

    public void close() {
        joinedMetadata.close();
    }

    public RecordMetadata getJoinedMetadata() {
        return joinedMetadata;
    }

    public int getProbeColumnCount() {
        return probeColumnCount;
    }

    ExpressionNode getBuildOnFilter() {
        return buildOnFilter;
    }

    ObjList<QueryColumn> getColumns() {
        return columns;
    }

    ExpressionNode getPostJoinFilter() {
        return postJoinFilter;
    }

    private static RecordSink newKeySink(
            @Nullable Class<RecordSink> clazz,
            RecordMetadata inputMetadata,
            ListColumnFilter filter,
            BitSet symbolAsString,
            BitSet stringAsVarchar,
            BitSet timestampAsNanos
    ) {
        return RecordSinkFactory.getInstance(clazz, inputMetadata, filter, null, null,
                symbolAsString, stringAsVarchar, timestampAsNanos);
    }

    private static TableColumnMetadata copyColumn(RecordMetadata metadata, int column, boolean isSymbolTableStatic) {
        return new TableColumnMetadata(metadata.getColumnName(column), metadata.getColumnType(column),
                IndexType.NONE, 0, isSymbolTableStatic, null);
    }

    private static int indexOf(IntList columns, int value) {
        return columns.indexOf(value, 0, columns.size());
    }

    private static ExpressionNode remap(ExpressionNode node, RecordMetadata resolved, IntList indexes, RecordMetadata target) throws SqlException {
        if (node == null) {
            return null;
        }
        CharSequence token = node.token;
        if (node.type == ExpressionNode.LITERAL) {
            int index = indexes.getQuick(resolved.getColumnIndex(node.token));
            if (index < 0) {
                throw SqlException.$(node.position, "missing hash join input column: ").put(node.token);
            }
            token = target.getColumnName(index);
        }
        ExpressionNode copy = ExpressionNode.FACTORY.newInstance().of(node.type, token, node.precedence, node.position);
        copy.paramCount = node.paramCount;
        copy.lhs = remap(node.lhs, resolved, indexes, target);
        copy.rhs = remap(node.rhs, resolved, indexes, target);
        for (int i = 0; i < node.args.size(); i++) {
            copy.args.add(remap(node.args.getQuick(i), resolved, indexes, target));
        }
        return copy;
    }

    private static int requireColumn(IntList columns, int baseColumn) throws SqlException {
        int index = indexOf(columns, baseColumn);
        if (index < 0) {
            throw SqlException.$(0, "missing hash join input column: ").put(baseColumn);
        }
        return index;
    }

    public IntList getBuildColumns() {
        return builds.getQuick(0).getBuildColumns();
    }

    public int getBuildKeyColumn() {
        return builds.getQuick(0).getBuildKeyColumn();
    }

    public IntList getBuildKeyColumns() {
        return builds.getQuick(0).getBuildKeyColumns();
    }

    public HashJoinGroupByKeys getKeys() {
        return builds.getQuick(0).getKeys();
    }

    public ColumnTypes getKeyTypes() {
        return builds.getQuick(0).getKeyTypes();
    }

    public RecordMetadata getPayloadMetadata() {
        return builds.getQuick(0).getPayloadMetadata();
    }

    public int getProbeKeyColumn() {
        return builds.getQuick(0).getProbeKeyColumn();
    }

    public IntList getProbeKeyColumns() {
        return builds.getQuick(0).getProbeKeyColumns();
    }

    public IntList getSymbolKeyBuildColumns() {
        return builds.getQuick(0).getSymbolKeyBuildColumns();
    }

    public IntList getSymbolKeyProbeColumns() {
        return builds.getQuick(0).getSymbolKeyProbeColumns();
    }

    public boolean isSymbolKey() {
        return builds.getQuick(0).isSymbolKey();
    }

    public boolean isKeyCapacityPresized() {
        return builds.getQuick(0).isKeyCapacityPresized();
    }

    public boolean isKeyStaged() {
        return builds.getQuick(0).isKeyStaged();
    }

    public RecordSink newBuildKeySink() {
        return builds.getQuick(0).newBuildKeySink();
    }

    public RecordSink newProbeKeySink() {
        return builds.getQuick(0).newProbeKeySink();
    }

    public HashJoinGroupByRecord newRecord() {
        return builds.getQuick(0).newRecord();
    }

    /** One physical build's key, payload and slot-record binding; borrows both input metadatas. */
    public static final class Build {
        private final IntList buildColumns = new IntList();
        // Borrowed for the looping sink, which reads the source types when a caller instantiates it.
        private final RecordMetadata buildInputMetadata;
        private final IntList buildKeyColumns = new IntList();
        private final ListColumnFilter buildKeyFilter = new ListColumnFilter();
        @Nullable
        private final Class<RecordSink> buildKeySinkClass;
        private final BitSet buildKeyStringAsVarchar = new BitSet();
        private final BitSet buildKeySymbolAsString = new BitSet();
        private final BitSet buildKeyTimestampAsNanos = new BitSet();
        private final String condition;
        private final boolean hasStaticSymbolTables;
        private final boolean isKeyCapacityPresized;
        private final HashJoinGroupByKeys keys;
        private final ArrayColumnTypes keyTypes = new ArrayColumnTypes();
        private final GenericRecordMetadata payloadMetadata = new GenericRecordMetadata();
        private final int probeColumnCount;
        private final RecordMetadata probeInputMetadata;
        private final IntList probeKeyColumns = new IntList();
        private final ListColumnFilter probeKeyFilter = new ListColumnFilter();
        @Nullable
        private final Class<RecordSink> probeKeySinkClass;
        // Per-column encoding flags of one input, indexed the way RecordSinkFactory indexes them. The
        // two inputs keep their own sets, because one set shared by both would force an encoding onto
        // whatever column happens to sit at the same index on the other side.
        private final BitSet probeKeyStringAsVarchar = new BitSet();
        private final BitSet probeKeySymbolAsString = new BitSet();
        private final BitSet probeKeyTimestampAsNanos = new BitSet();
        // Compiled indexes of the SYMBOL key columns, in key order, one entry per SYMBOL pair.
        private final IntList symbolKeyBuildColumns = new IntList();
        private final IntList symbolKeyProbeColumns = new IntList();

        private Build(CairoConfiguration configuration, BytecodeAssembler asm, HashJoinGroupByCandidate candidate,
                      int build, RecordMetadata probeMetadata, IntList mappedProbeKeys,
                      RecordMetadata buildMetadata, IntList buildBaseColumns) throws SqlException {
            if (mappedProbeKeys.size() != candidate.getKeys(build).size()
                    || buildBaseColumns.size() != buildMetadata.getColumnCount()) {
                throw SqlException.$(0, "hash join input mapping size mismatch");
            }
            probeColumnCount = probeMetadata.getColumnCount();
            keys = candidate.getKeys(build);
            isKeyCapacityPresized = candidate.isKeyCapacityPresized();
            // Symbol keys translate through, and payload symbols resolve with, the inputs' static tables.
            boolean hasStaticSymbolTables = true;
            StringSink conditionSink = Misc.getThreadLocalSink();
            for (int i = 0, n = keys.size(); i < n; i++) {
                final int probeColumn = mappedProbeKeys.getQuick(i);
                final int buildColumn = requireColumn(buildBaseColumns, keys.getBuildColumn(i));
                // The inputs compiled after the analysis, so they carry the analysed types or nothing.
                if (probeMetadata.getColumnType(probeColumn) != keys.getProbeType(i)
                        || buildMetadata.getColumnType(buildColumn) != keys.getBuildType(i)) {
                    throw SqlException.$(0, "hash join input key type mismatch");
                }
                if (ColumnType.isSymbol(keys.getProbeType(i)) && !probeMetadata.isSymbolTableStatic(probeColumn)) {
                    hasStaticSymbolTables = false;
                }
                if (ColumnType.isSymbol(keys.getBuildType(i)) && !buildMetadata.isSymbolTableStatic(buildColumn)) {
                    hasStaticSymbolTables = false;
                }
                probeKeyColumns.add(probeColumn);
                buildKeyColumns.add(buildColumn);
                if (keys.isTranslatedSymbol(i)) {
                    symbolKeyProbeColumns.add(probeColumn);
                    symbolKeyBuildColumns.add(buildColumn);
                }
                if (i > 0) {
                    conditionSink.putAscii(" and ");
                }
                conditionSink.put(probeMetadata.getColumnName(probeColumn))
                        .putAscii('=').put(candidate.getBuildModel(build).getName()).putAscii('.').put(buildMetadata.getColumnName(buildColumn));
            }
            condition = conditionSink.toString();
            probeInputMetadata = probeMetadata;
            buildInputMetadata = buildMetadata;
            if (keys.isIntKeyed()) {
                // The INT layout reads its key straight off the probe record, translating it when the
                // key is a SYMBOL pair, so it stages nothing and needs no sink.
                probeKeySinkClass = null;
                buildKeySinkClass = null;
            } else {
                for (int i = 0, n = keys.size(); i < n; i++) {
                    final int probeColumn = probeKeyColumns.getQuick(i);
                    final int buildColumn = buildKeyColumns.getQuick(i);
                    keyTypes.add(keys.getType(i));
                    // A ListColumnFilter entry is a 1-based index whose sign asks for a skip; a key
                    // column is always copied, so every entry is positive.
                    probeKeyFilter.add(probeColumn + 1);
                    buildKeyFilter.add(buildColumn + 1);
                    if (keys.isSymbolAsString(i)) {
                        // Both sides write the symbol's text, so keys from two dictionaries compare.
                        probeKeySymbolAsString.set(probeColumn);
                        buildKeySymbolAsString.set(buildColumn);
                    }
                    if (keys.isProbeStringAsVarchar(i)) {
                        probeKeyStringAsVarchar.set(probeColumn);
                    }
                    if (keys.isBuildStringAsVarchar(i)) {
                        buildKeyStringAsVarchar.set(buildColumn);
                    }
                    if (keys.isProbeTimestampAsNanos(i)) {
                        probeKeyTimestampAsNanos.set(probeColumn);
                    }
                    if (keys.isBuildTimestampAsNanos(i)) {
                        buildKeyTimestampAsNanos.set(buildColumn);
                    }
                }
                probeKeySinkClass = RecordSinkFactory.getInstanceClass(configuration, asm, probeMetadata,
                        probeKeyFilter, null, null, probeKeySymbolAsString, probeKeyStringAsVarchar, probeKeyTimestampAsNanos);
                buildKeySinkClass = RecordSinkFactory.getInstanceClass(configuration, asm, buildMetadata,
                        buildKeyFilter, null, null, buildKeySymbolAsString, buildKeyStringAsVarchar, buildKeyTimestampAsNanos);
            }
            IntList required = candidate.getRequiredBuildColumns(build);
            for (int i = 0; i < required.size(); i++) {
                int column = requireColumn(buildBaseColumns, required.getQuick(i));
                buildColumns.add(column);
                boolean isStatic = buildMetadata.isSymbolTableStatic(column);
                if (ColumnType.isSymbol(buildMetadata.getColumnType(column)) && !isStatic) {
                    hasStaticSymbolTables = false;
                }
                payloadMetadata.add(copyColumn(buildMetadata, column, isStatic));
            }
            this.hasStaticSymbolTables = hasStaticSymbolTables;
        }

        public IntList getBuildColumns() {
            return buildColumns;
        }

        public int getBuildKeyColumn() {
            return buildKeyColumns.getQuick(0);
        }

        public IntList getBuildKeyColumns() {
            return buildKeyColumns;
        }

        public String getCondition() {
            return condition;
        }

        public HashJoinGroupByKeys getKeys() {
            return keys;
        }

        public ColumnTypes getKeyTypes() {
            return keyTypes;
        }

        public RecordMetadata getPayloadMetadata() {
            return payloadMetadata;
        }

        public int getProbeColumnCount() {
            return probeColumnCount;
        }

        public int getProbeKeyColumn() {
            return probeKeyColumns.getQuick(0);
        }

        public IntList getProbeKeyColumns() {
            return probeKeyColumns;
        }

        public IntList getSymbolKeyBuildColumns() {
            return symbolKeyBuildColumns;
        }

        public IntList getSymbolKeyProbeColumns() {
            return symbolKeyProbeColumns;
        }

        public boolean hasStaticSymbolTables() {
            return hasStaticSymbolTables;
        }

        public boolean isSymbolKey() {
            return keys.isSymbolKey();
        }

        public boolean isKeyCapacityPresized() {
            return isKeyCapacityPresized;
        }

        public boolean isKeyStaged() {
            return !keys.isIntKeyed();
        }

        public RecordSink newBuildKeySink() {
            return isKeyStaged() ? newKeySink(buildKeySinkClass, buildInputMetadata, buildKeyFilter,
                    buildKeySymbolAsString, buildKeyStringAsVarchar, buildKeyTimestampAsNanos) : null;
        }

        public RecordSink newProbeKeySink() {
            return isKeyStaged() ? newKeySink(probeKeySinkClass, probeInputMetadata, probeKeyFilter,
                    probeKeySymbolAsString, probeKeyStringAsVarchar, probeKeyTimestampAsNanos) : null;
        }

        public HashJoinGroupByRecord newRecord() {
            return new HashJoinGroupByRecord(probeColumnCount, payloadMetadata);
        }
    }
}
