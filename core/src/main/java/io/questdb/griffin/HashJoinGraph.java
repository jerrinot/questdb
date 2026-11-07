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

import io.questdb.griffin.model.IQueryModel;
import io.questdb.griffin.model.JoinContext;
import io.questdb.std.IntList;
import io.questdb.std.ObjList;

/** Compiler-time connectivity of an inner equality region. Source indexes remain unchanged. */
final class HashJoinGraph {
    private HashJoinGraph() {
    }

    static Order order(IQueryModel join, int probe) {
        int count = join.getJoinModels().size();
        IntList available = new IntList();
        available.add(probe);
        IntList indexes = new IntList();
        ObjList<JoinContext> contexts = new ObjList<>();
        IntList original = join.getOrderedJoinModels();
        while (available.size() < count) {
            JoinContext next = null;
            for (int i = 0; i < count && next == null; i++) {
                int source = original.getQuick(i);
                if (!available.contains(source)) {
                    next = connectedKeys(join, source, available);
                }
            }
            if (next == null) {
                return null;
            }
            indexes.add(next.slaveIndex);
            contexts.add(next);
            available.add(next.slaveIndex);
        }
        return new Order(indexes, contexts);
    }

    /** Retain every equality, including composite and cyclic constraints, across root changes. */
    private static JoinContext connectedKeys(IQueryModel join, int source, IntList available) {
        JoinContext result = null;
        IntList order = join.getOrderedJoinModels();
        for (int i = 1; i < order.size(); i++) {
            JoinContext edge = join.getJoinModels().getQuick(order.getQuick(i)).getJoinContext();
            for (int k = 0; k < edge.aIndexes.size(); k++) {
                int a = edge.aIndexes.getQuick(k), b = edge.bIndexes.getQuick(k);
                if ((a == source && available.contains(b)) || (b == source && available.contains(a))) {
                    if (result == null) {
                        result = new JoinContext();
                        result.slaveIndex = source;
                    }
                    result.aIndexes.add(a);
                    result.aNames.add(edge.aNames.getQuick(k));
                    result.aNodes.add(edge.aNodes.getQuick(k));
                    result.bIndexes.add(b);
                    result.bNames.add(edge.bNames.getQuick(k));
                    result.bNodes.add(edge.bNodes.getQuick(k));
                    result.parents.add(a == source ? b : a);
                }
            }
        }
        return result;
    }

    static IQueryModel baseTable(IQueryModel input, IQueryModel join) {
        IQueryModel current = input;
        while (current != null) {
            if (hasBarrier(current) || (current != join && current.getJoinModels().size() != 1)) {
                return null;
            }
            if (current.getTableName() != null) {
                return current.getTableNameFunction() == null && current.getLatestBy().size() == 0 ? current : null;
            }
            if (!isProjection(current)) {
                return null;
            }
            current = current.getNestedModel();
        }
        return null;
    }

    static boolean hasBarrier(IQueryModel model) {
        return !model.isOptimisable() || model.getSharedRefCount() > 0
                || model.getLimitLo() != null || model.getLimitHi() != null || model.getUnionModel() != null
                || model.getSelectModelType() == IQueryModel.SELECT_MODEL_DISTINCT
                || model.getSampleBy() != null;
    }

    /** Only scan predicates count; post-join predicates cannot bound an input's build size. */
    static boolean hasInputFilter(IQueryModel input) {
        IQueryModel current = input;
        while (current != null) {
            if (current.getWhereClause() != null || current.getConstWhereClause() != null) {
                return true;
            }
            if (current.getTableName() != null) {
                return false;
            }
            current = current.getNestedModel();
        }
        return false;
    }

    static boolean isProjection(IQueryModel model) {
        return !hasBarrier(model) && (model.getSelectModelType() == IQueryModel.SELECT_MODEL_CHOOSE
                || model.getSelectModelType() == IQueryModel.SELECT_MODEL_VIRTUAL
                || model.getSelectModelType() == IQueryModel.SELECT_MODEL_NONE);
    }

    record Order(IntList builds, ObjList<JoinContext> keys) {
    }
}
