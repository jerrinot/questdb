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

import io.questdb.cairo.ColumnType;
import java.util.ArrayList;
import java.util.List;
import static org.questdb.experimental.codegen.Pipeline.*;

/** Composable physical operators. Expressions reference each operator's input schema. */
public sealed interface PhysicalPlan permits PhysicalPlan.Scan, PhysicalPlan.Filter, PhysicalPlan.Project,
        PhysicalPlan.HashJoin, PhysicalPlan.Aggregate {
    List<Column> columns();

    record Scan(String table,List<Column> columns) implements PhysicalPlan {
        public Scan {columns=List.copyOf(columns);checkColumns(columns);if(table==null||table.isEmpty())throw new IllegalArgumentException("Table required");}
    }
    record Filter(PhysicalPlan input,Predicate predicate) implements PhysicalPlan {
        @Override public List<Column> columns(){return input.columns();}
    }
    record Project(PhysicalPlan input,List<Scalar> expressions,List<String> names) implements PhysicalPlan {
        public Project {
            expressions=List.copyOf(expressions);names=List.copyOf(names);
            if(expressions.isEmpty()||expressions.size()!=names.size())throw new IllegalArgumentException("Projection arity mismatch");
        }
        @Override public List<Column> columns() {
            var result=new ArrayList<Column>();
            for(int i=0;i<names.size();i++)result.add(new Column(i,names.get(i),type(expressions.get(i))));
            return List.copyOf(result);
        }
    }
    /** Inner equijoin, retaining duplicate build keys. Null equality follows QuestDB integer joins. */
    record HashJoin(PhysicalPlan build,PhysicalPlan probe,List<Scalar> buildKeys,List<Scalar> probeKeys) implements PhysicalPlan {
        public HashJoin {
            buildKeys=List.copyOf(buildKeys);probeKeys=List.copyOf(probeKeys);
            if(buildKeys.isEmpty()||buildKeys.size()!=probeKeys.size())throw new IllegalArgumentException("Join key arity mismatch");
        }
        @Override public List<Column> columns() {
            var result=new ArrayList<Column>();
            for(var c:build.columns())result.add(new Column(result.size(),"build."+c.name(),c.columnType()));
            for(var c:probe.columns())result.add(new Column(result.size(),"probe."+c.name(),c.columnType()));
            return List.copyOf(result);
        }
    }
    record Aggregate(PhysicalPlan input,List<Scalar> keys,List<Pipeline.Aggregate> aggregates) implements PhysicalPlan {
        public Aggregate {
            keys=List.copyOf(keys);aggregates=List.copyOf(aggregates);
            new Pipeline(input.columns(),new All(),keys,aggregates); // shared type/state constraints
            if(aggregates.stream().anyMatch(a->!(a instanceof RowAggregate)))throw new IllegalArgumentException("Use nested DISTINCT in composed plans");
        }
        Pipeline pipeline(){return new Pipeline(input.columns(),new All(),keys,aggregates);}
        @Override public List<Column> columns() {
            var result=new ArrayList<Column>();
            for(var key:keys)result.add(new Column(result.size(),"key"+result.size(),type(key)));
            for(int i=0;i<aggregates.size();i++)result.add(new Column(result.size(),"aggregate"+i,ColumnType.LONG));
            return List.copyOf(result);
        }
    }
    private static int type(Scalar scalar){return scalar instanceof Column c?c.columnType():scalar instanceof IPv4Subtract?ColumnType.IPv4:scalar instanceof Add a?a.columnType():scalar instanceof Subtract s?s.columnType():scalar instanceof Multiply m?m.columnType():ColumnType.LONG;}
    private static void checkColumns(List<Column> columns) {
        if(columns.isEmpty())throw new IllegalArgumentException("Scan needs a row source");
        for(int i=0;i<columns.size();i++)if(columns.get(i).index()!=i)throw new IllegalArgumentException("Non-consecutive column index");
    }
}
