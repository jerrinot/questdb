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

import java.util.ArrayList;
import java.util.List;
import static org.questdb.experimental.codegen.Ir.*;
import static org.questdb.experimental.codegen.Pipeline.*;

/** Generated append-only tuple storage, shared by join payload chains and the result sink. */
final class RowStore {
    static final int HEAD=0,TAIL=8,NEXT=16,REMAINING=24,COUNT=32,BYTES=40;
    private RowStore(){ }
    static Var append(Builder b,Expr store,Row row) {
        long stride=8L*(row.values().size()+1); // link followed by normalized SQL values
        int rowsPerChunk=(int)Math.max(1,65536/stride);
        b.when(eq(load(at(store,REMAINING)),i64(0)), grow->{
            Var chunk=grow.let(new Allocate(i64(stride*rowsPerChunk)));
            grow.when(eq(chunk,nil()),failed->failed.exit(2));
            grow.store(MemoryType.ADDRESS,at(store,NEXT),chunk);
            grow.store(at(store,REMAINING),i64(rowsPerChunk));
        });
        Var record=b.let(new Load(MemoryType.ADDRESS,at(store,NEXT)));
        b.store(MemoryType.ADDRESS,at(store,NEXT),at(record,stride));
        b.store(at(store,REMAINING),binary(Op.SUB,load(at(store,REMAINING)),i64(1)));
        b.store(MemoryType.ADDRESS,record,nil());
        for(int i=0;i<row.values().size();i++) {
            var value=row.values().get(i);
            b.store(at(record,8L*(i+1)),new Select(value.isNull(),i64(Long.MIN_VALUE),value.data()));
        }
        b.store(at(store,COUNT),add(load(at(store,COUNT)),i64(1)));
        return record;
    }
    static void result(Builder b,Expr store,Row row) {
        Var record=append(b,store,row);
        Var tail=b.let(new Load(MemoryType.ADDRESS,at(store,TAIL)));
        b.when(ne(tail,nil()),link->link.store(MemoryType.ADDRESS,tail,record));
        b.when(eq(tail,nil()),first->first.store(MemoryType.ADDRESS,at(store,HEAD),record));
        b.store(MemoryType.ADDRESS,at(store,TAIL),record);
    }
    static Row read(Builder b,Expr record,List<Column> columns) {
        List<Value> values=new ArrayList<>();
        for(int i=0;i<columns.size();i++) {
            Var value=b.let(load(at(record,8L*(i+1))));
            values.add(new Value(value,columns.get(i).nullable()?eq(value,i64(Long.MIN_VALUE)):bool(false)));
        }
        return new Row(columns,values);
    }
}
