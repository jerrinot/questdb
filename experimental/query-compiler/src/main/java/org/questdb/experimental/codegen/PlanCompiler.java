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

import java.util.*;
import java.util.function.BiConsumer;
import static org.questdb.experimental.codegen.Ir.*;
import static org.questdb.experimental.codegen.Pipeline.*;

/** Operator translators emit fused pipelines; stateful operators create dependency-ordered phases. */
final class PlanCompiler {
    enum StateKind { GROUP, JOIN, OUTPUT }
    record State(StateKind kind,long entryBytes,List<Long> initial,boolean global) { }
    record Phase(PhysicalPlan source,int inputState,int outputState,Program program) { }
    record Plan(List<State> states,List<Phase> phases,List<Column> output) { }
    private final IdentityHashMap<PhysicalPlan,Integer> slots=new IdentityHashMap<>();
    private final Set<PhysicalPlan> visited=Collections.newSetFromMap(new IdentityHashMap<>());
    private final List<State> states=new ArrayList<>();
    private final List<Phase> phases=new ArrayList<>();
    private final ScanLowering.Mode scanMode;

    private PlanCompiler(ScanLowering.Mode scanMode) { this.scanMode = scanMode; }

    static Plan lower(PhysicalPlan root) {
        return lower(root, ScanLowering.Mode.MASKED);
    }
    static Plan lower(PhysicalPlan root, ScanLowering.Mode scanMode) {
        var c=new PlanCompiler(scanMode);c.prepare(root);
        int output=c.states.size();c.states.add(new State(StateKind.OUTPUT,0,List.of(),false));
        var phase=c.new Context();
        c.emit(root,phase,phase.root,(b,row)->RowStore.result(b,phase.state(output),row));
        phase.root.exit(0);c.phases.add(c.phase(root,output,phase));
        return new Plan(List.copyOf(c.states),List.copyOf(c.phases),root.columns());
    }
    private void prepare(PhysicalPlan node) {
        if(!visited.add(node))return;
        switch(node) {
            case PhysicalPlan.Scan ignored -> { }
            case PhysicalPlan.Filter f -> prepare(f.input());
            case PhysicalPlan.Project p -> prepare(p.input());
            case PhysicalPlan.HashJoin j -> {
                prepare(j.build());prepare(j.probe());
                slots.put(node,states.size());
                states.add(new State(StateKind.JOIN,8L*(j.buildKeys().size()+2),List.of(0L),false));
                var phase=new Context();
                emit(j.build(),phase,phase.root,(b,row)->buildJoin(b,phase,j,row));
                phase.root.exit(0);phases.add(phase(j.build(),slots.get(j),phase));
            }
            case PhysicalPlan.Aggregate a -> {
                prepare(a.input());slots.put(node,states.size());
                List<Long> initial=new ArrayList<>();
                for(var aggregate:a.aggregates()) {
                    initial.add(aggregate.initialValue());
                    for(int i=1;i<aggregate.stateLongs();i++)initial.add(0L);
                }
                states.add(new State(StateKind.GROUP,a.pipeline().entryBytes(),List.copyOf(initial),a.keys().isEmpty()));
                var phase=new Context();
                emit(a.input(),phase,phase.root,(b,row)->a.pipeline().emitGroupBy(b,row,phase.state(slots.get(a))));
                phase.root.exit(0);phases.add(phase(a.input(),slots.get(a),phase));
            }
        }
    }
    private Phase phase(PhysicalPlan input,int output,Context context) {
        var source=source(input);
        int inputSlot=source instanceof PhysicalPlan.Aggregate aggregate?slots.get(aggregate):-1;
        if(inputSlot>=output||context.stateAddresses.keySet().stream().anyMatch(slot->slot>output))
            throw new IllegalStateException("Pipeline refers to an unfinished dependency");
        return new Phase(source,inputSlot,output,new Program(context.root.block()));
    }
    private static PhysicalPlan source(PhysicalPlan node) {
        return switch(node) {
            case PhysicalPlan.Scan s -> s;
            case PhysicalPlan.Aggregate a -> a;
            case PhysicalPlan.Filter f -> source(f.input());
            case PhysicalPlan.Project p -> source(p.input());
            case PhysicalPlan.HashJoin j -> source(j.probe());
        };
    }
    private void emit(PhysicalPlan node,Context context,Builder b,BiConsumer<Builder,Row> consume) {
        ScanPlan scanStage = ScanPlan.from(node);
        if (scanStage != null) {
            ScanLowering.emit(scanStage, b, consume, scanMode);
            return;
        }
        switch(node) {
            case PhysicalPlan.Scan ignored -> throw new AssertionError("Scan stage was not planned");
            case PhysicalPlan.Filter filter -> emit(filter.input(),context,b,(next,row)->
                    next.when(filter.predicate().emit(row),accepted->consume.accept(accepted,row)));
            case PhysicalPlan.Project project -> emit(project.input(),context,b,(next,row)->{
                List<Value> values=new ArrayList<>();
                for(var expression:project.expressions()) {
                    var value=expression.emit(row);
                    // Materialize expression once while retaining its null flag.
                    values.add(new Value(next.let(value.data()),next.let(value.isNull())));
                }
                consume.accept(next,new Row(project.columns(),values));
            });
            case PhysicalPlan.HashJoin join -> emit(join.probe(),context,b,(next,row)->probeJoin(next,context,join,row,consume));
            case PhysicalPlan.Aggregate aggregate -> {
                Var index=b.let(i64(0));Pipeline p=aggregate.pipeline();
                b.loop(binary(Op.LT,index,ROWS),loop->{
                    Var entry=loop.let(new Offset(COLUMNS,binary(Op.MUL,index,i64(p.entryBytes()))));
                    loop.when(ne(load(entry),i64(0)),occupied->{
                        List<Value> values=new ArrayList<>();
                        for(int i=0;i<aggregate.columns().size();i++) {
                            long offset=i<aggregate.keys().size()?8L*(i+1):p.aggregateOffset(i-aggregate.keys().size());
                            Var value=occupied.let(load(at(entry,offset)));
                            values.add(new Value(value,aggregate.columns().get(i).nullable()?eq(value,i64(Long.MIN_VALUE)):bool(false)));
                        }
                        consume.accept(occupied,new Row(aggregate.columns(),values));
                    });
                    loop.assign(index,add(index,i64(1)));
                });
            }
        }
    }
    private void buildJoin(Builder b,Context context,PhysicalPlan.HashJoin join,Row row) {
        Expr state=context.state(slots.get(join));
        Var entry=GroupTable.findOrInsert(b,state,keys(b,row,join.buildKeys()),List.of(i64(0)));
        Var record=RowStore.append(b,at(state,TableState.BYTES),row);
        Expr head=at(entry,8L*(join.buildKeys().size()+1));
        b.store(MemoryType.ADDRESS,record,new Load(MemoryType.ADDRESS,head));
        b.store(MemoryType.ADDRESS,head,record);
    }
    private void probeJoin(Builder b,Context context,PhysicalPlan.HashJoin join,Row probe,BiConsumer<Builder,Row> consume) {
        Expr state=context.state(slots.get(join));
        Var entry=GroupTable.findExisting(b,state,keys(b,probe,join.probeKeys()),states.get(slots.get(join)).entryBytes());
        b.when(ne(entry,nil()),found->{
            Var record=found.let(new Load(MemoryType.ADDRESS,at(entry,8L*(join.buildKeys().size()+1))));
            Var matches=found.let(i64(0));
            found.loop(ne(record,nil()),chain->{
                chain.when(eq(binary(Op.AND,matches,i64(255)),i64(0)),Builder::poll);
                Row build=RowStore.read(chain,record,join.build().columns());
                List<Value> values=new ArrayList<>(build.values());values.addAll(probe.values());
                consume.accept(chain,new Row(join.columns(),values));
                chain.assign(record,new Load(MemoryType.ADDRESS,record));
                chain.assign(matches,add(matches,i64(1)));
            });
        });
    }
    private static List<Var> keys(Builder b,Row row,List<Scalar> expressions) {
        List<Var> keys=new ArrayList<>();
        for(var expression:expressions){var value=expression.emit(row);keys.add(b.let(new Select(value.isNull(),i64(Long.MIN_VALUE),value.data())));}
        return keys;
    }
    private final class Context {
        final Builder root=new Builder();
        final Map<Integer,Var> stateAddresses=new HashMap<>();
        Var state(int slot){return stateAddresses.computeIfAbsent(slot,s->root.let(new Load(MemoryType.ADDRESS,at(STATE,s*8L))));}
    }
}
