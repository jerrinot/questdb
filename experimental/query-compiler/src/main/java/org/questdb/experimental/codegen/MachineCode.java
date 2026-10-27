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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.questdb.experimental.codegen.Ir.*;

/** Backend-neutral register/label stream. No SQL operators or query shapes reach the native bridge. */
final class MachineCode {
    // Wire ABI shared with asmjit_backend.cpp. A version guards mismatched native builds.
    static final int MAGIC=0x51444952, VERSION=6, HEADER_BYTES=24, INSTRUCTION_BYTES=24;
    static final int CONST=1,MOVE=2,ADD=3,SUB=4,MUL=5,AND=6,XOR=7,USHR=8,
            EQ=9,NE=10,LT=11,GE=12,LOAD=13,STORE=14,LABEL=15,JUMP=16,
            BR_FALSE=17,ALLOC=18,FREE=19,POLL=20,RETURN=21,COMPARE_BRANCH=22,BR_TRUE=23,CTZ=24,PREFETCH=25,FADD=26,FSUB=27,FMUL=28,FDIV=29,TO_DOUBLE=30,SDIV=31,ATOMIC_LOAD=32,COMPARE_EXCHANGE=33,ATOMIC_STORE=34,ATOMIC_ADD=35;
    private record Instruction(int op,int dst,int a,int b,long immediate) { }
    private final List<Instruction> code=new ArrayList<>();
    private final Map<Integer,Integer> variables=new HashMap<>(Map.of(0,0,1,1,2,2));
    private int registers=3,labels;

    static byte[] lower(Program program) {
        var lowering=new MachineCode();lowering.block(program.body());
        return lowering.encode();
    }
    static byte[] lower(SsaProgram graph) {
        var lowering=new MachineCode();
        var blocks=graph.executableBlocks();
        lowering.registers=graph.nextValue;
        for(int id=0;id<graph.nextValue;id++)lowering.variables.put(id,id);
        lowering.labels=blocks.stream().mapToInt(b->b.id).max().orElseThrow()+1;
        lowering.jump(0);
        for(int blockIndex=0;blockIndex<blocks.size();blockIndex++) {
            var b=blocks.get(blockIndex);
            int next=blockIndex+1<blocks.size()?blocks.get(blockIndex+1).id:-1;
            lowering.bind(b.id);
            for(var s:b.code) lowering.block(new Block(List.of(s instanceof Define d?new Assign(d.variable(),d.value()):s)));
            switch(b.end) {
                case SsaProgram.Jump j -> {if(j.target()!=next)lowering.jump(j.target());}
                case SsaProgram.Branch c -> {
                    if(c.yes()==next)lowering.branch(c.condition(),c.no(),false);
                    else {lowering.branch(c.condition(),c.yes(),true);if(c.no()!=next)lowering.jump(c.no());}
                }
                case SsaProgram.Exit e -> lowering.emit(RETURN,0,lowering.expression(e.value()),0,0);
            }
        }
        return lowering.encode();
    }
    private byte[] encode() {
        removeUnreachable();
        var bytes=ByteBuffer.allocate(Math.addExact(HEADER_BYTES,Math.multiplyExact(code.size(),INSTRUCTION_BYTES)))
                .order(ByteOrder.LITTLE_ENDIAN);
        bytes.putInt(MAGIC).putInt(VERSION).putInt(registers).putInt(labels).putInt(code.size()).putInt(0);
        for(var i:code)bytes.putInt(i.op).putInt(i.dst).putInt(i.a).putInt(i.b).putLong(i.immediate);
        return bytes.array();
    }
    /** Constant branches can disconnect loop bodies. Do not send dead predecessors into register allocation. */
    private void removeUnreachable() {
        int[] positions=new int[labels];
        for(int i=0;i<code.size();i++)if(code.get(i).op==LABEL)positions[code.get(i).dst]=i;
        boolean[] reachable=new boolean[code.size()];
        var pending=new ArrayDeque<Integer>();pending.add(0);
        while(!pending.isEmpty()) {
            int position=pending.removeLast();
            if(position>=code.size()||reachable[position])continue;
            reachable[position]=true;var instruction=code.get(position);
            switch(instruction.op) {
                case JUMP -> pending.add(positions[instruction.dst]);
                case BR_FALSE,BR_TRUE,COMPARE_BRANCH -> {
                    pending.add(positions[instruction.dst]);pending.add(position+1);
                }
                case RETURN -> { }
                default -> pending.add(position+1);
            }
        }
        int output=0;
        for(int i=0;i<code.size();i++)if(reachable[i])code.set(output++,code.get(i));
        code.subList(output,code.size()).clear();
    }
    private void emit(int op,int dst,int a,int b,long immediate){code.add(new Instruction(op,dst,a,b,immediate));}
    private int reg(){return registers++;}
    private int label(){return labels++;}
    private void bind(int label){emit(LABEL,label,0,0,0);}
    private void jump(int label){emit(JUMP,label,0,0,0);}
    private int constant(long value){int r=reg();emit(CONST,r,0,0,value);return r;}
    private int variable(Var variable){return variables.get(variable.id());}

    private int expression(Expr expression) {
        return switch(expression) {
            case Var v -> variable(v);
            case Literal l -> constant(l.value());
            case Binary b -> {
                if(b.op()==Op.BOTH)yield select(b.left(),b.right(),bool(false));
                if(b.op()==Op.EITHER)yield select(b.left(),bool(true),b.right());
                int left=expression(b.left()),right=expression(b.right()),result=reg();
                int op=switch(b.op()) {
                    case FADD->FADD;case FSUB->FSUB;case FMUL->FMUL;case FDIV->FDIV;case SDIV->SDIV;
                    case ADD->ADD;case SUB->SUB;case MUL->MUL;case AND->AND;case XOR->XOR;case USHR->USHR;
                    case EQ->EQ;case NE->NE;case LT->LT;case GE->GE;
                    default->throw new AssertionError(b.op());
                };
                emit(op,result,left,right,0);yield result;
            }
            case Load l -> {var address=address(l.address());int result=reg();emit(LOAD,result,address.base(),address.offset(),l.width().bytes);yield result;}
            case Offset o -> {int address=expression(o.address()),offset=expression(o.bytes()),result=reg();emit(ADD,result,address,offset,0);yield result;}
            case PopulationCount ignored -> throw new IllegalArgumentException("Population count requires portable lowering");
            case TrailingZeros z -> { int input=expression(z.value()),result=reg();emit(CTZ,result,input,0,0);yield result; }
            case VectorMask ignored -> throw new IllegalArgumentException("Vector mask requires portable lowering");
            case AtomicLoad l -> {int address=expression(l.address()),result=reg();emit(ATOMIC_LOAD,result,address,0,0);yield result;}
            case AtomicAdd a -> {int address=expression(a.address()),value=expression(a.value()),result=reg();emit(ATOMIC_ADD,result,address,value,0);yield result;}
            case CompareExchange c -> {int address=expression(c.address()),expected=expression(c.expected()),value=expression(c.value()),result=reg();emit(COMPARE_EXCHANGE,result,address,expected,value);yield result;}
            case SelectRows ignored -> throw new IllegalArgumentException("Selection requires portable lowering");
            case Select s -> select(s.condition(),s.yes(),s.no());
            case ToDouble c -> {int input=expression(c.value()),result=reg();emit(TO_DOUBLE,result,input,0,0);yield result;}
            case Allocate a -> {int bytes=expression(a.bytes()),result=reg();emit(ALLOC,result,bytes,0,0);yield result;}
        };
    }
    private record Address(int base,int offset) { }
    private Address address(Expr expression) {
        if(expression instanceof Offset o&&o.bytes() instanceof Literal l&&l.value()==(int)l.value())
            return new Address(expression(o.address()),(int)l.value());
        return new Address(expression(expression),0);
    }
    /** Preserve short-circuit control flow without materializing temporary boolean values. */
    private void branch(Expr condition,int target,boolean whenTrue) {
        if(condition instanceof Literal l) {if((l.value()!=0)==whenTrue)jump(target);return;}
        if(condition instanceof Binary b) {
            if((b.op()==Op.EQ||b.op()==Op.NE)&&b.left().type()==Type.BOOL) {
                if(b.right() instanceof Literal l) {
                    branch(b.left(),target,whenTrue==(b.op()==Op.EQ?l.value()!=0:l.value()==0));return;
                }
                if(b.left() instanceof Literal l) {
                    branch(b.right(),target,whenTrue==(b.op()==Op.EQ?l.value()!=0:l.value()==0));return;
                }
            }
            if(b.op()==Op.BOTH||b.op()==Op.EITHER) {
                boolean direct=b.op()==Op.BOTH?!whenTrue:whenTrue;
                if(direct){branch(b.left(),target,whenTrue);branch(b.right(),target,whenTrue);}
                else {int end=label();branch(b.left(),end,!whenTrue);branch(b.right(),target,whenTrue);bind(end);}
                return;
            }
            int op=switch(b.op()) {
                case EQ->whenTrue?EQ:NE;case NE->whenTrue?NE:EQ;
                case LT->whenTrue?LT:GE;case GE->whenTrue?GE:LT;
                default->0;
            };
            if(op!=0){int left=expression(b.left()),right=expression(b.right());emit(COMPARE_BRANCH,target,left,right,op);return;}
        }
        emit(whenTrue?BR_TRUE:BR_FALSE,target,expression(condition),0,0);
    }
    private int select(Expr condition,Expr yes,Expr no) {
        int result=reg(),otherwise=label(),end=label();
        branch(condition,otherwise,false);
        emit(MOVE,result,expression(yes),0,0);jump(end);
        bind(otherwise);emit(MOVE,result,expression(no),0,0);bind(end);
        return result;
    }
    private void block(Block block) {
        for(var statement:block.statements())switch(statement) {
            case Define d -> {int target=reg();variables.put(d.variable().id(),target);emit(MOVE,target,expression(d.value()),0,0);}
            case Assign a -> emit(MOVE,variable(a.variable()),expression(a.value()),0,0);
            case AtomicStore atomic -> {int address=expression(atomic.address()),value=expression(atomic.value());emit(ATOMIC_STORE,0,address,value,0);}
            case Store s -> {var address=address(s.address());int value=expression(s.value());emit(STORE,address.offset(),address.base(),value,s.width().bytes);}
            case Free f -> {int address=expression(f.address()),bytes=expression(f.bytes());emit(FREE,0,address,bytes,0);}
            case Poll ignored -> emit(POLL,0,0,0,0);
            case Call ignored -> throw new IllegalArgumentException("Expand helpers before machine lowering");
            case Prefetch p -> {var address=address(p.address());emit(PREFETCH,address.offset(),address.base(),0,p.write()?1:0);}
            case If i -> {int end=label();branch(i.condition(),end,false);block(i.body());bind(end);}
            case While w -> {
                int head=label(),end=label();bind(head);branch(w.condition(),end,false);
                block(w.body());jump(head);bind(end);
            }
            case Return r -> emit(RETURN,0,expression(r.value()),0,0);
        };
    }
}
