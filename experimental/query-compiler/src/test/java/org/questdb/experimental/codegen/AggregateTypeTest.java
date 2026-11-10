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

import io.questdb.cairo.CairoEngine;
import io.questdb.cairo.DefaultCairoConfiguration;
import io.questdb.cairo.sql.RecordCursorFactory;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.table.AsyncGroupByRecordCursorFactory;
import io.questdb.griffin.engine.table.AsyncGroupByNotKeyedRecordCursorFactory;
import io.questdb.mp.*;
import io.questdb.std.MemoryTag;
import io.questdb.std.Unsafe;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.util.List;

import static org.junit.Assert.*;

public class AggregateTypeTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private final Backend backend = new LlvmBackend();
    private CompilerOptions options = CompilerOptions.DEFAULT;
    private CairoEngine engine;
    private SqlExecutionContextImpl context;
    private WorkerPool pool;
    private ResolvedQueryCompiler compiler;
    private long memory;

    private void configure(CompilerOptions options) throws java.io.IOException {
        this.options = options;
        compiler = new ResolvedQueryCompiler(backend, temporary.newFolder().toPath(), options);
    }

    @Before public void setUp() throws Exception {
        memory = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        var cfg = new DefaultCairoConfiguration(temporary.newFolder().toString()) {
            @Override public int getSqlPageFrameMaxRows() { return 65; }
            @Override public int getSqlPageFrameMinRows() { return 33; }
            @Override public int getGroupByShardingThreshold() { return 2; }
            @Override public int getGroupByMapCapacity() { return 16; }
            @Override public long getGroupByParallelTopKThreshold() { return 0; }
        };
        engine = new CairoEngine(cfg);
        pool = new WorkerPool(new WorkerPoolConfiguration() {
            @Override public int getWorkerCount() { return 2; }
        });
        WorkerPoolUtils.setupQueryJobs(pool, engine);
        pool.start();
        context = new SqlExecutionContextImpl(engine, 2).with(
                cfg.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
        compiler = new ResolvedQueryCompiler(backend, temporary.newFolder().toPath(), options);
    }

    @After public void tearDown() {
        if (pool != null) pool.halt();
        if (context != null) context.close();
        if (engine != null) engine.close();
        assertEquals(memory, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
    }

    @Test public void compilerOptionsRemainIsolatedThroughDeferredCompilationAndReopen() throws Exception {
        engine.execute("create table options_test as (select cast(x%17 as int) k,x v from long_sequence(1031))", context);
        // SUM(v+constant) is rewritten by SqlOptimiser before expression admission.
        String sql = "select k,sum(v*k) from options_test where v>0 group by k";
        configure(options.withExpressions(true).withLocalCapacity(64).withPrefetchRows(0));
        try (var enabled = compile(sql)) {
            // Construct an independent compiler before the first cursor prepares deferred kernels.
            configure(options.withExpressions(false).withLocalCapacity(128).withPrefetchRows(32));
            try (var disabled = compile(sql)) {
                check(sql, enabled);
                check(sql, disabled);
                check(sql, enabled);
                check(sql, disabled);
                assertTrue(nativeRows(enabled) > 0);
                assertEquals(0, nativeRows(disabled));
            }
        }
    }

    @Test public void floatingRemainderComposesWithTypedConsumersAndExceptionalInputs() throws Exception {
        engine.execute("create table remainders as (select cast(x%17 as int) k,cast('s'||(x%7) as symbol) s,"
                + "cast('v'||(x%11) as varchar) label,cast(case when x%13=0 then null else (x%97-48)*0.25 end as float) f,"
                + "cast((x%17-8)*0.25 as float) g,case when x%13=0 then null else (x%97-48)*0.25 end d,"
                + "(x%17-8)*0.25 e,timestamp_sequence('2024-01-01',1000000) ts from long_sequence(4099)) timestamp(ts) partition by day bypass wal",context);
        for(String expression:List.of("f%g","d%e","f%cast(1.75 as float)","d%(-1.75)","(f%g)+cast(0.25 as float)")) {
            // Ordinary parallel MIN/MAX can pick either signed zero for equal zeros.
            // Shift quarter-step extrema off zero; raw IR checks retain exact zero bits.
            String aggregates="count("+expression+"),sum("+expression+"),min(("+expression+")+cast(0.125 as float)),max(("+expression+")+cast(0.125 as float))";
            for(String key:List.of("","k,s","label")) {
                String sql="select "+(key.isEmpty()?"":key+",")+aggregates+" from remainders"
                        +(key.isEmpty()?" where k>=0":" group by "+key);
                try(var candidate=compile(sql)) {check(sql,candidate);assertTrue(sql,nativeRows(candidate)>0);}
            }
        }
        for(String sql:List.of(
                "select f%g h,s,count(),sum(d%e) from remainders group by h,s",
                "select count(),sum(case s when 's1' then f%g else d%e end) from remainders where d%e<1.0",
                "select k,sum(m%1.75),max(n%2.25) from (select k,s,min(d) m,max(e) n from remainders group by k,s) group by k")) {
            try(var candidate=compile(sql)) {check(sql,candidate);assertTrue(sql,nativeRows(candidate)>0);}
        }
        engine.execute("alter table remainders add column late double",context);
        engine.execute("insert into remainders values (1,'s1','v',1.5,0.75,1.5,0.75,'2024-01-02',0.5)",context);
        String sql="select k,count(d%late),sum(d%late),max(d%late) from remainders group by k";
        try(var candidate=compile(sql)) {check(sql,candidate);assertTrue(nativeRows(candidate)>0);}
        engine.execute("alter table remainders convert partition to parquet list '2024-01-01'",context);
        try(var candidate=compile(sql)) {check(sql,candidate);check(sql,candidate);assertTrue(nativeRows(candidate)>0);}
        engine.execute("create table rem_edges (f float,g float,d double,e double)",context);
        double[] values={Double.NaN,Double.POSITIVE_INFINITY,Double.NEGATIVE_INFINITY,-0.0,0,Double.MIN_VALUE,
                -Double.MIN_VALUE,Double.MIN_NORMAL,Double.MAX_VALUE,-Double.MAX_VALUE,Float.MIN_VALUE,Float.MAX_VALUE,1,-1,1.75,-17.25};
        try(var writer=engine.getWriter(engine.verifyTableName("rem_edges"),"test")) {
            for(double left:values) for(double right:values) {
                var row=writer.newRow();row.putFloat(0,(float)left);row.putFloat(1,(float)right);
                row.putDouble(2,left);row.putDouble(3,right);row.append();
            }
            writer.commit();
        }
        for(String query:List.of("select count(f%g),min(f%g),max(f%g),count(d%e),min(d%e),max(d%e) from rem_edges",
                "select f%g h,count() from rem_edges group by h","select d%e h,count() from rem_edges group by h")) {
            try(var candidate=compile(query)) {check(query,candidate);assertTrue(query,nativeRows(candidate)>0);}
        }
    }

    @Test public void charactersPreserveUnsignedOrderNullsAndComposition() throws Exception {
        engine.execute("create table characters (k int,s symbol,label varchar,c char,i int,ts timestamp) timestamp(ts) partition by day bypass wal",context);
        int[] values={0,1,48,57,127,255,32767,32768,55296,57343,65535};
        try(var writer=engine.getWriter(engine.verifyTableName("characters"),"test")) {
            for(int n=0;n<4099;n++) {
                var row=writer.newRow(n*1000000L);row.putInt(0,n%17);row.putSym(1,"s"+n%7);
                row.putVarchar(2,new io.questdb.std.str.Utf8String("v"+n%11));
                row.putChar(3,(char)values[n%values.length]);
                row.putInt(4,n%13==0?Integer.MIN_VALUE:n%19==0?-1:values[n%values.length]+65536);row.append();
            }
            writer.commit();
        }
        for(String expression:List.of("c","cast(i as char)","case when k>8 then c end",
                "case s when 's1' then c else cast(i as char) end")) {
            String aggregates="count(),count("+expression+"),min("+expression+"),max("+expression+")";
            for(String key:List.of("","k","c,s","label,c")) {
                String sql="select "+(key.isEmpty()?"":key+",")+aggregates+" from characters"
                        +(key.isEmpty()?" where k>=0":" group by "+key);
                try(var candidate=compile(sql)) {check(sql,candidate);check(sql,candidate);assertTrue(sql,nativeRows(candidate)>0);}
            }
        }
        for(String sql:List.of(
                "select cast(i as char) h,s,count(),min(c),max(c) from characters group by h,s",
                "select case c when cast(65535 as char) then 1 when cast(0 as char) then 2 else 3 end h,min(c),max(c) from characters group by h",
                "select count(),min(c),max(c) from characters where c<cast(32768 as char)",
                "select count(),min(c),max(c) from characters where c>=cast(32768 as char)",
                "select count(),min(c),max(c) from characters where c=null or c=cast(65535 as char)",
                "select k,min(a),max(b),sum(n) from (select k,s,min(c) a,max(c) b,count() n from characters group by k,s) group by k")) {
            try(var candidate=compile(sql)) {check(sql,candidate);assertTrue(sql,nativeRows(candidate)>0);}
        }
        engine.execute("alter table characters add column late char",context);
        engine.execute("insert into characters values (1,'new','v',cast(65535 as char),65535,'2024-01-01',cast(32768 as char))",context);
        String sql="select late,s,count(late),min(late),max(late),min(c),max(c) from characters group by late,s";
        try(var candidate=compile(sql)) {check(sql,candidate);assertTrue(nativeRows(candidate)>0);}
        engine.execute("alter table characters convert partition to parquet list '1970-01-01'",context);
        try(var candidate=compile(sql)) {
            check(sql,candidate);assertTrue(nativeRows(candidate)>0);
            engine.execute("truncate table characters",context);check(sql,candidate);
        }
    }

    @Test public void characterNumericGettersKeepOrdinaryDigitParsing() throws Exception {
        engine.execute("create table digits as (select cast(cast(48+x%10 as int) as char) c from long_sequence(4099))",context);
        String sql="select sum(c),avg(c) from digits";
        try(var candidate=compile(sql)) {check(sql,candidate);assertEquals(0,nativeRows(candidate));}
    }

    @Test public void integerDivisionAndRemainderComposeWithNullsAndRowDivisors() throws Exception {
        engine.execute("create table integer_division (k int,s symbol,label varchar,i int,j int,l long,m long)",context);
        long[] values={Long.MIN_VALUE,Long.MIN_VALUE+1,Long.MAX_VALUE,Integer.MIN_VALUE,Integer.MIN_VALUE+1,
                Integer.MAX_VALUE,-97,-3,-1,0,1,3,97};
        try(var writer=engine.getWriter(engine.verifyTableName("integer_division"),"test")) {
            for(int n=0;n<2003;n++) {
                var row=writer.newRow();row.putInt(0,n%17);row.putSym(1,"s"+n%7);
                row.putVarchar(2,new io.questdb.std.str.Utf8String("v"+n%11));
                row.putInt(3,(int)values[n%values.length]);row.putInt(4,(int)values[n/values.length%values.length]);
                row.putLong(5,values[n%values.length]);row.putLong(6,values[n/values.length%values.length]);row.append();
            }
            writer.commit();
        }
        for(String expression:List.of("i/j","l/m","i%j","l%m","i/(-3)","l%(-1)","i%3","l/3")) {
            // Bound SUM to avoid ordinary schedule-dependent sentinel overflow; extrema
            // and the direct projection still check every extreme quotient/remainder.
            String aggregates="count("+expression+"),sum(("+expression+")%1000003),min("+expression+"),max("+expression+")";
            for(String key:List.of("","k,s","label")) {
                String sql="select "+(key.isEmpty()?"":key+",")+aggregates+" from integer_division"
                        +(key.isEmpty()?" where k>=0":" group by "+key);
                try(var candidate=compile(sql)) {check(sql,candidate);assertTrue(sql,nativeRows(candidate)>0);}
            }
        }
        for(String sql:List.of(
                "select i%j r,s,count(),sum((l/m)%1000003),max(l%m) from integer_division group by r,s",
                "select count(),count_distinct(case s when 's1' then i%j end),sum(case when k%5=0 then (l/m)%1000003 end) "
                        +"from integer_division where i/j>0 or l%m=0",
                "select count(),sum(i/j),min(l%m) from integer_division where i/j=null and l%m=null")) {
            try(var candidate=compile(sql)) {check(sql,candidate);check(sql,candidate);assertTrue(sql,nativeRows(candidate)>0);}
        }
        String projection="select i/j,i%j,l/m,l%m from integer_division";
        try(var candidate=compile(projection)) {check(projection,candidate);check(projection,candidate);}
    }

    @Test public void scalarDistinctComposesWithMixedStatesFiltersAndDictionaryReopen() throws Exception {
        engine.execute("create table scalar_distinct as (select cast(x as int) k,"
                + "cast(case when x%13=0 then null else x%2047 end as int) i,"
                + "case when x%17=0 then null else x%4093*4294967297 end l,"
                + "cast(case when x%19=0 then null else 's'||x%129 end as symbol) s,"
                + "cast(case when x%23=0 then null else x%4096-2048 end as int)::ipv4 ip,"
                + "cast(127 as byte) b from long_sequence(5003))",context);
        String aggregates="count(),count_distinct(i),count_distinct(l),count_distinct(s),count_distinct(ip),"
                + "sum(i),min(l),max(i),avg(i),bit_and(b)";
        for (String predicate : List.of("k>=0", "k%3=1", "k>4900", "k=13", "k<0")) {
            String sql="select "+aggregates+" from scalar_distinct where "+predicate;
            try(var candidate=compile(sql)) {
                check(sql,candidate);check(sql,candidate);
                if(!predicate.equals("k<0")) assertTrue(sql,nativeRows(candidate)>0);
            }
        }
        String sql="select count(),count_distinct(case s when 's1' then i end),"
                +"count_distinct(case when k>2000 then l end),count_distinct(case when k<4000 then ip end),"
                +"bit_and(b) from scalar_distinct where k>=0";
        try(var candidate=compile(sql)) {
            check(sql,candidate);assertTrue(nativeRows(candidate)>0);
            engine.execute("truncate table scalar_distinct",context);check(sql,candidate);
            engine.execute("insert into scalar_distinct values (1,12,42,'new','128.0.0.0',63),"
                    +"(2,13,43,'s1','255.255.255.255',31)",context);check(sql,candidate);check(sql,candidate);
        }
    }

    @Test public void scalarDistinctPreservesTopsParquetAndEmptyStates() throws Exception {
        engine.execute("create table distinct_frames as (select cast(x as int) k,x v,"
                +"timestamp_sequence('2024-01-01',1000000L) ts from long_sequence(503)) "
                +"timestamp(ts) partition by day",context);
        engine.execute("alter table distinct_frames add column late int",context);
        engine.execute("insert into distinct_frames values (700,42,'2024-01-02',9),"
                +"(701,43,'2024-01-02',10)",context);
        String sql="select count(),count_distinct(v),count_distinct(late),sum(v) from distinct_frames where k>=0";
        try(var candidate=compile(sql)) {check(sql,candidate);check(sql,candidate);assertTrue(nativeRows(candidate)>0);}
        engine.execute("alter table distinct_frames convert partition to parquet list '2024-01-01'",context);
        try(var candidate=compile(sql)) {check(sql,candidate);check(sql,candidate);assertTrue(nativeRows(candidate)>0);}
        engine.execute("alter table distinct_frames alter column late type long",context);
        try(var candidate=compile(sql)) {check(sql,candidate);check(sql,candidate);}
        for(String predicate:List.of("k<0","k=700","k<503")) {
            sql="select count(),count_distinct(v),count_distinct(late) from distinct_frames where "+predicate;
            try(var candidate=compile(sql)) {check(sql,candidate);check(sql,candidate);}
        }
    }

    @Test public void floatingIntegerCastsPreserveRangeChecksRoundedBoundsAndNulls() throws Exception {
        engine.execute("create table floating_casts (k int,s symbol,label varchar,f float,d double)",context);
        double[] values={Double.NaN,Double.POSITIVE_INFINITY,Double.NEGATIVE_INFINITY,-0.0,0,
                Double.MIN_VALUE,-Double.MIN_VALUE,Double.MAX_VALUE,-Double.MAX_VALUE,1.9,-1.9,
                127,Math.nextUp(127.0),127.5,-128,Math.nextDown(-128.0),-128.5,
                32767,32767.5,-32768,-32768.5,2147483647,Math.nextUp(2147483647.0),
                Math.nextDown(0x1p31f),0x1p31,-0x1p31,Math.nextDown(-0x1p31),
                Math.nextDown(0x1p63),0x1p63,Math.nextUp(0x1p63),-0x1p63,Math.nextDown(-0x1p63)};
        try(var writer=engine.getWriter(engine.verifyTableName("floating_casts"),"test")) {
            for(int i=0;i<2003;i++) {
                var row=writer.newRow();row.putInt(0,i%17);row.putSym(1,"s"+i%7);row.putVarchar(2,new io.questdb.std.str.Utf8String("v"+i%11));
                row.putFloat(3,(float)values[i%values.length]);row.putDouble(4,values[i%values.length]);row.append();
            }
            writer.commit();
        }
        for(String source:List.of("f","d")) for(String type:List.of("byte","short","int","long")) {
            String expression=source+"::"+type;
            String aggregates="count("+expression+"),min("+expression+"),max("+expression+"),"
                    +"bit_and("+expression+"),bit_or("+expression+"),count(cast("+expression+" as double))";
            for(String key:List.of("","k","s","k,s","label")) {
                String sql="select "+(key.isEmpty()?"":key+",")+aggregates+" from floating_casts"
                        +(key.isEmpty()?" where k>=0":" group by "+key);
                try(var candidate=compile(sql)) {check(sql,candidate);check(sql,candidate);assertTrue(sql,nativeRows(candidate)>0);}
            }
            String sql="select "+expression+" v,label,count(),min(k) from floating_casts group by v,label";
            try(var candidate=compile(sql)) {check(sql,candidate);assertTrue(nativeRows(candidate)>0);}
            sql="select k,count(),bit_and(cast(case s when 's1' then "+source+" end as "+type+")) "
                    +"from floating_casts where "+expression+">0 group by k";
            try(var candidate=compile(sql)) {check(sql,candidate);assertTrue(nativeRows(candidate)>0);}
        }
    }

    @Test public void integerNarrowingCastsPreserveLowBitsAndNullGetters() throws Exception {
        engine.execute("create table narrow_values as (select cast(x%17 as int) k,cast(x%7 as symbol) s,"
                + "cast('v'||x%11 as varchar) label,cast(x%65535-32767 as short) h,"
                + "cast(case x%7 when 0 then null when 1 then 2147483647 when 2 then -2147483647 "
                + "when 3 then 32768 when 4 then -32769 when 5 then 128 else -129 end as int) i,"
                + "case x%11 when 0 then null when 1 then 9223372036854775807 when 2 then -9223372036854775807 "
                + "when 3 then 2147483648 when 4 then -2147483649 when 5 then 65535 when 6 then 32768 "
                + "when 7 then 128 when 8 then -129 when 9 then 4294967295 else 4294967296 end l "
                + "from long_sequence(2003))",context);
        for (String expression : List.of("l::byte", "l::short", "l::int", "i::byte", "i::short", "h::byte",
                "cast(case s when '1' then l end as byte)", "cast(case when k>8 then l end as int)",
                "cast(cast(l as int) as byte)", "cast(cast(l as int) as long)")) {
            String aggregates="count("+expression+"),min("+expression+"),max("+expression+"),bit_and("+expression+"),"
                    +"bit_or("+expression+"),count(cast("+expression+" as double))";
            for (String key : List.of("", "k", "s", "k,s", "label")) {
                String sql="select "+(key.isEmpty()?"":key+",")+aggregates+" from narrow_values"
                        +(key.isEmpty()?" where k>=0":" group by "+key);
                try (var candidate=compile(sql)) { check(sql,candidate); assertTrue(sql,nativeRows(candidate)>0); }
            }
        }
        for (String type : List.of("byte", "short", "int")) {
            String sql="select cast(l as "+type+") v,label,count(),min(l) from narrow_values group by v,label";
            try (var candidate=compile(sql)) { check(sql,candidate); check(sql,candidate); assertTrue(sql,nativeRows(candidate)>0); }
        }
        String sql="select k,count(),bit_and(l::byte),max(l::short),count(l::int) from narrow_values where l::byte>0 group by k";
        try (var candidate=compile(sql)) { check(sql,candidate); assertTrue(nativeRows(candidate)>0); }
        sql="select l::byte,l::short,l::int,cast(l::int as long) from narrow_values";
        try (var candidate=compile(sql)) {
            check(sql,candidate); check(sql,candidate);
            var base=candidate;
            while(base!=null && !(base instanceof io.questdb.griffin.engine.table.VirtualRecordCursorFactory)) base=base.getBaseFactory();
            assertNotNull(base);
            assertTrue(((io.questdb.griffin.engine.table.VirtualRecordCursorFactory)base).getProjectedRows()>0);
        }
    }

    @Test public void bitwiseFamiliesComposeAcrossWidthsAndKeys() throws Exception {
        engine.execute("create table bits as (select cast(x%17 as int) k,cast(x%7 as symbol) s,"
                + "cast('v'||x%11 as varchar) label,cast(x%127+1 as byte) b,cast(x%32767+1 as short) h,"
                + "cast(case when x%13=0 then null else x%65535+1 end as int) i,"
                + "case when x%13=0 then null else x%65535+1 end l from long_sequence(2003))",context);
        for (String argument : List.of("b", "h", "i", "l", "case s when '1' then b else cast(127 as byte) end",
                "case when k>8 then h else cast(32767 as short) end", "case when k>8 then i end", "-l")) {
            String aggregates="count(),bit_and("+argument+"),bit_or("+argument+"),bit_xor("+argument+")";
            for (String key : List.of("", "k", "s", "k,s", "label")) {
                String sql="select "+(key.isEmpty()?"":key+",")+aggregates+" from bits"
                        +(key.isEmpty()?" where k>=0":" group by "+key);
                try (var candidate=compile(sql)) { check(sql,candidate); check(sql,candidate); assertTrue(sql,nativeRows(candidate)>0); }
            }
        }
        for (String predicate : List.of("k<0", "k=1", "k>=0 and k<2")) {
            String sql="select bit_and(b),bit_and(h),bit_or(i),bit_xor(l) from bits where "+predicate;
            try (var candidate=compile(sql)) { check(sql,candidate); check(sql,candidate); }
        }
    }

    @Test public void narrowBitwiseFirstRowsSurviveEmptyFramesTopsAndReopen() throws Exception {
        engine.execute("create table bit_frames as (select cast(x as int) k,cast(127 as byte) b,"
                + "cast(32767 as short) h,timestamp_sequence(0,1000000) ts from long_sequence(503)) "
                + "timestamp(ts) partition by day",context);
        engine.execute("alter table bit_frames add column late byte",context);
        engine.execute("insert into bit_frames values (600,63,16383,'1970-01-02',7)",context);
        // The first several frames select no rows. New scalar state must remain
        // empty until a selected row, while column tops exercise Java/native mixing.
        String sql="select bit_and(b),bit_and(h),bit_and(late),bit_or(late),bit_xor(late) from bit_frames where k>490";
        try (var candidate=compile(sql)) {
            check(sql,candidate); check(sql,candidate); assertTrue(nativeRows(candidate)>0);
            engine.execute("truncate table bit_frames",context); check(sql,candidate);
            engine.execute("insert into bit_frames values (700,31,8191,'1970-01-03',3)",context); check(sql,candidate);
        }
        engine.execute("alter table bit_frames convert partition to parquet list '1970-01-03'",context);
        for (String predicate : List.of("k>0", "k<0")) {
            sql="select bit_and(b),bit_and(h) from bit_frames where "+predicate;
            try (var candidate=compile(sql)) { check(sql,candidate); }
        }
    }

    @Test public void booleanValuesComposeAcrossAggregateConsumers() throws Exception {
        engine.execute("create table boolean_values as (select cast(x%17 as int) k,cast(x%7 as symbol) s,"
                + "x%3=0 b,x%5=0 c,case when x%13=0 then null else (x%31-15)*0.25 end d "
                + "from long_sequence(2003))", context);
        for (String expression : List.of("b", "not b", "b and c", "b or c", "b=c", "b!=c", "k>8", "d>1.25", "s='1'",
                "case b when true then c else not c end", "case b when false then c end",
                "case when k>8 then b else c end", "case s when '1' then b when '2' then c end")) {
            String aggregates = "count(),bool_and(" + expression + "),bool_or(" + expression + "),"
                    + "round(avg(" + expression + "),6),min(" + expression + "),max(" + expression + ")";
            for (String key : List.of("", "k", "s", "k,s")) {
                String sql = "select " + (key.isEmpty() ? "" : key + ",") + aggregates + " from boolean_values"
                        + (key.isEmpty() ? " where k>=0" : " group by " + key);
                try (var candidate = compile(sql)) {
                    check(sql, candidate); check(sql, candidate);
                    assertTrue(sql, nativeRows(candidate)>0);
                }
            }
        }
        String labels = "select case b when false then 'no'::varchar when true then 'yes'::varchar end label,"
                + "count(),bool_and(b) from boolean_values group by label";
        try (var candidate=compile(labels)) { check(labels,candidate); assertTrue(nativeRows(candidate)>0); }
        String nullEquality = "select count(),bool_and(b=null),bool_or(b=null),avg(b=null) from boolean_values where k>=0";
        try (var candidate=compile(nullEquality)) { check(nullEquality,candidate); }
        for (String predicate : List.of("k<0", "b", "not b", "b and c", "b or c")) {
            String sql = "select bool_and(b),bool_or(b),min(b),max(b),avg(b) from boolean_values where " + predicate;
            try (var candidate = compile(sql)) { check(sql, candidate); check(sql, candidate); }
        }
    }

    @Test public void booleanKeysCastsTopsAndProjectionPreserveFalse() throws Exception {
        engine.execute("create table bool_keys as (select cast(x%17 as int) k,cast(x%7 as symbol) s,"
                + "cast('v'||x%11 as varchar) v,x%3=0 b,timestamp_sequence(0,1000000) ts "
                + "from long_sequence(1003)) timestamp(ts) partition by day", context);
        engine.execute("alter table bool_keys add column late boolean", context);
        engine.execute("insert into bool_keys values (1,'present','present',true,'1970-01-02',true)",context);
        for (String key : List.of("b", "not b", "b,k", "b,s", "b,v", "case when k>8 then b end")) {
            String sql = "select " + key + ",count(),bool_and(late),bool_or(b),avg(b) from bool_keys group by " + key;
            try (var candidate = compile(sql)) { check(sql, candidate); check(sql, candidate); assertTrue(sql,nativeRows(candidate)>0); }
        }
        for (String type : List.of("byte", "short", "int", "long", "float", "double", "date", "timestamp", "timestamp_ns")) {
            String sql = "select k,count(cast(b as " + type + ")),min(cast(b as " + type + ")),"
                    + "max(cast(case when k>8 then b end as " + type + ")) from bool_keys group by k";
            try (var candidate = compile(sql)) { check(sql, candidate); assertTrue(sql,nativeRows(candidate)>0); }
        }
        String sql = "select b,late,count(),bool_and(b),bool_or(late),avg(late) from bool_keys group by b,late";
        try (var candidate = compile(sql)) {
            check(sql,candidate);
            engine.execute("insert into bool_keys values (1,'new','new',true,'1970-01-02',true)",context);
            check(sql,candidate);
            engine.execute("alter table bool_keys convert partition to parquet list '1970-01-01'",context);
            check(sql,candidate);
            engine.execute("truncate table bool_keys",context); check(sql,candidate);
            engine.execute("insert into bool_keys values (1,'fresh','fresh',false,'1970-01-03',true)",context);
            check(sql,candidate);
        }
        sql = "select not b,b or late,case when k>0 then b end,cast(late as long) from bool_keys";
        try (var candidate = compile(sql)) {
            check(sql,candidate); check(sql,candidate);
            var factory=candidate;
            while (factory!=null && !(factory instanceof io.questdb.griffin.engine.table.VirtualRecordCursorFactory)) factory=factory.getBaseFactory();
            assertNotNull(factory);
            assertTrue(((io.questdb.griffin.engine.table.VirtualRecordCursorFactory)factory).getProjectedRows()>0);
        }
    }

    @Test public void ipv4ComputedKeysKeepTheirOwnNullSentinel() throws Exception {
        engine.execute("create table ip_edges (k int,ip ipv4,label varchar)",context);
        engine.execute("insert into ip_edges values (1,null,'same'),(1,'0.0.0.1','same'),"
                + "(1,'128.0.0.1','same'),(1,'255.255.255.255','same'),(2,'127.255.255.255',null)",context);
        for (String suffix : List.of("k", "label")) {
            String sql="select ip-1 address," + suffix + ",count() n from ip_edges group by address," + suffix;
            try (var candidate=compile(sql)) { check(sql,candidate); assertTrue(nativeRows(candidate)>0); }
        }
    }

    @Test public void ipv4AggregatesUseUnsignedOrderAndPreserveCastNulls() throws Exception {
        engine.execute("create table ips as (select cast(x%17 as int) k,cast(x%7 as symbol) s,"
                + "cast('v'||x%11 as varchar) label,cast(case when x%13=0 then null "
                + "when x%5=0 then -2147483647 when x%5=1 then 2147483647 when x%5=2 then -1 "
                + "else x%257 end as int)::ipv4 ip from long_sequence(2003))",context);
        engine.execute("insert into ips values (1,'edge','edge','128.0.0.0'),(1,'edge','edge',null)",context);
        for (String argument : List.of("ip", "ip-1", "case when k>8 then ip end", "case s when '1' then ip else null::ipv4 end")) {
            String aggregates="count("+argument+"),min("+argument+"),max("+argument+"),count(cast("+argument+" as int))";
            for (String key : List.of("", "k", "s", "k,s", "label")) {
                String sql="select "+(key.isEmpty()?"":key+",")+aggregates+" from ips"
                        +(key.isEmpty()?" where k>=0":" group by "+key);
                try (var candidate=compile(sql)) { check(sql,candidate); check(sql,candidate); assertTrue(sql,nativeRows(candidate)>0); }
            }
        }
        for (String key : List.of("ip-1", "cast(cast(ip as int) as ipv4)", "case when k>8 then ip end")) {
            for (String suffix : List.of("",",k",",label")) {
                String sql="select "+key+" address"+suffix+",count(),min(ip),max(ip) from ips group by address"+suffix;
                try (var candidate=compile(sql)) { check(sql,candidate); assertTrue(sql,nativeRows(candidate)>0); }
            }
        }
        String sql="select count(ip),min(ip),max(ip) from ips where k<0";
        try (var candidate=compile(sql)) { check(sql,candidate); check(sql,candidate); }
    }

    @Test public void ipv4DistinctRetainsZeroEmptySlotsAndFullAddressDomain() throws Exception {
        engine.execute("create table ip_distinct as (select cast(x%7 as int) k,cast(x%3 as symbol) s,"
                + "cast('v'||x%5 as varchar) label,cast(case when x%13=0 then null else x%4096-2048 end as int)::ipv4 ip "
                + "from long_sequence(12003))",context);
        engine.execute("insert into ip_distinct values (1,'edge','edge','128.0.0.0'),(1,'edge','edge','255.255.255.255')",context);
        for (String key : List.of("k", "s", "k,s", "label")) {
            String sql="select "+key+",count_distinct(ip),count_distinct(ip-1),"
                    +"count_distinct(case s when '1' then ip end),min(ip),max(ip) from ip_distinct group by "+key;
            try (var candidate=compile(sql)) { check(sql,candidate); check(sql,candidate); assertTrue(sql,nativeRows(candidate)>0); }
        }
        String sql="select k,count_distinct(ip) from ip_distinct group by k";
        try (var candidate=compile(sql)) {
            check(sql,candidate); engine.execute("truncate table ip_distinct",context); check(sql,candidate);
            engine.execute("insert into ip_distinct values (1,'edge','edge','128.0.0.0'),(1,'edge','edge',null),"
                    +"(1,'edge','edge','255.255.255.255'),(2,'edge','edge',null)",context); check(sql,candidate);
        }
    }

    @Test public void neumaierSumsComposeAcrossNumericTypesAndFinalizedConsumers() throws Exception {
        engine.execute("create table neumaier as (select cast(x%17 as int) k,cast(x%7 as symbol) s,"
                + "cast(x%251-125 as byte) b,cast(x%503-251 as short) h,"
                + "cast(case when x%13=0 then null else x%97-48 end as int) i,"
                + "case when x%13=0 then null else x%97-48 end l,"
                + "cast(case when x%13=0 then null else (x%97-48)*0.25 end as float) f,"
                + "case when x%13=0 then null else (x%97-48)*0.25 end d,"
                + "timestamp_sequence('2024-01-01',1000000) ts from long_sequence(5003)) "
                + "timestamp(ts) partition by day", context);
        for (String column : List.of("b", "h", "i", "l", "f", "d")) {
            String aggregates = "count(),nsum(" + column + "),nsum(case s when '1' then " + column
                    + " when '2' then -" + column + " end),sum(l),min(d),max(d),count_distinct(l)";
            for (String key : List.of("", "k", "s", "k,s")) {
                String sql = "select " + (key.isEmpty() ? "" : key + ",") + aggregates + " from neumaier"
                        + (key.isEmpty() ? " where k>=0" : " group by " + key);
                try (var candidate = compile(sql)) {
                    check(sql, candidate); check(sql, candidate);
                    assertTrue(sql, nativeRows(candidate)>0);
                }
            }
        }
        for (String suffix : List.of(" order by n", " order by n desc limit 3")) {
            String sql = "select k,nsum(d*1.0) n from neumaier group by k" + suffix;
            try (var candidate = compile(sql)) {
                checkRanked(sql, candidate);
                assertTrue(sql, nativeRankedRows(candidate, !suffix.contains("limit"))>0);
            }
        }
        String nested = "select count(n),sum(n),min(n),max(n) from "
                + "(select k,nsum(d*1.0) n from neumaier group by k)";
        try (var candidate = compile(nested)) { check(nested,candidate); check(nested,candidate); }
        engine.execute("alter table neumaier add column late double", context);
        String sql = "select k,nsum(d*1.0),nsum(late*1.0) from neumaier group by k";
        try (var candidate = compile(sql)) {
            check(sql,candidate);
            engine.execute("insert into neumaier values (1,'1',1,1,1,1,0.25,0.25,'2024-01-01T02:00',1.25)",context);
            check(sql,candidate);
            engine.execute("alter table neumaier convert partition to parquet list '2024-01-01'",context);
            check(sql,candidate); assertTrue(nativeRows(candidate)>0);
        }
        engine.execute("truncate table neumaier",context);
        try (var candidate = compile("select nsum(d),nsum(late) from neumaier")) {
            check("select nsum(d),nsum(late) from neumaier",candidate);
        }
    }

    @Test public void statisticalMomentsComposeAcrossNumericTypes() throws Exception {
        engine.execute("create table moments as (select cast(x%17 as int) k,cast(x%7 as symbol) s,"
                + "cast(x%251-125 as byte) b,cast(x%65521-32760 as short) h,"
                + "cast(case when x%13=0 then null else x%97-48 end as int) i,"
                + "case when x%13=0 then null else x%97-48 end l,"
                + "cast(case when x%13=0 then null else (x%97-48)*0.25 end as float) f,"
                + "case when x%13=0 then null else (x%97-48)*0.25 end d "
                + "from long_sequence(5003))", context);
        // Parallel update/merge order can change the last bits of moments. Ordered state
        // checks compare every bit; SQL composition checks compare six decimal places.
        for (String column : List.of("b", "h", "i", "l", "f", "d")) {
            String moments = "count(" + column + "),round(var_pop(" + column + "),6),"
                    + "round(var_samp(" + column + "),6),round(stddev_pop(" + column + "),6),"
                    + "round(stddev_samp(" + column + "),6),min(" + column + "),max(" + column + ")";
            for (String key : List.of("", "k", "s", "k,s")) {
                String sql = "select " + (key.isEmpty() ? "" : key + ",") + moments + " from moments"
                        + (key.isEmpty() ? " where k>=0" : " group by " + key);
                try (var candidate = compile(sql)) {
                    check(sql, candidate); check(sql, candidate);
                    assertTrue(sql, nativeRows(candidate) > 0);
                }
            }
        }
        String sql = "select k,round(variance(case s when '1' then f when '2' then -f end),6),"
                + "round(stddev(case when s!='3' then d+0.25 end),6) from moments group by k";
        try (var candidate = compile(sql)) { check(sql, candidate); assertTrue(nativeRows(candidate)>0); }
        sql = "select var_pop(d),var_samp(d),stddev(d),stddev_pop(d) from moments where k<0";
        try (var candidate = compile(sql)) { check(sql, candidate); check(sql, candidate); }
    }

    @Test public void covarianceComposesAcrossNumericPairsAndNulls() throws Exception {
        engine.execute("create table pairs as (select cast(x%17 as int) k,cast(x%7 as symbol) s,"
                + "cast(x%251-125 as byte) b,cast(x%503-251 as short) h,"
                + "cast(case when x%13=0 then null else x%97-48 end as int) i,"
                + "case when x%19=0 then null else x%73-36 end l,"
                + "cast(case when x%23=0 then null else (x%59-29)*0.25 end as float) f,"
                + "case when x%31=0 then null else (x%41-20)*0.25 end d "
                + "from long_sequence(5003))", context);
        String[] columns = {"b", "h", "i", "l", "f", "d"};
        for (int c = 0; c < columns.length; c++) {
            String y = columns[c], x = columns[(c+1)%columns.length];
            String aggregates = "count(),covar_pop(" + y + "," + x + "),"
                    + "covar_samp(" + x + "," + y + "),"
                    + "covar_pop(case s when '1' then " + y + " end," + x + ")";
            for (String key : List.of("", "k", "s", "k,s")) {
                String sql = "select " + (key.isEmpty() ? "" : key + ",") + aggregates + " from pairs"
                        + (key.isEmpty() ? " where k>=0" : " group by " + key);
                try (var candidate = compile(sql)) {
                    checkApproximateTail(sql, candidate, 3); checkApproximateTail(sql, candidate, 3);
                    assertTrue(sql, nativeRows(candidate)>0);
                }
            }
        }
        String sql = "select covar_pop(d,f),covar_samp(f,d) from pairs where k<0";
        try (var candidate = compile(sql)) { check(sql, candidate); }
    }

    @Test public void ordinaryKeyProbingPreservesFirstAggregateUpdates() throws Exception {
        engine.execute("create table batch_first as (select cast(x%17 as string) text_key,"
                + "cast(x%13 as decimal(18,0)) decimal_key,cast(x%7+32 as short) h,"
                + "case when x%11=0 then null else (x%31-15)*0.25 end d,"
                + "case when x%13=0 then null else x%19-9 end l from long_sequence(5003))",context);
        // STRING/DECIMAL keys keep ordinary probing, but aggregate batches are generated.
        // Empty map values are not necessarily the state produced by computeFirst.
        String aggregates="count(),bit_and(h),nsum(d),weighted_avg(d,l),var_pop(d),stddev_samp(d),"
                + "covar_pop(d,l),corr(d,l),regr_slope(d,l),regr_intercept(d,l),regr_r2(d,l)";
        for(String key:List.of("text_key","decimal_key","text_key,decimal_key")) {
            String sql="select "+aggregates+" from batch_first group by "+key;
            try(var candidate=compile(sql)) {
                checkApproximateTail(sql,candidate,9);checkApproximateTail(sql,candidate,9);
                assertTrue(sql,nativeRows(candidate)>0);
            }
        }
    }

    @Test public void regressionFamilyComposesAcrossNumericPairsAndNulls() throws Exception {
        engine.execute("create table regression as (select cast(x%17 as int) k,cast(x%7 as symbol) s,"
                + "cast(x%251-125 as byte) b,cast(x%503-251 as short) h,"
                + "cast(case when x%13=0 then null else x%97-48 end as int) i,"
                + "case when x%19=0 then null else x%73-36 end l,"
                + "cast(case when x%23=0 then null else (x%59-29)*0.25 end as float) f,"
                + "case when x%31=0 then null else (x%41-20)*0.25 end d,"
                + "timestamp_sequence('2024-01-01',1000000L) ts from long_sequence(5003)) timestamp(ts) partition by day", context);
        String[] columns = {"b", "h", "i", "l", "f", "d"};
        for (int c = 0; c < columns.length; c++) {
            String y = columns[c], x = columns[(c+1)%columns.length];
            String aggregates = "count(),corr("+y+","+x+"),regr_slope("+y+","+x+"),"
                    + "regr_intercept("+y+","+x+"),regr_r2("+y+","+x+"),"
                    + "corr(case s when '1' then "+y+" end,"+x+")";
            for (String key : List.of("", "k", "s", "k,s")) {
                String sql = "select " + (key.isEmpty() ? "" : key + ",") + aggregates + " from regression"
                        + (key.isEmpty() ? " where k>=0" : " group by " + key);
                try (var candidate = compile(sql)) {
                    checkApproximateTail(sql, candidate, 5); checkApproximateTail(sql, candidate, 5);
                    assertTrue(sql, nativeRows(candidate)>0);
                }
            }
        }
        String aggregates = "corr(d,f),regr_slope(d,f),regr_intercept(d,f),regr_r2(d,f)";
        try (var candidate = compile("select "+aggregates+" from regression where k<0")) {
            check("select "+aggregates+" from regression where k<0",candidate);
        }
        engine.execute("alter table regression add column late double",context);
        String sql="select k,corr(late,d),regr_slope(late,d),regr_intercept(late,d),regr_r2(late,d),corr(d,f) from regression group by k";
        try(var candidate=compile(sql)) {
            checkApproximateTail(sql,candidate,5);
            engine.execute("insert into regression (k,s,d,f,late,ts) values (1,'new',1,2,3,'2024-01-02'),(1,'new',2,4,6,'2024-01-02T00:00:01')",context);
            checkApproximateTail(sql,candidate,5);
            engine.execute("alter table regression convert partition to parquet list '2024-01-01'",context);
            checkApproximateTail(sql,candidate,5);checkApproximateTail(sql,candidate,5);
        }
    }

    @Test public void floatArithmeticAndCastsComposeWithOrdinaryAggregateOverloads() throws Exception {
        engine.execute("create table float_expr as (select cast(x%257 as int) k,cast(x%17 as symbol) s,"
                + "cast(x%7-3 as byte) b,cast(x%31-15 as short) h,cast(x%31-15 as int) i,x%31-15 l,"
                + "case when x%13=0 then null else (x%31-15)*0.25 end d,"
                + "cast(case when x%13=0 then null else (x%31-15)*0.25 end as float) f,"
                + "cast(x%8+1 as float) g from long_sequence(16421))", context);
        // Positive g avoids MIN's merge-order-dependent tie between +0.0 and -0.0.
        // Ordered state/IR tests compare those bits exactly; this parallel test compares complete results.
        String aggregates = "count(f+g),sum(f+g),sum(f-g),min(f*g),max(f/g),avg(f/g),count(f/(g-g)),"
                + "sum(cast(b as float)),sum(cast(h as float)),sum(cast(i as float)),sum(cast(l as float)),"
                + "min(cast(d as float)),max(cast(d as float)),"
                + "sum(case when s='2' then f+g else f-g end)";
        for (String key : List.of("", "k", "s", "k,s")) {
            String sql = "select " + (key.isEmpty() ? "" : key + ",") + aggregates + " from float_expr"
                    + (key.isEmpty() ? "" : " group by " + key);
            try (var candidate = compile(sql)) { check(sql, candidate); check(sql, candidate); assertTrue(sql, nativeRows(candidate) > 0); }
        }
        String sql = "select k,sum(f+g),avg(f/g) from float_expr where f/g>0.25 group by k";
        try (var candidate = compile(sql)) { check(sql, candidate); assertTrue(sql, nativeRows(candidate) > 0); }
    }

    @Test public void floatNarrowingPreservesRangeChecksNullsAndDirectIntegerRounding() throws Exception {
        engine.execute("create table narrow_float (k int,l long,d double)", context);
        double[] values = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, -0.0, 0,
                Float.MIN_VALUE / 2.0, Math.nextUp(Float.MIN_VALUE / 2.0), Float.MIN_NORMAL,
                Float.MAX_VALUE, Math.nextUp((double) Float.MAX_VALUE), -Float.MAX_VALUE,
                Math.nextDown(-(double) Float.MAX_VALUE), Double.MAX_VALUE, 1.0 + 0x1p-24, Math.nextUp(1.0 + 0x1p-24)};
        long[] integers = {Long.MIN_VALUE, Long.MAX_VALUE, 0, 1, -1,
                (1L << 53) + (1L << 29) + 1, -((1L << 53) + (1L << 29) + 1)};
        try (var writer = engine.getWriter(engine.verifyTableName("narrow_float"), "test")) {
            for (int i = 0; i < values.length; i++) {
                var row = writer.newRow(); row.putInt(0, i); row.putLong(1, integers[i % integers.length]);
                row.putDouble(2, values[i]); row.append();
            }
            writer.commit();
        }
        String sql = "select k,count(cast(d as float)),min(cast(d as float)),max(cast(l as float)),"
                + "max(cast(cast(l as double) as float)),min(cast(d+d as float)),"
                + "min(case when k>3 then cast(d as float) else null::float end) from narrow_float group by k";
        try (var candidate = compile(sql)) { check(sql, candidate); check(sql, candidate); assertTrue(nativeRows(candidate) > 0); }
    }

    @Test public void numericNegationComposesAcrossTypesAndAggregateInputs() throws Exception {
        engine.execute("create table negatives as (select cast(x%257 as int) k,cast(x%17 as symbol) s,"
                + "cast(x%256-128 as byte) b,cast(x%65536-32768 as short) h,"
                + "cast(case when x%13=0 then null else x%31-15 end as int) i,"
                + "case when x%13=0 then null else (x%31-15)*4294967297 end l,"
                + "cast(case when x%13=0 then null else (x%31-15)*0.25 end as float) f,"
                + "case when x%13=0 then null else (x%31-15)*0.25 end d from long_sequence(16421))", context);
        engine.execute("insert into negatives values (0,'edge',-128,-32768,null,null,-0.0,-0.0),"
                + "(1,'edge',127,32767,2147483647,9223372036854775807,0.0,0.0)", context);
        String aggregates = "sum(-b),sum(-h),sum(-i),min(-l),max(-l),sum(-f),min(-f),max(-f),avg(-d)";
        for (String key : List.of("", "k", "s", "k,s")) {
            String sql = "select " + (key.isEmpty() ? "" : key + ",") + aggregates + " from negatives"
                    + (key.isEmpty() ? "" : " group by " + key);
            try (var candidate = compile(sql)) { check(sql, candidate); assertTrue(sql, nativeRows(candidate) > 0); }
        }
        for (String sql : List.of("select -i g,count(),sum(-l) from negatives group by g",
                "select sum(-i),avg(-f) from negatives where -i>0 and -d<4.0",
                "select k,sum(case when s='2' then -l else l end) from negatives group by k")) {
            try (var candidate = compile(sql)) { check(sql, candidate); check(sql, candidate); assertTrue(sql, nativeRows(candidate) > 0); }
        }
    }

    @Test public void floatInputsReuseDoubleAggregatesAndPackedPartitions() throws Exception {
        configure(options.withLocalCapacity(64));
        context.changePageFrameSizes(64, 2048);
        engine.execute("create table floats as (select cast(x%257 as int) k,cast(x%257 as varchar) s,"
                + "cast(x%17 as int) i,cast(case when x%13=0 then null else (x%41-20)*0.25 end as float) f,"
                + "timestamp_sequence('2024-01-01',1000000) ts from long_sequence(16421)) "
                + "timestamp(ts) partition by day bypass wal", context);
        for (String sql : List.of("select s,sum(i),avg(f) from floats group by s",
                "select k,sum(cast(f as double)),avg(f) from floats group by k",
                "select k,avg(f) from floats where cast(f as double)>-1.0 group by k",
                "select avg(f) from floats where k>0")) {
            try (var candidate = compile(sql)) {
                check(sql, candidate); check(sql, candidate);
                assertTrue(sql, nativeRows(candidate) > 0);
                if (sql.startsWith("select s,")) {
                    long partitioned = 0;
                    for (var factory = candidate; factory != null; factory = factory.getBaseFactory()) {
                        if (factory instanceof AsyncGroupByRecordCursorFactory group)
                            for (int i = -1; i < 2; i++)
                                if (group.getAtom().getCompiledUpdater(i) instanceof AggregateSlot slot)
                                    partitioned += slot.inputPartitionRows();
                    }
                    assertTrue("FLOAT values must survive packed input capture and merge", partitioned > 0);
                }
            }
        }
        engine.execute("alter table floats add column late float", context);
        String sql = "select k,avg(f),avg(late) from floats group by k";
        try (var candidate = compile(sql)) {
            check(sql, candidate);
            engine.execute("insert into floats values (1,'1',1,0.5,'2024-01-02',1.25)", context);
            check(sql, candidate);
            assertTrue(nativeRows(candidate) > 0);
        }
        for (String keys : List.of("", "k", "s", "k,i")) {
            sql = "select " + (keys.isEmpty() ? "" : keys + ",")
                    + "count(f),sum(f),min(f),max(f),avg(f),"
                    + "sum(case when i<9 then f else null::float end),"
                    + "min(case when i<9 then f else null::float end) from floats"
                    + (keys.isEmpty() ? "" : " group by " + keys);
            try (var candidate = compile(sql)) {
                check(sql, candidate); check(sql, candidate);
                assertTrue(sql, nativeRows(candidate) > 0);
            }
        }
        engine.execute("alter table floats convert partition to parquet list '2024-01-01'", context);
        sql = "select s,sum(i),avg(f) from floats group by s";
        try (var candidate = compile(sql)) { check(sql, candidate); }
        engine.execute("truncate table floats", context);
        try (var candidate = compile("select avg(f) from floats where k>0")) { check("select avg(f) from floats where k>0", candidate); }
    }

    @Test public void floatScalarRegistersPreserveEveryUpdateRoundingAndNonfiniteRule() throws Exception {
        context.changePageFrameSizes(4096, 4096);
        engine.execute("create table rounding_float (k int,f float)", context);
        for (float[] pattern : new float[][]{{Float.NaN, -0.0f}, {-0.0f, Float.NaN},
                {0x1p24f, 1.0f, 1.0f, -0x1p24f},
                {Float.NaN, Float.POSITIVE_INFINITY, Float.NEGATIVE_INFINITY, 0.25f,
                        Float.MIN_VALUE, -Float.MIN_VALUE, Float.MAX_VALUE, -Float.MAX_VALUE}}) {
            engine.execute("truncate table rounding_float", context);
            try (var writer = engine.getWriter(engine.verifyTableName("rounding_float"), "test")) {
                for (int i = 0; i < 2051; i++) {
                    var row = writer.newRow(); row.putInt(0, 1); row.putFloat(1, pattern[i % pattern.length]); row.append();
                }
                writer.commit();
            }
            for (String key : List.of("", "k")) {
                String argument = "case when k=1 then f else null::float end";
                String sql = "select " + (key.isEmpty() ? "" : "k,") + "sum(" + argument + "),count(" + argument
                        + "),min(" + argument + "),max(" + argument + ") from rounding_float"
                        + (key.isEmpty() ? "" : " group by k");
                try (var candidate = compile(sql)) {
                    check(sql, candidate); check(sql, candidate);
                    assertTrue(sql, nativeRows(candidate) > 0);
                }
            }
        }
    }

    @Test public void symbolConditionsShareAggregateStatesAndRefreshOnReopen() throws Exception {
        engine.execute("create table symbol_cases as (select cast(x%7 as int) k,cast(x%7 as varchar) v,"
                + "cast(case when x%5=0 then null when x%2=0 then 'alpha' else 'beta' end as symbol) s,"
                + "cast(case when x%7=0 then null else x%31 end as int) i,"
                + "case when x%7=0 then null else x*10000000000 end l,"
                + "case when x%7=0 then null else (x%29)*0.25 end d "
                + "from long_sequence(1003))", context);
        String aggregates = "count(case when s='alpha' then i end),"
                + "sum(case s when 'alpha' then l when 'new' then 0-l else 0 end),"
                + "min(case when s in ('beta','new',null) then i end),"
                + "max(case when s!='alpha' then l end),"
                + "avg(case when s='new' then d when s='alpha' then d else null end)";
        for (String key : List.of("", "k", "v", "s")) {
            String literal = "new" + key;
            String sql = "select " + (key.isEmpty() ? "" : key + ",") + aggregates.replace("'new'", "'" + literal + "'")
                    + " from symbol_cases" + (key.isEmpty() ? "" : " group by " + key);
            try (var candidate = compile(sql)) {
                check(sql, candidate);
                assertTrue(sql, nativeRows(candidate) > 0);
                engine.execute("insert into symbol_cases values (1,'1','" + literal + "',17,10000000001,3.25)", context);
                check(sql, candidate);
                assertTrue(sql, nativeRows(candidate) > 0);
            }
        }
    }

    @Test public void symbolConditionsSurviveCapturedInputsAndDictionaryReset() throws Exception {
        configure(options.withLocalCapacity(64));
        context.changePageFrameSizes(64, 2048);
        engine.execute("create table captured_cases as (select cast(x%257 as int) k,cast(x%257 as varchar) v,"
                + "cast(case when x%7=0 then null else cast(x%23 as string) end as symbol) s,"
                + "x n,timestamp_sequence('2024-01-01',1000000) ts from long_sequence(16421)) "
                + "timestamp(ts) partition by day bypass wal", context);
        for (String key : List.of("k", "v", "k,v")) {
            String sql = "select " + key + ",count(case when s='new' then n end),"
                    + "sum(case s when '1' then n when 'new' then n+1 else null end),"
                    + "min(case when s in ('1',null) then n end),max(case when s!='1' then n end),"
                    + "avg(case when s='1' then cast(n as double) end) from captured_cases where k>0 group by " + key;
            try (var candidate = compile(sql)) {
                check(sql, candidate);
                long captured = 0;
                for (var factory = candidate; factory != null; factory = factory.getBaseFactory()) {
                    if (factory instanceof AsyncGroupByRecordCursorFactory group)
                        for (int i = -1; i < 2; i++)
                            if (group.getAtom().getCompiledUpdater(i) instanceof AggregateSlot slot)
                                captured += slot.inputPartitionRows();
                }
                assertTrue("SYMBOL conditions must survive captured input replay: " + key, captured > 0);
                check(sql, candidate);
            }
            String distinctSql = "select " + key + ",count_distinct(case when s='1' then n end),"
                    + "count(case when s='1' then v end),min(case when s in ('1',null) then v end),"
                    + "max(case when s='new' then v end),min(case k when 1 then v when 2 then 'λ'::varchar end),"
                    + "max(case n when 1 then v when 2 then '雪'::varchar end) "
                    + "from captured_cases group by " + key;
            try (var candidate = compile(distinctSql)) {
                check(distinctSql, candidate);
                assertTrue(distinctSql, nativeRows(candidate) > 0);
            }
        }
        String sql = "select k,count(case when s='new' then n end),sum(case when s='1' then n end) "
                + "from captured_cases group by k";
        try (var candidate = compile(sql)) {
            check(sql, candidate);
            engine.execute("truncate table captured_cases", context);
            check(sql, candidate);
            engine.execute("insert into captured_cases values (1,'1','new',5,'2024-01-02'),"
                    + "(2,'2','1',9,'2024-01-02'),(3,'3',null,null,'2024-01-02')", context);
            check(sql, candidate);
            assertTrue(nativeRows(candidate) > 0);
            engine.execute("alter table captured_cases convert partition to parquet list '2024-01-02'", context);
            check(sql, candidate);
        }
    }

    @Test public void temporalAggregatesKeepUnitsNullsAndOrdinaryTimestampShortcuts() throws Exception {
        engine.execute("create table times as (select cast(x%257 as int) k,cast(x%257 as varchar) v,"
                + "cast(case when x%7=0 then null else x%1001-500 end as date) d,"
                + "cast(case when x%7=0 then null else (x%1001-500)*1001 end as timestamp) u,"
                + "cast(case when x%7=0 then null else (x%1001-500)*1000001 end as timestamp_ns) n,"
                + "timestamp_sequence('2024-01-01',1000000) ts from long_sequence(16421)) "
                + "timestamp(ts) partition by day bypass wal", context);
        for (String column : List.of("d", "u", "n")) {
            String values = "count(" + column + "),min(" + column + "),max(" + column + "),"
                    + "min(case when k>3 then " + column + " end),max(case k when 1 then " + column + " end)";
            for (String key : List.of("", "k", "v", "k,v")) {
                String sql = "select " + (key.isEmpty() ? "" : key + ",") + values
                        + " from times where k>0" + (key.isEmpty() ? "" : " group by " + key);
                try (var candidate = compile(sql)) {
                    check(sql, candidate); check(sql, candidate);
                    assertTrue(sql, nativeRows(candidate) > 0);
                }
            }
        }
        String sql = "select min(ts),max(ts),count() from times";
        try (var candidate = compile(sql)) {
            check(sql, candidate);
            assertEquals("Keep ordinary designated-timestamp frame extrema", 0, nativeRows(candidate));
        }
        engine.execute("alter table times add column late timestamp_ns", context);
        sql = "select k,count(late),min(late),max(case when k>0 then late end),min(d),max(u) from times group by k";
        try (var candidate = compile(sql)) {
            check(sql, candidate);
            engine.execute("insert into times values (1,'1',-3,-1001,-1000001,'2024-01-02',-5)", context);
            check(sql, candidate);
            assertTrue(nativeRows(candidate) > 0);
            engine.execute("alter table times convert partition to parquet list '2024-01-01'", context);
            check(sql, candidate);
            engine.execute("truncate table times", context);
            check(sql, candidate);
        }
    }

    @Test public void temporalNumericGettersKeepRawUnitsAndNulls() throws Exception {
        engine.execute("create table temporal_numeric as (select cast(x%17 as int) k,cast(x%7 as symbol) s,"
                + "cast(case when x%13=0 then null else x%97-48 end as date) d,"
                + "cast(case when x%13=0 then null else (x%97-48)*1000 end as timestamp) u,"
                + "cast(case when x%13=0 then null else (x%97-48)*1000000 end as timestamp_ns) n "
                + "from long_sequence(5003))", context);
        for (String column : List.of("d", "u", "n")) {
            String values = "count(" + column + "::float),sum(" + column + "::double),"
                    + "min(" + column + "::double),max(" + column + "::float),avg(" + column + "::double),"
                    + "sum(" + column + "),avg(" + column + "),"
                    + "sum((case s when '1' then " + column + " end)::double)";
            for (String key : List.of("", "k", "s", "k,s")) {
                String sql = "select " + (key.isEmpty() ? "" : key + ",") + values + " from temporal_numeric"
                        + (key.isEmpty() ? " where k>=0" : " group by " + key);
                try (var candidate = compile(sql)) {
                    check(sql, candidate); check(sql, candidate);
                    assertTrue(sql, nativeRows(candidate) > 0);
                }
                sql = "select " + (key.isEmpty() ? "" : key + ",") + "count(),var_pop(" + column
                        + "),stddev(" + column + "),covar_pop(" + column + "," + column + ") from temporal_numeric"
                        + (key.isEmpty() ? " where k>=0" : " group by " + key);
                try (var candidate = compile(sql)) {
                    checkApproximateTail(sql, candidate, 3); assertTrue(sql, nativeRows(candidate)>0);
                }
            }
        }
    }

    @Test public void temporalGettersPreserveTruncationWrappingAndCheckedCastFallback() throws Exception {
        engine.execute("create table temporal_casts as (select cast(x%3 as int) k,"
                + "cast(case when x%3=2 then null when x%3=0 then 9223372036854775807 else -1001*x end as date) d,"
                + "cast(case when x%3=2 then null when x%3=0 then 9223372036854775807 else -1001*x end as timestamp) u,"
                + "cast(case when x%3=2 then null when x%3=0 then 9223372036854775807 else -1000001*x end as timestamp_ns) n "
                + "from long_sequence(259))", context);
        String values = "count(d::long),min(u::long),max(n::long),min(d::timestamp),max(d::timestamp_ns),"
                + "min(n::timestamp),max(u::date),min(n::date),min(case when k=0 then d::timestamp else u end)";
        for (String key : List.of("", "k")) {
            String sql = "select " + (key.isEmpty() ? "" : key + ",") + values + " from temporal_casts"
                    + (key.isEmpty() ? "" : " group by " + key);
            try (var candidate = compile(sql)) {
                check(sql, candidate);
                assertTrue(sql, nativeRows(candidate) > 0);
            }
        }
        // The out-of-range microseconds are in an unchosen branch. Ordinary CASE is lazy.
        String sql = "select min(case when k=0 then n else u::timestamp_ns end) from temporal_casts";
        try (var candidate = compile(sql)) {
            check(sql, candidate);
            assertEquals(0, nativeRows(candidate));
        }
        sql = "select min(u::timestamp_ns) from temporal_casts";
        context.setQueryCodeGenerator(null);
        Class<? extends RuntimeException> failure;
        try (var ordinary = Demo.compileSql(engine, context, sql, true)) {
            failure = assertThrows(RuntimeException.class, () -> Demo.readSql(ordinary, context, 0)).getClass();
        }
        try (var candidate = compile(sql)) {
            assertThrows(failure, () -> Demo.readSql(candidate, context, 0));
            assertEquals(0, nativeRows(candidate));
        }
    }

    @Test public void symbolCountSharesOrdinaryStateAcrossMapsAndScalarMerge() throws Exception {
        engine.execute("create table counts as (select cast(x%7 as int) k,x id,"
                + "cast(case when x%7=0 then null else cast(x%17 as string) end as symbol) s,"
                + "timestamp_sequence('2024-01-01',1000000) ts from long_sequence(1003)) timestamp(ts) partition by day bypass wal", context);
        for (String sql : List.of("select k,count(s),count() from counts group by k",
                "select s,count(s) from counts group by s", "select count(s) from counts", "select count(s),count() from counts",
                "select k,count(s) from counts where s in ('1','2',null) group by k")) {
            try (var candidate = compile(sql)) {
                check(sql, candidate);
                assertTrue(sql, nativeRows(candidate) > 0);
                check(sql, candidate);
            }
        }
        engine.execute("alter table counts add column late symbol", context);
        String sql = "select k,count(s),count(late) from counts group by k";
        try (var candidate = compile(sql)) {
            check(sql, candidate);
            engine.execute("insert into counts values (1,1004,'new','2024-01-01T01:00:00','present')", context);
            check(sql, candidate);
            assertTrue(nativeRows(candidate) > 0);
        }
        engine.execute("alter table counts convert partition to parquet list '2024-01-01'", context);
        try (var candidate = compile(sql)) { check(sql, candidate); }
        engine.execute("truncate table counts", context);
        try (var candidate = compile("select count(s) from counts")) { check("select count(s) from counts", candidate); }
    }

    @Test public void dynamicSymbolCountKeepsOrdinaryNullAndKeyAssignmentSemantics() throws Exception {
        engine.execute("create table dynamic_counts as (select x id,cast(x%7 as int) k,"
                + "cast(case when x%3=0 then null else x%11 end as long) v from long_sequence(259))", context);
        String sql = "select k,count(cast(v as symbol)) from dynamic_counts group by k";
        try (var candidate = compile(sql)) {
            check(sql, candidate);
            assertEquals(0, nativeRows(candidate));
        }
    }

    @Test public void intAndSymbolDistinctPreserveNullsGrowthMergeAndDictionaryReopen() throws Exception {
        engine.execute("create table distinct_types (k int,id long,i int,s symbol capacity 1024,ts timestamp) "
                + "timestamp(ts) partition by day bypass wal", context);
        engine.execute("insert into distinct_types select cast(x%7 as int) k,x id,"
                + "cast(case when x%7=0 then null else x%257-128 end as int) i,"
                + "cast(case when x%7=0 then null else cast(x%257 as string) end as symbol) s,"
                + "timestamp_sequence('2024-01-01',1000000) ts from long_sequence(4003)", context);
        for (String sql : List.of("select k,count_distinct(i),count_distinct(s),count_distinct(id) from distinct_types group by k",
                "select s,count_distinct(i),count_distinct(s),count() from distinct_types group by s",
                "select k,count_distinct(i),count_distinct(s) from distinct_types where s in ('1','2',null) group by k")) {
            try (var candidate = compile(sql)) {
                check(sql, candidate);
                assertTrue(sql, nativeRows(candidate) > 0);
                check(sql, candidate);
            }
        }
        for (String argument : List.of("i", "s")) {
            String sql = "select k,count_distinct(" + argument + ") n from distinct_types group by k order by n desc limit 7";
            try (var candidate = compile(sql)) {
                check(sql, candidate);
                long collected = 0;
                for (var factory = candidate; factory != null; factory = factory.getBaseFactory()) {
                    if (factory instanceof AsyncGroupByRecordCursorFactory group) {
                        for (int i = -1; i < 2; i++) {
                            if (group.getAtom().getCompiledUpdater(i) instanceof AggregateSlot slot) collected += slot.topKCounters()[0];
                        }
                    }
                }
                assertTrue("Generated ranking reads finalized INT/SYMBOL counts", collected > 0);
            }
        }
        engine.execute("alter table distinct_types add column late symbol", context);
        String sql = "select k,count_distinct(i),count_distinct(s),count_distinct(late) from distinct_types group by k";
        try (var candidate = compile(sql)) {
            check(sql, candidate);
            engine.execute("insert into distinct_types values (1,4004,2147483647,'new','2024-01-02','present')", context);
            engine.execute("insert into distinct_types values (1,4005,-2147483647,'other','2024-01-02T00:00:01',null)", context);
            check(sql, candidate);
            assertTrue(nativeRows(candidate) > 0);
        }
        engine.execute("alter table distinct_types convert partition to parquet list '2024-01-01'", context);
        try (var candidate = compile(sql)) { check(sql, candidate); }
        String dynamic = "select k,count_distinct(cast(i as symbol)) from distinct_types group by k";
        try (var candidate = compile(dynamic)) { check(dynamic, candidate); assertEquals(0, nativeRows(candidate)); }
        engine.execute("truncate table distinct_types", context);
        try (var candidate = compile(sql)) { check(sql, candidate); }
    }

    @Test public void integerAveragesUseFloatingSumsAndOrdinaryScalarResults() throws Exception {
        engine.execute("create table averages as (select cast(x%7 as short) g,cast(x%17 as int) k,"
                + "cast(case when x%7=0 or x<31 then null else x%257-128 end as int) i,"
                + "case when x%7=0 or x%13=0 then null else x*10000000 end l,cast(x%5 as short) s,"
                + "timestamp_sequence('2024-01-01',1000000) ts from long_sequence(2003)) timestamp(ts) partition by day bypass wal", context);
        for (String sql : List.of("select g,avg(i),avg(l),avg(s) from averages group by g",
                "select g,avg(i+1),avg(l-1),count(i),sum(l) from averages where k>=0 group by g",
                "select avg(i),avg(l),avg(s) from averages where k>=0",
                "select avg(i+1),avg(l-1) from averages where k>0")) {
            try (var candidate = compile(sql)) {
                check(sql, candidate);
                assertTrue(sql, nativeRows(candidate) > 0);
                check(sql, candidate);
            }
        }
        engine.execute("alter table averages add column late int", context);
        String sql = "select g,avg(i),avg(l),avg(late) from averages group by g";
        try (var candidate = compile(sql)) {
            check(sql, candidate);
            engine.execute("insert into averages values (1,1,17,10000000000,1,'2024-01-02',42)", context);
            check(sql, candidate);
        }
        engine.execute("alter table averages convert partition to parquet list '2024-01-01'", context);
        try (var candidate = compile(sql)) { check(sql, candidate); }
        engine.execute("truncate table averages", context);
        sql = "select avg(i),avg(l),avg(s) from averages where k>=0";
        try (var candidate = compile(sql)) { check(sql, candidate); }
    }

    @Test public void doubleAggregatesPreserveNullableStateAcrossFramesAndMerges() throws Exception {
        engine.execute("create table doubles as (select cast(x%7 as short) g,cast(x%17 as int) k,"
                + "case when x%7=0 or x<31 then null else (x%257-128)*0.25 end d,"
                + "case when x%13=0 then null else (x%31-15)*1024.0 end e,"
                + "timestamp_sequence('2024-01-01',1000000) ts from long_sequence(2003)) timestamp(ts) partition by day bypass wal", context);
        for (String sql : List.of("select g,count(d),sum(d),avg(d),min(d),max(d),avg(e) from doubles group by g",
                "select k,sum(d),avg(e),min(d),max(d) from doubles where k>=0 group by k",
                "select count(d),sum(d),avg(d),min(d),max(d) from doubles where k>=0")) {
            try (var candidate = compile(sql)) {
                check(sql, candidate);
                assertTrue(sql, nativeRows(candidate) > 0);
                check(sql, candidate);
            }
        }
        engine.execute("alter table doubles add column late double", context);
        String sql = "select g,count(late),sum(d),avg(late),min(late),max(late) from doubles group by g";
        try (var candidate = compile(sql)) {
            check(sql, candidate);
            engine.execute("insert into doubles values (1,1,17.25,1024,'2024-01-02',42.5)", context);
            check(sql, candidate);
        }
        engine.execute("alter table doubles convert partition to parquet list '2024-01-01'", context);
        try (var candidate = compile(sql)) { check(sql, candidate); }
        engine.execute("truncate table doubles", context);
        sql = "select count(d),sum(d),avg(d),min(d),max(d) from doubles where k>=0";
        try (var candidate = compile(sql)) { check(sql, candidate); }
    }

    @Test public void doubleExpressionsFuseWithFilteredGroupedAndScalarConsumers() throws Exception {
        engine.execute("create table expressions as (select cast(x%7 as short) g,cast(x%17 as int) k,"
                + "case when x%7=0 or x<31 then null else (x%257-128)*0.25 end d,"
                + "cast(case when x%13=0 then null else x%101-50 end as int) i,"
                + "timestamp_sequence('2024-01-01',1000000) ts from long_sequence(2003)) timestamp(ts) partition by day bypass wal", context);
        for (String sql : List.of("select g,sum(d*1.5+0.25),avg(d) from expressions where d>1.0 and d<30.0 group by g",
                "select g,sum(d-cast(i as double)),avg(cast(i+1 as double)),count(d/0.0) from expressions group by g",
                "select g,sum(d/2.0),avg(d+0.25),count(d) from expressions where d!=null group by g",
                "select g,count() from expressions where d=i*0.25 group by g",
                "select g,count() from expressions where d!=i*0.25 group by g",
                "select sum(d*2.0+0.25),count() from expressions",
                "select avg(i+1),sum(d*2.0),count() from expressions",
                "select sum(d*2.0),avg(d) from expressions where d>=0.0 or d=null")) {
            try (var candidate = compile(sql)) {
                check(sql, candidate);
                assertTrue(sql, nativeRows(candidate) > 0);
                check(sql, candidate);
            }
        }
        engine.execute("alter table expressions add column late double", context);
        String sql = "select g,sum(d+late),avg(late/2.0) from expressions where late=null or late>=0.0 group by g";
        try (var candidate = compile(sql)) {
            check(sql, candidate);
            engine.execute("insert into expressions values (1,1,17.25,42,'2024-01-02',1.5)", context);
            check(sql, candidate);
        }
        engine.execute("alter table expressions convert partition to parquet list '2024-01-01'", context);
        try (var candidate = compile(sql)) { check(sql, candidate); }
        engine.execute("truncate table expressions", context);
        try (var candidate = compile(sql)) { check(sql, candidate); }
    }

    @Test public void compensatedSumsShareScalarAndTypedMapPipelinesButKeepTheirResultGetter() throws Exception {
        engine.execute("create table compensated as (select cast(x%7 as int) k,(x%7)*4294967297 l,"
                + "cast(x%7 as symbol) s,cast(x%7 as double) g,"
                + "case when x%7=0 or x<31 then null else cast(x%101-50 as double) end d,"
                + "x%37-18 v from long_sequence(2003))", context);
        String aggregates = "ksum(d*1.0),sum(v),avg(v),min(d),max(d),count()";
        for (String key : List.of("k", "l", "s", "g")) {
            String sql = "select " + key + "," + aggregates + " from compensated group by " + key;
            try (var candidate = compile(sql)) {
                check(sql, candidate); check(sql, candidate);
                assertTrue(sql, nativeRows(candidate) > 0);
            }
        }
        for (String sql : List.of("select " + aggregates + " from compensated",
                "select ksum(d*1.0) from compensated where k>0",
                "select k,ksum(d) from compensated where k>=0 group by k")) {
            try (var candidate = compile(sql)) {
                check(sql, candidate); check(sql, candidate);
                assertTrue(sql, nativeRows(candidate) > 0);
            }
        }
        for (String suffix : List.of(" order by n", " order by n desc limit 3")) {
            String sql = "select k,ksum(d*1.0) n from compensated group by k" + suffix;
            try (var candidate = compile(sql)) {
                checkRanked(sql, candidate);
                assertTrue(nativeRows(candidate) > 0);
                assertEquals("KSUM needs its count-guarded getter", 0, nativeRankedRows(candidate, !suffix.contains("limit")));
            }
        }
        String nested = "select count(n),sum(n) from (select k,ksum(d*1.0) n from compensated group by k)";
        try (var candidate = compile(nested)) { check(nested, candidate); check(nested, candidate); }
        engine.execute("alter table compensated add column late double", context);
        String sql = "select k,ksum(d*1.0),ksum(late*1.0) from compensated group by k";
        try (var candidate = compile(sql)) {
            check(sql, candidate);
            engine.execute("insert into compensated values (1,4294967297,'1',1.0,0.25,1,1.25)", context);
            check(sql, candidate);
            assertTrue(nativeRows(candidate) > 0);
        }
        engine.execute("truncate table compensated", context);
        try (var candidate = compile("select ksum(d*1.0) from compensated")) {
            check("select ksum(d*1.0) from compensated", candidate);
        }
    }

    @Test public void compensatedScalarChunksPreserveOrderedRoundingAndNullPrefixes() throws Exception {
        pool.halt();
        context.changePageFrameSizes(4096, 4096); // One ordered frame, crossing two native chunk boundaries.
        engine.execute("create table rounding (d double)", context);
        String sql = "select ksum(d*1.0),nsum(d*1.0),avg(d*1.0) from rounding";
        try (var candidate = compile(sql)) {
            for (double[] pattern : new double[][]{{Double.NaN, -0.0}, {-0.0, Double.NaN},
                    {1e16, 1.0, 1.0, -1e16, Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, 0.1}}) {
                engine.execute("truncate table rounding", context);
                try (var writer = engine.getWriter(engine.verifyTableName("rounding"), "test")) {
                    for (int i = 0; i < 2051; i++) {
                        var row = writer.newRow();
                        row.putDouble(0, pattern[i % pattern.length]);
                        row.append();
                    }
                    writer.commit();
                }
                check(sql, candidate); check(sql, candidate);
                assertTrue(nativeRows(candidate) > 0);
            }
        }
    }

    @Test public void unsupportedComputedKeyRetainsOrdinaryAverageFinalization() throws Exception {
        engine.execute("create table fallback as (select cast(x%17-8 as int) i,(x%31-15)*0.25 d from long_sequence(1003))", context);
        String sql = "select abs(i) k,avg(i),avg(d) from fallback group by k";
        try (var candidate = compile(sql)) { check(sql, candidate); check(sql, candidate); }
    }

    @Test public void floatKeysPreserveRawBitsThroughExpressionsAndCompositeLayouts() throws Exception {
        engine.execute("create table float_keys (v long,i int,s symbol,label varchar,ts timestamp) timestamp(ts) partition by day bypass wal", context);
        engine.execute("insert into float_keys select x,cast(x%3 as int),cast(x%3 as symbol),'old'::varchar,"
                + "timestamp_sequence('2024-01-01',1000000) from long_sequence(33)", context);
        engine.execute("alter table float_keys add column f float", context);
        int[] bits = {0, 0x80000000, 0x7fc00000, 0x7fc01234, 0xffc05678, 0x7f800001, 0xff800001,
                0x7f800000, 0xff800000, 1, 0x80000001, 0x3f800000, 0x3f800001};
        try (var writer = engine.getWriter(engine.verifyTableName("float_keys"), "test")) {
            for (int n = 0; n < 5003; n++) {
                var row = writer.newRow(1704153600000000L+n); row.putLong(0, n); row.putInt(1, n%3);
                row.putSym(2, Integer.toString(n%3)); row.putVarchar(3, new io.questdb.std.str.Utf8String(n%2==0 ? "ascii" : "žluť"));
                row.putFloat(5, Float.intBitsToFloat(bits[n%bits.length])); row.append();
            }
            writer.commit();
        }
        for (String key : List.of("f", "-f", "case when i>0 then f else null::float end",
                "f*2::float+0.25::float", "cast(v as float)", "cast(v/3.0 as float)")) {
            String sql = "select " + key + " g,count() n,sum(v) total from float_keys group by g";
            context.setQueryCodeGenerator(null);
            java.util.Map<Long,List<Long>> expected;
            try (var ordinary = engine.select(sql, context)) { expected = rawFloatingGroups(ordinary); }
            if (key.equals("f")) assertEquals(bits.length, expected.size());
            try (var candidate = compile(sql)) {
                assertEquals(expected, rawFloatingGroups(candidate)); assertEquals(expected, rawFloatingGroups(candidate));
                assertTrue(sql, nativeMapRows(candidate)>0);
            }
        }
        for (String keys : List.of("i,f", "f,s", "f,label")) {
            String sql = "select " + keys + ",count(),sum(v),min(f),max(f) from float_keys group by " + keys;
            try (var candidate = compile(sql)) { check(sql, candidate); assertTrue(nativeMapRows(candidate)>0); }
        }
        engine.execute("alter table float_keys convert partition to parquet list '2024-01-01'", context);
        String sql = "select f,count(),sum(v) from float_keys group by f";
        try (var candidate = compile(sql)) { check(sql, candidate); }
        engine.execute("truncate table float_keys", context);
        try (var candidate = compile(sql)) { assertTrue(rawFloatingGroups(candidate).isEmpty()); }
    }

    @Test public void doubleKeysPreserveRawBitsCompositeLayoutsAndColumnTops() throws Exception {
        engine.execute("create table keys (v long,i int,s symbol,label varchar,ts timestamp) timestamp(ts) partition by day bypass wal", context);
        engine.execute("insert into keys select x,cast(x%3 as int),cast(x%3 as symbol),'old'::varchar,"
                + "timestamp_sequence('2024-01-01',1000000) from long_sequence(33)", context);
        engine.execute("alter table keys add column d double", context);
        double[] values = {0.0,-0.0,Double.NaN,Double.longBitsToDouble(0x7ff8000000001234L),
                Double.longBitsToDouble(0x7ff8000000005678L),Double.POSITIVE_INFINITY,Double.NEGATIVE_INFINITY,
                Double.MIN_VALUE,-Double.MIN_VALUE,1,1.00000000005,-17.25};
        try (var writer = engine.getWriter(engine.verifyTableName("keys"), "test")) {
            for (int n = 0; n < 5003; n++) {
                var row = writer.newRow(1704153600000000L + n);
                row.putLong(0, n); row.putInt(1, n%3); row.putSym(2, Integer.toString(n%3));
                row.putVarchar(3, new io.questdb.std.str.Utf8String(n%2==0 ? "ascii" : "žluť"));
                row.putDouble(5, values[n%values.length]); row.append();
            }
            writer.commit();
        }
        String direct = "select d,count() n,sum(v) total from keys group by d";
        context.setQueryCodeGenerator(null);
        java.util.Map<Long,List<Long>> expected;
        try (var ordinary = engine.select(direct, context)) { expected = rawFloatingGroups(ordinary); }
        assertEquals(values.length, expected.size()); // Canonical NULL shares the column-top group.
        for (String sql : List.of(direct,
                "select d*2.0+0.25 k,count(),sum(v) from keys group by k",
                "select i,d,count(),sum(v) from keys group by i,d",
                "select d,s,count(),sum(v) from keys group by d,s",
                "select d,label,count(),sum(v) from keys group by d,label")) {
            try (var candidate = compile(sql)) {
                check(sql, candidate); check(sql, candidate);
                assertTrue(sql, nativeMapRows(candidate) > 0);
                if (sql.equals(direct)) assertEquals(expected, rawFloatingGroups(candidate));
            }
        }
        try (var candidate = compile(direct + " limit 2")) {
            var selected = rawFloatingGroups(candidate);
            assertEquals(2, selected.size());
            for (var entry : selected.entrySet()) assertEquals(expected.get(entry.getKey()), entry.getValue());
            assertTrue(nativeMapRows(candidate) > 0);
            assertTrue("Admission retains only complete selected groups", nativeMapRows(candidate) < 5036);
        }
        engine.execute("alter table keys convert partition to parquet list '2024-01-01'", context);
        try (var candidate = compile(direct)) { assertEquals(expected, rawFloatingGroups(candidate)); }
        engine.execute("alter table keys convert partition to parquet list '2024-01-02'", context);
        context.setQueryCodeGenerator(null);
        try (var ordinary = engine.select(direct, context)) { expected = rawFloatingGroups(ordinary); }
        try (var candidate = compile(direct)) { assertEquals(expected, rawFloatingGroups(candidate)); }
        engine.execute("truncate table keys", context);
        try (var candidate = compile(direct)) { assertTrue(rawFloatingGroups(candidate).isEmpty()); }
    }

    private java.util.Map<Long,List<Long>> rawFloatingGroups(RecordCursorFactory factory) throws Exception {
        var result = new java.util.TreeMap<Long,List<Long>>();
        try (var cursor = factory.getCursor(context)) {
            var record = cursor.getRecord();
            while (cursor.hasNext()) assertNull(result.put(factory.getMetadata().getColumnType(0) == io.questdb.cairo.ColumnType.FLOAT
                            ? (long) Float.floatToRawIntBits(record.getFloat(0)) : Double.doubleToRawLongBits(record.getDouble(0)),
                    List.of(record.getLong(1), record.getLong(2))));
        }
        return result;
    }

    private long nativeMapRows(RecordCursorFactory factory) {
        for (; factory != null; factory = factory.getBaseFactory()) {
            if (factory instanceof AsyncGroupByRecordCursorFactory group) {
                long rows = 0;
                for (int i = -1; i < 2; i++)
                    if (group.getAtom().getCompiledUpdater(i) instanceof AggregateSlot slot) rows += slot.getNativeMapRows();
                return rows;
            }
        }
        return 0;
    }

    @Test public void varcharCountPreservesNullsEmptyStringsColumnTopsAndParquet() throws Exception {
        engine.execute("create table strings as (select cast(x%7 as int) k,x v,"
                + "timestamp_sequence('2024-01-01',1000000) ts from long_sequence(33)) timestamp(ts) partition by day bypass wal", context);
        engine.execute("alter table strings add column s varchar", context);
        engine.execute("insert into strings select cast(x%7 as int),x,timestamp_sequence('2024-01-02',1000000),"
                + "cast(case when x%11=0 then null when x%13=0 then '' when x%3=0 then '中' "
                + "else 'long-shared-prefix-'||cast(x%29 as string) end as varchar) from long_sequence(1003)", context);
        for (String sql : List.of("select k,count(s),count() from strings group by k",
                "select s,count(s),count() from strings group by s",
                "select k,s,count(s),sum(v) from strings group by k,s",
                "select count(s) from strings",
                "select count(s),count() from strings",
                "select sum(v),count() from strings",
                "select k,count(case when v<500 then s when v<750 then '' else null::varchar end) from strings group by k",
                "select count(case when k<3 then s else null::varchar end) from strings where v>100",
                "select k,count(s),min(s),max(s) from strings group by k")) {
            try (var candidate = compile(sql)) {
                check(sql, candidate); check(sql, candidate);
                // Fully batched aggregates keep their existing execution.
                if (sql.equals("select sum(v),count() from strings")) assertEquals(0, nativeRows(candidate));
                else assertTrue(sql, nativeRows(candidate) > 0);
            }
        }
        String sql = "select k,count(s),count() from strings group by k";
        for (String partition : List.of("2024-01-01", "2024-01-02")) {
            engine.execute("alter table strings convert partition to parquet list '" + partition + "'", context);
            try (var candidate = compile(sql)) { check(sql, candidate); assertTrue(nativeRows(candidate) > 0); }
            try (var candidate = compile("select count(s) from strings")) { check("select count(s) from strings", candidate); }
        }
        // A joined row has no VARCHAR descriptor ABI yet; retain ordinary execution.
        engine.execute("create table dim as (select cast(x as int) k from long_sequence(7))", context);
        String join = "select count(s) from strings join dim on strings.k=dim.k";
        try (var candidate = compile(join)) { check(join, candidate); }
        engine.execute("truncate table strings", context);
        try (var candidate = compile("select count(s) from strings")) { check("select count(s) from strings", candidate); }
    }

    @Test public void numericRankingUsesFinalValuesAndIeeeOrdering() throws Exception {
        configure(options.withLocalCapacity(64));
 assertNumericRanking();
    }

    private void assertNumericRanking() throws Exception {
        engine.execute("create table ranking (k long,d double,i int,h short,ts timestamp) timestamp(ts) partition by day bypass wal", context);
        double[] edges = {Double.NaN, Double.longBitsToDouble(0xfff8000000001234L),
                Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, -0.0, 0.0,
                Double.MIN_VALUE, -Double.MIN_VALUE, 1.0, 1.00000000005, -17.25};
        try (var writer = engine.getWriter(engine.verifyTableName("ranking"), "test")) {
            for (int n = 0; n < 1642; n++) {
                // Unequal counts make ordering partial sums observably wrong for AVG.
                int key = n < 1542 ? n % 514 : 300;
                var row = writer.newRow(1704067200000000L + n);
                row.putLong(0, key);
                row.putDouble(1, key < edges.length ? edges[key] : (key - 257) * 0.25);
                row.putInt(2, key % 17 == 0 ? Integer.MIN_VALUE : key - 257);
                row.putShort(3, (short) (key - 257)); row.append();
            }
            writer.commit();
        }
        for (String aggregate : List.of("min(d)", "max(d)", "avg(d)", "avg(i)", "min(i)", "max(h)")) {
            for (String suffix : List.of("desc limit 64", "asc limit 65")) {
                String sql = "select " + aggregate + " v from ranking group by k order by v " + suffix;
                try (var candidate = compile(sql)) {
                    checkRanked(sql, candidate); checkRanked(sql, candidate);
                    assertTrue(sql, nativeRankedRows(candidate, false) > 0);
                }
            }
        }
        for (String suffix : List.of("desc", "asc limit 31,95")) {
            String sql = "select avg(d) v from ranking group by k order by v " + suffix;
            try (var candidate = compile(sql)) {
                checkRanked(sql, candidate);
                assertTrue(sql, nativeRankedRows(candidate, suffix.equals("desc")) > 0);
            }
        }
        engine.execute("alter table ranking add column late double", context);
        String sql = "select min(late) v from ranking group by k order by v desc limit 65";
        try (var candidate = compile(sql)) {
            checkRanked(sql, candidate);
            engine.execute("insert into ranking values (515,1.0,1,1,'2024-01-02',-0.0),(516,2.0,2,2,'2024-01-02',0.0)", context);
            checkRanked(sql, candidate);
        }
        engine.execute("alter table ranking convert partition to parquet list '2024-01-01'", context);
        try (var candidate = compile(sql)) { checkRanked(sql, candidate); }
        engine.execute("truncate table ranking", context);
        try (var candidate = compile(sql)) { checkRanked(sql, candidate); }
    }

    private void checkRanked(String sql, RecordCursorFactory candidate) throws Exception {
        var actual = rankedValues(candidate);
        context.setQueryCodeGenerator(null);
        try (var ordinary = Demo.compileSql(engine, context, sql, true)) { assertEquals(sql, rankedValues(ordinary), actual); }
        finally { context.setQueryCodeGenerator(compiler); }
    }

    private List<Long> rankedValues(RecordCursorFactory factory) throws Exception {
        var result = new java.util.ArrayList<Long>();
        boolean floating = factory.getMetadata().getColumnType(0) == io.questdb.cairo.ColumnType.DOUBLE;
        try (var cursor = factory.getCursor(context)) {
            var record = cursor.getRecord();
            while (cursor.hasNext()) result.add(floating ? Double.doubleToLongBits(record.getDouble(0)) : record.getLong(0));
            cursor.toTop();
            int i = 0;
            while (cursor.hasNext()) assertEquals(result.get(i++).longValue(), floating ? Double.doubleToLongBits(record.getDouble(0)) : record.getLong(0));
            assertEquals(result.size(), i);
        }
        return result;
    }

    private long nativeRankedRows(RecordCursorFactory factory, boolean fullSort) {
        for (; factory != null; factory = factory.getBaseFactory()) {
            if (factory instanceof AsyncGroupByRecordCursorFactory group) {
                long rows = 0;
                for (int i = -1; i < 2; i++) if (group.getAtom().getCompiledUpdater(i) instanceof AggregateSlot slot)
                    rows += fullSort ? slot.sortedRows() : slot.topKCounters()[0];
                return rows;
            }
        }
        return 0;
    }

    private RecordCursorFactory compile(String sql) throws Exception {
        context.setQueryCodeGenerator(compiler);
        return Demo.compileSql(engine, context, sql, true);
    }

    private void check(String sql, RecordCursorFactory candidate) throws Exception {
        var actual = Demo.readSql(candidate, context, candidate.getMetadata().getColumnCount());
        context.setQueryCodeGenerator(null);
        try (var ordinary = Demo.compileSql(engine, context, sql, true)) {
            assertEquals(sql, Demo.readSql(ordinary, context, ordinary.getMetadata().getColumnCount()), actual);
        } finally { context.setQueryCodeGenerator(compiler); }
    }

    private void checkApproximateTail(String sql, RecordCursorFactory candidate, int floatingColumns) throws Exception {
        var actual = Demo.readSql(candidate, context, candidate.getMetadata().getColumnCount());
        context.setQueryCodeGenerator(null);
        try (var ordinary = Demo.compileSql(engine, context, sql, true)) {
            var expected = Demo.readSql(ordinary, context, ordinary.getMetadata().getColumnCount());
            assertApproximateTail(sql, expected, actual, floatingColumns);
        } finally { context.setQueryCodeGenerator(compiler); }
    }

    static void assertApproximateTail(String sql, List<CompiledGroupBy.ResultRow> expected,
                                      List<CompiledGroupBy.ResultRow> actual, int floatingColumns) {
        assertEquals(sql, expected.size(), actual.size());
        for (int row = 0; row < expected.size(); row++) {
            var left = new java.util.ArrayList<>(expected.get(row).keys()); left.addAll(expected.get(row).values());
            var right = new java.util.ArrayList<>(actual.get(row).keys()); right.addAll(actual.get(row).values());
            assertEquals(left.size(), right.size());
            int prefix = left.size()-floatingColumns;
            assertEquals(sql, left.subList(0, prefix), right.subList(0, prefix));
            for (int column = prefix; column < left.size(); column++) {
                double a = Double.longBitsToDouble(left.get(column)), b = Double.longBitsToDouble(right.get(column));
                // These statistical fixtures use bounded quarter-valued inputs. Parallel merge
                // order changes rounding; exact ordered states are checked in DoubleStateTest.
                if (Double.isFinite(a)) {
                    assertTrue(sql, Double.isFinite(b));
                    assertEquals(sql, a, b, 1e-9 * Math.max(1.0, Math.abs(a)));
                } else assertEquals(sql, left.get(column), right.get(column));
            }
        }
    }

    private long nativeRows(RecordCursorFactory factory) {
        for (; factory != null; factory = factory.getBaseFactory()) {
            if (factory instanceof AsyncGroupByRecordCursorFactory group) {
                long rows = 0;
                for (int i = -1; i < 2; i++)
                    if (group.getAtom().getCompiledUpdater(i) instanceof AggregateSlot slot) rows += slot.updatedRows();
                return rows;
            }
            if (factory instanceof AsyncGroupByNotKeyedRecordCursorFactory group)
                return group.getAtom().getCompiledAggregateNativeRows();
        }
        return 0;
    }
}
