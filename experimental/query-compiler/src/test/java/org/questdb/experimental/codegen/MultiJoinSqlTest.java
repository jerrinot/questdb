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

import io.questdb.cairo.*;
import io.questdb.cairo.sql.*;
import io.questdb.cairo.map.Map;
import io.questdb.cairo.map.MapValue;
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.TextPlanSink;
import io.questdb.griffin.engine.QueryCodeGenerator;
import io.questdb.griffin.engine.functions.GroupByFunction;
import io.questdb.griffin.engine.groupby.GroupByKernel;
import io.questdb.griffin.engine.join.CompiledJoin;
import io.questdb.griffin.engine.table.AsyncHashJoinGroupByRecordCursorFactory;
import io.questdb.mp.*;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class MultiJoinSqlTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private CairoEngine engine;
    private SqlExecutionContextImpl context;
    private WorkerPool pool;
    private ResolvedQueryCompiler compiler;
    private long buildThreshold = Long.MAX_VALUE;
    private long payloadCopyLimit = 128L << 20;
    private int shardThreshold = Integer.MAX_VALUE;
    private int maxFrameRows = 256;
    private long memory;
    private long joinMemory;

    @Before public void setUp() throws Exception {
        memory = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        joinMemory = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP);
        var config = new DefaultCairoConfiguration(temporary.newFolder().toString()) {
            @Override public int getSqlPageFrameMaxRows() { return maxFrameRows; }
            @Override public int getSqlPageFrameMinRows() { return 64; }
            @Override public long getSqlParallelHashJoinGroupByBuildParallelMinRows() { return buildThreshold; }
            @Override public long getSqlParallelHashJoinGroupByBuildRowsPerPartition() { return 64; }
            @Override public long getSqlParallelHashJoinGroupByPayloadCopyMaxSize() { return payloadCopyLimit; }
            @Override public int getGroupByShardingThreshold() { return shardThreshold; }
            @Override public int getSqlSmallMapKeyCapacity() { return 4; }
        };
        engine = new CairoEngine(config);
        pool = new WorkerPool(() -> 2);
        WorkerPoolUtils.setupQueryJobs(pool, engine);
        pool.start();
        context = new SqlExecutionContextImpl(engine, 2).with(
                config.getFactoryProvider().getSecurityContextFactory().getRootContext(), null, null, -1, null);
        compiler = new ResolvedQueryCompiler(new LlvmBackend(), temporary.newFolder().toPath());
        engine.execute("create table p as (select cast(case when x%19=0 then null else x%31-15 end as int) k,"
                + "cast(case when x%13=0 then null else x%17-8 end as int) j,"
                + "case when x%11=0 then null else x end v from long_sequence(2003))", context);
        engine.execute("create table a as (select cast(case when x%19=0 then null else x%31-15 end as int) k,"
                + "case when x%5=0 then null else x*11 end v from long_sequence(93))", context);
        engine.execute("create table b as (select cast(case when x%13=0 then null else x%17-8 end as int) k,"
                + "case when x%7=0 then null else x*17 end v from long_sequence(51))", context);
    }

    @After public void tearDown() {
        if (pool != null) pool.halt();
        if (engine != null) assertEquals(0, engine.getBusyReaderCount());
        if (context != null) context.close();
        if (engine != null) engine.close();
        assertEquals(memory, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
        assertEquals(joinMemory, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JOIN_MAP));
    }

    @Test public void varcharPayloadsGroupAcrossTypedDependentJoins() throws Exception {
        // Enough full frames to sample both ordinary and lookahead kernels, including tails.
        maxFrameRows = 4096;
        engine.execute("create table vp as (select cast(x%31 as int) k,(x%31-15)*4294967297 l,"
                + "cast('K'||cast(x%31 as string) as symbol) s,x v from long_sequence(200003))", context);
        engine.execute("create table va as (select cast((93-x)%31 as int) k,((93-x)%31-15)*4294967297 l,"
                + "cast('K'||cast((93-x)%31 as string) as symbol) s,cast(x%17 as int) nk,x v,"
                + "cast(case when x%7=0 then null when x%7=1 then '' when x%7=2 then 'é中' "
                + "when x%7=3 then '" + "long-ä-".repeat(300) + "' else 'name-'||cast(x%13 as string) end as varchar) name "
                + "from long_sequence(93))", context);
        engine.execute("create table vb as (select cast((51-x)%17 as int) k,x v,"
                + "cast(case when x%5=0 then null else 'other-'||cast(x%9 as string) end as varchar) name "
                + "from long_sequence(51))", context);
        shardThreshold = 4;
        for (long threshold : new long[]{Long.MAX_VALUE, 1}) {
            buildThreshold = threshold;
            for (String key : new String[]{"p.k=a.k", "p.l=a.l", "p.s=a.s", "p.l=a.l and p.s=a.s"}) {
                String from = " from vp p join va a on " + key + " join vb b on a.nk=b.k";
                matches("select a.name,count(),sum(p.v)" + from + " group by a.name", 1, true, compiler);
                matches("select b.name,count(),sum(p.v)" + from + " group by b.name", 1, true, compiler);
                matches("select a.name,count(),count(b.name),sum(p.v),min(a.v),max(b.v)" + from + " group by a.name", 1, true, compiler);
                matches("select a.name,b.name,p.k,count(),sum(a.v),min(p.v),max(b.v),avg(p.v)" + from
                        + " group by a.name,b.name,p.k", 3, true, compiler);
            }
        }
        String from = " from vp p join va a on p.k=a.k join vb b on a.nk=b.k";
        matches("select count(a.name),sum(length(a.name)),min(length(b.name)),max(p.v)" + from, 0, true, compiler);
        matches("select case when a.v>20 then a.name else b.name end n,count(),sum(p.v)" + from
                + " group by n", 1, true, compiler);
        matches("select case when a.v>20 then a.name else cast('other' as varchar) end n,count(),sum(p.v)" + from
                + " group by n", 1, true, compiler);
        matches("select a.name,a.v*0.5 d,count(),sum(p.v*0.5),min(a.v*0.5),max(b.v*0.5),avg(p.v*0.5)"
                + from + " group by a.name,d", 2, true, compiler);
        matches("select a.name,count(),sum(p.v) from vp p join va a on p.k=a.k group by a.name", 1, true, compiler);
        matches("select a.name,count(),sum(p.v)" + from + " where p.v>a.v group by a.name", 1, true, compiler);
        String sql = "select a.name,count(),sum(p.v)" + from + " group by a.name";
        AtomicBoolean fail = new AtomicBoolean(true);
        context.setQueryCodeGenerator(intercepted(false, fail));
        try (var factory = Demo.compileSql(engine, context, sql, true)) {
            assertThrows(CairoException.class, () -> Demo.readSql(factory, context, 1));
            fail.set(false);
            context.setQueryCodeGenerator(null);
            try (var control = Demo.compileSql(engine, context, sql, true)) {
                assertEquals(Demo.readSql(control, context, 1), Demo.readSql(factory, context, 1));
            }
            // Reuse the compiled factory while the frozen row identities and key bytes change.
            engine.execute("truncate table va", context);
            assertTrue(Demo.readSql(factory, context, 1).isEmpty());
            engine.execute("insert into va select cast(x%31 as int),(x%31-15)*4294967297,"
                    + "cast('K'||cast(x%31 as string) as symbol),cast(x%17 as int),x,"
                    + "cast(case when x%3=0 then null else 'replacement-'||cast(x%11 as string) end as varchar) "
                    + "from long_sequence(93)", context);
            try (var control = Demo.compileSql(engine, context, sql, true)) {
                assertEquals(Demo.readSql(control, context, 1), Demo.readSql(factory, context, 1));
            }
            engine.execute("truncate table vp", context);
            engine.execute("insert into vp select cast(x%31 as int),(x%31-15)*4294967297,"
                    + "cast('K'||cast(x%31 as string) as symbol),x from long_sequence(1000)", context);
            // Disable the destination cache while keeping the probe larger than the builds,
            // so row-count changes do not invalidate this factory's join orientation.
            try (var control = Demo.compileSql(engine, context, sql, true)) {
                assertEquals(Demo.readSql(control, context, 1), Demo.readSql(factory, context, 1));
            }
        }
    }

    @Test public void growingJoinedGroupsPreserveWideAggregateState() throws Exception {
        maxFrameRows = 4096;
        buildThreshold = 1;
        shardThreshold = 4;
        engine.execute("create table gp as (select (x%8193-4096)*4294967297 k,x v from long_sequence(1000003))", context);
        engine.execute("create table ga as (select (x-4097)*4294967297 k,"
                + "((x*73)%8193-4096)*4294967297 nk,x v,"
                + "cast(case when x%19=0 then null else 'g-'||cast(x as string) end as varchar) name "
                + "from long_sequence(8193))", context);
        // Group state crosses the local-cache admission boundary as the maps grow.
        // The key comes from the dependent build, including NULL VARCHAR values.
        matches("select b.k,b.name,count(),sum(p.v),min(a.v),max(b.v),avg(p.v),sum(p.v*0.5) "
                + "from gp p join ga a on p.k=a.k join ga b on a.nk=b.k group by b.k,b.name", 2, true, compiler);
    }

    @Test public void varcharPayloadsHandleColumnTopsReopenAndParquetFallback() throws Exception {
        engine.execute("create table vp as (select cast(x%31 as int) k,x v,timestamp_sequence('2024-01-01',60000000L) ts from long_sequence(2003)) timestamp(ts) partition by day", context);
        engine.execute("alter table vp add label varchar", context);
        engine.execute("insert into vp select cast(x%31 as int),x,timestamp_sequence('2024-01-03',60000000L),cast('p-'||cast(x%5 as string) as varchar) from long_sequence(2003)", context);
        engine.execute("create table va as (select cast(x%31 as int) k,cast(x%17 as int) nk,x v,"
                + "timestamp_sequence('2024-01-01',3600000000L) ts from long_sequence(48)) timestamp(ts) partition by day", context);
        engine.execute("alter table va add label varchar", context);
        engine.execute("insert into va select cast(x%31 as int),cast(x%17 as int),x,"
                + "timestamp_sequence('2024-01-03',3600000000L),cast(case when x%3=0 then null else 'a-'||cast(x%7 as string) end as varchar) from long_sequence(48)", context);
        String sql = "select p.label,a.label,count(),count(a.label),sum(p.v),min(a.v),max(b.v) "
                + "from vp p join va a on p.k=a.k join b on a.nk=b.k group by p.label,a.label";
        shardThreshold = 4;
        buildThreshold = 1;
        matches(sql, 2, true, compiler);
        context.setQueryCodeGenerator(compiler);
        try (var factory = Demo.compileSql(engine, context, sql, true)) {
            RecordCursorFactory base = factory;
            while (!(base instanceof AsyncHashJoinGroupByRecordCursorFactory)) base = base.getBaseFactory();
            var atom = ((AsyncHashJoinGroupByRecordCursorFactory) base).getAtom();
            for (int round = 0; round < 4; round++) {
                long nativeBefore = atom.getCompiledJoinNativeRows();
                context.setQueryCodeGenerator(null);
                try (var control = Demo.compileSql(engine, context, sql, true)) {
                    var expected = Demo.readSql(control, context, 2);
                    assertEquals(expected, Demo.readSql(factory, context, 2));
                    assertEquals(expected, Demo.readSql(factory, context, 2));
                }
                if (round < 3) assertTrue(atom.getCompiledJoinNativeRows() > nativeBefore);
                else assertEquals(nativeBefore, atom.getCompiledJoinNativeRows());
                if (round == 0) engine.execute("insert into va values(1,1,999,'2024-01-05','new-中')", context);
                else if (round == 1) engine.execute("alter table vp convert partition to parquet where ts < '2024-01-03'", context);
                else if (round == 2) engine.execute("alter table va convert partition to parquet where ts < '2024-01-04'", context);
            }
        }
    }

    @Test public void typedKeysPreserveDictionaryDomainsAndDuplicateProducts() throws Exception {
        engine.execute("create table tp as (select x%31-15 k, (x%31-15)*4294967297 l, cast('K'||cast(x%31-15 as string) as symbol) s, cast(x%31%7 as int) j,x v from long_sequence(2003))", context);
        engine.execute("create table td as (select (93-x)%31-15 k, ((93-x)%31-15)*4294967297 l, cast('K'||cast((93-x)%31-15 as string) as symbol) s,cast((93-x)%31%7 as int) j,x v from long_sequence(93))", context);
        for (long limit : new long[]{0, 128L << 20}) {
            payloadCopyLimit = limit;
            buildThreshold = limit == 0 ? Long.MAX_VALUE : 1;
            for (String key : new String[]{"p.l=d.l", "p.s=d.s", "p.l=d.l and p.j=d.j", "p.s=d.s and p.j=d.j"}) {
                matches("select count(),sum(p.v),sum(d.v) from tp p join td d on " + key, 0, true, compiler);
                matches("select p.j,count(),sum(p.v),sum(d.v) from tp p join td d on " + key + " group by p.j", 1, true, compiler);
                matches("select count(),sum(p.v),sum(d.v),sum(e.v) from tp p join td d on " + key + " join td e on p.s=e.s", 0, true, compiler);
                matches("select count(),sum(p.v),sum(d.v),sum(e.v) from tp p join td d on " + key + " join td e on p.s=e.s", 0, true, intercepted(true, null));
            }
        }
    }

    @Test public void dependentKeysPreserveDuplicatePositionsAndDictionaryDomains() throws Exception {
        engine.execute("create table dp as (select cast(x%31 as int) k,(x%31-15)*4294967297 l,cast('K'||cast(x%31 as string) as symbol) s,cast(x%3 as int) j,x v from long_sequence(2003))", context);
        engine.execute("create table da as (select cast((93-x)%31 as int) k,((93-x)%31-15)*4294967297 l,cast('K'||cast((93-x)%31 as string) as symbol) s,cast(x%17-8 as int) nk,(x%17-8)*4294967297+4503599627370496 nl,cast('P'||cast(x%17 as string) as symbol) ns,x v from long_sequence(93))", context);
        engine.execute("create table db as (select cast((51-x)%17-8 as int) k,((51-x)%17-8)*4294967297+4503599627370496 l,cast('P'||cast((51-x)%17 as string) as symbol) s,cast((51-x)%3 as int) j,x v from long_sequence(51))", context);
        for (long copy : new long[]{0, 128L << 20}) {
            payloadCopyLimit = copy;
            buildThreshold = copy == 0 ? Long.MAX_VALUE : 1;
            shardThreshold = 4;
            for (String type : new String[]{"k", "l", "s"}) {
                String from = " from dp p join da a on p." + type + "=a." + type + " join db b on a.n" + type + "=b." + type;
                for (QueryCodeGenerator generator : new QueryCodeGenerator[]{compiler, intercepted(true, null)}) {
                    matches("select count(),sum(p.v),sum(a.v),sum(b.v)" + from, 0, true, generator);
                    matches("select a.v,b.v,count(),sum(p.v)" + from + " group by a.v,b.v", 2, true, generator);
                    matches("select p.j,count(),sum(a.v),sum(b.v)" + from + " and p.j=b.j group by p.j", 1, true, generator);
                }
            }
        }
        matches("select count(),sum(p.v),sum(a.v),sum(b.v),sum(c.v) from dp p join da a on p.k=a.k join db b on a.ns=b.s join da c on p.l=c.l", 0, true, compiler);
        matches("select count(),sum(p.v),sum(a.v),sum(b.v),sum(c.v) from dp p join da a on p.k=a.k join db b on a.ns=b.s join da c on p.l=c.l", 0, true, intercepted(true, null));
        matches("select count(),sum(p.v),sum(a.v),sum(b.v),sum(c.v) from dp p join da a on p.l=a.l join db b on a.ns=b.s join db c on a.nl=c.l", 0, true, compiler);
        matches("select count(),sum(p.v),sum(a.v),sum(b.v),sum(c.v) from dp p join da a on p.s=a.s join db b on a.nk=b.k join da c on b.l=c.nl", 0, true, compiler);
        matches("select count(),sum(p.value),sum(a.v),sum(b.v) from (select s,k,j,v value,l from dp) p join da a on p.s=a.s join db b on a.nl=b.l where a.v<b.v and p.value>10", 0, true, compiler);
    }

    @Test public void connectedJoinsDoNotDependOnFromOrder() throws Exception {
        String[] types = {"int", "long", "symbol", "date", "timestamp", "timestamp_ns"};
        String[] orders = {"rp p,ra a,rb b", "rp p,rb b,ra a", "ra a,rp p,rb b",
                "ra a,rb b,rp p", "rb b,rp p,ra a", "rb b,ra a,rp p"};
        for (String type : types) {
            String value = type.equals("symbol") ? "'k'||cast(x%17 as string)"
                    : type.equals("int") ? "x%17-8" : "(x%17-8)*4294967297+4503599627370496";
            String next = value.replace("%17", "%11");
            engine.execute("create table rp as (select cast(case when x%19=0 then null else " + value
                    + " end as " + type + ") k,cast(x%3 as int) j,x v from long_sequence(2003))", context);
            engine.execute("create table ra as (select cast(case when x%19=0 then null else " + value.replace("x%", "(93-x)%")
                    + " end as " + type + ") k,cast(case when x%13=0 then null else " + next
                    + " end as " + type + ") n,cast(x%3 as int) j,x v from long_sequence(93))", context);
            engine.execute("create table rb as (select cast(case when x%13=0 then null else " + next.replace("x%", "(51-x)%")
                    + " end as " + type + ") k,cast(x%3 as int) j,x v from long_sequence(51))", context);
            for (String order : orders) {
                matches("select p.j,count(),sum(p.v),min(a.v),max(b.v) from " + order
                                + " where p.k=a.k and a.n=b.k group by p.j",
                        "select p.j,count(),sum(p.v),min(a.v),max(b.v) from rp p,ra a,rb b"
                                + " where p.k=a.k and a.n=b.k group by p.j", 1, true, compiler);
            }
            // A cycle introduces a build key from two already produced inputs. Keep both
            // equalities, the composite key and the non-equality predicate when rerooting.
            matches("select p.j,count(),sum(p.v),min(a.v),max(b.v) from rb b,ra a,rp p"
                    + " where p.k=a.k and p.j=a.j and a.n=b.k and p.j=b.j and a.v>b.v group by p.j",
                    "select p.j,count(),sum(p.v),min(a.v),max(b.v) from rp p,ra a,rb b"
                            + " where p.k=a.k and p.j=a.j and a.n=b.k and p.j=b.j and a.v>b.v group by p.j",
                    1, true, compiler);
            engine.execute("drop table rp", context);
            engine.execute("drop table ra", context);
            engine.execute("drop table rb", context);
        }
    }

    @Test public void dependentNullsMissingMatchesAndReopen() throws Exception {
        engine.execute("create table da as (select cast(x%7-3 as int) k,case when x%5=0 then null else (x%11-5)*4294967297 end l,cast(case when x%5=0 then null else 'P'||cast(x%11 as string) end as symbol) s,x v from long_sequence(35))", context);
        engine.execute("create table db as (select case when x%5=0 then null else ((22-x)%11-5)*4294967297 end l,cast(case when x%5=0 then null else 'P'||cast((22-x)%11 as string) end as symbol) s,x v from long_sequence(22))", context);
        buildThreshold = 1;
        for (String key : new String[]{"a.l=b.l", "a.s=b.s", "a.l=b.l and a.s=b.s"}) {
            String sql = "select a.v,b.v,count(),sum(p.v) from p join da a on p.k=a.k join db b on " + key + " group by a.v,b.v";
            matches(sql, 2, true, compiler);
            context.setQueryCodeGenerator(compiler);
            try (var factory = Demo.compileSql(engine, context, sql, true)) {
                for (int round = 0; round < 3; round++) {
                    context.setQueryCodeGenerator(null);
                    try (var control = Demo.compileSql(engine, context, sql, true)) {
                        assertEquals(sql, Demo.readSql(control, context, 2), Demo.readSql(factory, context, 2));
                    }
                    if (round == 0) engine.execute("insert into da values (0,9223372036854775807,'new-parent',100)", context);
                    else engine.execute("insert into db values (9223372036854775807,'new-parent',101)", context);
                }
                engine.execute("truncate table db", context);
                assertTrue(Demo.readSql(factory, context, 2).isEmpty());
                engine.execute("insert into db values (9223372036854775807,'new-parent',102)", context);
                context.setQueryCodeGenerator(null);
                try (var control = Demo.compileSql(engine, context, sql, true)) {
                    assertEquals(Demo.readSql(control, context, 2), Demo.readSql(factory, context, 2));
                }
            }
        }
    }

    @Test public void reducedDependenciesPreserveDuplicateBranchesAndMixedKeys() throws Exception {
        engine.execute("create table rp as (select cast(case when x%11=0 then null else x%64 end as int) k,case when x%11=0 then null else (x%64-32)*4294967297 end l,cast(case when x%11=0 then null else 'K'||cast(x%64 as string) end as symbol) s,cast(x%3 as int) j,x v from long_sequence(8192))", context);
        engine.execute("create table ra as (select cast(case when x%11=0 then null else (128-x)%64 end as int) k,case when x%11=0 then null else ((128-x)%64-32)*4294967297 end l,cast(case when x%11=0 then null else 'K'||cast((128-x)%64 as string) end as symbol) s,cast(case when x%7=0 then null else x%17 end as int) nk,case when x%7=0 then null else (x%17-8)*4294967297 end nl,cast(case when x%7=0 then null else 'P'||cast(x%17 as string) end as symbol) ns,cast(x%3 as int) j,x v from long_sequence(128))", context);
        engine.execute("create table rb as (select cast(case when x%7=0 then null else (17-x)%17 end as int) k,case when x%7=0 then null else ((17-x)%17-8)*4294967297 end l,cast(case when x%7=0 then null else 'P'||cast((17-x)%17 as string) end as symbol) s,cast((17-x)%3 as int) j,x v from long_sequence(17))", context);
        for (long copy : new long[]{0, 128L << 20}) {
            payloadCopyLimit = copy;
            buildThreshold = copy == 0 ? Long.MAX_VALUE : 1;
            shardThreshold = 4;
            for (String type : new String[]{"k", "l", "s"}) {
                String from = " from rp p join ra a on p." + type + "=a." + type + " join rb b on a.n" + type + "=b." + type;
                matches("select a.v,count(),sum(p.v),sum(b.v)" + from + " where b.k%5=0 group by a.v", 1, true, compiler);
                matches("select count(),sum(p.v),sum(b.v)" + from + " and a.j=b.j where b.k%5=0", 0, true, compiler);
                matches("select count(),sum(p.v),sum(b.v)" + from + " where b.k is null", 0, true, compiler);
                matches("select count()" + from, 0, true, compiler);
                matches("select count()" + from, 0, true, intercepted(true, null));
                matches("select p.v,count()" + from + " group by p.v", 1, true, compiler);
                matches("select b.v,count()" + from + " group by b.v", 1, true, compiler);
                matches("select count()" + from + " where p.v>b.v", 0, true, compiler);
                matches("select count()" + from + " and p.j=b.j", 0, true, compiler);
                matches("select count()" + from + " join rb c on a.n" + type + "=c." + type, 0, true, compiler);
                // A deeper subtree and another independent root must compose without visiting payloads.
                matches("select p.j,count()" + from + " join rb c on b.j=c.j join ra d on p." + type + "=d." + type
                        + " group by p.j", 1, true, compiler);
                String composite = from.replace(" join rb b", " and p.j=a.j join rb b");
                matches("select count()" + composite + " and a.j=b.j", 0, true, compiler);
                matches("select count(),sum(p.v),sum(b.v)" + composite + " where b.k%5=0", 0, true, compiler);
                matches("select a.v,count(),sum(p.v),sum(b.v)" + composite + " and a.j=b.j where b.k%5=0 group by a.v", 1, true, compiler);
                // The additional fact-row key cannot be cached by producer identity.
                matches("select count(),sum(p.v),sum(b.v)" + from + " and p.j=b.j where b.k%5=0", 0, true, compiler);
                // Opposing branch conditions must agree on the same producer duplicate.
                matches("select count(),sum(p.v),sum(b.v),sum(c.v)" + from + " join rb c on a.n" + type + "=c." + type + " where b.k%2=0 and c.k%2=1", 0, true, compiler);
                // All build tables are nonempty, but every producer row has a zero subtree weight.
                // Scalar zero is insufficient: an empty join must not create probe-key groups.
                matches("select p.v,count()" + from + " join rb c on a.n" + type + "=c." + type
                        + " where b.k%2=0 and c.k%2=1 group by p.v", 1, true, compiler);
            }
        }
    }

    @Test public void cancelledDependencyPreparationReleasesProbesAndReopens() throws Exception {
        engine.execute("create table rp as (select (x%64-32)*4294967297 k,x v from long_sequence(8192))", context);
        engine.execute("create table ra as (select (x-33)*4294967297 k,x%8 j,x v from long_sequence(64))", context);
        engine.execute("create table rb as (select x*2 j,x v from long_sequence(4))", context);
        String sql = "select count(),sum(p.v),sum(a.v),sum(b.v) from rp p join ra a on p.k=a.k join rb b on a.j=b.j";
        context.setQueryCodeGenerator(null);
        java.util.List<CompiledGroupBy.ResultRow> expected;
        try (var control = Demo.compileSql(engine, context, sql, true)) {
            expected = Demo.readSql(control, context, 0);
        }
        context.setQueryCodeGenerator(compiler);
        try (var factory = Demo.compileSql(engine, context, sql, true)) {
            for (String phase : new String[]{"prepare", "prepareLinks"}) {
                AtomicBoolean hit = new AtomicBoolean();
                var breaker = new AtomicBooleanCircuitBreaker(engine) {
                    @Override public void statefulThrowExceptionIfTrippedTimeThrottled() {
                        if (StackWalker.getInstance().walk(frames -> frames.anyMatch(frame ->
                                frame.getClassName().equals(JoinDependencyFilter.class.getName())
                                        && frame.getMethodName().equals(phase)))) {
                            hit.set(true);
                            cancel();
                        }
                        super.statefulThrowExceptionIfTrippedNoThrottle();
                    }
                };
                SqlExecutionCircuitBreaker original = context.getCircuitBreaker();
                context.with(breaker);
                try {
                    assertThrows(CairoException.class, () -> Demo.readSql(factory, context, 0));
                    assertTrue("cancelled inside " + phase, hit.get());
                } finally { context.with(original); }
                assertEquals(expected, Demo.readSql(factory, context, 0));
            }
            engine.execute("truncate table rb", context);
            engine.execute("insert into rb select x*2-1,x from long_sequence(4)", context);
            context.setQueryCodeGenerator(null);
            try (var control = Demo.compileSql(engine, context, sql, true)) {
                assertEquals(Demo.readSql(control, context, 0), Demo.readSql(factory, context, 0));
            }
        }
    }

    @Test public void cancelledSubtreesReopenWithChangedDictionariesAndEmptyGroups() throws Exception {
        engine.execute("create table rp as (select cast('K'||(x%64) as symbol) s,x v from long_sequence(8192))", context);
        engine.execute("create table ra as (select cast('K'||(64-x) as symbol) s,cast('P'||(x%8) as symbol) t,x v from long_sequence(64))", context);
        engine.execute("create table rb as (select cast('P'||(x*2) as symbol) t from long_sequence(4))", context);
        String sql = "select p.s,count() from rp p join ra a on p.s=a.s join rb b on a.t=b.t group by p.s";
        buildThreshold = 1;
        shardThreshold = 4;
        context.setQueryCodeGenerator(null);
        java.util.List<CompiledGroupBy.ResultRow> expected;
        try (var control = Demo.compileSql(engine, context, sql, true)) { expected = Demo.readSql(control, context, 1); }
        context.setQueryCodeGenerator(compiler);
        try (var factory = Demo.compileSql(engine, context, sql, true)) {
            for (String phase : new String[]{"prepareSubtrees", "sumChainWeights"}) {
                AtomicBoolean hit = new AtomicBoolean();
                var breaker = new AtomicBooleanCircuitBreaker(engine) {
                    @Override public void statefulThrowExceptionIfTrippedTimeThrottled() {
                        if (StackWalker.getInstance().walk(frames -> frames.anyMatch(frame -> frame.getMethodName().equals(phase)))) {
                            hit.set(true);
                            cancel();
                        }
                        super.statefulThrowExceptionIfTrippedNoThrottle();
                    }
                };
                SqlExecutionCircuitBreaker original = context.getCircuitBreaker();
                context.with(breaker);
                try {
                    assertThrows(CairoException.class, () -> Demo.readSql(factory, context, 1));
                    assertTrue("cancelled inside " + phase, hit.get());
                } finally { context.with(original); }
                assertEquals(expected, Demo.readSql(factory, context, 1));
            }
            engine.execute("truncate table rb", context);
            engine.execute("insert into rb select cast('P'||(x*2-1) as symbol) from long_sequence(4)", context);
            engine.execute("insert into rp values ('new-root',8193)", context);
            engine.execute("insert into ra values ('new-root','new-child',65)", context);
            engine.execute("insert into rb values ('new-child')", context);
            context.setQueryCodeGenerator(null);
            try (var control = Demo.compileSql(engine, context, sql, true)) {
                assertEquals(Demo.readSql(control, context, 1), Demo.readSql(factory, context, 1));
            }
            engine.execute("truncate table rb", context);
            assertTrue(Demo.readSql(factory, context, 1).isEmpty());
        }
    }

    @Test public void cancelledCountPreparationPreservesFallbackAndReopens() throws Exception {
        String sql = "select count() from p join a on p.k=a.k join b on p.j=b.k";
        context.setQueryCodeGenerator(null);
        java.util.List<CompiledGroupBy.ResultRow> expected;
        try (var control = Demo.compileSql(engine, context, sql, true)) { expected = Demo.readSql(control, context, 0); }
        context.setQueryCodeGenerator(compiler);
        try (var factory = Demo.compileSql(engine, context, sql, true)) {
            AtomicBoolean hit = new AtomicBoolean();
            var breaker = new AtomicBooleanCircuitBreaker(engine) {
                @Override public void statefulThrowExceptionIfTrippedTimeThrottled() {
                    if (StackWalker.getInstance().walk(frames -> frames.anyMatch(frame ->
                            frame.getMethodName().equals("copyChainCounts")))) { hit.set(true); cancel(); }
                    super.statefulThrowExceptionIfTrippedNoThrottle();
                }
            };
            SqlExecutionCircuitBreaker original = context.getCircuitBreaker();
            context.with(breaker);
            try {
                assertThrows(CairoException.class, () -> Demo.readSql(factory, context, 0));
                assertTrue(hit.get());
            } finally { context.with(original); }
            assertEquals(expected, Demo.readSql(factory, context, 0));
            engine.execute("truncate table b", context);
            context.setQueryCodeGenerator(null);
            try (var control = Demo.compileSql(engine, context, sql, true)) {
                assertEquals(Demo.readSql(control, context, 0), Demo.readSql(factory, context, 0));
            }
        }
    }

    @Test public void dependentColumnTopsUseTypedNulls() throws Exception {
        engine.execute("create table da as (select cast(x%31-15 as int) k,x v from long_sequence(93))", context);
        engine.execute("alter table da add column l long", context);
        engine.execute("alter table da add column s symbol", context);
        engine.execute("insert into da values (0,94,4294967297,'parent')", context);
        engine.execute("create table db (l long,s symbol,v long)", context);
        engine.execute("insert into db values (null,null,11),(4294967297,'parent',12),(null,null,13)", context);
        for (long copy : new long[]{0, 128L << 20}) {
            payloadCopyLimit = copy;
            buildThreshold = copy == 0 ? Long.MAX_VALUE : 1;
            for (String key : new String[]{"a.l=b.l", "a.s=b.s", "a.l=b.l and a.s=b.s"}) {
                matches("select count(),sum(p.v),sum(a.v),sum(b.v) from p join da a on p.k=a.k join db b on " + key, 0, true, compiler);
            }
        }
    }

    @Test public void laterBuildGrowthStillRequestsRecompilation() throws Exception {
        String sql = "select count(),sum(p.v),sum(a.v),sum(b.v) from p join a on p.k=a.k join b on a.v=b.v";
        context.setQueryCodeGenerator(compiler);
        try (var factory = Demo.compileSql(engine, context, sql, true)) {
            Demo.readSql(factory, context, 0);
            engine.execute("insert into b select cast(x as int),x from long_sequence(5000)", context);
            assertThrows(TableReferenceOutOfDateException.class, () -> Demo.readSql(factory, context, 0));
        }
    }

    @Test public void legacyCompilerHookCannotDropTypedKeyComponents() {
        var called = new AtomicBoolean();
        QueryCodeGenerator legacy = new QueryCodeGenerator() {
            @Override public GroupByKernel compile(ColumnTypes inputs, ColumnTypes values, ObjList<GroupByFunction> functions) {
                return null;
            }

            @Override public CompiledJoin compileJoin(ColumnTypes joined, int probeColumns, ColumnTypes values,
                    ObjList<GroupByFunction> functions, int key, MapValue value,
                    Function filter, int groupKey, Map map) {
                called.set(true);
                return null;
            }
        };
        var sources = new ObjList<CompiledJoin.Build>();
        for (int[] types : new int[][]{{ColumnType.LONG}, {ColumnType.SYMBOL}, {ColumnType.INT, ColumnType.INT}}) {
            sources.clear();
            sources.add(new CompiledJoin.Build(new int[types.length], types, 0));
            assertNull(legacy.compileJoin(null, 2, null, null, sources, null, null, -1, null));
            assertFalse(called.get());
        }
        sources.clear();
        sources.add(new CompiledJoin.Build(0, 0));
        legacy.compileJoin(null, 1, null, null, sources, null, null, -1, null);
        assertTrue(called.get());
    }

    @Test public void temporalKeysUseTheirOrdinaryRepresentations() throws Exception {
        buildThreshold = 1;
        for (String type : new String[]{"date", "timestamp", "timestamp_ns"}) {
            engine.execute("create table p_" + type + " as (select cast(case when x%7=0 then null else (x%31-15)*4294967297 end as " + type + ") k,cast(x%3 as int) j,x v from long_sequence(2003))", context);
            engine.execute("create table d_" + type + " as (select cast(case when x%7=0 then null else ((93-x)%31-15)*4294967297 end as " + type + ") k,cast((93-x)%3 as int) j,x v from long_sequence(93))", context);
            String from = " from p_" + type + " p join d_" + type + " d on p.k=d.k";
            matches("select count(),sum(p.v),sum(d.v)" + from, 0, true, compiler);
            matches("select p.j,count(),sum(p.v),sum(d.v)" + from + " and p.j=d.j group by p.j", 1, true, compiler);
            matches("select count(),sum(p.v),sum(d.v),sum(e.v)" + from + " join d_" + type + " e on p.k=e.k", 0, true, compiler);
        }
    }

    @Test public void calendarYearsReadProbeAndBuildValuesInEveryTemporalUnit() throws Exception {
        shardThreshold = 4;
        for (String type : new String[]{"date", "timestamp", "timestamp_ns"}) {
            for (String source : new String[]{"cp", "ca"}) {
                engine.execute("create table " + source + "_" + type + " as (select cast(x%31-15 as int) k,"
                        + "cast(x%17-8 as int) j,cast(case when x%23=0 then null "
                        + "when x%23=1 then 9223372036854775807L when x%23=2 then -9223372036854775807L "
                        + "when x%23=3 then -1L when x%23=4 then 0L when x%23=5 then 1152921504606846976L "
                        + "else x*7046029254386353131L end as " + type + ") t,x v from long_sequence("
                        + (source.equals("cp") ? 2003 : 93) + "))", context);
            }
            String from = " from cp_" + type + " p join ca_" + type + " a on p.k=a.k join b on p.j=b.k";
            matches("select year(p.t),count(),sum(p.v),min(year(a.t)),max(year(a.t))"
                    + from + " group by year(p.t)", 1, true, compiler);
            matches("select extract(year from a.t),count(),sum(year(p.t)),sum(a.v)"
                    + from + " group by extract(year from a.t)", 1, true, compiler);
        }
    }

    @Test public void extractFieldLiteralsReachExistingTemporalBindings() throws Exception {
        for (String type : new String[]{"timestamp", "timestamp_ns"}) {
            String table = "extract_" + type;
            engine.execute("create table " + table + " as (select cast(x%31-15 as int) k,cast(x%17-8 as int) j,"
                    + "cast(case when x%19=0 then null else (x-1000)*1000000001L end as " + type
                    + ") t,x v from long_sequence(2003))", context);
            for (String part : new String[]{"hour", "minute", "second"}) {
                String key = "extract(" + part + " from p.t)";
                matches("select " + key + ",count(),sum(a.v),min(b.v) from " + table
                        + " p join a on p.k=a.k join b on p.j=b.k group by " + key, 1, true, compiler);
            }
        }
    }

    @Test public void temporalPayloadsPreserveUnitsNullsAndColumnTops() throws Exception {
        shardThreshold = 4;
        for (String type : new String[]{"date", "timestamp", "timestamp_ns"}) {
            String parent = "parent_" + type;
            String child = "child_" + type;
            engine.execute("create table " + parent + " as (select cast(x%31-15 as int) k,x v from long_sequence(93))", context);
            engine.execute("alter table " + parent + " add column t " + type, context);
            engine.execute("insert into " + parent + " values (0,94,4294967297),(1,95,-4294967297),(2,96,9223372036854775807)", context);
            engine.execute("create table " + child + " (t " + type + ",v long)", context);
            engine.execute("insert into " + child + " values (null,11),(null,13),(4294967297,17),(-4294967297,19),(9223372036854775807,23)", context);
            String from = " from p join " + parent + " a on p.k=a.k join " + child + " b on a.t=b.t";
            for (long copy : new long[]{0, 128L << 20}) {
                payloadCopyLimit = copy;
                buildThreshold = copy == 0 ? Long.MAX_VALUE : 1;
                // t is both a dependent join key and an observed build-side group key.
                // matches checks native execution and opens the same factory twice.
                matches("select count(),sum(p.v),sum(a.v),min(b.v),max(b.v)" + from, 0, true, compiler);
                matches("select a.t,p.j,count(),sum(p.v),sum(a.v),min(b.v),max(b.v)" + from
                        + " group by a.t,p.j", 2, true, compiler);
            }
        }
    }

    @Test public void typedNullsExtremeKeysAndDictionaryReopen() throws Exception {
        engine.execute("create table tp as (select case when x%7=0 then null else (x%13-6)*4294967297 end l, cast(case when x%11=0 then null else 'K'||cast(x%13 as string) end as symbol) s,cast(x%3 as int) j,x v from long_sequence(2003))", context);
        engine.execute("create table td as (select case when x%7=0 then null else ((39-x)%13-6)*4294967297 end l, cast(case when x%11=0 then null else 'K'||cast((39-x)%13 as string) end as symbol) s,cast((39-x)%3 as int) j,x v from long_sequence(39))", context);
        engine.execute("insert into tp values (9223372036854775807,'extreme',0,7),(-9223372036854775807,'low',0,9)", context);
        engine.execute("insert into td values (9223372036854775807,'extreme',0,11),(-9223372036854775807,'low',0,13)", context);
        buildThreshold = 1;
        shardThreshold = 4;
        for (String key : new String[]{"p.l=d.l", "p.s=d.s", "p.s=d.s and p.j=d.j", "p.l=d.l and p.j=d.j"}) {
            String sql = "select p.j,count(),sum(p.v),sum(d.v) from tp p join td d on " + key + " group by p.j";
            matches(sql, 1, true, compiler);
            context.setQueryCodeGenerator(compiler);
            try (var factory = Demo.compileSql(engine, context, sql, true)) {
                for (int round = 0; round < 2; round++) {
                    context.setQueryCodeGenerator(null);
                    try (var control = Demo.compileSql(engine, context, sql, true)) {
                        assertEquals(sql, Demo.readSql(control, context, 1), Demo.readSql(factory, context, 1));
                    }
                    engine.execute("insert into td select x*4294967297,cast('new-'||cast(x as string) as symbol),cast(x%3 as int),x from long_sequence(17)", context);
                    engine.execute("insert into tp select x*4294967297,cast('new-'||cast(x as string) as symbol),cast(x%3 as int),x from long_sequence(17)", context);
                }
            }
        }
    }

    @Test public void aggregateFamiliesPreserveNullsAndDuplicateProductsAcrossTypedBranches() throws Exception {
        engine.execute("create table gp as (select cast(x%64 as int) k,(x%64-32)*4294967297 l,"
                + "cast('K'||cast(x%64 as string) as symbol) s,cast(x%4 as int) j,"
                + "case when x%4=0 or x%7=0 then null else x%23-11 end v,"
                + "case when x%4=0 or x%7=0 then null else (x%23-11)*0.25 end d from long_sequence(8192))", context);
        engine.execute("create table ga as (select cast((128-x)%64 as int) k,((128-x)%64-32)*4294967297 l,"
                + "cast('K'||cast((128-x)%64 as string) as symbol) s,cast(x%17 as int) nk,"
                + "(x%17)*4294967297+4503599627370496 nl,cast('P'||cast(x%17 as string) as symbol) ns from long_sequence(128))", context);
        engine.execute("create table gb as (select cast((51-x)%17 as int) k,((51-x)%17)*4294967297+4503599627370496 l,"
                + "cast('P'||cast((51-x)%17 as string) as symbol) s,"
                + "case when x%3=0 then null else x%11-5 end v from long_sequence(51))", context);
        String probeAggregates = "count(),count(p.v),count(p.s),count(p.d),sum(p.k),min(p.k),max(p.k),sum(p.v),avg(p.v),min(p.v),max(p.v),"
                + "sum(p.d),avg(p.d),min(p.d),max(p.d),ksum(p.d)";
        shardThreshold = 4;
        for (long copy : new long[]{0, 128L << 20}) {
            payloadCopyLimit = copy;
            buildThreshold = copy == 0 ? Long.MAX_VALUE : 1;
            for (String key : new String[]{"k", "l", "s"}) {
                String from = " from gp p join ga a on p." + key + "=a." + key
                        + " join gb b on a.n" + key + "=b." + key
                        + " join gb c on a.n" + key + "=c." + key;
                // Exercise both a closed forest and an observed build followed by an unobserved leaf.
                for (String aggregates : new String[]{probeAggregates, probeAggregates + ",sum(b.v),count(b.v),ksum(cast(b.v as double))"}) {
                    matches("select " + aggregates + from, 0, true, compiler);
                    // j=0 is an all-NULL group for both probe aggregate value columns.
                    matches("select p.j," + aggregates + from + " group by p.j", 1, true, compiler);
                }
            }
        }
    }

    @Test public void foldingUnobservedInputsPreservesObservedValueOrder() throws Exception {
        pool.halt();
        engine.execute("create table op as (select cast(0 as int) k,x id from long_sequence(512))", context);
        engine.execute("create table oa as (select cast(0 as int) k from long_sequence(2))", context);
        engine.execute("create table ob as (select cast(0 as int) k,"
                + "case when x=1 then 1.0 when x=2 then -1e16 else 1e16 end d from long_sequence(3))", context);
        for (String joins : new String[]{" join oa a on p.k=a.k join ob b on p.k=b.k",
                " join ob b on p.k=b.k join oa a on p.k=a.k"}) {
            String sql = "select p.id,count(),sum(b.d),avg(b.d) from op p" + joins + " group by p.id";
            // The same frozen maps and join order, with the ordinary Java updater,
            // provide the oracle for rounding-sensitive values.
            context.setQueryCodeGenerator(intercepted(true, null));
            java.util.List<CompiledGroupBy.ResultRow> expected;
            try (var control = Demo.compileSql(engine, context, sql, true)) {
                assertTrue(plan(control).contains("compiledJoinPipeline"));
                expected = Demo.readSql(control, context, 1);
            }
            context.setQueryCodeGenerator(compiler);
            try (var candidate = Demo.compileSql(engine, context, sql, true)) {
                assertTrue(plan(candidate).contains("compiledJoinPipeline"));
                assertEquals(sql, expected, Demo.readSql(candidate, context, 1));
                assertEquals(sql, expected, Demo.readSql(candidate, context, 1));
            }
        }
    }

    @Test public void independentBuildsBindPayloadsFiltersAndDuplicateProducts() throws Exception {
        for (long limit : new long[]{0, 128L << 20}) {
            payloadCopyLimit = limit;
            buildThreshold = limit == 0 ? Long.MAX_VALUE : 1;
            matches("select count(),sum(p.v),sum(a.v),sum(b.v) from p join a on p.k=a.k join b on p.j=b.k", 0, true, compiler);
            matches("select count(),sum(p.v),sum(a.v),sum(b.v) from p join a on p.k=a.k join b on p.j=b.k where p.v>5 and a.v>20 and b.v>30 and a.v<b.v", 0, true, compiler);
            matches("select count(),sum(p.v),sum(a.v),sum(b.v) from a join p on p.k=a.k join b on p.j=b.k", 0, true, compiler);
        }
        matches("select count(),sum(c.v) from p join a on p.k=a.k join b on p.j=b.k join a c on p.k=c.k where a.v<c.v", 0, true, compiler);
        matches("select count() from p join a on p.k=a.k join b on p.j=b.k", 0, true, compiler);
        matches("select count(),sum(p.v),sum(a.v),sum(b.v) from p join b on p.j=b.k join a on p.k=a.k", 0, true, compiler);
        matches("select count(),sum(p.amount),sum(a.v),sum(b.v) from (select j,v amount,k from p) p join a on p.k=a.k join b on p.j=b.k", 0, true, compiler);
    }

    @Test public void groupingAndFallbackPreserveDuplicatePositionsAcrossGrowth() throws Exception {
        buildThreshold = 1;
        shardThreshold = 4;
        for (QueryCodeGenerator generator : new QueryCodeGenerator[]{compiler, intercepted(true, null)}) {
            matches("select p.k,count(),sum(p.v),sum(a.v),sum(b.v) from p join a on p.k=a.k join b on p.j=b.k group by p.k", 1, true, generator);
            matches("select b.v,count(),sum(a.v) from p join a on p.k=a.k join b on p.j=b.k where a.v<b.v group by b.v", 1, true, generator);
        }
        engine.execute("alter table p add column late long", context);
        engine.execute("insert into p select cast(x%31-15 as int),cast(x%17-8 as int),x,x*9 from long_sequence(301)", context);
        matches("select count(),sum(p.late),sum(a.v),sum(b.v) from p join a on p.k=a.k join b on p.j=b.k", 0, true, compiler);
    }

    @Test public void unsupportedShapesRestoreOrdinaryPlan() throws Exception {
        matches("select count(),sum(p.v) from p join a on p.k=a.k join b on a.k=b.k", 0, true, compiler);
        matches("select count(),sum(p.v) from p join a on p.k=a.k and p.v=a.v join b on p.j=b.k", 0, true, compiler);
        matches("select count(),sum(p.v) from p left join a on p.k=a.k join b on p.j=b.k", 0, false, compiler);
        matches("select count_distinct(p.v) from p join a on p.k=a.k join b on p.j=b.k where a.v>20", 0, false, compiler);
    }

    @Test public void compositeGroupsShareTheExistingKeyEncoding() throws Exception {
        engine.execute("alter table a add column label symbol", context);
        engine.execute("alter table b add column label symbol", context);
        engine.execute("insert into a select cast(x%31-15 as int),x,cast('a-'||(x%7) as symbol) from long_sequence(93)", context);
        engine.execute("insert into b select cast(x%17-8 as int),x,cast('b-'||(x%5) as symbol) from long_sequence(51)", context);
        shardThreshold = 4;
        for (long limit : new long[]{0, 128L << 20}) {
            payloadCopyLimit = limit;
            buildThreshold = limit == 0 ? Long.MAX_VALUE : 1;
            for (QueryCodeGenerator generator : new QueryCodeGenerator[]{compiler, intercepted(true, null)}) {
                matches("select p.k,a.label,b.label,count(),count(a.label),count(b.label),sum(p.v),sum(a.v-b.v) from p join a on p.k=a.k join b on p.j=b.k group by p.k,a.label,b.label", 3, true, generator);
                matches("select a.v,b.v,count(),sum(p.v) from p join a on p.k=a.k join b on p.j=b.k group by a.v,b.v", 2, true, generator);
                matches("select p.k+1,a.label,count(),sum(b.v) from p join a on p.k=a.k join b on p.j=b.k group by p.k+1,a.label", 2, true, generator);
            }
        }
        matches("select p.k,length(a.label),count() from p join a on p.k=a.k join b on p.j=b.k group by p.k,length(a.label)", 2, false, compiler);
    }

    @Test public void preparedLayoutsRebindAcrossMixedUniqueAndDuplicateBuilds() throws Exception {
        long liveCode = LlvmBackend.liveFunctions();
        buildThreshold = 1;
        shardThreshold = 4;
        engine.execute("create table rp as (select cast(x%16 as int) k,x%16+4503599627370496 l,cast('K'||(x%16) as symbol) s,cast(x%3 as int) j,x%23-11 v from long_sequence(10003))", context);
        String rows = "select cast(x%16 as int) k,x%16+4503599627370496 l,cast('K'||(x%16) as symbol) s,cast(x%16 as int) nk,x%16+4503599627370496 nl,cast('K'||(x%16) as symbol) ns,case when x%7=0 then null else x%29-14 end v,case when x%5=0 then null else (x%17-8)*0.25 end d from long_sequence(";
        engine.execute("create table ra as ("+rows+"48))", context);
        engine.execute("create table rb as ("+rows+"16))", context);
        engine.execute("create table rc as ("+rows+"48))", context);
        for (String type : new String[]{"k", "l", "s"}) {
            String sql = "select p.j,count(),sum(p.v),sum(a.v),sum(b.v),sum(c.v),min(c.v),max(c.d),avg(c.d),count(c.s) from rp p join ra a on p."+type+"=a."+type+" join rb b on a.n"+type+"=b."+type+" join rc c on b."+type+"=c."+type+" group by p.j";
            context.setQueryCodeGenerator(compiler);
            try (var factory = Demo.compileSql(engine, context, sql, true)) {
                assertTrue(plan(factory), plan(factory).contains("compiledJoinPipeline"));
                for (int round = 0; round < 5; round++) {
                    context.setQueryCodeGenerator(null);
                    try (var control = Demo.compileSql(engine, context, sql, true)) {
                        var expected = Demo.readSql(control, context, 1);
                        assertEquals(type+" round "+round, expected, Demo.readSql(factory, context, 1));
                        assertEquals(type+" reused round "+round, expected, Demo.readSql(factory, context, 1));
                    }
                    if (round == 0) {
                        engine.execute("truncate table rb", context);
                        engine.execute("insert into rb "+rows+"48)", context);
                    } else if (round == 1) {
                        engine.execute("truncate table ra", context);
                        engine.execute("insert into ra "+rows+"16)", context);
                    } else if (round == 2) engine.execute("truncate table rc", context);
                    else if (round == 3) {
                        engine.execute("insert into rc "+rows+"48)", context);
                    }
                }
            }
            payloadCopyLimit = 128L << 20;
            engine.execute("truncate table ra", context);
            engine.execute("insert into ra "+rows+"48)", context);
            engine.execute("truncate table rb", context);
            engine.execute("insert into rb "+rows+"16)", context);
        }
        assertEquals(liveCode, LlvmBackend.liveFunctions());
    }

    @Test public void symbolResidualsUseEachInputsDictionaryAndRebindOnReopen() throws Exception {
        shardThreshold = 4;
        engine.execute("alter table p add column label symbol", context);
        engine.execute("insert into p select cast(x%31-15 as int),cast(x%17-8 as int),x,"
                + "cast(case when x%5=0 then null else 'key-'||(x%3) end as symbol) from long_sequence(2003)", context);
        engine.execute("create table labels as (select cast(x-15 as int) k,"
                + "cast(case when x%5=0 then null else 'key-'||((31-x)%3) end as symbol) label from long_sequence(31))", context);
        String from = " from p join labels a on p.k=a.k join b on p.j=b.k where ";
        for (long copy : new long[]{0, 128L << 20}) {
            payloadCopyLimit = copy;
            for (String predicate : new String[]{
                    "(p.label='key-1' and a.label='key-2') or (p.label='key-2' and a.label='key-1')",
                    "(p.label in ('key-1',null,'absent','key-1') and a.label!='key-2') or a.label='absent'",
                    "p.label='x' or a.label='key-1'", "p.label=null or a.label!=null"}) {
                matches("select p.j,count(),sum(p.v),min(b.v),max(b.v)" + from + predicate + " group by p.j", 1, true, compiler);
            }
        }
        String sql = "select count(),sum(p.v),min(b.v),max(b.v)" + from + "p.label='key-2' or a.label in ('new','absent',null,'new')";
        context.setQueryCodeGenerator(compiler);
        try (var factory = Demo.compileSql(engine, context, sql, true)) {
            assertTrue(plan(factory), plan(factory).contains("compiledJoinPipeline"));
            RecordCursorFactory base = factory;
            while (!(base instanceof AsyncHashJoinGroupByRecordCursorFactory)) base = base.getBaseFactory();
            var atom = ((AsyncHashJoinGroupByRecordCursorFactory) base).getAtom();
            for (int round = 0; round < 3; round++) {
                context.setQueryCodeGenerator(null);
                try (var control = Demo.compileSql(engine, context, sql, true)) {
                    var expected = Demo.readSql(control, context, 0);
                    long before = atom.getCompiledJoinNativeRows();
                    assertEquals(expected, Demo.readSql(factory, context, 0));
                    assertEquals(expected, Demo.readSql(factory, context, 0));
                    assertTrue(atom.getCompiledJoinNativeRows() > before);
                }
                engine.execute("truncate table labels", context);
                engine.execute("insert into labels select cast(x%31-15 as int),cast(case when x%5=0 then null else '"
                        + (round == 0 ? "new" : "key-2") + "' end as symbol) from long_sequence(62)", context);
                engine.execute("truncate table p", context);
                engine.execute("insert into p select cast(x%31-15 as int),cast(x%17-8 as int),x,"
                        + "cast(case when x%5=0 then null else 'key-'||((" + (4007 + round)
                        + "-x)%3) end as symbol) from long_sequence(4006)", context);
            }
        }
    }

    @Test public void numericCasesUseResolvedBranchesAcrossJoinedInputs() throws Exception {
        engine.execute("create table cases as (select cast(x%31-15 as int) k,"
                + "cast(x%17-8 as byte) n,cast(x%17-8 as short) s,"
                + "cast(case when x%7=0 then null else x%17-8 end as int) i,"
                + "case when x%7=0 then null else (x%17-8)*4294967297L end l,"
                + "cast(case when x%7=0 then null else (x%17-8)*0.5 end as float) f,"
                + "case when x%7=0 then null else (x%17-8)*0.5 end d,"
                + "cast(case when x%5=0 then null else 'key-'||(x%3) end as symbol) label from long_sequence(93))", context);
        String from = " from p join cases a on p.k=a.k join b on p.j=b.k";
        for (long copy : new long[]{0, 128L << 20}) {
            payloadCopyLimit = copy;
            for (String column : new String[]{"n", "s", "i", "l", "f", "d"}) {
                String type = switch (column) {
                    case "n" -> "byte"; case "s" -> "short"; case "i" -> "int";
                    case "l" -> "long"; case "f" -> "float"; default -> "double";
                };
                String value = "case when p.v<700 then a." + column
                        + " when p.v<1400 then case when a.label in ('key-1',null) then cast(3 as " + type + ") end end";
                // Existing aggregate admission is narrower than numeric expression support.
                // Keep the CASE's resolved width, then widen for this complete aggregate set.
                if (column.equals("n") || column.equals("s")) value = "cast(" + value + " as int)";
                if (column.equals("f")) value = "cast(" + value + " as double)";
                matches("select p.j,count(" + value + "),sum(" + value + "),min(" + value + "),max(" + value
                        + "),avg(" + value + ")" + from + " group by p.j", 1, true, compiler);
            }
            String key = "case when p.v<700 then a.i when p.v<1400 then a.l else p.j end";
            String value = "case when a.label='key-1' then a.i when p.v<1400 then a.d else b.v end";
            matches("select " + key + ",count(),sum(" + value + "),min(" + value + "),max(" + value
                    + ")" + from + " group by " + key, 1, true, compiler);
        }
        // Unsupported conversions must retain ordinary short-circuit evaluation.
        engine.execute("alter table cases add column invalid varchar", context);
        engine.execute("update cases set invalid='not-a-number'", context);
        matches("select sum(case when p.v>0 then a.i else cast(a.invalid as long) end)"
                + from + " where p.v>0", 0, false, compiler);
    }

    @Test public void integralSwitchesPreserveKeysNullsAndReboundSymbols() throws Exception {
        engine.execute("create table choices as (select cast(x%31-15 as int) k,"
                + "case when x%11=0 then null else (x%3-1)*4294967297L end l,"
                + "cast(case when x%7=0 then null else 'key-'||(x%3) end as symbol) s,x*0.5 v from long_sequence(93))", context);
        String from = " from p join choices a on p.k=a.k join b on p.j=b.k";
        for (String value : new String[]{
                "case a.k when -1 then p.v when 0 then a.v else b.v end",
                "case a.l when -4294967297L then p.v when 4294967297L then a.v when null then b.v end",
                "case a.s when 'key-1' then p.v else a.v end",
                "case a.s when 'key-1' then p.v when 'key-2' then a.v when null then b.v end",
                "case a.s when 'key-1' then p.v when 'key-2' then a.v when 'key-0' then b.v when 'absent' then 11 else 0 end",
                "case a.s when 'key-1' then 9 end", "case a.s when 'absent' then 9 else -1 end"}) {
            matches("select p.j,count(" + value + "),sum(" + value + "),min(" + value + "),max(" + value
                    + ")" + from + " group by p.j", 1, true, compiler);
        }
        StringBuilder large = new StringBuilder("case a.k");
        for (int i = 0; i < 17; i++) large.append(" when ").append(i).append(" then a.v");
        matches("select sum(" + large + " else p.v end)" + from, 0, false, compiler);

        String sql = "select count(case a.s when 'new' then 1 end),sum(case a.s when 'new' then a.v "
                + "when 'key-1' then p.v when 'key-2' then b.v when null then 11 else 0 end)" + from;
        context.setQueryCodeGenerator(compiler);
        try (var factory = Demo.compileSql(engine, context, sql, true)) {
            assertTrue(plan(factory), plan(factory).contains("compiledJoinPipeline"));
            RecordCursorFactory base = factory;
            while (!(base instanceof AsyncHashJoinGroupByRecordCursorFactory)) base = base.getBaseFactory();
            var atom = ((AsyncHashJoinGroupByRecordCursorFactory) base).getAtom();
            for (int round = 0; round < 3; round++) {
                context.setQueryCodeGenerator(null);
                try (var control = Demo.compileSql(engine, context, sql, true)) {
                    var expected = Demo.readSql(control, context, 0);
                    long before = atom.getCompiledJoinNativeRows();
                    assertEquals(expected, Demo.readSql(factory, context, 0));
                    assertEquals(expected, Demo.readSql(factory, context, 0));
                    assertTrue(atom.getCompiledJoinNativeRows() > before);
                }
                engine.execute("truncate table choices", context);
                engine.execute("insert into choices select cast(x%31-15 as int),null,cast(case when x%7=0 then null else '"
                        + (round == 0 ? "new" : "key-1") + "' end as symbol),x*0.5 from long_sequence(62)", context);
            }
        }
    }

    @Test public void compositeSymbolGroupsRebindAfterBuildChanges() throws Exception {
        engine.execute("create table labels as (select cast(x-15 as int) k,cast('old-'||(x%3) as symbol) label from long_sequence(31))", context);
        String sql = "select p.j,labels.label,count(),sum(b.v) from p join labels on p.k=labels.k join b on p.j=b.k group by p.j,labels.label";
        context.setQueryCodeGenerator(compiler);
        try (var factory = Demo.compileSql(engine, context, sql, true)) {
            assertTrue(plan(factory), plan(factory).contains("compiledJoinPipeline"));
            for (int round = 0; round < 3; round++) {
                context.setQueryCodeGenerator(null);
                try (var control = Demo.compileSql(engine, context, sql, true)) {
                    assertEquals(Demo.readSql(control, context, 2), Demo.readSql(factory, context, 2));
                }
                engine.execute("truncate table labels", context);
                if (round == 0) engine.execute("insert into labels select cast(x-15 as int),cast(case when x%3=0 then null else 'new-'||(x%5) end as symbol) from long_sequence(31)", context);
            }
        }
    }

    @Test public void compositeNumericWidthsIncludeNullsAndUnalignedValues() throws Exception {
        engine.execute("create table widths as (select cast(x%31-15 as int) k,cast(x-70 as byte) n,cast(x*3-100 as short) s,"
                + "case when x%5=0 then null else (x%9-4)*0.5 end d from long_sequence(93))", context);
        shardThreshold = 4;
        matches("select p.k,widths.n,widths.s,widths.d,count(),sum(b.v) from p join widths on p.k=widths.k join b on p.j=b.k group by p.k,widths.n,widths.s,widths.d", 4, true, compiler);
        matches("select cast(p.k as long),b.v,count() from p join a on p.k=a.k join b on p.j=b.k group by cast(p.k as long),b.v", 2, true, compiler);
    }

    @Test public void nativeFailureDrainsAndReopensWithChangedAndEmptyBuilds() throws Exception {
        buildThreshold = 1;
        AtomicBoolean fail = new AtomicBoolean(true);
        String sql = "select count(),sum(p.v),sum(b.v) from p join a on p.k=a.k join b on p.j=b.k";
        context.setQueryCodeGenerator(intercepted(false, fail));
        try (var factory = Demo.compileSql(engine, context, sql, true)) {
            assertTrue(plan(factory).contains("compiledJoinPipeline"));
            assertThrows(CairoException.class, () -> Demo.readSql(factory, context, 0));
            fail.set(false);
            context.setQueryCodeGenerator(null);
            try (var control = Demo.compileSql(engine, context, sql, true)) {
                assertEquals(Demo.readSql(control, context, 0), Demo.readSql(factory, context, 0));
            }
            engine.execute("truncate table b", context);
            assertEquals(0, Demo.readSql(factory, context, 0).getFirst().values().getFirst().longValue());
            engine.execute("insert into b select cast(x-8 as int),x from long_sequence(17)", context);
            try (var control = Demo.compileSql(engine, context, sql, true)) {
                assertEquals(Demo.readSql(control, context, 0), Demo.readSql(factory, context, 0));
            }
        }
    }

    @Test public void cancellationInSecondBuildReleasesFirstBuildAndCanRetry() throws Exception {
        String sql = "select count(),sum(p.v),sum(b.v) from p join a on p.k=a.k join b on p.j=b.k";
        context.setQueryCodeGenerator(null);
        java.util.List<CompiledGroupBy.ResultRow> expected;
        try (var baseline = Demo.compileSql(engine, context, sql, true)) {
            expected = Demo.readSql(baseline, context, 0);
        }
        for (long threshold : new long[]{Long.MAX_VALUE, 1}) {
            buildThreshold = threshold;
            context.setQueryCodeGenerator(compiler);
            try (var factory = Demo.compileSql(engine, context, sql, true)) {
                RecordCursorFactory base = factory;
                while (!(base instanceof AsyncHashJoinGroupByRecordCursorFactory)) base = base.getBaseFactory();
                var atom = ((AsyncHashJoinGroupByRecordCursorFactory) base).getAtom();
                Thread owner = Thread.currentThread();
                AtomicBoolean hit = new AtomicBoolean();
                var breaker = new AtomicBooleanCircuitBreaker(engine) {
                    private void checkSecondBuild() {
                        if (!hit.get() && Thread.currentThread() == owner && atom.getFrozenBuild() != null
                                && StackWalker.getInstance().walk(frames -> frames.anyMatch(frame ->
                                    frame.getClassName().equals("io.questdb.griffin.engine.table.HashJoinBuildContext")
                                    && frame.getMethodName().equals("build")))) {
                            hit.set(true);
                            cancel();
                        }
                    }
                    @Override public int getState() { checkSecondBuild(); return super.getState(); }
                    @Override public void statefulThrowExceptionIfTrippedNoThrottle() {
                        checkSecondBuild(); super.statefulThrowExceptionIfTrippedNoThrottle();
                    }
                };
                SqlExecutionCircuitBreaker original = context.getCircuitBreaker();
                context.with(breaker);
                try {
                    assertThrows(CairoException.class, () -> Demo.readSql(factory, context, 0));
                    assertTrue("cancelled after first build froze", hit.get());
                    assertNull(atom.getFrozenBuild());
                } finally {
                    context.with(original);
                }
                assertEquals(expected, Demo.readSql(factory, context, 0));
            }
        }
    }

    @Test public void boundedIntegralHeadsPreserveNullsDuplicatesAndFullWidthDomains() throws Exception {
        String[] types = {"int", "long", "long", "symbol"};
        String[] bases = {"0", "9223372036854775777", "-9223372036854775807", "0"};
        for (int kind = 0; kind < types.length; kind++) {
            String value = kind == 3 ? "'s-'||cast(x%17 as string)" : "x%17+" + bases[kind];
            String reverse = kind == 3 ? "'s-'||cast((53-x)%17 as string)" : "(53-x)%17+" + bases[kind];
            engine.execute("create table np" + kind + " as (select cast(case when x%13=0 then null else " + value
                    + " end as " + types[kind] + ") k,cast(x%7 as int) j,"
                    + "case when x%11=0 then null else x%97-48 end v,"
                    + "case when x%5=0 then null else (x%31-15)/4.0 end d from long_sequence(5003))", context);
            engine.execute("create table na" + kind + " as (select cast(case when x%19=0 then null else " + reverse
                    + " end as " + types[kind] + ") k,x%7-3 w from long_sequence(53))", context);
            String from = " from np" + kind + " p join na" + kind + " a on p.k=a.k join b on p.j=b.k";
            matches("select count(),count(p.v),sum(p.v),min(p.v),max(p.v),avg(p.v),"
                    + "sum(p.d),min(p.d),max(p.d),avg(p.d),sum(a.w)" + from, 0, true, compiler);
            matches("select p.j,count(),sum(p.v),min(a.w),max(a.w),avg(p.d)" + from + " group by p.j", 1, true, compiler);
        }
    }

    private QueryCodeGenerator intercepted(boolean fallback, AtomicBoolean fail) {
        return (QueryCodeGenerator) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{QueryCodeGenerator.class}, (proxy, method, args) -> {
            Object result = invoke(method, compiler, args);
            if (!(result instanceof CompiledJoin code)) return result;
            return (CompiledJoin) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{CompiledJoin.class}, (p, m, a) -> {
                Object value = invoke(m, code, a);
                if (!(value instanceof CompiledJoin.Updater updater)) return value;
                return Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[]{CompiledJoin.Updater.class}, (pp, mm, aa) -> {
                    if (mm.getName().equals("updateMulti") || mm.getName().equals("updateKeyedMulti")) {
                        if (fallback) return false;
                        if (fail != null && fail.get()) throw CairoException.nonCritical().put("injected multiway failure");
                    }
                    return invoke(mm, updater, aa);
                });
            });
        });
    }

    private static Object invoke(java.lang.reflect.Method method, Object target, Object[] args) throws Throwable {
        try { return method.invoke(target, args); }
        catch (InvocationTargetException e) { throw e.getCause(); }
    }

    private void matches(String sql, int keyCount, boolean compiled, QueryCodeGenerator generator) throws Exception {
        matches(sql, sql, keyCount, compiled, generator);
    }

    private void matches(String sql, String controlSql, int keyCount, boolean compiled, QueryCodeGenerator generator) throws Exception {
        context.setQueryCodeGenerator(null);
        try (var baseline = Demo.compileSql(engine, context, controlSql, true)) {
            var expected = Demo.readSql(baseline, context, keyCount);
            context.setQueryCodeGenerator(generator);
            try (var candidate = Demo.compileSql(engine, context, sql, true)) {
                String plan = plan(candidate);
                assertEquals(sql + " / " + plan, compiled, plan.contains("compiledJoinPipeline"));
                if (compiled && sql.indexOf(" join ") != sql.lastIndexOf(" join ")) assertTrue(plan, plan.contains("buildCount:"));
                try (var ignored = candidate.getCursor(context)) { }
                assertEquals(sql, expected, Demo.readSql(candidate, context, keyCount));
                assertEquals(sql, expected, Demo.readSql(candidate, context, keyCount));
                if (compiled && generator == compiler) {
                    RecordCursorFactory base = candidate;
                    while (!(base instanceof AsyncHashJoinGroupByRecordCursorFactory)) base = base.getBaseFactory();
                    assertTrue(sql, ((AsyncHashJoinGroupByRecordCursorFactory) base).getAtom().getCompiledJoinNativeRows() > 0);
                }
            }
        }
    }

    private String plan(RecordCursorFactory factory) {
        var sink = new TextPlanSink(); sink.of(factory, context);
        var text = new StringBuilder();
        for (int i = 1; i <= sink.getLineCount(); i++) text.append(sink.getLine(i)).append('\n');
        return text.toString();
    }
}
