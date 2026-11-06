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
import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.functions.bind.BindVariableServiceImpl;
import io.questdb.mp.WorkerPool;
import io.questdb.mp.WorkerPoolUtils;
import io.questdb.std.MemoryTag;
import io.questdb.std.ObjList;
import io.questdb.std.Unsafe;
import io.questdb.test.QueryCodegenTestSupport;
import io.questdb.test.griffin.fuzz.GeneratedQuery;
import io.questdb.test.griffin.fuzz.QueryRunner;
import io.questdb.test.griffin.fuzz.QueryShape;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/** Run through GriffinTestRunner so the existing compiler and native-call audit apply. */
public class SqlCompositionStressTest {
    @Rule public TemporaryFolder temporary = new TemporaryFolder();
    private CairoEngine engine;
    private SqlExecutionContextImpl context;
    private WorkerPool pool;
    private QueryRunner runner;
    private long memory, functions;
    private final List<String> failures = new ArrayList<>();
    private int queries, nativeQueries, floatingMatches, expectedRejections;

    @Before public void setUp() throws Exception {
        memory = Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT);
        functions = LlvmBackend.liveFunctions();
        var config = new DefaultCairoConfiguration(temporary.newFolder().toString()) {
            @Override public int getSqlPageFrameMaxRows() { return 128; }
            @Override public int getSqlPageFrameMinRows() { return 64; }
            @Override public int getGroupByShardingThreshold() { return 256; }
            @Override public int getSqlSmallMapKeyCapacity() { return 4; }
        };
        engine = new CairoEngine(config);
        pool = new WorkerPool(() -> 2);
        WorkerPoolUtils.setupQueryJobs(pool, engine);
        pool.start();
        context = QueryCodegenTestSupport.configure(new SqlExecutionContextImpl(engine, 2).with(
                config.getFactoryProvider().getSecurityContextFactory().getRootContext(),
                new BindVariableServiceImpl(config), null, -1, null));
        runner = new QueryRunner(engine, context, true, false, true, true, new ObjList<>(), null);
        engine.execute("create table f as (select x id,cast(case when x%19=0 then null else x%31-15 end as int) i,"
                + "case when x%17=0 then null else 4503599627370496+(x%31)*4294967297 end l,"
                + "case when x%13=0 then null else (x%127-63)/8.0 end d,"
                + "cast(case when x%11=0 then null else 's'||(x%31) end as symbol) s,"
                + "cast(case when x%7=0 then null when x%7=1 then '' when x%7=2 then 'žluťoučký🐈' "
                + "when x%7=3 then 'a,b|c' else 'https://example.org/'||(x%31) end as varchar) v,"
                + "cast(x*1000000 as timestamp) ts from long_sequence(4099)) timestamp(ts) partition by hour", context);
        engine.execute("create table g as (select x id,cast(case when x%19=0 then null else (197-x)%31-15 end as int) i,"
                + "case when x%17=0 then null else 4503599627370496+((197-x)%31)*4294967297 end l,"
                + "cast(case when x%11=0 then null else 's'||((197-x)%31) end as symbol) s,"
                + "x/8.0 d,cast(x*7000000 as timestamp) ts from long_sequence(197)) timestamp(ts)", context);
        engine.execute("create table edge as (select x id,cast(x%5 as int) k,"
                + "case when x%5=0 then null when x%5=1 then 9223372036854775807 "
                + "when x%5=2 then -9223372036854775807 when x%5=3 then 9007199254740993 else -1 end l,"
                + "case when x%5=0 then null when x%5=1 then 1e100 when x%5=2 then -1e100 "
                + "when x%5=3 then -0.0 else 0.125 end d from long_sequence(1027))", context);
    }

    @After public void tearDown() {
        if (pool != null) pool.halt();
        if (context != null) context.close();
        if (engine != null) { assertEquals(0, engine.getBusyReaderCount()); engine.close(); }
        assertEquals(memory, Unsafe.getMemUsedByTag(MemoryTag.NATIVE_JIT));
        assertEquals(functions, LlvmBackend.liveFunctions());
    }

    @Test public void compositions() {
        String[] keys = {"i", "l", "s", "d", "v", "i,s", "length(v)",
                "length(lower(s))", "case when i<0 then cast(i as long) else l end"};
        for (String key : keys) {
            check("typed-mixed", "select " + key + ",count(),count(l),sum(i+3),sum(l),min(l),max(l),avg(l),"
                    + "sum(d),min(d),max(d),avg(d) from f where not (i<0 and s in ('s2','s9')) group by " + key);
            check("distinct-null", "select " + key + ",count_distinct(l),count_distinct(i),min(v),max(v) "
                    + "from f where i is null or (v is not null and l>0) group by " + key);
        }
        for (String predicate : new String[]{"id<0", "i is null", "id>0"}) {
            check("empty-and-null", "select count(),count(i),sum(i),sum(l+1),min(l),max(l),avg(l),"
                    + "sum(d),min(d),max(d),avg(d),count_distinct(s) from f where " + predicate);
            check("nested-map", "select k,count(),sum(n),min(lo),max(hi) from "
                    + "(select i k,s,count() n,min(l) lo,max(l) hi from f where " + predicate
                    + " group by i,s) group by k order by k");
        }
        check("extreme-long", "select k,count(),count(l),sum(l),min(l),max(l),avg(l) from edge group by k");
        check("extreme-double", "select k,count(d),sum(d),min(d),max(d),avg(d) from edge group by k");
        check("overflow-expressions", "select k,sum(l*3+7),min(l-13),max(l+29),count_distinct(l*7) from edge group by k");
        check("double-conditional", "select i,sum(case when d is null then -0.0 when d<0 then d*2 else d/2 end),"
                + "min(case when i<0 then d else null end),max(d) from f group by i");
        check("case-varchar", "select case when i<0 then v when s is null then cast('🐈' as varchar) else cast(s as varchar) end k,"
                + "count(),sum(length(v)),min(v),max(v) from f group by k order by count() desc,k limit 23");
        check("case-temporal", "select case when i<0 then ts else cast(l as timestamp) end k,count(),min(l),max(l) "
                + "from f group by k order by k limit 7,31");
        check("nested-cte", "with a as (select i,s,sum(d) total,count() n from f group by i,s),"
                + " b as (select i,count() groups,sum(n) n,min(total) lo,max(total) hi from a group by i) "
                + "select case when i<0 then -1 else 1 end side,sum(n),min(lo),max(hi) from b group by side");
        check("outer-filter-sort", "select i,n,hi-lo spread from "
                + "(select i,count() n,min(l) lo,max(l) hi from f group by i) where n>3 order by spread desc,i limit 3,17");
        for (String key : new String[]{"i", "l", "s", "i,s"}) {
            checkJoin("inner-" + key, "select a.i,count(),sum(a.l),min(b.id),max(a.d),avg(b.d) "
                    + "from f a join g b on (" + key + ") where a.id>b.id and (a.i<0 or b.d>2) group by a.i");
            checkJoin("left-" + key, "select a.i,count(),count(b.id),sum(a.d),min(b.l),max(b.id) "
                    + "from f a left join g b on (" + key + ") group by a.i");
            checkJoin("three-way-" + key, "select a.i,count(),sum(a.d),min(b.l),max(c.id) "
                    + "from f a join g b on (" + key + ") join g c on b.i=c.i where b.id<c.id group by a.i");
            check("asof-" + key, "select a.i,count(),count(b.id),sum(a.d),min(b.l),max(b.id) "
                    + "from f a asof join (select * from g where id>3) b on (" + key + ") tolerance 20s group by a.i");
        }
        check("union-aggregate", "select k,sum(n),min(lo),max(hi) from "
                + "(select i k,count() n,min(l) lo,max(l) hi from f where i<0 group by i "
                + "union all select i k,count() n,min(l) lo,max(l) hi from f where i>=0 or i is null group by i) group by k");
        check("window-on-aggregate", "select i,n,sum(n) over (order by i rows between 2 preceding and current row) "
                + "from (select i,count() n from f group by i) order by i");
        check("aggregate-on-window", "select i,count(),min(n),max(n),sum(n) from "
                + "(select i,row_number() over (partition by i order by ts) n from f) group by i");
        check("sample-by", "select ts,s,count(),min(l),max(l),sum(d) from f sample by 5m align to calendar");
        check("latest-aggregate", "select s,count(),sum(d),min(l),max(l) from "
                + "(select * from f latest on ts partition by s) group by s");
        check("horizon-literal", "select h.offset,a.ts,b.id from f a horizon join g b list (0s,2s,4s) as h "
                + "where a.v!='https://example.org/3' order by h.offset,a.ts");
        check("horizon-symbol", "select h.offset,a.ts,b.id from f a horizon join g b list (0s,2s,4s) as h "
                + "where a.s like 's1%' order by h.offset,a.ts");
        check("window-join-literal", "select a.ts,count(b.id),min(b.l),max(b.l) from f a window join g b "
                + "range between 2 second preceding and 2 second following where a.v!='https://example.org/3' order by a.ts");
        check("window-join-symbol", "select a.ts,count(b.id),min(b.l),max(b.l) from f a window join g b "
                + "range between 2 second preceding and 2 second following where a.s like 's1%' order by a.ts");
        check("top-k-offset", "select s,v,count() n,min(l),max(l) from f group by s,v order by n desc,s,v limit 9,47");
        check("negative-limit", "select s,v,count() n,min(l),max(l) from f group by s,v order by n desc,s,v limit -23");
        check("regexp-key", "select regexp_replace(v,'^https?://([^/]+)/.*$','$1') k,count(),min(v),max(v),sum(length(v)) "
                + "from f where v is not null group by k order by k");
        for (String symbol : new String[]{"s0", "missing", "s19"}) {
            String literal = "select i,count(),sum(l),min(d),max(d) from f where s='" + symbol + "' and l>100 group by i";
            ObjList<String> names = new ObjList<>(); names.add("sym"); names.add("floor");
            ObjList<String> values = new ObjList<>(); values.add(symbol); values.add("100");
            check("bind-symbol", new GeneratedQuery(literal, true).withBind(
                    "select i,count(),sum(l),min(d),max(d) from f where s=:sym::string and l>:floor::long group by i", names, values));
        }
        String expression = "l";
        for (int depth = 0; depth < 32; depth++) expression = "case when i=" + (depth-16) + " then l+" + depth + " else (" + expression + ") end";
        check("deep-case", "select s,sum(" + expression + "),min(l),max(l) from f group by s");
        StringBuilder wide = new StringBuilder("select i,count()");
        for (int i=0;i<64;i++) wide.append(",sum(l+").append(i).append(") a").append(i);
        check("wide-aggregate", wide + " from f where i is not null group by i");
        String predicate = "i is null";
        for (int i=0;i<64;i++) predicate = "(" + predicate + " or (i=" + (i-32) + " and l>" + i + "))";
        check("deep-predicate", "select s,count(),sum(l),min(d),max(d) from f where " + predicate + " group by s");
        System.out.println("COMPOSITION result queries="+queries+" nativeQueries="+nativeQueries+" floatingToleranceMatches="+floatingMatches+" failures="+failures.size());
        assertTrue(String.join("\n", failures), failures.isEmpty());
        if (context.getQueryCodeGenerator() != null) assertTrue("No native coverage", nativeQueries>0);
    }

    @Test public void shapeBoundaries() throws Exception {
        // Selected-row tails around vector/frame and sharding boundaries; physical tails are tested separately.
        for (int n : new int[]{0,1,2,31,32,33,63,64,65,127,128,129,255,256,257}) {
            check("tail-"+n, "select s,count(),count(l),sum(l),min(l),max(l),avg(d),count_distinct(v) from f where id<="+n+" group by s");
            check("tail-sort-"+n, "select s,count() n,min(l),max(l) from f where id<="+n+" group by s order by n desc,s limit 3");
            check("tail-sparse-"+n, "select count(),sum(l),min(d),max(d) from f where id<="+n+" and (id%64=1 or i is null)");
        }
        // Vary empty sides and ON/WHERE placement independently of key representation.
        for (String key : new String[]{"i","l","s","i,s"}) {
            for (String side : new String[]{"a","b"}) {
                for (String join : new String[]{"join","left join","right join"}) {
                    String a=side.equals("a")?"(select * from f where id<0)":"f";
                    String b=side.equals("b")?"(select * from g where id<0)":"g";
                    checkJoin("empty-"+side+"-"+join+"-"+key, "select a."+key.split(",")[0]+",count(),count(a.id),count(b.id),sum(a.l),min(b.id),max(b.id) from "+a+" a "+join+" "+b+" b on ("+key+") group by a."+key.split(",")[0]);
                }
            }
            String on=key.contains(",")?"a.i=b.i and a.s=b.s":"a."+key+"=b."+key;
            checkJoin("outer-on-"+key, "select a.i,count(),count(b.id),sum(a.l),min(b.id),max(b.id) from f a left join g b on ("+on+" and b.id%3=0) group by a.i");
            checkJoin("outer-where-"+key, "select a.i,count(),count(b.id),sum(a.l),min(b.id),max(b.id) from f a left join g b on ("+on+") where b.id is null or b.id%3=0 group by a.i");
            checkJoin("anti-"+key, "select a.i,count(),sum(a.l),min(a.id),max(a.id) from f a left join (select * from g where id<17) b on ("+key+") where b.id is null group by a.i");
        }
        // Build-side aggregation, probe-side aggregation, repeated CTEs, and nullable membership.
        for (String key : new String[]{"i","l","s"}) {
            checkJoin("aggregate-join-aggregate-"+key,"select a."+key+",sum(a.n*b.n),min(a.lo),max(b.hi) from "
                    +"(select "+key+",count() n,min(id) lo from f group by "+key+") a join "
                    +"(select "+key+",count() n,max(id) hi from g group by "+key+") b on ("+key+") group by a."+key);
            checkJoin("distinct-build-"+key,"select a.i,count(),sum(a.l),min(a.id),max(a.id) from f a join (select distinct "+key+" from g) b on ("+key+") group by a.i");
            for (String neg : new String[]{"","not "}) {
                checkMembership(key,"subquery-membership-"+key+"-"+neg,"select s,count(),sum(l),min(id),max(id) from f where "+key+" "+neg+"in (select "+key+" from g where id%3=0) group by s");
                checkMembership(key,"empty-membership-"+key+"-"+neg,"select s,count(),sum(l),min(id),max(id) from f where "+key+" "+neg+"in (select "+key+" from g where id<0) group by s");
            }
        }
        checkJoin("shared-cte","with a as (select s,count() n,min(id) lo,max(id) hi from f group by s) select x.s,x.n+y.n,x.lo,y.hi from a x join a y on x.s=y.s where x.n>10 order by x.s");
        for (String op : new String[]{"union","union all","intersect","except","except all"}) {
            check("set-"+op,"select s,count(),sum(n),min(lo),max(hi) from (select s,count() n,min(l) lo,max(l) hi from f where i<3 group by s "+op+" select s,count() n,min(l) lo,max(l) hi from f where i>0 group by s) group by s");
        }
        // Multiple variable-width keys: equal concatenations must remain distinct tuples.
        engine.execute("create table pairs as (select x id,cast(case x%6 when 0 then null when 1 then '' when 2 then 'a' when 3 then 'ab' when 4 then '🐈' else 'a|b' end as varchar) a,"
                +"cast(case x%6 when 0 then '' when 1 then null when 2 then 'bc' when 3 then 'c' when 4 then 'ž' else '|c' end as varchar) b,cast(x%7 as int) k from long_sequence(1027))",context);
        for(String key:new String[]{"a,b","b,a,k","a,cast(b as symbol)","lower(a),upper(b)","coalesce(a,cast('' as varchar)),b"}) {
            check("variable-tuple-"+key,"select "+key+",count(),sum(id),count_distinct(k),min(a),max(b) from pairs group by "+key);
        }
        for(String predicate:new String[]{"i in (null,0,1)","i not in (null,0,1)","not (i=1 or i is null)","coalesce(i,99)>0", "nullif(i,0) is null","case when i is null then true when i<0 then d>0 else v is null end", "(s like 's1%' or v ~ '^https') and not (l is null and d<0)", "length(v)>3 and substring(v,2,3)!='abc'"}) {
            check("null-boolean","select s,count(),sum(l),min(d),max(d),count_distinct(v) from f where "+predicate+" group by s");
        }
        for(String type:new String[]{"byte","short","int","long","float","double","timestamp","timestamp_ns"}) {
            check("cast-group-"+type,"select cast(i as "+type+") k,count(),sum(l),min(l),max(l) from f group by k order by k");
        }
        check("mixed-supported-aggregate","select s,count(),sum(d),ksum(d),nsum(d),stddev(d),variance(d),first(l),last(l),min(v),max(v) from f group by s");
        check("divide-by-zero","select i,sum(l/(i-i)),sum(d/(i-i)),min(l%(i-i)),max(l) from f group by i");
        check("branch-only-invalid-cast","select s,count(),sum(case when id<0 then cast(v as long) else l end) from f group by s");
        check("double-sort-nulls","select i,s,d,id from f where id<513 order by d desc,s,i,id limit 17,65");
        for(String bounds:new String[]{"0","1","63","64","65","-1","-65","10,10","65,129","-129,-65"}) {
            check("limit-"+bounds,"select s,v,count() n,min(l),max(l) from f group by s,v order by n desc,s,v limit "+bounds);
        }
        for(int width:new int[]{63,64,65}) {
            StringBuilder sql=new StringBuilder("select i");
            for(int c=0;c<width;c++) sql.append(",sum(l+").append(c).append(")+min(l) c").append(c);
            check("wide-"+width,sql+" from f where id<257 group by i order by i");
        }
        finish("shape-boundaries");
    }

    @Test public void temporalAndStorageBoundaries() throws Exception {
        engine.execute("create table nanos as (select id,i,l,s,d,cast(ts as timestamp_ns) ts from f) timestamp(ts) partition by hour",context);
        engine.execute("create table ties as (select x id,cast(x%7-3 as int) i,(x%7-3)*4294967297 l,cast('s'||(x%7) as symbol) s,cast(x/3*1000000 as timestamp) ts from long_sequence(1027)) timestamp(ts)",context);
        for(String key:new String[]{"i","l","s"}) {
            for(String join:new String[]{"asof","lt"}) {
                for(String tolerance:new String[]{""," tolerance 1s"}) {
                    check("temporal-tie-"+join+"-"+key+tolerance,"select a.i,count(),count(b.id),sum(b.id),min(b.id),max(b.id) from nanos a "+join+" join ties b on ("+key+")"+tolerance+" group by a.i");
                }
            }
            for(String prevailing:new String[]{"include","exclude"}) {
                check("window-key-"+key+"-"+prevailing,"select a.ts,count(b.id),sum(b.id),min(b.id),max(b.id) from nanos a window join ties b on a."+key+"=b."+key+" range between 1 second preceding and 1 second following "+prevailing+" prevailing where a.s like 's1%' order by a.ts");
            }
            check("horizon-key-"+key,"select h.offset,a.s,count(),count(b.id),sum(b.id),min(b.id),max(b.id) from nanos a horizon join ties b on a."+key+"=b."+key+" list (-1s,0s,1s) as h where a.s like 's1%' order by h.offset,a.s");
        }
        check("multi-horizon","select h.offset,a.s,count(b.id),min(c.id),max(b.id) from nanos a horizon join ties b on a.s=b.s horizon join g c on a.s=c.s list (0s,1s,2s) as h where a.s like 's1%' order by h.offset,a.s");
        engine.execute("alter table f add column late symbol",context);
        engine.execute("alter table f add column extra varchar",context);
        engine.execute("insert into f select 4099+x,cast(x%31-15 as int),4503599627370496+(x%31)*4294967297,x/8.0,cast('s'||x%31 as symbol),cast('appended' as varchar),cast(7200000000+x*1000000 as timestamp),cast(case when x%3=0 then null else 'late'||x%5 end as symbol),cast(case when x%7=0 then null else 'extra'||x%11 end as varchar) from long_sequence(257)",context);
        for(int parquet=0;parquet<2;parquet++) {
            if(parquet==1) engine.execute("alter table f convert partition to parquet where ts<'1970-01-01T01:00:00'",context);
            check("storage-group-"+parquet,"select late,extra,count(),sum(l),min(v),max(v),count_distinct(s) from f group by late,extra");
            check("storage-filter-"+parquet,"select s,count(),sum(l),min(l),max(l) from f where late is null or (late like 'late1%' and extra!='extra3') group by s");
            check("storage-sort-"+parquet,"select late,extra,s,count() n,min(l),max(l) from f group by late,extra,s order by n desc,late,extra,s limit 31");
            checkJoin("storage-join-"+parquet,"select a.late,count(),sum(a.l),min(b.id),max(b.id) from f a join g b on (s) where a.extra is null or a.late like 'late1%' group by a.late");
            check("storage-window-"+parquet,"select a.ts,count(b.id),min(b.id),max(b.id) from f a window join g b range between 1 second preceding and 1 second following where a.late is null or a.late like 'late1%' order by a.ts");
            check("storage-asof-"+parquet,"select a.late,count(),sum(b.id),min(b.id),max(b.id) from f a asof join g b on (s) where a.late is null or a.late like 'late1%' group by a.late");
        }
        finish("temporal-storage");
    }

    @Test public void reusedFactoriesAcrossDataChanges() throws Exception {
        context.setJitMode(io.questdb.cairo.SqlJitMode.JIT_MODE_ENABLED);
        context.getBindVariableService().setStr(0,"s1%");
        String[] sqls={
                "select h.offset,a.ts,b.id from f a horizon join g b list (0s,2s,4s) as h where a.s like $1 and a.v!='missing' order by h.offset,a.ts",
                "select a.ts,count(b.id),min(b.id),max(b.id) from f a window join g b range between 2 second preceding and 2 second following where a.s like $1 and a.v!='missing' order by a.ts",
                "select s,count() n,sum(l),min(l),max(l) from f where s like $1 and v!='missing' group by s order by n desc,s limit 7",
                "select a.s,count(),sum(b.id),min(b.id),max(b.id) from f a asof join g b on (s) where a.s like $1 and a.v!='missing' group by a.s order by a.s",
                "select h.offset,a.ts,b.id from f a horizon join g b on (s) list (0s,2s,4s) as h where a.s like $1 and a.v!='missing' order by h.offset,a.ts",
                "select a.ts,count(b.id),min(b.id),max(b.id) from f a window join g b on (s) range between 2 second preceding and 2 second following where a.s like $1 and a.v!='missing' order by a.ts"
        };
        var factories=new ArrayList<io.questdb.cairo.sql.RecordCursorFactory>();
        try {
            for(String sql:sqls) factories.add(Demo.compileSql(engine,context,sql,true));
            for(int phase=0;phase<6;phase++) {
                if(phase==1) engine.execute("insert into f values (5001,1,4503599627370496,1.0,'s100','new','1970-01-01T02:00:00'),(5002,2,4503599627370497,2.0,'s1000','new','1970-01-01T02:00:01')",context);
                if(phase==2) engine.execute("insert into f values (5003,1,4503599627370498,3.0,'s111','o3','1970-01-01T00:00:00.500000')",context);
                if(phase==3) {
                    engine.execute("truncate table f",context);
                    engine.execute("insert into f values (1,1,4503599627370496,1.0,'s19','new','1970-01-01T00:00:01'),(2,2,4503599627370497,2.0,'s1','new','1970-01-01T00:00:02'),(3,3,4503599627370498,3.0,null,'new','1970-01-01T00:00:03')",context);
                }
                if(phase==4) {
                    engine.execute("truncate table g",context);
                    engine.execute("insert into g values (1,1,4503599627370496,'s19',1.0,'1970-01-01T00:00:00'),(2,2,4503599627370497,'s1',2.0,'1970-01-01T00:00:01')",context);
                }
                if(phase==5) engine.execute("insert into g values (3,2,4503599627370497,'s1',3.0,'1970-01-01T00:00:01'),(4,3,4503599627370498,null,4.0,'1970-01-01T00:00:02')",context);
                for(String pattern:new String[]{"s1%","missing%","s%",null}) {
                    context.getBindVariableService().setStr(0,pattern);
                    for(int q=0;q<sqls.length;q++) {
                        var factory=factories.get(q);
                        long before=QueryCodegenTestSupport.executions();
                        try(var cursor=factory.getCursor(context)) {cursor.hasNext();cursor.toTop();cursor.hasNext();}
                        var actual=Demo.readSql(factory,context,factory.getMetadata().getColumnCount());
                        var compiler=context.getQueryCodeGenerator();
                        context.setQueryCodeGenerator(null);
                        context.setJitMode(io.questdb.cairo.SqlJitMode.JIT_MODE_DISABLED);
                        try(var stock=Demo.compileSql(engine,context,sqls[q],true)) {
                            assertEquals("phase="+phase+" pattern="+pattern+" sql="+sqls[q],Demo.readSql(stock,context,stock.getMetadata().getColumnCount()),actual);
                        } finally {
                            context.setQueryCodeGenerator(compiler);
                            context.setJitMode(io.questdb.cairo.SqlJitMode.JIT_MODE_ENABLED);
                        }
                        long delta=QueryCodegenTestSupport.executions()-before;
                        queries++;if(delta>0)nativeQueries++;
                        System.out.println("REUSE phase="+phase+" pattern="+pattern+" query="+q+" nativeCalls="+delta);
                    }
                }
            }
        } finally {for(var factory:factories)factory.close();}
        finish("cached-data-changes");
    }

    @Test public void physicalTailsAndAdditionalJoinTypes() throws Exception {
        engine.execute("create table tail as (select * from f where id<0) timestamp(ts)",context);
        for(int n:new int[]{0,1,2,31,32,33,63,64,65,127,128,129,255,256,257}) {
            engine.execute("truncate table tail",context);
            engine.execute("insert into tail select * from f where id<="+n,context);
            long nativeBefore=QueryCodegenTestSupport.executions();
            check("physical-native-scalar-"+n,"select count(),count(l),sum(l),min(l),max(l),avg(d) from tail where id>0");
            if(n>0 && context.getQueryCodeGenerator()!=null) assertTrue("Scalar physical tail did not execute native code: "+n,QueryCodegenTestSupport.executions()>nativeBefore);
            nativeBefore=QueryCodegenTestSupport.executions();
            check("physical-native-group-"+n,"select s,count(),sum(l),min(l),max(l),avg(d) from tail where id>0 group by s");
            if(n>0 && context.getQueryCodeGenerator()!=null) assertTrue("Grouped physical tail did not execute native code: "+n,QueryCodegenTestSupport.executions()>nativeBefore);
            check("physical-tail-scalar-"+n,"select count(),count(l),sum(l),min(l),max(l),avg(d),count_distinct(s) from tail");
            check("physical-tail-group-"+n,"select s,count(),sum(l),min(l),max(l),count_distinct(v) from tail group by s");
            check("physical-tail-filter-"+n,"select s,count() n,sum(l),min(l),max(l) from tail where s like 's1%' or v is null group by s order by n desc,s limit 7");
        }
        for(String key:new String[]{"d","cast(d as float)","cast(i as short)","cast(i as byte)","cast(s as varchar)"}) {
            for(String join:new String[]{"join","left join"}) {
                checkJoin("other-join-key-"+key+"-"+join,"select a.k,count(),count(b.id),sum(a.l),min(b.id),max(b.id) from "
                        +"(select "+key+" k,id,l from f) a "+join+" (select "+key+" k,id from g) b on (k) group by a.k");
            }
        }
        for(String join:new String[]{"join","left join"}) {
            checkJoin("variable-composite-"+join,"select a.s,count(),sum(a.id),min(b.id),max(b.id) from f a "+join+" (select * from f where id<31) b on (a.v=b.v and a.s=b.s) group by a.s");
        }
        check("cross-aggregate","select a.s,count(),sum(a.id*b.id),min(a.l),max(b.id) from (select * from f where id<32) a cross join (select * from g where id<8) b group by a.s");
        checkJoin("non-equi","select a.s,count(),sum(a.id*b.id),min(a.l),max(b.id) from (select * from f where id<32) a join (select * from g where id<8) b on a.i<b.i group by a.s");
        checkJoin("mixed-left-chain","select a.s,count(),sum(b.id),min(c.id),max(c.id) from f a left join (select * from g where id<19) b on (s) left join g c on b.i=c.i where c.id is null or c.id<9 group by a.s");
        finish("physical-tails-types");
    }

    @Test public void orderedAndFloatingOutputs() throws Exception {
        for(String order:new String[]{"n desc,s,v","s,v,n desc","v desc,s desc,n","lo desc,s,v"}) {
            for(String limit:new String[]{""," limit 1"," limit 63"," limit 64"," limit 65"," limit 17,65"," limit -65"," limit -129,-65"}) {
                ordered("ordered-group", "select s,v,count() n,min(l) lo,max(l) hi from f group by s,v order by "+order+limit);
            }
        }
        for(String order:new String[]{"v","v desc","d desc,s,i,id","l,i desc,id","s desc,v,id"}) {
            String projection=order.startsWith("v")?"v":"id,i,l,s,v,d";
            ordered("ordered-scan", "select "+projection+" from f where id<1027 order by "+order+" limit 17,65");
        }
        engine.execute("create table ieee (id long,k int,d double)",context);
        double[] expectedBits={Double.NaN,Double.POSITIVE_INFINITY,Double.NEGATIVE_INFINITY,0.0,-0.0,1e-310,-1e-310,1e308,-1e308};
        try(var writer=engine.getWriter(engine.verifyTableName("ieee"),"SQL boundary fixture")) {
            for(int i=0;i<expectedBits.length;i++) {
                var row=writer.newRow();row.putLong(0,i+1);row.putInt(1,i);row.putDouble(2,expectedBits[i]);row.append();
            }
            writer.commit();
        }
        try(var fixture=engine.select("select d from ieee order by id",context);var cursor=fixture.getCursor(context)) {
            for(double expected:expectedBits) {
                assertTrue(cursor.hasNext());
                assertEquals("Verify IEEE fixture",Double.doubleToLongBits(expected),Double.doubleToLongBits(cursor.getRecord().getDouble(0)));
            }
            assertFalse(cursor.hasNext());
        }
        for(String sql:new String[]{
                "select id,d,d*2,d+1,d/2 from ieee where d is not null order by id",
                "select k,count(d),min(d),max(d),sum(d),avg(d) from ieee group by k order by k",
                "select id,d from ieee where d<0 or d>0 or d is null order by id",
                "select id,d from ieee order by d,id limit 1,8"}) {
            ordered("ordered-ieee",sql);
        }
        finish("ordered-floating");
    }

    private void ordered(String label,String sql) throws Exception {
        var compiler=context.getQueryCodeGenerator();
        context.setJitMode(io.questdb.cairo.SqlJitMode.JIT_MODE_ENABLED);
        long before=QueryCodegenTestSupport.executions();
        try(var candidate=Demo.compileSql(engine,context,sql,true)) {
            context.setQueryCodeGenerator(null);
            context.setJitMode(io.questdb.cairo.SqlJitMode.JIT_MODE_DISABLED);
            try(var stock=Demo.compileSql(engine,context,sql,true)) {
                context.setQueryCodeGenerator(compiler);
                context.setJitMode(io.questdb.cairo.SqlJitMode.JIT_MODE_ENABLED);
                try(var expected=stock.getCursor(context);var actual=candidate.getCursor(context)) {
                    io.questdb.test.tools.TestUtils.assertEquals(expected,stock.getMetadata(),actual,candidate.getMetadata(),false);
                    expected.toTop();actual.toTop();
                    io.questdb.test.tools.TestUtils.assertEquals(expected,stock.getMetadata(),actual,candidate.getMetadata(),false);
                }
                if(label.equals("ordered-ieee")) {
                    assertEquals("IEEE values compared as bits: "+sql,
                            Demo.readSql(stock,context,stock.getMetadata().getColumnCount()),
                            Demo.readSql(candidate,context,candidate.getMetadata().getColumnCount()));
                }
            }
        } catch(AssertionError failure) {failures.add(label+": "+sql+"\n"+failure);}
        finally {context.setQueryCodeGenerator(compiler);context.setJitMode(io.questdb.cairo.SqlJitMode.JIT_MODE_ENABLED);}
        long delta=QueryCodegenTestSupport.executions()-before;
        queries++;if(delta>0)nativeQueries++;
        System.out.println("COMPOSITION "+label+" nativeCalls="+delta+" sql="+sql);
    }

    private void checkMembership(String key,String label,String sql) {
        if(key.equals("s")){check(label,sql);return;}
        // InLongFunctionFactory rejects CURSOR arguments; verify the same rejection with and without LLVM.
        var result=runner.run(new GeneratedQuery(sql,true));
        assertTrue(label+": "+result.getFailure(),result.isSkipped());
        assertEquals("SqlException: cannot compare LONG with type CURSOR",result.getSkipReason());
        expectedRejections++;
        System.out.println("REJECTION "+label+" sql="+sql);
    }

    private void finish(String family) {
        System.out.println("COMPOSITION result family="+family+" queries="+queries+" nativeQueries="+nativeQueries+" floatingToleranceMatches="+floatingMatches+" expectedRejections="+expectedRejections+" failures="+failures.size());
        assertTrue(String.join("\n",failures),failures.isEmpty());
        if(context.getQueryCodeGenerator()!=null)assertTrue("No native coverage",nativeQueries>0);
    }
    private void check(String label, String sql) { check(label, new GeneratedQuery(sql, true)); }
    private void checkJoin(String label, String sql) { check(label, new GeneratedQuery(sql, true).withShape(QueryShape.HASH_JOIN_GROUP_BY)); }
    private void check(String label, GeneratedQuery query) {
        long calls = QueryCodegenTestSupport.executions();
        var result = runner.run(query);
        long delta = QueryCodegenTestSupport.executions()-calls;
        queries++;
        if (delta>0) nativeQueries++;
        System.out.println("COMPOSITION " + label + " nativeCalls="+delta+" sql="+query.sql());
        if (result.isFailed()) { failures.add(label+": "+result.getFailure()); result.getFailure().printStackTrace(System.out); }
        if (result.isSkipped()) {
            if ("rowset matches with floating-point tolerance".equals(result.getSkipReason())) floatingMatches++;
            else failures.add(label+": unexpected rejection: "+result.getSkipReason());
        }
    }
}
