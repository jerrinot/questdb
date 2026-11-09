package org.questdb.experimental.codegen;

import io.questdb.griffin.SqlExecutionContextImpl;
import io.questdb.griffin.engine.QueryCodeGenerator;
import io.questdb.griffin.engine.join.CompiledJoin;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.List;

/** Benchmark-only inspection and counterfactuals; no production policy switches. */
public class RtaCompilerSetup {
    private static final List<CompiledJoin> joins = new ArrayList<>();
    private static final IdentityHashMap<CompiledJoin, Boolean> groupCaches = new IdentityHashMap<>();
    private static final List<QueryCodeGenerator.JoinFilter> filters = new ArrayList<>();
    private static final String forcedLookahead = System.getProperty("bench.lookahead");
    private static final boolean javaJoin = Boolean.getBoolean("bench.javaJoin");
    private static final boolean disableGroupCache = Boolean.getBoolean("bench.noGroupCache");

    public static void install(SqlExecutionContextImpl context, Path output) {
        // Native-row counters already live in worker-owned state. Counting every
        // invocation with one shared atomic would introduce benchmark contention.
        var compiler = new ResolvedQueryCompiler(new LlvmBackend(), output.resolve("native"));
        context.setQueryCodeGenerator((QueryCodeGenerator) Proxy.newProxyInstance(
                RtaCompilerSetup.class.getClassLoader(), new Class<?>[]{QueryCodeGenerator.class}, (proxy, method, args) -> {
                    Object result = invoke(method, compiler, args);
                    if (result instanceof QueryCodeGenerator.JoinFilter filter) filters.add(filter);
                    if (!(result instanceof CompiledJoin join)) return result;
                    joins.add(join);
                    return Proxy.newProxyInstance(RtaCompilerSetup.class.getClassLoader(), new Class<?>[]{CompiledJoin.class}, (p, m, a) -> {
                        // Ordinary fallback owns its preparation. Do not charge the
                        // Java control for native code or state it never consumes.
                        if (javaJoin && (m.getName().equals("prepareBuilds") || m.getName().equals("preparePayloads"))) return null;
                        // preparePayloads uses probeRows only for destination-cache admission.
                        // Leave prepareBuilds and all join/filter state unchanged.
                        if (disableGroupCache && m.getName().equals("preparePayloads")) {
                            a = a.clone();
                            a[1] = 0L;
                        }
                        Object value = invoke(m, join, a);
                        if (m.getName().equals("preparePayloads")) {
                            groupCaches.put(join, ((JoinGroupCache) field(join, "groupCache")).isAllocated());
                        }
                        if (forcedLookahead != null && m.getName().equals("preparePayloads")) force(join);
                        if (!(value instanceof CompiledJoin.Updater updater)) return value;
                        return Proxy.newProxyInstance(RtaCompilerSetup.class.getClassLoader(), new Class<?>[]{CompiledJoin.Updater.class}, (pp, mm, aa) -> {
                            if (javaJoin && mm.getName().equals("prepareDependencies")) return null;
                            if (javaJoin && (mm.getName().equals("updateMulti") || mm.getName().equals("updateKeyedMulti"))) return false;
                            return invoke(mm, updater, aa);
                        });
                    });
                }));
    }

    public static void report(int run) throws Exception {
        for (int id = 0; id < joins.size(); id++) {
            Object join = joins.get(id);
            Object programs = field(join, "specializedPrograms");
            if (programs == null) programs = field(join, "programs");
            long nanos = 0;
            int count = 0;
            for (Object variant : (List<?>) field(programs, "owned")) {
                long time = ((CompiledKernel) field(variant, "code")).compilationNanos();
                nanos += time;
                if (time > 0) count++;
            }
            System.out.printf("JOIN_COMPILE run=%d id=%d compiled=%d backend_ns=%d group_cache=%s%n",
                    run, id, count, nanos, groupCaches.getOrDefault(join, false));
            Object[][] policies = (Object[][]) field(programs, "choices");
            var seen = new IdentityHashMap<Object, Boolean>();
            for (int row = 0; row < policies.length; row++) {
                for (int mode = 0; mode < policies[row].length; mode++) {
                    Object policy = policies[row][mode];
                    if (policy == null || seen.put(policy, true) != null) continue;
                    int winner = (int) field(policy, "winner");
                    Object[] alternatives = (Object[]) field(policy, "alternatives");
                    Object phase = field(policy, "phase");
                    System.out.printf("JOIN_CHOICE run=%d id=%d prefetch=%d membership=%d winner=%d ahead=%s phase=%s costs=%s%n",
                            run, id, row, mode, winner, field(alternatives[winner], "lookahead"),
                            field(phase, "choice"), Arrays.toString((double[]) field(policy, "costs")));
                }
            }
        }
        for (int id = 0; id < filters.size(); id++) {
            var filter = filters.get(id).compiled().rows();
            System.out.printf("FILTER run=%d id=%d input=%d output=%d%n", run, id, filter.getNativeInputRows(), filter.getNativeOutputRows());
        }
    }

    private static void force(CompiledJoin join) throws Exception {
        Object programs = field(join, "specializedPrograms");
        if (programs == null) throw new IllegalStateException("No specialized join for diagnostic force");
        var seen = new IdentityHashMap<Object, Boolean>();
        int forced = 0;
        for (Object[] row : (Object[][]) field(programs, "choices")) {
            for (Object policy : row) {
                if (policy == null || seen.put(policy, true) != null) continue;
                Object[] alternatives = (Object[]) field(policy, "alternatives");
                for (int index = 0; index < alternatives.length; index++) {
                    if (!field(alternatives[index], "lookahead").toString().equals(forcedLookahead)) continue;
                    var phase = policy.getClass().getDeclaredField("phase");
                    phase.setAccessible(true);
                    var constructor = phase.getType().getDeclaredConstructor(int.class, int.class, double[].class);
                    constructor.setAccessible(true);
                    phase.set(policy, constructor.newInstance(index, Integer.MAX_VALUE, null));
                    var winner = policy.getClass().getDeclaredField("winner");
                    winner.setAccessible(true);
                    winner.setInt(policy, index);
                    forced++;
                    break;
                }
            }
        }
        if (forced == 0) throw new IllegalStateException("Unavailable diagnostic lookahead: " + forcedLookahead);
        System.out.printf("FORCED_CHOICE ahead=%s policies=%d group_cache=%s%n", forcedLookahead, forced,
                ((JoinGroupCache) field(join, "groupCache")).isAllocated());
    }

    private static Object field(Object object, String name) throws Exception {
        var field = object.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(object);
    }

    private static Object invoke(Method method, Object object, Object[] args) throws Throwable {
        try { return method.invoke(object, args); }
        catch (InvocationTargetException e) { throw e.getCause(); }
    }
}
