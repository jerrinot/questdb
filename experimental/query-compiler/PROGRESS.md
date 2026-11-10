# Query compiler breakthrough log

Retained changes, measured effects and important counterexamples. Measurements
below belong to their dated builds and protocols; they are not a current score.
Current coverage and entrypoints: [README](README.md). Open work: [TODO](TODO.md).
Detailed earlier entries remain in Git history; raw evidence stays at its existing
location rather than being copied into another archive. Links under `target/`
refer to local, Git-ignored experiment artifacts.

## 2026-11-10 — Retire the experimental C and AsmJit backends

LLVM is now the sole compiler backend. Removed the alternative emitters, native
instruction bridge, helper/vector lowering, custom optimizers, SSA destruction
and three obsolete compiler switches: 2,162 fewer main-source lines. SSA
construction and verification remain; LLVM emission and optimization are unchanged.
Tests retain worker/type/optimization-level coverage and ordinary SQL oracles.
Handwritten performance references and QuestDB's ordinary filter JIT remain.
Archived fast-compilation experiments are preserved as historical evidence.

A fresh module compilation and all 705 module checks pass. The existing composition
stress case also passes with LLVM enabled: 66 SQL cases, 56 exercising generated
code, 14,337 native calls. The current-core build exposed a stale QueryRunner
constructor call after the master merge; the fixture now supplies the optional
map-page override argument. No wall-clock performance claim accompanies this cleanup.

## 2026-11-09 — Retry policy: a real cost, no general default

Literature-informed scratch controls compare frame-reset backoff, retained learning
and bounded work-based retries, at unchanged and 4x flush ceilings. On 500M rows
with a million LONG groups, retained learning cuts the grown arm's map trials
**50.4M -> 9.16M**, partial states **49.23M -> 8.94M**, instructions **173.15 ->
162.50/row**, and time **1.307–1.330 -> 1.225–1.240 s**. The original ceiling's
timing gain is unproven. Retained learning still pays a fill per frame; fixed-work
retries remove that dependence but spend too much work on inputs with consistently
low local key reuse.

Alternating locality rejects the combined larger-map/retained-learning policy:
**0.936–0.953 -> 1.281–1.306 s**, with fewer rows combined locally and more partition
traffic. Disabling bypass while retaining bounded flushing shrinks the ceiling
gap to **1.343–1.369 vs 1.378–1.383 s**, both slower than the original adaptive path.
This implicates bypass placement without proving the residual hardware cause.
**599 complete 500M-row validations and 18 existing LLVM lifecycle tests pass.**
No compiler default changed. Next: profile the residual always-aggregate pair
before another policy. [Evidence and limits](target/state-backoff-20261109/REPORT.md).

## 2026-11-09 — Independent flush ceilings and an observer correction

An isolated compiler experiment changes only the count/byte flush ceilings,
preserving initial map sizing, sharding, ordinary owners and final merge. With
500M rows and three balanced fresh-process comparisons, a 4x ceiling gives
**942–962 -> 372–373 ms** for FLOAT/SYMBOL, **1,041–1,080 -> 406–408 ms** for
DOUBLE/SYMBOL and **1,306–1,329 -> 535–539 ms** with eleven aggregates. The first
case's controller traffic falls **43.91 -> 8.41 GB/query**, cycles **69.79 ->
31.21/row**, and partition payload from **20.81 GB to 9 MB** of write/read work.
Small maps retain their performance. Million-group and changing-distribution
controls regress about **6–12%**: unsuccessful preaggregation can grow fourfold
before the existing compact-input backoff. A fixed multiplier is not retained.

M1 compact controls retain essentially identical cycles/instructions per row;
frequency drift limits small wall-time conclusions. A larger 64,643-group control
exposes a capacity effect: grown local maps reduce peak RSS **13.2–13.6 -> 4.5 GB**
and eliminate measured input reads and process swap, taking **1.963–2.121 ->
1.590–1.594 s**. Account for partition buffers as well as the local hash table.

The retained code fix is in the benchmark observer: periodic `numa_maps` reads
walk page tables under `mmap_lock`. Kernel tracing and an identical-binary A/B show
**11–16% median overhead** on the million-group controls, versus **0–4%** on the
moderate controls. `bench/run.py` no longer samples that file during execution;
NUMA placement diagnostics run separately. All numbers above use the corrected
observer. There are **1,480 accepted complete-result validations** across AMD/ARM,
plus 20 correct outputs from excluded runs. Compiler production defaults are
unchanged. The retry experiment above follows up on the exploration-work cost;
reversible growth remains unqualified.
Evidence and limits: [experiment report](target/state-growth-20261109/REPORT.md).

## 2026-11-09 — Aggregation state layout: headroom and counterexamples

These earlier timings include the former `numa_maps` observer. Its overhead is
workload-dependent; use the corrected cohorts above for the growth experiment.

Experiments on `2b56fbd5eb`, with 500M input rows per execution, vary INT/LONG/
FLOAT/DOUBLE/SYMBOL keys, composite keys, COUNT/SUM/AVG/MIN/MAX state, cardinality,
skew and changing distributions. Three balanced fresh-process comparisons on
Ryzen 9950X reproduce 1.010–1.056s → 0.433–0.438s for the FLOAT/SYMBOL control
using the existing unbounded diagnostic. Controller traffic falls 45→9GB/query;
479M input records plus 20.5M partial states account for 20.81GB of avoidable
write/read payload. Wider aggregates and other key types also gain substantially.

The million-group LONG case reverses: bounded 1.293–1.371s versus direct
2.244–2.306s, with traffic increasing 50→126GB despite fewer instructions.
Load-wait cycles/row rise 27.5→156.2. Direct throughput barely improves from eight
to sixteen cores at about 55–58GB/s, near the 63.2GB/s mixed-stream calibration.
A million-group LONG/SYMBOL control confirms a reversal within OrderedMap too.
Small and skewed controls regress; the same compact SQLs that benefit on AMD
regress on M1. These fixed-path experiments do not qualify an adaptive policy.

Source and counterfactuals expose three coupled decisions: the local bound also
sets initial map capacity and disables ordinary sharding. At 889 groups, reducing
bounded execution to the direct path's 2,048 hash slots reproduces its slowdown
without partitioning. Increasing OrderedMap's capacity alone can instead trigger
its unchanged byte bound earlier. The independent-ceiling experiment above
separates sizing, flushing and sharding through existing owners.
No production behavior or default changed. Results, hardware limits and an
excluded temporal-null CASE admission shape are in
[the experiment report](target/state-layout-20261109/REPORT.md).

## 2026-11-09 — Restore VARCHAR expression admission

Aggregate binding initializes its shared string helper before resolving arguments,
then captures the completed descriptors after the loop. This removes the accidental
dependency on aggregate order for `AVG(length(varchar))` and related expressions;
numeric-only consumers retain their existing admission boundary. The capture test
now exercises both aggregate orders. The two existing checks fail all 12 backend/
worker combinations before the fix; 24 focused checks pass afterward, including
Unicode, nulls, column tops, Parquet and helper lifetimes across LLVM/C/AsmJit.

On 99,997,497 ClickBench rows and eight physical Ryzen 9950X cores, three rotated
fresh-process comparisons (five initial + five measured executions) give parent /
regressed / repaired medians of **255 / 277 / 255 ms for Q27** and
**1,095 / 2,088 / 1,097 ms for Q28**. Q28 returns from about 307B instructions to
120B and restores 81,032,736 native aggregate rows per execution. This is restored
admission, not a new algorithm or a claim about the limiting hardware resource.

All 43 queries were also rerun in three fresh processes under the repository
ClickBench protocol. The warm query-time sum is 7.825s repaired versus 8.699s
regressed and 7.761s parent; those full-suite baselines precede the repaired runs.
All 387 new protocol responses and 210 focused responses validate, including tied
LIMIT groups. Native participation changes only for Q27/Q28. Evidence and the
standard HTML are in `target/perf-regression-20261108/restoration/`.

## 2026-11-08 — Architecture milestones 4–5 and scope decisions

Scalar scan emission and slot binding now share one descriptor. Scalar/join
argument allocations start initialized; joins reset function cardinality on each
binding and validate symbol/key/staging resume requests. Poisoning the cardinality
word reproduces the old first-call bug (`expected 0, got 12345`) and passes with
the fix. Both ASOF strategies share region binding and validate progress before
publication; deliberately corrupted native progress is rejected on repeated
reopens, with the allocator context released even when publication fails.

`CompilerOptions` retains planning choices through aggregate/key/filter binding,
nested consumers and deferred compilation. Tests supply immutable options rather
than changing global properties. Backend-lowering and diagnostic defaults retain
process scope; C/AsmJit SSA controls receive explicit snapshots in tests, preserving
both lowering paths without global mutation. Defaults and generated algorithms
are unchanged. The isolation test uses a two-column expression because source
inspection confirmed that ordinary planning rewrites SUM(column + constant)
before expression admission.

Twenty-three standalone executors, demos and matrix helpers moved to the existing
test source set; launchers include that output. Production output contains none
of those classes. Eleven historical plans moved to `archive/`, with source/doc
links preserved. Current scope decisions retain C/AsmJit IR parity and adaptive
learning per kernel/thread. Capability regrouping and compiled execution in
default core CI remain explicit open work, detailed in ARCHITECTURE.

Validation: full core/module test-source build; 195 distinct focused checks pass
(185 integration/lifecycle/options checks, four specification checks and six
standalone executor controls; the five filter-fusion checks also passed after the
last test cleanup). All 91 captured LLVM programs across 17 SQL shapes match the
milestone-3 baseline byte for byte. Shell launchers pass syntax checks. No wall-time
profiling or performance claim. Scratch evidence: `/tmp/query-architecture-final`.

## 2026-11-08 — Architecture milestone 3: one aggregate binding/result contract

`AggregateSpecs` replaces the ordinary binding class chain, separate output-reader
list and per-update implementation checks. Each supported exact class has one
specification for argument binding, state slot types, generated update factory,
consumer requirements and result reader. One validator checks bounds, types and
slot ownership. Actual map offsets, DISTINCT set parameters, timestamp precision,
Top-K contiguity and Boolean/KSUM ordinary readers retain their existing contracts.
Rosti keeps its separate vector semantics. No admission toggle or runtime row
dispatch was added.

The focused specification tests compare against ordinary `initValueTypes()`,
reject wrong types in every declared cell and overlapping slots, and check
displaced layouts, exact-class admission and reader selection. Full core/module
test-source build passes. All 255 selected checks pass, including mixed native/
Java state, nested finalization/ranking, typed aggregate families, ASOF, inner
joins and grouped Top-K across the existing backend controls. All 91 captured
LLVM programs across 17 SQL shapes are byte-identical before and after. No timing
claim; descriptor initialization/rebinding remains next. Scratch evidence:
`/tmp/query-architecture-m3`. Current design: [ARCHITECTURE](ARCHITECTURE.md).

## 2026-11-08 — Architecture milestone 2: aggregate transitions and protocol checks

`AggregateUpdates` now owns empty-state conversion and first/next emission for
ordinary batches, generated probes, scalar and join consumers, nested maps,
partitions, ASOF and Rosti. Consumers supply their actual freshness evidence;
register promotion, partial-state adoption and physical algorithms stay local.
ASOF's Java-first-row precondition is explicit. No runtime dispatch was added.

Delegating module tests check owner/worker preparation and draining, shared reset
ordering and ASOF handoff. Decline checks observe fresh and populated scalar
state. Deliberate omitted worker preparation, omitted Java first-row update and
a write before decline each fail at the intended check.

Testing optional capabilities independently exposed an ASOF boundary bug: after
a native aggregate consumed exactly to an RHS frame end, a declined forward scan
fell into Java's `do/while` and read beyond the frame. This produced wrong results
with scalar filtering and a JVM crash with compiled filtering. Supplying the
compiled forward scan masked it. The scalar loop now skips exhausted frames;
the regression exercises both filtering paths, native strategies and reopening.

Validation: full core/module test-source build; 246 unique focused checks pass,
including all 32 module ASOF and 8 ordinary adaptive-ASOF tests. All 85 captured
baseline LLVM programs across 15 SQL shapes are byte-identical. Join seed emission
now omits redundant initialization of already-compatible ordinary state; mixed
aggregate join tests cover that change, outside the captured-IR comparison.
These are structural/correctness results, with no timing claim or claim that the
entire optional SPI is checked. Scratch evidence: `/tmp/query-architecture-m2`.
Remaining binding/descriptor work is in [ARCHITECTURE](ARCHITECTURE.md).

## 2026-11-08 — Architecture milestone 1: execution ownership and freshness

Shared VARCHAR maps now initialize and reset through the aggregate execution
owner; worker scratch clear/reopen cannot reset their binding. Finishing remains
slot-local because it borrows scratch and rehash callbacks. Join insertion always
publishes a fresh-group marker, independently of aggregate initialization flags.
Existing profile output counts actual plain/staged adaptive map selections;
learning and execution strategies are unchanged.

The new scratch-isolation regression failed on LLVM, C and AsmJit before the fix.
All 114 focused checks pass, including shared-map growth/fallback/failure/reopen,
generated joins, adaptive policy and core HORIZON lifecycle checks. Twelve shared
map/staged-path checks also pass with profiling enabled. A scratch runner's
multi-method regex initially skipped parameterized cases; corrected grouping
ran all 30 selected lifecycle cases before this result was recorded. The full
core/module test-source build passes. All 78 captured LLVM programs across twelve
SQL shapes are byte-identical before and after. This is structural/correctness
evidence, not a timing or speedup claim. Raw checks: `/tmp/query-architecture-m1`.
The remaining architecture milestones are recorded in [ARCHITECTURE](ARCHITECTURE.md).

## 2026-11-08 — Generalize typed values and aggregate state across consumers

Shared lowering now carries static-SYMBOL conditions, explicit F32 values,
DATE/both timestamp units, BOOLEAN, IPv4 and CHAR through applicable scalar,
keyed, nested-map, inner-join and both ASOF consumers. Primitive and composite
join keys reuse ordinary byte encodings. Numeric casts, unary negation, integer
division/remainder and floating remainder preserve resolved width, sentinels and
ordinary rounding. This is contract coverage, not a claim that every combination
or storage representation generates; the [README](README.md#supported-scope)
records admission and fallback boundaries.

New shared aggregate families cover FLOAT state, bitwise and Boolean reductions,
NSUM, weighted average, variance/stddev, covariance, correlation and regression.
The same initialization/update/merge/finalization contracts serve existing
consumers. Scalar, inner-join and ASOF DISTINCT borrow existing owner/worker
allocators. Static-SYMBOL vector KSUM/NSUM preserve their distinct Rosti recurrence
and first-partial rules. No query-name recognition, new backend or user toggle.

Generality testing found issues that benchmark-shaped tests missed:

- Ordinary key probing copies setEmpty state; generated batch updates now use its
  existing new-entry bit for aggregates with different first-row seeds. Wrapped
  statistical counts no longer masquerade as fresh groups. First/next updates
  also preserve weighted-average signed zero and narrow BIT_AND identities.
- Cached factories release per-slot execution scratch on clear and rebind capture,
  dictionary and nested-merge descriptors on reopen. Shared VARCHAR query state
  resets only after work drains, independently of worker scratch reopening.
  Existing Griffin memory limits pass without raising allowances.
- Ordinary Boolean/bitwise/GEOMEAN factories incorrectly folded constant arguments:
  empty input lost its aggregate row and XOR lost parity. They now retain aggregate
  state. Three regression tests fail before and pass after; 97 core checks pass.
  GEOMEAN remains ordinary execution. Timestamp SWITCH also preserves the -1 key.

Exact mixed Java/native state checks cover NULLs, NaNs/infinities, signed zero,
integer/count overflow, merge adoption and finalization. SQL checks vary numeric
widths, expressions, composite keys, payload ownership, column tops, Parquet,
empty input, cancellation, memory failure and cursor reuse. Parallel statistical
results use explicit numerical bounds; ordered state comparisons remain exact.
The expanded existing fuzzer passes **5,000 queries on AMD and 5,000 on M1**:
753 native comparisons per host, 4,268 attempted, 6,822 compilations and
48,088/48,100 native calls. Fault/bind variants remain enabled; zero failures.
These counts do not mean all 5,000 SQL statements are fully generated.
The final full local suite passes **52 classes / 1,576 checks**, including LLVM,
C and AsmJit variants where applicable, after all lifecycle and first-state fixes.
The finished ARM consumer pass adds **176 checks across 11 classes**, including
both ASOF strategies, multiway joins and exact scalar/vector state. All pass.
The final clean source build includes all core/compiler production sources, all
module tests and changed core tests. All 378 compiler classes match the tested
build byte-for-byte; changed core executable bytecode agrees. Existing ordinary
join ownership/lifecycle/admission tests also pass on M1 (34 checks).
All changed core test classes plus the new constant-aggregate regressions pass
together with LLVM disabled: **210 tests**, zero failures or skips.

Independent controls use **500M rows**, five warmups plus three measured complete
executions, balanced fresh processes, AMD eight physical cores and M1 four
performance cores. Ordinary means this branch with generation disabled, not
QuestDB master. Ranges below are process warm medians in milliseconds; they belong
to their frozen family checkpoints, before later lifecycle fixes.

| Control | AMD ordinary → LLVM | M1 ordinary → LLVM |
|---|---:|---:|
| SYMBOL/temporal expressions | 1605–1685 → 347–348 | 4313–6459 → 918–1062 |
| Boolean reductions | 831–970 → 314–317 | 2316–3480 → 832–834 |
| Bitwise reductions | 686–692 → 309–310 | 1751–1752 → 769–771 |
| Scalar DISTINCT, 2M-value domain | 9990–10010 → 4670 | 26040–26210 → 7830–7870 |
| NSUM expressions | 1462–1791 → 412–413 | 4826–6156 → 1143–1159 |
| Weighted-average expressions | 1846–1868 → 404 | 6373–6559 → 1090–1105 |
| SYMBOL vector compensated sums | 930–933 → 144–148 | 2725–2740 → 467–499 |
| FLOAT/DOUBLE remainder | 1956–2079 → 1444–1454 | 6582–6591 → 3210–3214 |
| Correlation/regression | 2454–2585 → 614–622 | 8881–8937 → 1834–2002 |

Complete outputs agree (explicit ROUND(6) for parallel statistical controls).
Instruction/cycle counts, generated-symbol profiles and AMD controller traffic
support reduced repeated dispatch, expression evaluation and state traversal.
For correlation/regression, instructions fall 79–81%, versus 23% less AMD traffic;
the optimistic memory floor is 186ms, well below the measured ~618ms. For vector
compensated sums, instructions fall 81–85% and AMD traffic 12.56→6.15GB: fusion
removes repeated native Rosti passes as well as generic overhead. Its ~106ms
memory floor is closer to the measured 144–148ms, but does not prove saturation.
Measured windows have zero physical reads/major faults and no quota/OOM growth.
M1 lacks controller telemetry; unexplained timing/cycle-rate variation is retained
in the ranges. [Raw evidence and protocol](target/type-generality/perf/REPORT.md).

Important limits and counterexamples remain:

- FLOAT/SYMBOL grouping partitions excessively with AMD's smaller cache budget.
  Disabling bounded aggregation experimentally cuts traffic 44.65→8.68GB and
  ~1.0s→~0.43s. M1 already avoids that path. No policy change was retained: varied
  cardinality, state width and reuse must establish a safe general decision.
- Floating remainder spends roughly half its native CPU samples in libm. Fewer
  dispatch instructions do not remove the arithmetic cost; no fast-math shortcut.
- The inner-join DISTINCT control gains 9.3x/5.2x from ordinary parallel fusion
  before LLVM adds ~2.3x/~2.2x. Do not attribute the combined gain to compilation.
- Different ordinary JIT outcomes materially change instruction work between
  processes. Standalone floating/CHAR projection, checked temporal conversion,
  VARCHAR DISTINCT and broader vector shapes remain explicit gaps. Intel/NUMA
  performance remains unverified. No universal speedup or full-query claim.

The owned 14GB typed fixture was removed from each host after measurement.
Changes remain uncommitted; raw counters/results and build identities are retained.

## 2026-11-07 — Shorter exploration pays, unreliable ranking blocks retention

500M-row controls compare current LLVM `9100ce3eb6` with scratch chooser policies
on AMD and M1. Short sampling reduces AMD uniform first use **7.69–7.81 ->
5.62–5.73 s**, with identical generated IR. Load-wait cycles fall **258 -> 159 per
row**, RAM demand samples **1,603 -> 620**, and controller traffic only **145 ->
141 GB**: earlier use of existing lookahead hides more latency.

The scalar and 99.9%-hot controls reject shortening alone. Removing the initial
switching margin and CPU-time sampling do not establish reliable ranking. A short
screen followed by longer finalist confirmation saves AMD startup time, but one
M1 uniform process discards the faster direct-cache kernel: warm **8.64 -> 10.30 s**,
with **16% more instructions/row** at nearly unchanged cycles/CPU-time. Alternating
those kernels inside one warmed JVM confirms **8.77 vs 10.58 s**. Scalar register
promotion also changes direction: 6–9% faster on AMD, about 3% slower on M1 for this
control. **No chooser change retained.** The next experiment must validate ranking
against matched, warmed kernel trials before adding elimination or a remaining-work budget.

Benchmark cache-admission telemetry now records preparation rather than freed
state after cursor close. Five focused policy tests and complete-output oracles
pass; they do not establish good choices. [Evidence and counterexamples](target/exploration-budget/REPORT.md).

## 2026-11-07 — Gains transfer to ARM; chooser counterexample retained

Unchanged `9100ce3eb6`, LLVM 21.1.8 and JDK 25 on AMD Ryzen 9950X and Apple M1.
The portable harness now lives in `bench`; 79 focused tests pass on each host.
Three balanced fresh processes and complete-result oracles cover two 500M-row
families with COUNT/SUM/MIN/MAX/AVG and mixed numeric values:

- Dependent joins with 262K VARCHAR-containing groups, four physical/performance
  cores: same-plan Java **32.37–35.29 -> 3.41–3.50 s** AMD;
  **70.23–71.10 -> 8.54–9.29 s** M1. This isolates generated execution from the
  larger parallel-plan admission gain.
- No-join aggregation, 97 LONG groups: **1.269–1.439 -> 0.628–0.633 s** AMD;
  **1.827–2.100 -> 0.896–1.047 s** M1. Instructions/row fall from 359–394 to
  168 and from 307 to 130 respectively. M1 sysfs frequency and cycles/CPU-time
  disagree in these shorter runs; no constant-frequency or thermal claim.
- Group lookahead is useful for larger state but loses under extreme skew.
  Grouping across two builds correctly declines destination reuse. First-use
  exploration scales with alternatives and worker slots. A scratch CPU-clock
  chooser regresses scalar execution 7–9%; **no policy change retained**.

Fresh official ClickBench stock/LLVM: **22.007 -> 7.767 s** sum of best hot runs,
28/43 queries at least 1.5x faster, 34 fully generated and four partial. All 258
responses validate. RTAB's 20 supported cases and original SF100 Q1/Q3/Q5 also
validate; RTAB gains are dominated by dependent joins and include planning/filter
changes. H2O/SYMBOL 1BRC data is unavailable. ARM large-run key-type coverage is
narrower than AMD; Intel/NUMA is unverified. No production code changed.
Evidence and exclusions: [generality report](target/generality/REPORT.md),
[ClickBench HTML](target/generality/clickbench/clickbench.html).

## 2026-11-07 — Transfer selectivity before expensive joins

Ordinary runtime filtering now carries INT, full-width LONG, translated SYMBOL
and composite membership through intermediate build scans. Completed maps export
surviving outbound keys using the existing build order and frame workers. Exact
joins still determine duplicates, payloads and results. Fused dependency reduction
also admits composite producers; all child conditions must match the same row.

- RTABench 0027: previous LLVM **4.6–4.7 s -> 36–37 ms**; fresh stock master
  **35.3–35.6 s**. Orders shrink from 10.01M to 2,132 candidates; items from
  105.02M to 22,312. This is mainly avoided work, with ordinary joins and DECIMAL
  aggregation still completing the query.
- Native completed-map membership preparation: **9.77 -> 2.36 ms**, approximately
  **334 -> 51 instructions/key**. Compilation is an additional first-use cost.
- A conservative root-choice rule removes large unfiltered builds when the
  alternative's worst-case fixed-key state fits a storage-risk bound. The naive
  largest-table rule was rejected: a duplicate/distinct control regressed
  **3.59 -> 44.03 s**; the retained guard gives **3.48 s**.
- Releasing unobserved timestamp order through existing optimizer categories
  improves original 0026 **1.99 s -> 98 ms**. FIRST/LAST, unsorted LIMIT, windows,
  SAMPLE BY and temporal joins retain their ordering boundaries.
- Composite membership lanes improve a separate 500M-row control **263 -> 153 ms**,
  **74 -> 15 instructions/row**, **23 -> 13 cycles/row**. Compile plus first
  execution worsens about **598 -> 625–634 ms**. Blanket single-key vectorization
  regressed an AVX2-target SYMBOL control and was rejected.

Independent 500M-row controls cover key families, skew, low selectivity and
COUNT/SUM/MIN/MAX/AVG, including ordinary DECIMAL consumers. This is not full
semijoin reduction or a general cost-based join optimizer. Transfer abandonment
and filtered-input barriers remain open.
[Contracts and research](archive/DEPENDENT_JOIN_FILTER_PLAN.md).
Evidence: `target/dependent-transfer/`, including `q24-study.json`,
`q26-study.json`, `membership-study.json` and typed-control results.

## 2026-11-07 — Joined VARCHAR consumption and group-destination reuse

Joined VARCHAR values now use the existing frame decoder, expression bindings
and QuestDB maps. Eligible grouping inputs supplied by one frozen build can reuse
its row's group destination within each worker slot. Duplicate expansion and
aggregate updates remain exact. Map movement invalidates cached addresses;
query memory limits and cursor lifecycle bound their lifetime.

On 500M rows, 262,144 mixed INT/VARCHAR groups and eight physical cores:

| Path | Complete hot execution |
| --- | ---: |
| Stock master `df37676583`, same configuration | 81.94 s |
| Fused pipeline before destination reuse | 11.74 s |
| Destination reuse | 4.24 s |
| Reuse plus admitted group prefetch | 2.50–2.53 s |

The first large gain includes parallel pipeline admission. Destination reuse
reduces cycles/row **1033 -> 365** and loads/row **118 -> 53**. Prefetch then
reduces load-stalled cycles/row **326 -> 161**. Calibrated controller traffic is
about **141 GB/query at 55 GB/s**, against a matched read/write control near
**64 GB/s**: roughly **2.2 s** is an optimistic unchanged-traffic floor.

Random and 90%-skewed 500M-row controls retain the high-group gain. INT, full-width
LONG, SYMBOL and composite joins, mixed aggregate families, Unicode/NULL keys,
map growth, sharding, failure/retry and reopen are validated against ordinary SQL
or an independent arithmetic oracle. Build-side Parquet VARCHAR, capture helpers
and variable-width join keys remain outside this binding.

Plain and join-only lookahead remain candidates. Group prefetch becomes eligible
only when actual per-slot group counts exceed the existing cache-budget estimate.
Replacing join-only lookahead, admitting group prefetch to tiny states, and larger
lookahead distances were rejected. The retained distance is 16 rows. Backend
compilation for the high-group case grows **0.50 -> 0.80 s**; first execution still
improves **5.71 -> 4.99 s** in that cohort. Training can span several executions.
Small-state fresh-process variance of **5–11%** remains unresolved. AVX2 targeting
on this CPU is not evidence from another machine.

The matched 20-query RTABench sum of warm medians improves **12.64 -> 2.73 s** from
the native-export checkpoint; all results match stock. Root choice and newly
admitted consumers supply most savings. Remaining DECIMAL cases dominate the
suite. These are hot experiments, not an official benchmark score.
Evidence: `target/dependent-transfer/rtab-prefetch-v3-summary.json`,
`varchar-group-study.json`, `group-cache-controls.json`,
`group-prefetch-admission-study.json`, `group-prefetch-v3-controls.json`,
`group-prefetch-v3-high-hardware-summary.json` and calibrated UMC controls.

## 2026-11-06 — Reach the pipeline before tuning it

TPC-H SF100 exposed a crucial distinction: a compiled plan label can still execute
Java fallback. Reordered Q3 had zero native join rows because temporal payloads
were rejected. Exposing DATE and timestamp payloads through existing descriptors
improves **2.728 -> 2.217 s**, with **323,494,519 native probe rows/run** and exact
output checks. Cycles/lineitem fall approximately **190 -> 154**; concurrency
stays near eight. The original query's ordinary join plan was a separate gap.

Connected join-root analysis, temporal expressions, cursor-bound SYMBOL predicates,
and searched/simple CASE then widened admission without recognizing SQL text.
Adversarial SQL controls found and fixed filter reset/count-dispatch lifecycle
errors. Existing Griffin tests, seeded fuzz, native-row assertions and independent
results remain the acceptance checks.
Evidence: `target/tpch-control/RESULTS.md`, `target/temporal-payload/`,
`target/rtabench/report.md` and `target/dependent-transfer/`.

## 2026-11-06 — Specialize on frozen physical facts

Bounded integral builds can replace hashing with compressed chain-head arrays.
Admitted aligned payload copies can include links and row IDs while preserving
logical handles and duplicate order. Sparse/composite keys retain hashing.
Java and LLVM share these storage choices.

Generated joins specialize on uniqueness, layout and available dependency caches;
addresses and SYMBOL IDs remain execution-local. Against LLVM `6a72fc1248`,
independent 500M-row eight-core controls improve **633 -> 216 ms** for unique
INT joins and **6.19 -> 1.56 s** for random duplicate INT joins. Composite keys
also improve, **9.00 -> 6.47 s**. Compilation, temporary memory and workload
sensitivity remain costs; these are not fresh whole-suite or cross-CPU scores.
Adaptive lookahead subsequently reused staged roots rather than repeating probes.

## 2026-11-05 — Worker-state placement matters

Separate scalar aggregate states could share a cache line. Isolation in
`SimpleMapValue` removed that hazard without changing the chooser. A controlled
adverse placement experiment improves **628 -> 268 ms** on eight cores but is
unchanged on one core; instructions stay constant while ownership traffic and
load-wait cycles fall. The **2.35x** ratio is a placement counterfactual, not a
claimed default-engine speedup. This core fix has a separate worktree.

## 2026-11-03–04 — ASOF fusion preserves multiple strategies

Automatic selection and generated regions retain ordinary forward, lookup and
history strategies. Shared filtering and aggregate IR fuse into eligible forward
and multi-probe lookup regions; Java still owns frame transitions, history,
translation and fallback. The operator remains single-threaded.

On 500M RHS rows, 1M probes and one core, selective INT forward fusion improves
**679 -> 291 ms** versus previous LLVM adaptive execution, and **475 -> 291 ms**
versus the stronger LLVM Light control. Dense all-match execution is near parity.
A larger lookup region improves distant INT matches **1048 -> 921 ms** and
immediate matches **543 -> 342 ms**. LONG/SYMBOL controls also improve.
Universal batching and several narrow predicate strategies were rejected.
Contracts, controls and limitations: [ASOF_SELECTION_PLAN.md](archive/ASOF_SELECTION_PLAN.md).

## 2026-11-02–03 — General multiway joins and aggregate multiplicities

Typed INT/LONG/SYMBOL/composite joins gained dependent-key execution, backward
membership transfer and cached successful lookups. Closed unobserved subtrees
can fold duplicate counts. Extending consumption beyond COUNT preserves ordinary
SUM/AVG update order; MIN/MAX consume a repeated value once. DISTINCT and broader
observed-payload reductions require separate proofs.

On independent 500M-row duplicate-bearing joins, SUM gains **2.86–3.21x** and AVG
**2.52–2.67x** across typed keys versus the prior LLVM path. Unique joins gain
little; no-match/ordinary AVG controls retain roughly **3–4%** costs, and
compilation adds **56–73 ms**. These limits rule out a universal speedup claim.
Shared aggregate-state descriptions were validated with KSUM; existing Griffin
correctness and fuzz tests can run through LLVM.
Evidence: `target/join-generalization.md`, `target/join-repeated-inputs.md`,
`target/join-extrema.md`, `target/aggregate-ksum.md`, `target/ssb-current.md`.

## 2026-10-31 — Independent H2O control

Official db-benchmark data, 500M rows, frozen stock/LLVM and DuckDB 2.0.0.
Two-key SUM over 1M groups takes **12.814 / 0.913 / 2.683 s**. LLVM controller
traffic falls **164 -> 26.6 GB/query** and L2 demand misses **2.24 -> 0.24/row**;
peak RSS rises **6.27 -> 12.37 GiB**. The control also exposed FLOAT admission,
array sizing and grouped-ranking gaps; eight-core Q10 OOMs at 64 GiB in both
QuestDB modes. Later fixes are separate from these frozen scores.
Evidence: [target/h2o-control.md](target/h2o-control.md).

## 2026-10-29–30 — SYMBOL arrays and broader aggregate types

Snapshot-bounded SYMBOL IDs enable direct array updates and array-to-array merge,
using existing workers and aggregate definitions. Only final groups enter Rosti.
Official 1BRC SYMBOL/SHORT data, one billion rows and eight cores: fresh-factory
execution improves **0.856 -> 0.176 s**. Reused factories show a larger ratio that
includes stock's map-capacity regression. These are post-import SQL timings.
Independent 500M-row uniform/skew controls retain gains; oversized domains fall
back. Integer and DOUBLE aggregate/type coverage expanded after this experiment.
Evidence: [target/1brc.md](target/1brc.md).

The October 30 ClickBench round used repository scripts, 43 queries, 99,997,497
rows, eight cores and uncompressed DuckDB 2.0.0. Sums of best hot runs were
**22.552 s stock / 7.941 s LLVM / 13.673 s DuckDB**, with **34 fully generated,
4 partial and 5 ordinary queries**. This is dated coverage, not a current count;
all 387 responses were validated, including allowed ties and documented SQL
semantic differences. Evidence: `target/clickbench.html` and
`/tmp/query-clickbench-round-20261030/`.

## 2026-10-25–29 — Foundations and retained lessons

- Typed IR acquired C, AsmJit and in-process LLVM backends; the JVM backend was
  removed. SQL integration reuses resolved functions, existing maps, allocators,
  frame workers, sharding, cancellation and merge ownership.
- Handwritten controls showed that better aggregation representation can matter
  more than instruction selection. Bounded local aggregation and compact input
  partitions adapt at existing full-map exits; a fixed-prefix sampler failed on
  changing cardinality and was rejected.
- Top-K direction/capacity specialization and selective helper outlining reduced
  compilation substantially. Retained specialization became automatic; optional
  partition kernels compile on demand.
- Generated VARCHAR grouping, predicates, sorting, projection, DISTINCT and
  aggregate consumers widened coverage. Ranking/group admission must preserve
  exact ties, aggregate semantics and all work in the measured interval.
- Existing page-frame scheduling already supplies dynamic morsels. Smaller scan
  tasks regressed the Q8 control; no replacement scheduler was retained. NUMA
  placement still needs evidence from a suitable machine.
- Large phase gains did not always survive complete-query measurement. Keep
  preparation, merge, output, first-use compilation and configuration visible.
  Main design records: [PIPELINE_DESIGN.md](archive/PIPELINE_DESIGN.md),
  [BOUNDED_AGGREGATION.md](archive/BOUNDED_AGGREGATION.md),
  [AGGREGATE_DESIGN.md](archive/AGGREGATE_DESIGN.md).
