# Query compiler prototype progress

Keep this short: meaningful changes, what improved, and what we learned.
Routine runs print to the console; generated code is replaced on the next run.

## 2026-10-25 — First working pipeline on two backends

- One typed IR generates the scan, filter, hash probing and aggregate updates for
  both JVM (via javac) and native (via C) execution over QuestDB page frames.
- First measurement: 1M rows, 1,024 groups, filtered COUNT/COUNT(value)/SUM(value+3),
  one thread, Ryzen 9950X, JDK 25. Medians: QuestDB scalar **12.22 ms**, generated
  JVM **6.00 ms (2.04x)**, native **4.96 ms (2.46x)**. All results matched SQL;
  33 tests passed across both backends.
- Compilation/load cost: **165 ms JVM**, **46 ms native**. Reuse matters.
- Scope: synthetic workload with QuestDB parallel GROUP BY disabled; different
  hash-table implementations mean this does not isolate code generation's gain.

## 2026-10-25 — Controls added; DISTINCT wins depend on pair cardinality

- Added the normal QuestDB path and handwritten Java with the same layout/hash
  as generated code. DISTINCT now emits both pair deduplication and group-count
  collapse through the IR. All **47 tests pass**, including byte-for-byte state
  checks against handwritten kernels, null groups and table-capacity failures.
- Synthetic 1M rows / 1,024 groups, one execution thread, 20 warmups and 15
  measured runs. Latest medians in ms; all results matched SQL:

  | Workload | QuestDB scalar | QuestDB normal | Handwritten Java | Generated JVM | Native |
  | --- | ---: | ---: | ---: | ---: | ---: |
  | Filtered COUNT/COUNT(value)/SUM | 11.81 | 22.61 | 6.09 | 6.25 | 5.02 |
  | DISTINCT, 131,072 non-null pairs | 17.66 | 23.73 | 15.19 | 13.34 | 12.80 |
  | DISTINCT, 989,691 non-null pairs | 27.07 | 29.25 | 100.04 | 74.58 | 63.64 |

- Both DISTINCT outcomes reproduced in fresh processes. A 100k-row/100k-group
  singleton case also showed no win: generated JVM 25.97 ms, normal 25.82 ms,
  scalar 7.28 ms. Timings include allocation, copying and result sorting;
  compilation is separate (~190–200 ms JVM / ~105 ms native for two kernels).
  This is exploratory evidence on a shared host, not a full ClickBench run.
- The mixed-query handwritten control nearly matches generated JVM code: the
  engine gap cannot be credited to code generation alone. Flat DISTINCT wins
  with repeated pairs but regresses with nearly unique pairs. Keep it experimental;
  next compare generated code using QuestDB's existing DISTINCT algorithm before
  connecting this to the normal SQL planner.

## 2026-10-25 — Generated QuestDB's nested DISTINCT algorithm

- Added singleton-inline values and growable per-group sets to both backends.
  The IR emits QuestDB's set layout, hash, probing and rehashing; both use its
  allocator. The control calls the actual `GroupByLongHashSet`. The outer group
  map remains the prototype's. **53 tests pass**, including set-state comparison
  after every insertion and allocator-failure cleanup through native callbacks.
- Same synthetic 1M rows / 1,024 groups, one thread, 20 warmups / 15 measurements,
  core `8da4e347f9`. Latest medians in ms; both outcomes reproduced in fresh processes:

  | Unique non-null pairs | QuestDB scalar | QuestDB normal | Flat JVM | Nested Java control | Nested JVM | Nested native |
  | --- | ---: | ---: | ---: | ---: | ---: | ---: |
  | 131,072 | 16.37 | 21.76 | 13.38 | 7.28 | 9.08 | 6.04 |
  | 989,691 | 22.52 | 26.82 | 69.99 | 14.80 | 13.92 | 10.90 |

- Nested DISTINCT removes the nearly-unique-pair regression in these cases;
  native also falls from 60.74 ms flat to 10.90 ms nested. Generated JVM still
  loses to the Java control on repeated pairs, so generation alone is no guarantee.
- Singleton limitation remains: 100k rows / 100k groups takes 23.10 ms nested
  JVM / 22.33 ms native versus 7.00 ms scalar SQL (25.48 ms normal SQL).
  Use nested DISTINCT as the next prototype baseline, without claiming a general
  engine win. Allocation, result copying and sorting are timed; compilation is
  excluded. Shared-host timings establish outcomes, not a hardware bottleneck.

## 2026-10-25 — Mixed aggregation works; singleton slowdown was mostly comparison sorting

- One generated loop now updates SUM, COUNT and nested DISTINCT, sharing the
  scan and group lookup. Aggregate state widths support any order. **59 tests
  pass**, including independent nulls, column tops, rehashing across chunks and
  state-by-state checks against the Java control. Q9-style subset, without AVG
  or top-10; still synthetic, one thread and the prototype's outer map.
- Added phase timings and report both full time and time excluding the harness's
  canonical comparison sort. That subtraction is per sample; copying, allocation
  and cleanup remain included. No query here requests SQL ORDER BY.
- Mixed query, 1M rows / 1,024 groups, 20 warmups / 15 measurements. Latest
  medians in ms excluding comparison sorting; outcomes repeated in fresh processes:

  | Unique non-null pairs | QuestDB scalar | QuestDB normal | Java control | JVM | Native |
  | --- | ---: | ---: | ---: | ---: | ---: |
  | 131,072 | 18.91 | 24.27 | 8.22 | 10.07 | 7.13 |
  | 989,691 | 23.77 | 29.88 | 13.47 | 16.16 | 12.36 |

- Native takes about 8–13% less time than the Java control here; generated JVM
  still loses. Mixed aggregation has not established a larger generation benefit.
- **Correction to the earlier singleton conclusion:** 100k-row/100k-group nested
  DISTINCT takes ~4.53 ms versus scalar SQL ~6.12 ms after excluding comparison
  sorting. The full totals were 22.95 / 7.15 ms: sorting cost 18.51 / 1.01 ms.
  Scalar output was already ordered (zero adjacent descents); nested output had
  46,136 descents. Group-building itself took 1.64 / 3.25 ms. The mixed singleton
  case likewise takes native 5.60 ms versus scalar 8.99 ms without comparison
  sorting. These runs do not establish a singleton aggregation regression.

## 2026-10-25 — Unknown cardinality, broader evidence, and opt-in SQL

- Both backends now generate outer-table growth from 64 slots, preserving whole
  aggregate states. Added global aggregates, native allocation budgets using the
  caller's memory tracker when present, cancellation within growth/long probes,
  and cleanup/reuse after failure. **86 tests pass**, including mixed native/Parquet
  fallback after partial work, schema invalidation and caller-tracker accounting.
- Added a 15-case matrix spanning cardinality, skew, nulls, selectivity, key widths,
  aggregate mixes and three held-out distributions. Every warmup/result matched
  SQL. Tables grow without input-cardinality hints; peak retained allocation and
  actual R/G/P counts print to the console. No result archive.
- Selected matrix medians in ms, excluding only comparison sorting, 20 warmups /
  9 measurements, one thread on the shared Ryzen 9950X host:

  | Case | Scalar SQL | Normal SQL | Java control | JVM | Native |
  | --- | ---: | ---: | ---: | ---: | ---: |
  | Mixed, 1M rows / 1,024 groups | 10.56 | 17.27 | 5.77 | 5.92 | 5.47 |
  | Flat DISTINCT, 200k rows / 198k non-null pairs | 6.96 | 8.00 | 21.77 | 22.88 | 17.82 |
  | Held-out correlated DISTINCT, 370k rows / 7,920 groups | 16.04 | 19.29 | 10.76 | 14.82 | 13.55 |

- Native is not universally better than the same-algorithm control. Keep nested
  DISTINCT as the default; flat DISTINCT's near-unique-pair regression persists.
  Native peak allocation across the matrix ranged from 16 KiB to 41.83 MiB;
  this excludes boxed output/bookkeeping and is not comparable to SQL peak memory,
  which the runner does not instrument. Background I/O pressure remained high;
  these are exploratory outcomes, not a hardware bottleneck study or server win.
- Real-data check: original 32,561 rows from [UCI Adult](https://archive.ics.uci.edu/dataset/2/adult),
  four numeric columns, no replication/reordering. Both plans matched SQL.
  Twenty warmups left JVM compilation unsettled; with 200, the two-key aggregate
  took normal **0.564 ms**, JVM **0.636**, native **0.512**. Low-cardinality grouping
  took normal **0.431**, JVM **0.139**, native **0.150**. Backend choice is workload-dependent.
- Added an opt-in SQL adapter using QuestDB's optimized operator model, explicit
  COMPILED/NORMAL/AUTO modes, capability fallback, and a bounded IR code cache.
  Code survives eviction while leased; query data/scan factories are never cached.
  Unsupported operators/types use ordinary SQL. Cancellation/memory errors remain
  errors. This does not change production planner selection or worker scheduling.
- Cold economics still dominate: a 1M-row SQL count/sum case took native **3.16 ms**
  warm versus normal **4.25 ms**, but compilation cost **79 ms**. Cache-hit factory
  creation took **0.81 ms**. First-run-excess / warm-saving suggests ~74 further
  executions, an estimate excluding intermediate warmup costs. At 128 rows / 128
  groups, native **0.190 ms** lost to normal **0.140 ms**, even before compilation.
  Cache alone does not justify automatic admission. Keep compilation opt-in;
  remaining integration work is output-state ownership, worker-local state/merge,
  and an evidence-based admission policy. Multiple DISTINCT, spilling and SIMD
  remain outside this prototype stage.


## 2026-10-25 — Direct AsmJit compilation and composed operator pipelines

- Added an in-process x86-64 backend using the AsmJit revision selected by
  QuestDB. Typed IR lowers to generic registers/labels, then AsmJit's register
  allocator and executable code. No per-query source compiler or filesystem work.
  C and JVM remain independent backends of the same IR. Immediate operands,
  comparison branches and unreachable-code removal matter: naive emission made
  the 1M-row SQL count/sum case take 11.12 ms warm; selected emission takes 3.98 ms
  (C 3.19, normal SQL 4.31). These separate exploratory runs establish an outcome,
  not a hardware cause.
- Added composable scan/filter/project/inner-hash-join/aggregate operators and
  dependency-ordered pipelines. Duplicate/null/composite join keys, three-way
  joins, shared aggregate state and aggregate → join → aggregate all match SQL.
  Generated join payloads and output tuples share query memory/cancellation.
  **153 tests pass** across the three backends, including growth, failure cleanup,
  partial compilation, code lifetime and integer/branch semantics.
- Fresh compilation + first execution, one thread, five measurements after three
  infrastructure warmups; no query-code cache. 1M fact rows, 700 dimension rows:

  | Physical plan | AsmJit backend compile ms | C backend compile ms | AsmJit prepare + execute ms | C prepare + execute ms | Normal SQL ms |
  | --- | ---: | ---: | ---: | ---: | ---: |
  | Filter → group | 1.47 | 113.73 | 42.28 | 149.28 | 39.01 |
  | Join → group | 1.73 | 186.30 | 19.05 | 201.25 | 29.99 |
  | Aggregate → join → aggregate | 1.29 | 258.70 | 8.50 | 263.90 | 6.88 |

  C/AsmJit compile identical physical graphs; normal SQL additionally parses and
  optimizes SQL. Totals include scan-factory preparation, native state, output
  copying and query-state cleanup, excluding comparison sorting and code disposal.
  The first AsmJit sample in the 100k-row process spent 16.64 ms in backend setup
  and compilation; steady compiler-infrastructure medians are not process startup.
- All 15 matrix workloads matched SQL (20 warmups / 9 measurements). AsmJit/C
  execution-time ratio: median 1.19, range 0.89–3.70. The simple global byte
  aggregate remains 0.81 ms versus C 0.22; flat near-unique DISTINCT still loses
  badly to SQL. At 128 rows, fresh AsmJit preparation + execution takes 2.25–3.02 ms
  versus normal SQL 2.20–2.74 ms across the three shapes. Fast compilation is not
  sufficient grounds for automatic admission.
- Both UCI Adult plans also match SQL on the original 32,561 rows. With 200
  warmups, AsmJit takes 0.668 / 0.206 ms versus C 0.516 / 0.160 ms; the execution
  tradeoff persists on real data.
- Keep AsmJit as the fast compilation tier and C as the optimizing reference.
  Automatic tiering, an SSA optimizer, SQL join lowering, parallel state/merge,
  streaming results, wider types and spilling are still absent. The operator graph
  currently enters through the physical DSL; the existing narrow SQL adapter can
  use AsmJit. Production planning is unchanged. Measurements are synthetic,
  one-thread/shared-host observations with high background I/O pressure, not a
  general engine win. Reproduce with `run-compiler.sh`, `run-matrix.sh` and
  `run-reuse.sh`; keep console output rather than a result archive.

## 2026-10-25 — Why the AsmJit execution tier is slower

- Inspected emitted assembly and measured five cases, pinned to CPU 8, three
  interleaved rounds of 64 fresh-state executions after warmup. Compilation and
  comparison sorting are outside measurement; allocation/copy/cleanup remain.
  Cycles/instructions counted on the execution thread with 100% PMU running time.
  These cases explain concrete code-quality gaps, not every matrix case or a
  specific frontend/cache/port bottleneck.
- C → AsmJit instructions per input row: mixed **87.90 → 123.52**, nested unique
  **211.23 → 347.09**, selective **23.85 → 48.08**, short-key count **51.53 → 75.74**,
  global byte **16.23 → 50.28**. Mixed cycles/row: **27.54 → 34.34**; global byte:
  **2.42 → 9.38**. Task CPU time closely matched wall time; no major faults, and
  minor-fault counts were comparable within each pair. The host still has high
  background I/O pressure; this is a resident-data diagnostic.
- Assembly exposes missing constant/copy propagation (including hashing constant
  zero in the global aggregate), algebraic simplification (`imul ...,1`), indexed
  addressing, branch cleanup and loop-invariant specialization. Mixed AsmJit also
  spills scan addresses/index to the stack. The inspected C scan loops are scalar;
  vectorized scanning does not explain these cases.
- Counterfactual: C at `-O1` still takes **5.54 ms** mixed / **0.236 ms** global byte,
  versus `-O3` **5.25 / 0.231 ms** and AsmJit **6.53 / 0.884 ms** in the initial
  diagnostic. Most of this advantage does not require aggressive `-O3` passes.
  Prioritize generic IR simplification and x86 addressing/branch selection before
  wider SIMD work. No executor/code-generator changes made in this investigation.

## 2026-10-25 — Shared IR optimization before backend lowering

- Added effect-aware constant/copy propagation, arithmetic and comparison
  simplification, constant-branch elimination and dead-local removal. Every
  backend receives the optimized typed IR. Branches merge facts; loop-carried
  writes invalidate them. Loads/allocations retain evaluation order and lazy
  semantics. This is conservative structured dataflow, without SSA or alias
  analysis. A startup flag disables the stage for controlled comparisons.
- **170 tests pass**, including copies across writes/backedges, conditional state,
  aliased memory, signed boundary comparisons, wraparound, allocation counts and
  unselected effects. All 15 matrix cases and both original UCI Adult plans
  continue matching SQL across the three backends.
- Pinned execution-thread A/B, three interleaved C/AsmJit rounds of 64 executions
  per configuration, with compilation and comparison sorting excluded:

  | AsmJit case | Before ms | After ms | Instructions/input row before → after | Cycles/input row before → after |
  | --- | ---: | ---: | ---: | ---: |
  | Global byte, 500k rows | 0.853 | 0.523 | 50.28 → 30.77 | 9.04 → 5.55 |
  | Short-key count, 1M rows | 2.403 | 1.700 | 75.74 → 67.87 | 12.75 → 8.93 |
  | Mixed, 1M rows | 6.533 | 6.340 | 123.52 → 117.52 | 34.33 → 33.58 |

- Nearly-unique DISTINCT was effectively unchanged in the focused check. Across
  the 15-case matrix, median AsmJit/C execution ratio moves only **1.190 → 1.175**;
  the remaining gap is substantial. Other AsmJit cases varied from -8.2% to +1.5%,
  apart from the tiny case (+13 us); that tiny regression did not reproduce after
  10,000 warmups (3.93 → 3.57 us, overlapping ranges). These are shared-host
  exploratory timings with persistent background I/O pressure.
- Median AsmJit compilation excluding process initialization rises from **0.92 to
  1.31 ms** in the matrix. Fresh composed plans still compile in **1.54–1.95 ms**
  for the backend stage. Keep the pass; next investigate indexed addressing,
  redundant value computation and loop optimization. No query-specific rules.

## 2026-10-25 — Remove the JVM backend

- Removed generated Java compilation/class loading and JVM runner/test variants.
  AsmJit and C remain behind the shared typed IR and optimizer. The source emitter
  now emits ordinary while loops without the Java reachability workaround.
- Clean build: **117 tests pass**. Small-input matrix, flat/nested DISTINCT demo,
  SQL reuse/cache and composed-plan compiler runners all pass their SQL checks.

## 2026-10-25 — Shared SSA middle layer, kept opt-in

- Added explicit control-flow blocks, phi values, dominance verification,
  dominance-scoped value numbering, constant-edge pruning and pure loop-invariant
  motion. Memory effects separate reads, writes and runtime calls: loads can be
  reused within an unchanged block but are never hoisted. No alias analysis yet.
  Both C and AsmJit consume this layer with `-Dquestdb.codegen.ssa=true`.
- SSA destruction uses liveness/interference to coalesce phi values, parallel
  copies for genuine cycles, fallthrough layout and cheap-value rematerialization.
  Naive edge blocks and hoisted address temporaries initially caused large AsmJit
  regressions; correcting these was necessary, but did not establish a general win.
- **127 tests pass on both paths**, including nested loop swaps, zero-trip guarded
  loads, callback memory changes and invalid SSA rejection. All 15 matrix cases,
  both original Adult plans, composed plans and SQL cache/reuse match SQL. Backend,
  optimizer and composed-plan tests also pass with SSA optimization disabled.
- Current default versus SSA, pinned execution thread, three interleaved C/AsmJit
  rounds of 64 fresh-state executions after warmup; compilation and sorting excluded:

  | AsmJit case | Default ms | SSA ms | Instructions/input row default → SSA |
  | --- | ---: | ---: | ---: |
  | Mixed, 1M rows | 6.445 | 6.618 | 117.52 → 122.50 |
  | Short-key count, 1M rows | 1.694 | 1.854 | 67.91 → 68.20 |
  | Global byte, 500k rows | 0.522 | 0.501 | 30.81 → 31.84 |

- Matrix median AsmJit/C execution ratio was **1.172 → 1.187**; seven case
  medians regressed. The largest was short-key count (+41% in the unpinned matrix,
  +9% in the focused check). Backend compilation after initialization rose from
  roughly **1–2 ms to 2–11 ms**. Persistent host I/O pressure limits timing claims;
  the focused counters had 100% PMU running time. No hardware bottleneck established.
- Keep the structured path as default. SSA is a verified experimental foundation,
  not a performance breakthrough. Next address per-row temporaries/register pressure
  and analysis cost before enabling it by default. No query-specific rules added.

## 2026-10-25 — In-process LLVM backend

- Added `LlvmBackend` with LLVM 21.1, O0–O3 and default O2. Original typed IR
  lowers to verified SSA, then LLVM text crosses one FFM call into LLVM's native
  optimizer and ORC LLJIT. No query subprocess/files by default. LLVM keeps phi
  values and owns optimization; our experimental optimizer/SSA flags affect only
  C/AsmJit. Per-kernel resource trackers follow the existing cache-lease lifetime.
- **187 tests pass**, including all four optimization levels, wrapping arithmetic,
  unaligned stores, poll/phi edges, invalid-module recovery and code release.
  Matrix, composed plans, original Adult rows, SQL reuse and flat/nested demo
  runners match SQL. An initial lexer crash exposed the requirement for a trailing
  NUL in LLVM's input buffer; the bridge now owns a terminated copy, with regression
  coverage for exact-length Java buffers.
- Fifteen-case matrix, 20 warmups and five rotated samples: median LLVM-O2/C
  execution ratio **1.011**, LLVM-O2/AsmJit **0.865**. This is not universal:
  flat-unique remained 14.5% slower than C. Median compilation excluding the first
  case: **34.94 ms LLVM**, **1.17 ms AsmJit**, **86.8 ms external C**.
- Focused check: pinned execution thread, three rotated rounds of 64 fresh-state
  executions after 60 warmups; C uses Clang 21 with `-march=native`, matching LLVM's
  host targeting. Compilation and validation sorting excluded; allocation, copying
  and cleanup included. Median milliseconds per execution:

  | Case | AsmJit | LLVM O2 | Native Clang |
  | --- | ---: | ---: | ---: |
  | Mixed, 1M rows | 6.341 | 5.006 | 5.104 |
  | Short-key count, 1M rows | 1.690 | 1.439 | 1.424 |
  | Global byte, 500k rows | 0.520 | 0.242 | 0.236 |

- LLVM instructions/input row were 76.18 / 46.32 / 17.19 versus AsmJit's
  117.44 / 67.87 / 30.76. Counters ran at 100%, with no major faults. Shared-host
  I/O pressure persisted; these are serial prototype observations, not a hardware
  bottleneck diagnosis or a production engine comparison.
- Warm text emission plus parsing/initial verification cost **0.35–0.9 ms**
  (**3–5%** of compilation) across mixed, nested-unique and global-byte kernels,
  O1/O2, five warmups and eleven samples. Keep text for now: a binary bridge still
  has to construct LLVM IR. Optimization and machine-code generation dominate.
  Fresh 100k-row composed plans spend **40–56 ms** in the O2 backend versus
  **1.5–1.8 ms** in AsmJit; O1 saves some compilation but remains far slower to
  compile. LLVM provides a useful optimizing tier; automatic tier selection and
  its admission/reuse policy remain unimplemented.

## 2026-10-25 — Aggregate integration design and lifecycle checks

- Added the [integration design](AGGREGATE_DESIGN.md), with source references for
  resolution, planner shortcuts, state layouts, ownership and fallback constraints.
  It connects resolved functions first; shared Java/native definitions are a later
  experiment. No execution implementation changed.
- Freshly compiled core aggregate methods confirmed that AVG's `setEmpty` followed
  by `computeNext(5)` produces NaN, while `computeFirst(5)` produces 5. First-row
  handling must be explicit. SUM over `LONG_MAX,1,1` produces 1 sequentially but
  `LONG_MIN+1` under the checked split/merge schedule: sentinel overflow prevents
  assuming arbitrary regrouping is equivalent. COUNT empty and null/fill states
  were also verified to differ. These are lifecycle checks, not performance claims.

## 2026-10-25 — Parallel execution follows the Java algorithms

- Added generated scan/shard/merge execution for the existing COUNT, COUNT(value),
  SUM and nested DISTINCT operators on C, AsmJit and LLVM. Operator expansion is
  on hold. `GroupByShardingContext` / `GroupByMapFragment` supply the algorithm:
  current thresholds, 256 hash shards, independent shard merges, a retained
  sharding hint, and concatenated disjoint results. New keys copy complete states.
- DISTINCT merge follows `CountDistinctLongGroupByFunction.merge` and
  `GroupByLongHashSet.merge`, including source-pointer adoption and the existing
  size-based direction choice. All source allocators survive result consumption.
  Reuse `GroupByAllocatorFactory` and `PerWorkerLocks`; the initial custom
  allocation lock and copy-union merge were removed after checking the Java path.
- Direct Java-oracle tests cover 64 DISTINCT source/destination combinations on
  each backend: set headers/slots, pointer retention/adoption, growth, allocation
  and cleanup. COUNT/SUM merge states are checked against regular Java methods.
  Parallel tests include mixed aggregates, column tops, reuse, memory/capacity
  failure and latch-controlled failures during both scan and shard merge.
  Validation: 226 tests passing (full 223-test suite, then the expanded parallel
  suite adding global DISTINCT with empty shards); no core production changes.
- Performance validation now requires **at least 500M input rows per case**.
  `run-parallel.sh` defaults to that size and rejects smaller inputs. The row
  argument is absolute, replacing the old multiplier. The smaller matrix remains
  correctness/debugging coverage; its timings no longer support scale claims.
- Ran mixed COUNT/SUM, 1%-selective aggregation and repeated nested DISTINCT at
  **exactly 500,000,000 rows each**, with five full-data warmups and nine measured
  rounds in rotated order. All 252 executions matched the scalar SQL oracle;
  native worker row totals were checked on every execution. LLVM O2; all three
  backends' execution algorithms are unchanged by this benchmark-harness change.
  Median milliseconds include allocation, scan, merge, boxed result copy and
  cleanup; compilation and comparison sorting are excluded. P1/P2/P4/P8 use the
  parallel executor at 1/2/4/8 workers. SQL8 has seven real background query workers
  plus the caller; the prototype has N workers plus a coordinator. No CPU pinning.

  | 500M-row input | Original serial | P1 | P2 | P4 | P8 | SQL8 |
  | --- | ---: | ---: | ---: | ---: | ---: | ---: |
  | Mixed COUNT/SUM | 2339.109 | 1329.184 | 665.608 | 349.098 | 193.337 | 1118.362 |
  | 1%-selective COUNT/SUM | 297.125 | 311.870 | 169.780 | 120.204 | 119.689 | 25.602 |
  | Repeated nested DISTINCT | 2777.331 | 2749.991 | 1428.492 | 833.554 | 811.989 | 2056.081 |

- The selective case is **4.67x slower than SQL8**, reversing the earlier small-input
  comparison. Mixed COUNT/SUM scales 6.88x from P1 to P8. Repeated DISTINCT scales
  3.39x, with little improvement from four to eight workers. This is a workload-
  dependent result, not a general engine win or a hardware bottleneck diagnosis.
- Group counts are 1,024 / 8,191 / 1,024 respectively. Repeated DISTINCT has
  **131,072 non-null group/value pairs**: 500M input rows do not establish behavior
  for 500M unique pairs. At P1/P4/P8 its observed retained native state is
  10.77/41.52/82.64 MiB. Mixed/selective P8 retain 6.30 MiB; the native-state budget
  stayed at 512 MiB. SQL native peak allocation is not instrumented. Data columns
  occupy about 6.5 GB for each COUNT/SUM case and 6 GB for DISTINCT.
- Measured P8 ranges (nine samples): mixed 183.691–229.293 ms, selective
  116.011–122.182 ms, repeated DISTINCT 782.997–883.065 ms. Every measured variant
  recorded **zero process major faults and zero process storage-read bytes**.
  These are warm-page-cache execution measurements, excluding dataset creation.
  Comparisons with the original serial executor also include sharding, not only
  threads. Compilation still builds four kernels (roughly 50–59 ms for mixed,
  130–136 ms for DISTINCT in this run).
- This is still a prototype table and executor, not production dispatch/maps.
  Core map pre-sizing and eager source-map release are not reproduced; composed
  joins and the SQL adapter remain serial. Flat DISTINCT remains a separate serial
  experiment. Production runtime integration takes priority over new operators.
- Measurements used a stable copy of the installed core JAR after a concurrent
  build invalidated the first run's mutable class directory. The Ryzen 9950X host
  had no observed CPU/memory PSI pressure but sustained system-wide I/O pressure.
  These are exploratory execution results, not a hardware bottleneck diagnosis or
  an engine-wide performance claim. Native memory figures are sampled at joined
  barriers and do not capture every transient rehash peak.

## 2026-10-25 — Selective-scan diagnosis at 500M rows

- Reproduced the 1%-selective gap with five warmups and nine rotated samples:
  LLVM P8 **117.131 ms**, SQL8 **26.096 ms**. Forcing only QuestDB's compiled
  filter to scalar mode gives **44.709 ms**. All real-query results agree:
  5,000,000 matching rows and 8,191 groups from 500,000,000 input rows.
- `Pipeline.lower` eagerly loads every projected column before the predicate.
  Both optimized LLVM IR and actual JIT machine code retain the key and SUM-value
  loads for all rows, plus per-row column-base guards. The filter loop is scalar.
  QuestDB's existing path first produces matching row IDs with its compiled
  filter, then aggregates only those rows; its BYTE filter supports 32-row AVX2
  iterations. Whole-loop compilation has not reproduced this execution strategy.
- PMU intervals cover all existing JVM threads and five full executions each,
  repeated three times in rotated order, with 100% counter running time. Median
  retired instructions/input-row: prototype **15.77**, SQL SIMD **6.02**, SQL
  scalar JIT **12.89**; cycles/input-row: **8.51 / 2.02 / 3.88**. No major faults.
  These establish extra CPU work, not a measured DRAM-bandwidth bottleneck or an
  isolated attribution of the remaining gap to eager loads.
- A dispatch-only diagnostic replaces native kernels with no-ops (empty output,
  not a valid query): **5.426 ms** with 1M-row frames, **1.364 ms** with 8M frames.
  The real prototype changes only **117.131 -> 114.876 ms** with larger frames;
  merge is about 0.3–0.4 ms. Dispatch and merge do not explain most of the gap.
  Diagnostics stayed under `/tmp`; no execution algorithm changed. The next
  target is reproducing Java's filter-first execution before adding operators.

## 2026-10-25 — Staged scan compiler and measured strategy selection

- Studied data-centric compilation, vectorization versus fusion, relaxed operator
  fusion, Umbra/Flying Start, multi-level IR, adaptive execution, morsel scheduling,
  optimizer robustness, LLVM and AMD Zen 5 guidance. The source-validated design,
  primary references, alternatives and integration boundaries are in
  [PIPELINE_DESIGN.md](PIPELINE_DESIGN.md), written before implementation and
  updated when measurements disproved a choice.
- `ScanPlan` retains typed scan/filter/project structure and dependencies. Both
  frontends share demand-driven lowering. Requested input values are bound once
  before stateful work; unused inputs generate no reads. Projection substitution
  has a structural budget. Verified `VectorMask`/`TrailingZeros` operations carry
  read effects through SSA. LLVM emits vectors; C/AsmJit retain portable lowering.
  Actual JIT code uses AVX-512 `vptestmb`, `tzcnt` and `blsr`, with key/value reads
  after selection. Aggregate, hashing, allocation and merge algorithms are unchanged.
- Keep three physical strategies: the original eager input binding, scalar
  filtering before payload loads, and vector masks followed by ordered consumers.
  Query-local feedback samples equivalent executions and selects from measured
  cost. Parallel selection happens at the existing joined frame boundary, avoiding
  independent exploration workers delaying a frame. No query-name, cardinality
  or CPU-model gate. Every chunk executes once; failures are never replayed.
- A duplicated full-mask dense path failed its prediction: it increased code and
  compilation cost, hurt partial masks, and did not recover the original eager
  loop's dense-input performance. Removed it. Retaining the existing eager
  algorithm as a first-class alternative resolved that regression. This is why
  neither late loading nor vector execution is an unconditional optimization.
- **292 tests pass**, both normally and with the shared SSA path enabled, across
  LLVM, C and AsmJit. Coverage includes guarded-page tails/unused columns, all
  integer widths, high mask bits, nulls, column tops, overflow, projections,
  expression-expansion bounds, memory effects, allocation ordering, parallel
  failure cleanup, deterministic adaptation, and code-cache lease ownership.
- Final LLVM O2 comparison: **13 workloads, exactly 500M rows each**, five full-data
  warmups and nine rotated samples of six variants. All **1,092 executions**
  matched scalar SQL; native worker row totals were checked. Two additional shapes
  (`short-key-count`, `byte-global`) were tested after strategy design was frozen.
  Process affinity was CPUs 0–7: eight physical cores in one L3 domain on the
  Ryzen 9950X, without per-thread binding. Prototype: eight workers plus coordinator;
  SQL: seven background workers plus caller. Actual SQL plans were printed.
  Median milliseconds include allocation, scan, merge, copy and cleanup;
  compilation and comparison sorting are separate.

  | 500M-row workload | Original eager control | Adaptive | QuestDB |
  | --- | ---: | ---: | ---: |
  | Periodic 1% filter | 120.0 | 30.1 | 27.2 |
  | Mixed COUNT/SUM | 188.4 | 182.3 | 1379.9 |
  | Repeated nested DISTINCT | 741.5 | 739.5 | 2152.2 |
  | Wide key, mixed DISTINCT | 390.9 | 367.7 | 1684.7 |
  | Random 1% filter | 118.4 | 41.9 | 85.3 |
  | Random 50% filter | 353.7 | 222.3 | 921.0 |
  | No matches | 113.7 | 12.8 | 8.8 |
  | All rows match | 290.1 | 289.3 | 1439.4 |
  | Abruptly changing filter | 205.0 | 183.9 | 737.4 |
  | SHORT range predicate | 196.4 | 200.8 | 885.5 |
  | Nullable LONG expression | 231.8 | 230.4 | 752.8 |
  | SHORT key, COUNT | 99.1 | 100.2 | 525.7 |
  | BYTE global COUNT/SUM | 39.2 | 32.4 | 71.2 |

- The selective bottleneck improves **3.98x**, but adaptive execution remains
  **10.6% slower than SQL** there. Forcing masks gives 27.6 ms versus SQL 27.2 ms;
  exploration has a measurable price. SHORT-range and SHORT-key medians regress
  **2.2% and 1.1%** against the original control. No-match execution also remains
  slower than SQL. Unfiltered strategies compile identical IR, so their timing
  variation is not evidence of an optimization. DISTINCT still has only 131,072
  non-null group/value pairs, not 500M unique pairs.
- Fixed-work PMU validation covers all existing JVM threads, three rotated
  intervals per variant, each executing five 500M-row queries. Grouped events
  intermittently returned no counts; those intervals were rejected. The rerun
  with separate cycle/instruction events recorded **100% running time** throughout.
  Selective instructions/input-row: original **15.74**, late scalar **7.84**,
  masks **1.07**, adaptive **1.46**, SQL **6.08**. Cycles/input-row respectively:
  **9.27 / 3.03 / 1.56 / 1.74 / 2.10**. On random 50%, original → adaptive is
  **46.60 → 39.02 instructions/row**, **28.48 → 17.52 cycles/row**. These establish
  reduced CPU work; they do not establish a calibrated memory-bandwidth limit.
- Every measured matrix variant had zero process major faults and storage-read
  bytes; PMU intervals had zero major faults. The shared host still had substantial
  system-wide I/O pressure and occasional timing outliers. Use these as warm-cache
  prototype results, not production or engine-wide claims. The old/new controls
  share state algorithms; QuestDB still has different maps and scheduling.
- Preparing the adaptive query costs **60–330 ms** across these cases, about
  **133 ms** for the selective case. Filtered queries eagerly compile up to three
  scan versions; unfiltered ones compile one. Compilation admission/background
  tiering, production page-frame scheduling and resolved aggregate-definition
  integration remain separate gaps. No new operators or core production edits.
  `run-scan.sh` reproduces the main matrix; diagnostics and the two extra shape
  checks stayed under `/tmp`. No results archive was added.

## 2026-10-26 — Resolved aggregate code inside ordinary QuestDB execution

- Added an optional `GroupByBatchCompiler` at the ordinary keyed factory boundary.
  The LLVM/AsmJit/C implementation binds actual resolved COUNT, integer SUM and
  LONG DISTINCT classes to their assigned state slots. Core keeps its maps,
  probes, shared workers, slot allocators, query limits, sharding, Java merge and
  native result cursors. No alternate resolver, map, executor or output copy is
  needed on this path. Registration is explicit and absent by default.
- First measured the same map/update boundary: **500M rows**, five full warmups,
  nine balanced samples, three independent processes, one execution slot. Mixed
  COUNT/SUM fell from **3.35 s Java to 2.72 s LLVM** (about 19%). Valid fixed-work
  PMU runs fell from **193.57 to 158.65 instructions/row** and **37.50 to 31.01
  cycles/row**. The hardware-first study passed before core edits. This supports
  reduced CPU work, not a general memory-bandwidth claim.
- Ordinary SQL, 500M rows, 512 groups, seven workers plus caller, using the test
  configuration (batch 1,024, sharding threshold 1,000, legacy workers): stock mixed
  aggregation **531.0 ms**, disabled candidate **530.6 ms** (medians of three
  fresh process medians); paired enabled LLVM **482.2 ms**. In those paired
  processes Java was **553.7 ms**: multiple factories/generated sink classes
  affect the comparison, so the paired gain must not be presented as a 13%
  improvement over stock. COUNT was approximately neutral; its final disabled
  control was **266.4 ms** versus stock **264.8 ms**, within the frozen 3% envelope.
- Repeated DISTINCT with 64 groups/512 non-null pairs improved in all three fresh
  SQL processes: Java **655/643/822 ms**, LLVM **518/538/535 ms**. Java variability
  prevents a precise general speedup claim. LLVM at 1/2/4 slots took
  **3.62/1.84/0.97 s**. A held-out skew distribution improved **806 → 552 ms**;
  selective aggregation was neutral (**103.8 → 104.1 ms**, 470,588 accepted rows).
  All cases had 500M input rows, exact result checks and zero timed major faults
  or storage reads. Preparation is separate and remains synchronous; one DISTINCT
  run took **74.6 ms native vs 19.1 ms Java** to prepare its factory.
- Native-row accounting exposed the next gap: high group/pair cardinality activates
  existing Java sharding, and warmed native coverage becomes **zero**. Those
  controls stayed close to Java, but are not generated execution wins. Sharded
  batches, non-keyed aggregation and composed pipelines remain open; so do detailed
  runtime phase/memory instrumentation, result ownership outside keyed aggregation,
  and compilation admission/tiering. No new aggregate family was added.
- Added byte/state equivalence, SQL/alias/schema, column-top/lazy-Parquet conversion,
  no-consumption/reopen, partial construction/cleanup failure, cancellation inside
  native callbacks and native allocation-limit tests, plus wide composite keys
  through ordinary `OrderedMap` growth. The **93 focused integration
  tests pass** across all three backends, with SQL tests on both worker modes;
  the prototype checks total **385 passing tests** (379 in the full run plus six
  added map-growth cases), and **118 affected core tests pass**. Reused DISTINCT callbacks allocate zero Java
  bytes in the focused steady-state test. Native failures propagate without replay.
  The README has reproduction commands; only this breakthrough log is maintained.

## 2026-10-26 — Comparison with server execution defaults

- Corrected the comparison configuration: the earlier runs used normal SQL but
  the test configuration. The harness now optionally uses `PropServerConfiguration`
  and its query worker configuration: batch **2,048**, sharding threshold **10,000**,
  **FIBER_HOST**, filter JIT enabled. Both sides use seven workers plus the caller,
  pinned to CPUs 0–7; HTTP is disabled in this embedded harness. Each case still
  scans **500M rows**, with five warmups and nine rotated measured samples.
- Three fresh unmodified-core controls: grouped COUNT **263.65 ms**, mixed
  COUNT/SUM **529.55 ms** (median of process medians). LLVM across three paired
  processes: **262.80 ms** and **486.18 ms** respectively. COUNT is neutral;
  mixed execution is **8.2% shorter than stock**. In paired processes the ordinary
  Java path took **555.54 ms**, so its **12.5%** paired saving must not be reported
  as the stock-binary result. These are warm execution times, not cold query totals.
- Repeated DISTINCT, 64 groups/512 non-null pairs: paired normal SQL **662.59 ms**,
  LLVM **567.79 ms**; per-process reductions **10–18%**. The held-out skew case
  improved **800.82 → 551.82 ms** in the broader run; selective execution was
  neutral (**103.87 → 104.20 ms**, 470,588 accepted rows). COUNT, mixed and DISTINCT
  were repeated in three fresh paired processes; the broader cases have one such run.
- High group cardinality still executes Java after sharding: the broader run was
  **3,761 → 3,960 ms** (5.3% slower), while an isolated fresh-process repeat was
  **4,130 → 3,881 ms** (6.0% shorter). Both had **zero native rows** in every warm
  measured execution. The difference changes sign with process context; it is not
  evidence of a stable compiler benefit or regression. High-pair DISTINCT likewise
  used Java (**1,738 → 1,723 ms**). Sharded execution remains the integration gap.
- All **306 measured executions** passed exact result comparison, with zero timed
  major faults/storage reads. The shared host's system-wide I/O pressure remained
  high, so this is explicitly a warm-cache, single-query comparison. Preparation
  is separate: after compiler initialization, mixed factories took about
  **10.1 ms LLVM vs 3.8 ms Java**; DISTINCT **36.2 vs 3.2 ms**. There is no measured
  cold-total or concurrent-query win here. Only the harness/docs changed; raw runs
  remain temporary and no results archive was added.

## 2026-10-26 — Matched ordinary / integrated / full LLVM comparison

- Added `--resolved-three-way` to the existing runner. All modes share the exact
  table, SQL semantics, frozen current core binary and configuration, with affinity
  to CPUs 0–7. Each case has **500M input rows**, five full warmups and nine measured
  samples per mode, cycling all six mode orders. The four main workloads and the
  threshold counterfactual ran in **three fresh processes**; COUNT and selective
  controls ran in one. Execution includes allocation/build/merge/result collection/
  cleanup; compilation and comparison sorting remain separate.
- Server-default results below are medians of process medians, in milliseconds.
  Earlier workloads reuse `WorkloadMatrix` definitions; newer ones reuse the
  integration workloads. Thus old and new workload differences are explicit.

  | 500M-row workload | Ordinary SQL | Integrated updater | Full standalone LLVM |
  | --- | ---: | ---: | ---: |
  | Earlier mixed, 1,024 INT groups, filtered COUNT/SUM | 252.905 | 231.368 | 438.830 |
  | Newer mixed, 512 LONG groups, five aggregates | 555.988 | 492.905 | 480.987 |
  | Earlier DISTINCT, 1,024 groups / 131,072 pairs | 1,610.362 | 1,547.436* | 771.512 |
  | Newer COUNT/SUM/DISTINCT, 64 groups / 512 pairs | 768.142 | 531.601 | 249.025 |
  | Grouped COUNT, 512 groups (one process) | 262.127 | 262.205 | 161.603 |
  | Selective mixed, 470,588 accepted rows (one process) | 104.871 | 104.653 | 109.777 |

  `*` Every measured execution used Java after sharding: **zero native updates**.
  Its difference from ordinary SQL is not a generated-code gain. The full pipeline
  scanned all 500M rows natively on every run. Other integrated cases updated all
  accepted rows natively (428,571,429 for earlier mixed; 500M for newer mixed,
  DISTINCT and COUNT). Newer DISTINCT's integrated process medians ranged from
  **519.3 to 580.0 ms**; its full-pipeline advantage persisted in every process.
- A one-setting counterfactual establishes that the older sharding threshold is
  a major cause of the changing comparison. On the **same earlier mixed query**,
  server defaults (threshold **10,000**) give **252.9 / 231.4 / 438.8 ms**, with no
  sharding. Keeping the server configuration and changing only the threshold to
  **1,000** gives **755.6 / 753.3 / 188.5 ms**, all with 256 shards. The full
  prototype goes from **1.74x slower** than ordinary SQL to **4.01x faster**.
  The integrated updater again has zero warmed native coverage in the sharded
  case. Both implementations change algorithms at this threshold; this is not an
  LLVM backend regression or a recommendation to change the production default.
- Replayed the historical harness on the **same current frozen core**: its original
  selective-then-mixed sequence gives **1,374.377 ms ordinary SQL vs 181.501 ms
  adaptive full LLVM**, reproducing **7.57x** (previously 1,379.9 / 182.3 ms).
  The new three-way harness under the test configuration gives
  **1,110.867 / 1,110.625 / 181.834 ms** for that mixed query. These are single fresh
  diagnostic processes, each still with five warmups/nine samples. They establish
  that the historical result remains reproducible; they do not isolate the other
  test/server configuration differences or the original harness's scalar oracle,
  preceding workload and additional compiled variants. Only the threshold change
  above was isolated and repeated. The original test configuration's batch default
  is 2,048; the earlier resolved-only experiment explicitly requested 1,024.
- All **459 primary measured executions** passed exact SQL output comparison and
  input-row accounting, with **zero timed major faults or storage-read bytes**.
  The diagnostic replays add **135 measured executions**, also exact and free of
  timed major faults/storage reads: **594 measured executions in total**.
  The **102 focused parallel/integration tests pass**. The host's high global I/O
  pressure remains a limitation; these are warm-cache single-query observations.
  The ordinary control uses the same candidate core with compilation disabled,
  not a new stock-binary comparison. Full LLVM retains its prototype map, per-frame
  barriers, eight workers plus coordinator and 512 MiB state budget (nonbinding in
  these cases); SQL uses seven shared workers plus caller. Therefore the remaining
  gaps cannot be assigned to fusion, map layout or a hardware bottleneck from
  timings alone. Only benchmark instrumentation/docs changed in this comparison.

## 2026-10-26 — Generated probes over QuestDB maps

- The sharded updater-only bridge restored native coverage but did not improve the
  high-cardinality case (4,315 ms versus an earlier 4,158 ms stock control). Removed
  that bridge, its address buffers and map resize listeners. Reusing an algorithm
  does not require keeping its lookup loop in Java.
- `QuestMapProbe` now emits QuestDB's hash, shard selection and lookup for direct
  INT/LONG keys over the real `Unordered4Map`, `Unordered8Map` and fixed-key
  `OrderedMap`, fused with resolved aggregate updates. DISTINCT set operations were
  already generated. A missing group returns before its update; ordinary Java
  insertion/growth maintains map counters, allocation and tracking, then native
  execution resumes at that row. Existing workers, merge and cursors remain owners.
- Final 500M-row runs, five warmups/nine samples, CPUs 0–7, seven workers plus caller,
  server configuration, unchanged threshold 10,000 and batch 2,048. Stock is a clean
  build of HEAD `8da4e347`, not the disabled candidate. Each workload/variant has its
  own process; table size is checked and results are compared exactly.

  | Sharded workload | Stock ms | Final LLVM ms | Stock / LLVM |
  | --- | ---: | ---: | ---: |
  | COUNT/SUM, 131,072 groups | 3,965.491 | 2,004.177 | 1.98x |
  | DISTINCT, 1,024 groups | 1,659.679 | 870.860 | 1.91x |
  | COUNT/SUM/DISTINCT, 64 groups | 1,726.076 | 587.061 | 2.94x |

- All measured native runs above used generated probes/updates for **all 500M rows**,
  with 256 shards, matching output and zero timed storage reads/major faults. High
  COUNT/SUM fell from **542.7 to 94.8 instructions/row** and **344.7 to 176.8 cycles/row**.
  A separate fixed-work counter check on the fused path cut backend-stalled slots/row
  by **47%**; the stalled fraction rose, so low IPC/stall percentage alone would
  misdescribe this improvement.
- Controls remain limited to warm single-query execution on this shared host.
  Unsharded stock/LLVM controls before removing the unused bridge: mixed
  **535.9/491.4 ms**, COUNT **260.5/263.5 ms**, selective **104.2/104.5 ms**.
  Final disabled high control: **4,027.1 ms**; fresh stock process medians ranged
  **3,965.5–4,143.8 ms**. Initial disabled controls varied in both directions.
  Native DISTINCT process medians also varied **725.8–870.9 ms**; the table uses the
  final, slower result. These are initial mechanism results, not a portable 2x claim.
  Final native factory preparation was **56–114 ms**, excluded from execution.
- Kept checks focused: **12 backend/worker-mode executions pass**, covering the three
  map layouts, negative/null/zero keys, filtered/unfiltered reducer behavior through
  the tests and 500M runs, native coverage after reopen, and failure/drain/recovery.
  No broad suite or results archive. Active raw measurements remain in `/tmp`.
- Remaining boundaries: new-group insertion/growth, filter fusion, merge, wider keys,
  non-keyed/composed execution and compilation admission. Unsupported sharded key
  shapes keep ordinary Java execution. This closes the measured fixed-key sharded
  probe/update gap; it does not complete the whole runtime integration plan.

## 2026-10-26 — Profile the generated map loop before changing its layout

- Profiled the integrated LLVM path, 500M rows per case, CPUs 0–7, seven workers
  plus caller, unchanged server defaults, five warmups/three measured executions.
  Added optional native object/IR/address dumps (`QUERY_CODEGEN_PROFILE_DIR`),
  per-call insertion counts (`query.codegen.profile`) and separate diagnostic
  probe counters (`query.codegen.probeCounters`). Capture windows cover execution,
  result consumption and close; validation/sorting stay outside. No execution
  algorithm or core configuration changed; raw captures remain in `/tmp/query-map-profile`.
- Generated code accounts for **97.2% / 99.0% / 84.1%** of CPU samples in high
  COUNT/SUM, earlier DISTINCT and COUNT/SUM/DISTINCT respectively. Java insertion/setup,
  native boundary/poll overhead and merge each stay below **0.3%** in these captures.
  The last case spends another **14.5%** in the separate filter. Fresh basic-counter
  medians are **1,996.9 / 829.7 / 562.5 ms**; the last ranges **555.7–598.1 ms**.
- High COUNT/SUM averages **1.47 index slots per row and exactly one full-key
  comparison**. There are 1,048,576 worker-local insertions per query (0.21% of rows).
  Precise IBS attribution puts **94.9% of summed sampled native load-miss latency**
  at heap key/aggregate-state accesses and **4.1%** at the offsets index. These are
  latency-weighted sample shares, not wall-time shares. RAM-served native loads have
  median **431 cycles**. Separate non-multiplexed counter passes show **80.4%** backend
  stalled slots and a **99.57%** load-not-complete / not-complete retirement-stall ratio.
- Locality counterfactual: 16,384 instead of 131,072 groups, still 500M rows, the same
  256-shard algorithm/default threshold and **byte-identical generated object**.
  Execution falls **1,996.9 → 720.9 ms**, cycles/row **176.0 → 61.3**, while
  instructions/row rise **93.6 → 94.8** and branch misses/row also rise. Native IBS
  RAM-served load samples fall from **5.95% to 0.083%**. This diagnoses working-set
  sensitivity; it is a changed-workload experiment, not an implementation speedup.
- Both DISTINCT CPU profiles cluster at the first nested-set slot load; IBS maps
  the misses to `NestedDistinct.find`, rather than the collision-loop continuation.
  The earlier case's sampled first-slot loads include 157 RAM and 239 L3 hits out of
  630; COUNT/SUM/DISTINCT has 657 L3 and 33 RAM hits out of 1,004. A SIMD index alone
  would still have to service these first accesses and the dependent state loads.
- All completed queries match ordinary SQL exactly. Clean timing/counter runs have
  zero timed storage reads/major faults; CPU-profiler captures are for attribution
  and include attachment/I/O overhead. CPU scheduling delay is small (0.5–2.4% over
  warmup/capture lifetimes), available memory exceeds 75 GiB and memory PSI is zero.
  High COUNT/SUM UMC bus occupancy is **36.4% median / 36.7% peak** at 200ms resolution;
  this is system-wide evidence, not a calibrated bandwidth ceiling. Global I/O
  pressure and unavailable corrected hardware-error counters remain limitations.
  Rejected a 25%-multiplexed combined PMU pass; the separate accepted passes ran 100%.
- Next experiment: overlap independent probe/state accesses across rows and improve
  state locality, retaining the present maps as the control. Require lower memory
  stall/load-latency evidence **and full-query elapsed time**, including merge/output.
  These measurements do not yet establish that a SIMD-tag table is the best layout.

## 2026-10-26 — Prepare/prefetch/consume batches over the existing maps

- Kept a 32-row generated pipeline: prepare candidate entries, prefetch key/value
  lines and the first DISTINCT-set slot, then validate full keys and update in row
  order. Hits reuse the prepared candidate. Collisions/misses use the existing
  lookup path; Java insertion discards the batch before resuming. Scratch is **768
  bytes per slot**; nested-set pointers are never retained across updates. The typed
  IR now lowers cache hints through LLVM, AsmJit and C. No core/configuration change.
- Rejected standalone lookahead that repeated hashing/probing: it helped large
  state but regressed the combined DISTINCT case and the smaller-state control.
  Reusing candidates in the batch removes those observed regressions. The opt-in
  compiler defaults to 32 rows; `query.codegen.prefetchRows=0` preserves the old loop
  (its generated object matches the prior control byte for byte).
- Warm execution, **500M rows per case**, CPUs 0–7, seven workers plus caller,
  unchanged server threshold 10,000 and batch 2,048. Five warmups; initial three
  samples and a fresh five-sample repeat with A/B process order reversed. Below uses
  the faster old process median and slower new process median. Stock is a fresh
  three-sample run of the same clean HEAD binary, not candidate-disabled execution.

  | Workload | Stock ms | Previous LLVM ms | Batched LLVM ms |
  | --- | ---: | ---: | ---: |
  | COUNT/SUM, 131,072 groups | 4,015.743 | 1,990.160 | 1,248.679 |
  | DISTINCT, 1,024 groups | 1,458.904 | 875.700 | 493.781 |
  | COUNT/SUM/DISTINCT, 64 groups | 1,433.582 | 586.062 | 537.204 |

- Gains over the old generated loop are **37.3% / 43.6% / 8.3% less elapsed time**;
  high COUNT/SUM contributes most of the total saving. The 16,384-group control
  improves **720.9 → 551.0 ms**. A separate deterministic key distribution,
  `((x*198491317)%4294967291)%131072`, improves **863.6 → 739.3 ms** with identical
  generated code. These are synthetic warm-query results, not a portable engine-wide
  speedup; distributions and first-touch state layout still matter.
- High COUNT/SUM cycles/row fall **175.5 → 107.7** despite instructions/row rising
  **93.3 → 117.0**. Non-multiplexed backend-stalled slots/row fall **49.3%**; their
  fraction falls **80.4% → 66.2%**. Native sampled demand-load miss delay falls
  **33.9 → 15.2 cycles/sample** (DISTINCT **5.32 → 1.77**, combined **5.55 → 1.66**).
  IBS labels prefetch instructions as loads: exclude their exact disassembled PCs
  from these demand-load figures. Latency sums overlap and are not wall-time shares.
  Removing hints while keeping batches raises high COUNT/SUM to **1,855.2 ms**,
  supporting prefetch latency overlap as the main measured mechanism.
- Exact SQL results and full native coverage pass throughout; clean timing runs
  have zero storage reads/major faults. **12 focused backend/worker-mode tests pass**,
  including added scattered-row/partial-tail DISTINCT coverage, reopen, growth and
  failure recovery. Preparation is separate: about **65 ms** for COUNT/SUM and
  **115–118 ms** for DISTINCT. Existing filter, insertion, merge and output remain
  runtime-owned. Shared-host/cache-state limits remain; raw evidence and the validated
  bottleneck study are temporary under `/tmp/query-prefetch`, with no results archive.

## 2026-10-26 — Rejected an extra metadata-prefetch pass

- Exploratory three-pass variant: hash/shard and prefetch the first probe slot,
  resolve/prefetch candidates using the saved lookup, then consume in row order.
  Existing algorithms and insertion/restart rules stayed intact. Scratch grew from
  768 to 2,048 bytes per slot. Control was the existing 32-row two-pass pipeline;
  its generated object remained byte-identical. No core/server configuration change.
- **500M rows**, eight physical cores, five warmups. Three samples per case; high
  COUNT/SUM also repeated with five samples and reversed process order:

  | Workload | Control ms | Staged ms | Outcome |
  | --- | ---: | ---: | --- |
  | COUNT/SUM, 131,072 groups, first pair | 1,242.520 | 1,242.200 | Effectively tied |
  | COUNT/SUM, fresh reverse-order repeat | 1,262.115 | 1,231.215 | 2.4% faster |
  | DISTINCT, 1,024 groups | 483.024 | 516.571 | 6.9% slower |
  | COUNT/SUM/DISTINCT, 64 groups | 516.040 | 513.228 | 0.5% faster |

- The intended local effect happened: sampled first-slot demand loads hit L1
  **21.2% → 98.1%**. Overall backend-stalled slots/row fell only **2.3%**
  (fraction **66.6% → 65.8%**), while high COUNT/SUM instructions/row rose about
  **10.4%**. Aggregate-state loads still dominate sampled miss delay. This does not
  establish the exact remaining hardware queue/port limit or prove why state-load
  latency did not improve. IBS excludes software-prefetch PCs; all eight native
  query threads are represented and PMU counters ran without multiplexing.
- Exact SQL results/full native coverage pass on every run; no timed storage reads
  or major faults, over 75 GiB available and zero memory PSI. The exploratory variant
  passed the existing **12 focused backend/worker-mode checks**. Shared-host process
  variation limits claims about the small gains. Rejected the extra pass and restored
  the original source/build; only this finding is retained in the repository. Temporary
  evidence and the experimental patch are under `/tmp/query-probe-staging`.

## 2026-10-26 — Handwritten C reference exposes no substantial lowering gap

- Added a handwritten COUNT/SUM loop, independent of the typed IR, as an opt-in
  measurement reference (`query.codegen.handwritten=true` in `--resolved-sql`).
  Binding checks the resolved argument types and state offsets for COUNT(),
  COUNT(long), SUM(same long), SUM(int), SUM(short). No SQL-text or data-pattern
  recognition. It accepts arbitrary keys/values and filters. Both packed updates
  and sharded probes use the existing native ABI, 32-row scratch, actual QuestDB
  maps, Java insertion/growth, workers, merge, output and lifecycle.
- Clang 21.1.8 emits frontend IR with LLVM passes disabled; our unchanged LLVM
  21.1.8 O2/ORC path optimizes and executes it for the same host. This compares the
  handwritten algorithm with our IR construction/lowering, without introducing
  another executor or table. The normal generated object remains byte-identical.
- **500M rows**, CPUs 0–7, seven workers plus caller, unchanged server defaults,
  five warmups. Initial three samples and a fresh five-sample reverse-order repeat
  for high cardinality; three samples for each other input. Full warm execution
  includes merge/output/cleanup and excludes compilation/validation sorting:

  | COUNT/SUM input | Generated LLVM ms | Handwritten C ms |
  | --- | ---: | ---: |
  | 131,072 groups, initial pair | 1,248.577 | 1,233.473 |
  | 131,072 groups, reverse-order repeat | 1,239.995 | 1,242.126 |
  | 16,384 groups | 543.747 | 546.173 |
  | Alternate key distribution, 131,072 groups | 743.440 | 732.954 |

- The repeat's ranges overlap (LLVM **1,235.947–1,253.569 ms**, C
  **1,230.764–1,249.840 ms**); there is no substantial consistent execution gap.
  Repeat cycles/row are **107.96 / 107.43**, instructions/row **116.22 / 115.17**
  (LLVM / C). Native loops are scalar and **896 / 883 bytes**. Separate accepted
  PMU runs report **66.7% / 67.8%** backend-stalled slots, with counts/row within
  0.5%; no hardware-stall reduction is established. C's IBS demand-load samples
  still attribute most miss delay to aggregate state; software-prefetch PCs are
  excluded and all eight native query threads are represented.
- Exact SQL results/full native coverage pass on every measurement, with zero
  timed storage reads/major faults. Two new focused lifecycle checks (both worker
  modes) cover null/negative/zero keys, overflow, scattered rows, tails, growth,
  reuse and unsupported shapes; six existing backend/map checks and four LLVM
  backend checks also pass.
  Preparation was roughly **64 ms LLVM / 91 ms C** in the initial processes.
  Keep this reference, not a production replacement. It is a same-algorithm
  reference, **not a proven floor**, and says nothing yet about DISTINCT or other
  map layouts/algorithms. Temporary raw evidence is under `/tmp/query-handwritten`.

## 2026-10-26 — Changing the aggregation strategy exposes substantial headroom

- Widened the handwritten reference beyond the existing maps/runtime. The opt-in
  `native-hash` path fuses filter, full-LONG-key Rosti lookup/insertion/growth and
  five COUNT/SUM updates, with private maps and parallel merge. `native-auto`
  measures min/max first, then uses direct array aggregation when the entire key
  range fits a 512 MiB worker-state budget; otherwise it runs that Rosti path.
  Neither receives cardinality or key-range hints. Reuses actual `rosti.h` probing,
  the existing FFM binding, page-frame scan and benchmark/SQL validation harness.
- **500M rows, eight physical cores, unchanged server defaults**, frozen candidate
  and clean stock binaries from the preceding experiment. Five warmups; three
  samples per case, plus five-sample reverse-order repeats for the main pair.
  Full warm execution includes discovery/allocation/scan/merge/output/cleanup:

  | Input / path | Integrated LLVM ms | Native reference ms |
  | --- | ---: | ---: |
  | 131,072 compact keys, direct array, initial | 1,245.891 | 433.164 |
  | Same, reverse-order repeat | 1,243.732 | 413.124 |
  | Alternate key order, same range | 748.640 | 458.644 |
  | 16,384 compact keys | 545.394 | 263.741 |
  | 131,072 sparse keys, Rosti fallback | 1,233.954 | 2,828.055 |

- Fresh clean stock control: **4,144.499 ms** for the main input. Direct array
  execution demonstrates about **3x** headroom over current LLVM and **10x** over
  stock for that case; not a general engine speedup. Repeat ranges are
  **403.409–421.918 ms** native versus **1,238.850–1,246.780 ms** LLVM. Native
  discovery costs ~68 ms, aggregation ~339 ms and merge <1 ms; local state is
  40 MiB (5 MiB for 16K keys). Initial shared range summaries were replaced with
  private reductions: discovery fell from ~368 to ~69 ms. Full-query repeat
  cycles/input row: **107.74 -> 32.54**; instructions/input row:
  **116.32 -> 46.67**. No calibrated bandwidth ceiling or globally optimal path
  has been established.
- The straightforward fused Rosti reference alone takes **2,575.476 ms** on the
  main input. Its wide-range fallback correctly rejects the 13,726,934,760-value
  domain, but also loses badly. Keep the integrated LLVM hash path; a single
  native table is not the answer. The useful next compiler direction is guarded
  aggregation strategy selection, with admission cost and existing execution
  semantics accounted for. Direct indexing here applies to compact integer
  ranges; it is not a general perfect hash function. Related literature:
  [DuckDB parallel aggregation](https://duckdb.org/2022/03/07/aggregate-hashtable),
  [Gaffney/Patel, CIDR 2024](https://www.vldb.org/cidrdb/papers/2024/p65-gaffney.pdf).
- All measured results match SQL, all 500M rows are accounted for, and measured
  storage reads/major faults are zero. Focused correctness covers negative/zero/
  NULL keys, all-NULL sums, 64-bit key ranges, multiple frames, repeated calls,
  empty input and sparse fallback. This is handwritten Clang 21 O3 execution,
  **not yet generated by our LLVM backend**; its external build takes ~1.22 s
  versus ~64 ms for integrated LLVM preparation, excluded above. Native workers,
  allocation and fixed query shape remain isolated benchmark machinery. No
  production defaults changed; no DISTINCT/general SQL/overflow-equivalence claim.
  Temporary raw evidence: `/tmp/query-optimal`.

## 2026-10-26 — DuckDB 2.0 comparison: strategy selection matters in both engines

- Latest stable [DuckDB 2.0.0](https://duckdb.org/2026/10/21/announcing-duckdb-200),
  released October 21; official JDBC 2.0.0.0, runtime version verified. Added an
  optional `duckdb` mode using the existing fixture, result representation and
  timing/perf controls. All grouped values are checked exactly against QuestDB SQL.
  No dependency added to the default build or production execution changes.
- **500M rows, CPUs 0–7, eight execution threads, five warmups**, three measured
  queries per case plus a fresh five-sample main DuckDB repeat after the controls.
  Both use their native persistent storage. DuckDB retains defaults except thread
  count; checkpoint precedes warmup, and loading/preparation/validation sorting
  are excluded. Complete query/result conversion/close is included. Its main table
  is **2.1 GiB**, with BitPacking values; creation took ~22 s, checkpoint ~49 ms.
  Input types match BIGINT/BIGINT/INTEGER/SMALLINT. DuckDB returns HUGEINT sums;
  these inputs fit QuestDB LONG, and results agree. No attempt to narrow DuckDB's
  aggregate semantics or disable its optimizer/compression.

  | COUNT/SUM input | DuckDB default ms | Integrated LLVM ms |
  | --- | ---: | ---: |
  | 131,072 compact keys | 1,648.781 | 1,261.368 |
  | Alternate key order, same range | 775.575 | 756.719 |
  | 131,072 sparse keys | 1,637.919 | 1,236.911 |
  | 16,384 compact keys | 490.488 | 553.297 |

- Fresh main controls: clean stock **3,891.901 ms**, handwritten array reference
  **435.726 ms** including ~69 ms range discovery. DuckDB's first process median
  was 1,664.084 ms; repeat range **1,645.607–1,658.840 ms**. Default DuckDB selects
  Hash Group By, compresses eligible grouping keys to INTEGER and removes the
  proven-true filter. This is an engine comparison, not an isolated compiler test.
- Separately labelled `perfect_ht_threshold=20` diagnostic changes DuckDB's plan
  to Perfect Hash Group By and lowers the main median to **796.240 ms**
  (795.331–797.551). It also improves alternate ordering to **558.322 ms**
  (557.591–558.743), versus default DuckDB's 775.575 ms. Default is 12; the
  [release source](https://github.com/duckdb/duckdb/blob/v2.0.0/src/execution/physical_plan/plan_aggregate.cpp)
  measures this threshold in key-range bits. DuckDB uses stored statistics for
  admission; our array reference pays for discovery on every query. Main-query
  cycles/input row: default DuckDB **140.78**, perfect hash **67.02**, integrated
  LLVM **109.64**, array reference **33.68**. These counters do not establish the
  limiting hardware resource or attribute the entire gap to one mechanism.
- Every completed sample has exact SQL agreement, zero measured storage reads
  and zero major faults. Default DuckDB is roughly tied with LLVM on alternate
  ordering and wins at 16K groups; no engine-wide win is established. The array
  reference remains a fixed-shape handwritten experiment. Temporary raw evidence,
  binary SHA256, plans and settings: `/tmp/query-duckdb`.

## 2026-10-26 — First composed pipeline uses the ordinary join runtime and cursor

- Added an opt-in scalar INT inner-join pipeline: generated lookup, partition
  routing, duplicate-chain traversal and resolved probe-side COUNT/SUM updates.
  Reuses the actual `IntHashJoinBuild` tables/heap, `SimpleMapValue` offsets,
  shared workers, build publication, Java merge and ordinary result cursor.
  Aggregate binding/lowering is shared with the existing integrated aggregation.
  Filters keep their existing Java/JIT stages. No new executor, join table,
  whole-result copy or per-match Java callback. The existing unique-build proof
  selects lookup-only execution; duplicate updates retain Java's iteration order.
- Extended the existing runner with `--resolved-join`. **500M probe rows**, 1,048,576
  build rows, CPUs 0–7, seven background query workers plus the caller, server
  defaults, five warmups and three measured samples. Execution includes build,
  filtering/probing, merge, output and close; creation/compilation are separate.

  | Workload | Clean stock ms | Candidate, compiler disabled ms | LLVM ms |
  | --- | ---: | ---: | ---: |
  | Unique build keys | 1,490.586 | 1,568.899 | 612.964 |
  | Probe and build filters | 644.940 | 958.098 | 333.019 |
  | Duplicate build keys, partial probe matches | 2,357.894 | 2,329.915 | 745.543 |

- **Java process variation remains a measurement limit.** An earlier unique
  comparison was 1,358.342 / 533.961 ms disabled/LLVM. The filtered disabled/stock
  discrepancy above did not reproduce consistently: isolated five-sample repeats
  after ten warmups gave **836.532 ms disabled / 897.773 ms stock**. This does not
  establish a tight disabled-path regression bound. No execution defaults were
  changed to slow stock; these are narrow fixtures, not an engine-wide speedup.
- Unique-query scaling at **2 / 4 / 8 execution slots**: LLVM
  **2,029.887 / 1,142.863 / 612.964 ms**, ordinary candidate
  **5,022.586 / 3,317.996 / 1,568.899 ms**. All builds use the existing 32 partitions.
  At eight slots, build takes about 2–3 ms, probe/aggregation dominates, and scalar
  merge is about 0.02 ms. Tracked query memory sampled at 1 ms intervals reaches
  roughly **23–30 MiB** for both paths; this is a lower bound, not an exact peak.
  The initial sampler watched a replaced tracker and reported zero; those memory
  readings are discarded. Stock phase clocks are unavailable.
- Two simultaneous 500M-row queries share the same seven workers, with two client
  owners on CPUs 0–7. Pair completion medians: **3,009.794 ms Java/Java,
  1,925.870 ms Java/LLVM, 1,059.175 ms LLVM/LLVM**. Results are exact; this verifies
  progress and shared execution, not fairness under sustained oversubscription.
- Focused LLVM/AsmJit/C tests cover serial/partitioned and unique/duplicate builds,
  NULL/negative/zero keys, no matches/empty input, filters, tops, fallback, lazy
  opening, rewind/reopen, native failure, registry cancellation and owner-only
  progress after stopping the test workers. All 12 join cases and 27 aggregate
  kernel checks pass; existing integrated aggregate checks pass (78 cases,
  four existing skips). Measured results agree exactly with stock SQL; all
  measured single-query samples have zero major faults and storage reads.
- **Scope:** only scalar aggregates with probe-side arguments and an INT inner
  join. Keyed join aggregation, build payload access, post-join predicates,
  other key layouts and arbitrary `CompiledPlan` phases remain open. Count-only
  specializations stay ordinary. With zero background query workers, server
  defaults disable parallel joins; that planner policy remains unchanged, and
  owner-only runtime progress is a separate correctness check. Compilation
  admission and streaming non-aggregate output are still later gaps.
  Temporary evidence and pinned binaries: `/tmp/query-join-experiment`.

## 2026-10-26 — Joined values feed resolved predicates and ordinary keyed state

- Executed the first foundation slice. `ResolvedAggregateKernel.Updates` now
  consumes generated values/null flags, with frame loading as one producer;
  `QuestMapProbe` accepts an evaluated key. The join lowering reuses both for
  build/probe COUNT and integer SUM, resolved integer column comparisons, and a
  direct INT/LONG grouping key. LLVM, AsmJit and C share this boundary.
- Payloads borrow the existing page-address cache or existing copied rows; the
  build's copy policy is unchanged. Keyed updates use ordinary maps, shards,
  insertion and merge. On a new group, the kernel saves its probe row, duplicate
  link and key before mutation, lets Java insert/grow, refreshes borrowed views,
  then resumes the same match. No joined-row materialization or per-match callback.
- **500M probe rows**, 1,048,576 build rows, CPUs 0–7, seven query workers plus
  caller, unchanged server execution defaults, five warmups and three samples.
  Results below are execution medians; factory preparation/compilation is separate.

  | Workload | Clean stock ms | Candidate Java ms | LLVM ms |
  | --- | ---: | ---: | ---: |
  | Duplicate join into seven groups | 4,715.811 | 3,922.267 | 2,598.918 |
  | Join into 100,004 groups | 8,028.868 | 6,115.765 | 3,236.831 |
  | Skewed 100,004 groups | 3,249.724 | 3,326.768 | 1,879.125 |

- First two candidate rows are the final repeat with insertion counters; the skew
  measurement precedes that counter-only addition and uses the same generated
  loop. Earlier process medians for the first two fixtures were Java/LLVM
  **4,152/2,632** and **7,329/3,241 ms**. LLVM reproduces closely; Java's between-JVM
  variation changes the apparent speedup. Same-build gains across these repeats
  are roughly **1.5–2.3×**, not an engine-wide claim. Stock is a separate process
  control, not the baseline for attributing an LLVM-only improvement.
- The seven-group case needs **56** ordinary insertion exits; the high-cardinality
  case needs **800,032** (100,004 groups × eight slots), **0.16% of input rows**.
  This counts exits, not their time. It does not justify generated insertion.
  Build is about 3 ms, merge about 0.1/2 ms; probing/update dominates elapsed time.
  Sampled query memory is **46.144/109.691 MiB** for low/high cardinality on both
  paths, a lower bound. All measured samples have exact SQL agreement, 500M native
  input rows on LLVM, zero major faults and zero measured storage reads.
- The cross-input predicate fixture improves from **35,742 ms candidate Java to
  890 ms LLVM**, but the planner evidence matters: ordinary execution uses serial
  Hash Join Light, while the optional compiler admits the parallel fused join.
  This combines planner admission, parallelism and generation. Default admission
  is unchanged; an unsupported complete pipeline closes its temporary factory
  and restores the ordinary plan. Do not report this as a 40× compiler-only win.
- Focused validation: **128 passed, four existing skips** across resolved join,
  aggregate kernel and SQL aggregation tests. Coverage includes both payload
  representations, nulls/tops, duplicate-chain growth/resumption, sharding,
  unsupported-plan restoration, cancellation/native failure, reopen/rewind and
  owner-only keyed progress after workers stop. No broad suite was added.
- **Control gate remains open:** six balanced stock/disabled processes did not
  establish the 3% neutrality target; the unique control's process-median gap was
  about 5.4%. A same-binary counterfactual removing the two native dispatch checks
  did not improve it consistently. The cause is unproven; neither neutral overhead
  nor a specific CPU bottleneck is established. Next control work must attribute
  the between-JVM difference, not select favorable repeats.
- **Remaining foundations:** bounded/resumable output, general phase composition,
  compilation admission/concurrent code ownership and mixed numeric state. Source
  inspection moved join → DISTINCT to phase composition: the fused registry
  deliberately excludes auxiliary allocation and has no aggregate allocator.
  Bounded output also needs a real bulk reservation/reset contract: the existing
  `RecordArray` reserves one row at a time and `clear()` releases its backing.
  Active raw evidence stays temporary in `/tmp/query-foundations-active`; the
  plan's later steps have not been implemented or declared complete.

## 2026-10-26 — Generated unordered-map mutation, redistribution and merge

- Closed the first bounded map slice for direct INT/LONG keys on actual
  Unordered4Map/Unordered8Map: generated insertion, relocation, shard redistribution
  and aggregate merge, plus whole-frame unsharded updates. COUNT, integer SUM and
  LONG DISTINCT share their existing resolved update/merge descriptions. LLVM,
  AsmJit and C use the same IR. No query recognizers or replacement maps/executor.
  Java retains allocation/tracking, worker dispatch and cursor lifetimes.
- Two source-contract corrections mattered. Unordered4Map aligns entries to four
  bytes, not eight; relocation now takes the owner's actual stride. Java also
  reserves room for each whole configured batch. Omitting that reservation made
  native maps denser and DISTINCT about 6% slower. Generated code now checks the
  same batch boundaries and calls the owning map's capacity calculation only when
  growth is necessary. The 32-row prefetch window stops at reservation boundaries;
  no per-batch JNI transition is required. Turning prefetch off cut instructions
  without a convincing latency improvement, so that tuning was not adopted.
- **500M input rows per case**, CPUs 0–7, seven workers plus caller, unchanged server
  execution defaults, five warmups and three measured executions. These are full
  execution medians, including merge/output/close, with compilation separate.
  Stock, previous LLVM and final LLVM below run in separate processes with a single
  measured execution mode, matching workload order. Exact results agree.

  | Workload | Clean stock ms | Previous LLVM ms | Final LLVM ms |
  | --- | ---: | ---: | ---: |
  | COUNT + SUM, 512 groups | 339.451 | 335.905 | 335.248 |
  | COUNT + SUM, 131,072 groups | 2,404.345 | 961.560 | 937.522 |
  | COUNT + SUM, 90% hot key | 352.920 | 333.479 | 330.996 |
  | DISTINCT, 64 groups × 8 values | 396.276 | 357.575 | 366.116 |

- **Interpretation: more complete compilation, not a broad breakthrough.** High
  cardinality improves about 2.5% over the previous prototype; most of its 2.6×
  advantage over stock already existed. Low cardinality and skew are essentially
  unchanged. DISTINCT is about 2.4% slower in the final process; sample ranges
  overlap (previous 351–384 ms, final 359–372 ms), and the other final process was
  361 ms. Do not hide this behind a stock comparison or claim uniform gains.
- High-cardinality scan/update kernel invocations fall from **1,049,076 to 500**;
  all 500M rows use generated map operations. This does not count merge/rehash
  invocations. A separate matched PMU repeat gives **975.233 → 937.280 ms**,
  **85.777 → 84.203 instructions/input row**, and **84.762 → 81.264 cycles/input
  row**. Both events ran 100% of the measured interval; setup/warmup were excluded.
  The first five-event collection multiplexed at 59–60% and was discarded for
  hardware conclusions. Fewer crossings remove only a small share of total work;
  these counters do not establish a cache, bandwidth or execution-port bottleneck.
- All measured samples have zero major faults and storage reads. CPU/memory PSI
  and measured swap traffic were zero; the session had no CPU/memory quota or
  throttling/OOM events. Unrelated system I/O pressure remained present. This is
  an exploratory capability experiment, not a completed hardware bottleneck study.
- Keep controls separate: mixed Java/LLVM processes moved Java DISTINCT timing
  from roughly 436 to 532 ms. Forced Java-only controls did not reproduce that
  gap: previous **398.811 ms**, final **407.598 / 407.501 ms**. The mechanism of
  the mixed-mode effect is unproven. A wider OrderedMap fallback comparison in
  matched LLVM-only processes is **532.944 → 526.218 ms**; do not compare it to
  the 678–702 ms mixed-mode runs as though the measurement mode were immaterial.
  There is still no engine-wide disabled-path neutrality claim.
- Preparation grows because relocation/merge kernels are compiled too: in the
  narrow matrix, subsequent factory preparation rises from roughly **19–20 to
  32 ms**; the first DISTINCT factory was about **181 ms** versus **117 ms** in
  the initial comparison. Compilation admission remains a separate gap.
- **Validation:** 140 prototype checks pass, four existing skips; four existing
  core map fuzz/all-value-type checks also pass. Added coverage checks generated
  growth inside prepared windows, INT/LONG zero/negative/null keys, SUM nulls,
  Java-readable counters, ordinary reservation capacity, failed relocation
  cleanup, partial-state cancellation and slot reuse. SQL tests cover merge,
  redistribution, DISTINCT lifetimes, filters/tops, reopen and both worker modes.
- **Scope:** the server's map-selection threshold stays 32 bytes. Wider states,
  OrderedMap/composite/VARCHAR keys and joined-group insertion retain their
  previous paths. Sorting/filter lowering/output work remains, so this adds
  **zero fully LLVM ClickBench queries** under the agreed strict definition.
  Next slice: generated numeric Top-K/sort and its connection to aggregate output,
  following existing QuestDB algorithms. Design/order: [FOUNDATIONS_PLAN.md](FOUNDATIONS_PLAN.md).
  Temporary evidence and frozen binaries: `/tmp/query-map-mutation`.


## 2026-10-26 — Generated numeric Top-K: phase gain, no end-to-end breakthrough

- Closed bounded, single-LONG aggregate Top-K collection and worker-result merge
  on Unordered4Map/Unordered8Map. `QuestLongTopK` emits QuestDB's existing sorted
  buffer algorithm through the shared IR (LLVM, AsmJit, C). Same cutoff, binary/
  linear search, ties, overlapping movement and raw map row IDs; same buffers,
  worker dispatch, slot ownership and ordinary output cursor. Compilation happens
  only when the planner selects Long Top K. Unsupported readers/layouts retain
  their ordinary path. Detailed contract: [FOUNDATIONS_PLAN.md](FOUNDATIONS_PLAN.md#second-slice-generated-bounded-numeric-top-k).
- **500M rows**, COUNT+SUM, eight execution slots, unchanged server defaults,
  affinity 0–7, five warmups, three samples, exact SQL results and full consumption.
  Previous means the same native aggregation with generated Top-K disabled.

  | Groups / ordering | Stock Java | Previous | Generated Top-K |
  | --- | ---: | ---: | ---: |
  | 512 / ASC LIMIT 128 | 341.331 | 336.490 | 345.888 |
  | 131072 / DESC LIMIT 10 | 2325.972 | 915.604 | 934.073 |
  | 8388608 / DESC LIMIT 10 | 4823.118 | 2400.923 | 2387.737 |

  Times are median milliseconds, compilation excluded. The medium case repeated
  in reverse process order gives **929.288 → 920.588 ms**; the first apparent 2%
  regression is not stable. Small-case native range is 334.922–366.142 ms versus
  336.321–337.390 previous. Large-case ranges overlap: 2376.072–2414.422 previous,
  2372.980–2479.893 generated. No repeatable whole-query improvement established.
  Stock's ~2× large-case disadvantage already existed in aggregation. A disabled
  candidate Java control at 131K is 2291.801 ms versus stock 2325.972; this is a
  narrow control, not an engine-wide neutrality claim.
- **Phase isolation:** build the real 500M-row aggregation, keep its finalized maps,
  then repeatedly collect and merge Top-K using the ordinary parallel cursor path.
  Only this phase is timed: **8.808 [8.599–9.426] → 7.580 [7.540–7.803] ms** over
  8,388,608 groups. A 14% phase improvement saves ~1.2 ms, only ~0.05% of the full
  2.4 s query. Measured PMU intervals exclude aggregation/setup/warmup: instructions
  **74.092 → 48.671/group** and cycles **43.888 → 39.650/group**, both 100% running.
  These include the existing parallel dispatch and final merge. The generated
  whole-query counters prove all 8.39M groups and 80 worker-list entries (8×10)
  reach native Top-K. This does not establish a hardware resource bottleneck.
- All 36 full-query measured executions have zero major faults and storage reads.
  Phase runs also have zero major faults/reads (PMU records two minor faults in
  the candidate). CPU/memory PSI and sampled swapping were zero; no cgroup CPU
  throttling or OOM. Unrelated system I/O pressure persists. This remains an
  exploratory capability experiment, without a completed hardware bottleneck gate.
- First-factory preparation rises from roughly **84–88 to 118–122 ms** in separate
  JVMs; it includes the additional Top-K kernel and JVM startup effects. Queries
  without Long Top K do not compile that kernel.
- **Validation:** 125 checks passed, four existing skips, covering LLVM/AsmJit/C
  and legacy/fiber dispatch. Exact list values and row IDs/ties match Java for both
  key widths, NULLs/extremes, both directions and K=1/10/65/66/128/5000. Tests cover
  cancellation/publication, merge, rewind/reopen, parallel and sequential SQL
  collection and OrderedMap fallback. No extra test suite was added.
- **Scope:** no full-query coverage increment. General filter/projection lowering,
  wider/string/composite maps, unlimited/multi-column sort and OFFSET remain.
  Next work should connect resolved filter/projection semantics to shared IR for
  the first complete pipeline, then broaden keys. Raw evidence and frozen binaries
  are temporary: `/tmp/query-topk`; no results archive added.

### 2026-10-26 — resolved integer filtering through LLVM

- Deferred aggregation algorithm/profiling work is recorded in [TODO.md](TODO.md).
  Closed the next binding gap: resolved integer comparisons, NULL tests and Boolean
  trees now compile through shared IR. Reuses the engine's selection buffer,
  worker slots, cancellation and Parquet/filter/aggregation contracts. Unsupported
  expressions and runtime binds retain ordinary filtering. No SQL recognition.
- Added effect-aware row-ID compaction to IR, lowered to LLVM vector compress-stores;
  portable C/AsmJit lowering remains available. The initial scalar bit enumeration
  took 357 ms on the dense case; 64-lane compaction was worse at 379 ms. Eight-row
  masks avoid packing then splitting a 64-bit mask and bring it back to ~341 ms.
- **500M rows, eight slots, five warmups, three samples, unchanged server defaults**:
  COUNT+SUM and DESC LIMIT 10; medians in milliseconds, compilation excluded.

  | Groups / rows passing | Clean stock | Existing filter + LLVM aggregates/Top-K | LLVM filter + aggregates/Top-K |
  | --- | ---: | ---: | ---: |
  | 512 / 100% | 340.663 | 337.875 | 340.977 |
  | 512 / 47.059% | 199.233 | 203.329 | 198.246 |
  | 512 / 0.094% | 104.074 | 104.085 | 104.186 |
  | 131072 / 100% | 2342.632 | 926.635 | 928.115 |

- Reverse-order dense repeat: 340.213 native versus 336.961 existing-filter ms;
  a small ~1% overhead remains. Disabled candidate Java is 342.203 ms. This is
  capability progress at roughly existing speed, not a new performance breakthrough;
  the medium-cardinality gain over stock comes from the earlier aggregation work.
  Each measured native query filters all 500M rows and exactly matches stock results.
  Measured executions have zero major faults/storage reads; unrelated host I/O
  pressure persists. No hardware-bottleneck claim. First-factory preparation adds
  roughly 14–22 ms in these separate JVMs. Raw evidence stays in `/tmp/query-llvm-filter`.
- **221 checks passed, four existing skips.** Validation covers all three backends,
  LLVM O0–O3 compaction, NULLs/widening,
  Boolean trees, tails, store bounds/effects, cancellation/reuse, SQL fallback,
  both dispatchers and existing join/Parquet/cast paths. General projection,
  scalar aggregates and broader types/orderings remain; no audited ClickBench
  full-query coverage increment is claimed.

### 2026-10-26 — Generated OrderedMap: composite-key execution gain

- Closed fixed-width OrderedMap probing/insertion, growth rehash, redistribution,
  merge and bounded LONG Top-K through the shared IR. Direct BYTE/SHORT/INT/LONG
  keys, including composites and wider aggregate states, retain QuestDB's packed
  encoding, hash, compressed offsets, heap order and algorithms. Existing maps own
  allocation; existing slots/jobs own parallel execution. No query recognition or
  map-selection override. Design: [FOUNDATIONS_PLAN.md](FOUNDATIONS_PLAN.md#fourth-slice-fixed-width-orderedmap-execution).
- **500M rows, eight slots, server defaults, affinity 0–7, five warmups, three samples.**
  Medians below are milliseconds, including output consumption and close, excluding
  compilation. Previous is the same candidate with `query.codegen.ordered=false`;
  stock is the clean QuestDB build. All use numeric DESC LIMIT 10.

  | Workload | Stock | Previous | Generated OrderedMap |
  | --- | ---: | ---: | ---: |
  | LONG+INT key, 512 groups, COUNT+SUM | 506.390 | 524.475 | 376.267 |
  | LONG+INT key, 131072 groups, COUNT+SUM | 2989.244 | 2642.197 | 1018.013 |
  | LONG+INT key, 65536 groups, 90% one key, COUNT+SUM | 920.138 | 946.873 | 530.050 |
  | LONG key, 131072 groups, five aggregates / 40-byte state | 4044.611 | 1226.601 | 1236.609 |

- Composite keys improve **1.39–2.60× versus the previous prototype**. The medium
  candidate range is 1016–1023 ms versus 2639–2698 ms previous. Disabled candidate
  Java is 2945 ms versus stock 2989 ms. Small stock is a stable repeat; the first
  process measured 554 ms with a 514–586 ms range. Wide state is **0.8% slower**
  than previous: its large advantage over stock already existed. More generated
  phases alone do not guarantee a speedup. No hardware-bottleneck claim is made.
- Every reported query exactly matches its ordinary SQL result; native counters
  cover all 500M input rows and all final groups through Top-K. A first wide-state
  run ordered by SUM(short) selected different keys tied at the cutoff. The full
  131072-group result matches; the reported wide comparison orders by SUM(v).
  All measured runs have zero major faults/storage reads. Unrelated host I/O
  pressure persists. First native factory preparation increases from 62–71 to
  132–136 ms for composite keys, and 84 to 132 ms for wide state.
- **170 focused checks pass, four existing skips**, across LLVM/AsmJit/C and both
  dispatch modes. Coverage includes odd packed widths, NULLs, DISTINCT, growth,
  merge, ties/row IDs, cancellation/reuse, unsigned high-bit offsets, and cleanup
  when heap relocation succeeds but offset rehash fails.
- Refreshed all 43 ClickBench plans: Q41 now has generated composite aggregation,
  but still ordinary OFFSET/sorting. Complete generated coverage remains **2/43**
  (Q8/Q15), established on a small schema-correct fixture. SYMBOL/TIMESTAMP/IPv4,
  computed/variable-width keys and broader ordering remain. Temporary raw evidence
  and binaries: `/tmp/query-ordered`; no results archive added.

### 2026-10-26 — Shared integer expressions reach the native aggregation pipeline

- Resolved integer addition/subtraction, constants and widening now share one
  binding across filters, grouping keys and COUNT/SUM/LONG DISTINCT arguments.
  INT arithmetic wraps before widening; NULLs follow the actual getter width,
  including zero for BYTE/SHORT NULL getters. LLVM/AsmJit/C use the same IR.
  Existing map algorithms, selection vectors, prefetch batches and worker owners
  remain. Computed-key packing follows RecordSink order. Projection wrappers now
  forward native Top-K preparation through their existing column mappings.
- **500M input rows, eight slots, affinity 0–7, unchanged server defaults, five
  warmups, three samples.** Full execution/consumption/close medians in milliseconds;
  compilation excluded. Every reported result exactly matches ordinary SQL.

  | Workload | Clean stock | LLVM |
  | --- | ---: | ---: |
  | 512 groups, direct key, COUNT + SUM(v+w) | 436.309 | 386.354 |
  | 131072 groups, computed LONG key, COUNT + SUM(v) | 2401.273 | 956.783 |
  | 131072 composite groups, computed filter/keys/arguments, 47.058% passing | 1721.818 | 611.728 |
  | 65536 composite groups, computed filter/keys/arguments, 90% one key | 1533.580 | 567.895 |

- The new path is **1.13–2.81× faster than stock in these cases**. The earlier
  prototype with expression binding disabled takes 429.499, 2675.700 and 1742.174 ms
  for the first three cases. This extends existing native execution to additional
  query shapes; it does not establish a faster hash algorithm or a hardware ceiling.
  Final computed-key range: 952.684–959.074 ms; stock: 2397.201–2430.662 ms.
- **Controls:** the unchanged direct composite-key query is 1032.139 ms on frozen
  pre-change binaries and 1032.381 ms now. Disabled candidate Java for the computed
  LONG key is 2410.695 ms, close to repeated stock at 2401.273 ms. The first stock
  process was 2533.408 ms; the table uses the faster repeat. Native counters cover
  every selected row and final Top-K group; the selective case filters all 500M
  rows and updates 235289411. All timed samples have zero major faults/storage reads.
  CPU/memory pressure was low; unrelated host I/O pressure persists. No PMU-based
  bottleneck claim is made.
- First native factory preparation is **133–141 ms** versus ordinary preparation
  around 18–25 ms. That cost exceeds the small-case saving for a single execution;
  compilation admission/amortization remains important.
- **221 checks pass, four existing skips.** Tests cover all three backends, both
  dispatchers, computed scalar/composite keys, signed overflow, intermediate NULLs,
  widening, constants, DISTINCT, column tops, sharding/merge, reopen and Top-K through
  projection. The shared binder also passes existing joined-argument checks.
- Planner inspection matters: `SUM(v+1)` is rewritten into SUM/COUNT plus an output
  expression, so measurements use `SUM(v+w)` to exercise arithmetic inside the
  aggregate. General output arithmetic remains ordinary. All 43 ClickBench plans
  were refreshed: strict complete coverage stays **2/43**, Q8/Q15. Broader types,
  VARCHAR and ordering remain; filter/aggregation fusion is still a separate experiment.
  Design: [FOUNDATIONS_PLAN.md](FOUNDATIONS_PLAN.md#fifth-slice-shared-resolved-integer-expressions).
  Temporary evidence/binaries: `/tmp/query-expressions`; no results archive added.

## 2026-10-26 — Single VARCHAR grouping reaches the generated pipeline

Added a typed borrowed UTF-8 view and shared-IR decoding for native inline/split
and Parquet slice storage. Ordinary resolved SQL now generates the existing
`UnorderedVarcharMap` hashing, byte equality, probing, insertion, rehash,
redistribution and merge, followed by existing LONG aggregate Top-K. COUNT,
integer SUM and LONG DISTINCT reuse their existing generated updates. Direct
VARCHAR NULL predicates generate the normal selection vector. No SQL recognition,
new map algorithm, allocator, scheduler or altered execution default was added.

Ownership follows the existing map: stable native-page keys are borrowed; new
unstable keys yield before insertion so the owner can copy into the destination
map's tracked arena. Generated execution resumes without repeating an aggregate
update. NULL and empty differ; occupancy must be checked before masking the ASCII
flag for equality. A focused test caught that empty-slot/empty-string collision.
The VARCHAR control extension preserves the existing numeric/handwritten ABI.

500M input rows per run, seven workers plus caller on CPUs 0–7, unchanged server
settings (fiber dispatcher, sharding threshold 10,000, max unordered entry 32 bytes,
frame range 100k–1M). Five warmups and three measured samples. SQL groups a direct
VARCHAR key, computes COUNT/SUM, filters `IS NOT NULL`, and returns the largest ten
SUMs. NULL/empty keys occur in every fixture. Results compare exact UTF-8 bytes and
aggregate values against ordinary SQL, including cursor consumption and cleanup.
Compilation and fixture creation are excluded from execution time.

| Input key distribution | Result groups before Top-K | Clean stock median | LLVM median | Stock / LLVM |
| --- | ---: | ---: | ---: | ---: |
| Short inline keys, uniform | 513 | 833.450 ms | 562.897 ms | 1.48× |
| Longer keys sharing a prefix, uniform | 131,073 | 5,676.329 ms | 2,196.218 ms | 2.58× |
| Mixed inline/long/non-ASCII keys, skewed | 98,305 | 2,075.151 ms | 1,198.565 ms | 1.73× |

The disabled candidate Java control measured 832.195 ms on the first shape,
matching clean stock's 833.450 ms. The first LLVM run there was 568.768 ms; the
final repeat was 562.897 ms. Factory preparation was 142.5–145.1 ms for LLVM versus
23.6–25.1 ms for Java. Native counters cover all 500M filter inputs, all 482,758,621
selected map-update rows and every group entering Top-K. The two larger cases use
the ordinary 256-shard path. All measured samples had zero major faults and zero
storage-read bytes. Host I/O pressure remained high; these are hot execution
measurements, not storage-throughput or hardware-bottleneck findings. No PMU-based
ceiling or workload-wide speedup is claimed.

Validation: 197 focused tests passed, four existing skips, across LLVM/AsmJit/C
and both worker modes. Final VARCHAR checks also exercise DISTINCT and the optional
probe counters. Cases cover native and mixed native/Parquet frames, NULL/empty,
UTF-8 and shared prefixes, both sharding choices, growth, merge, Top-K, reopen,
column tops and lazy conversion. The 43-query actual-schema audit drains without
errors: 21 still reach generated updates, but strict complete coverage rises from
2/43 to **5/43: Q8, Q12, Q13, Q15 and Q33** (zero-based). Their fixture hashes
match Java. Q34 still has a Java constant projection and is excluded.

Boundaries: single direct VARCHAR keys only, within the planner's ordinary
unordered-map size limit. Mixed/composite strings, wider VARCHAR aggregate states,
string sorting/expressions and compound string predicates remain. Filtered Parquet
late materialization uses different row spaces per column and retains its existing
safe fallback; unfiltered Parquet is generated and ownership-tested, but no large
Parquet speedup is claimed. Unstable-key allocation currently yields once per new
group and rebuilds the prepared window; its high-cardinality cost remains unmeasured.
Next bounded slice is VARCHAR-bearing keys in the existing OrderedMap. See
[FOUNDATIONS_PLAN.md](FOUNDATIONS_PLAN.md) and [TODO.md](TODO.md).

Temporary binaries, commands, exact results and resource observations:
`/tmp/query-varchar`. Clean stock remains the unmodified `8da4e347f96563dc4e9d13916e00177e2af708f9`
build at `/tmp/query-sharded-stock/core/target/questdb-10.1.1-SNAPSHOT.jar`.

## 2026-10-27 — Composite VARCHAR keys and wider states use generated OrderedMap

Shared IR now emits the existing variable-key OrderedMap encoding, hashing,
probing, insertion, redistribution, merge and LONG aggregate Top-K. It admits
direct VARCHAR fields alongside supported integer fields and resolved integer
expressions, including multiple VARCHAR fields and aggregate states too wide for
the ordinary UnorderedVarcharMap limit. Existing COUNT, SUM and LONG DISTINCT
updates are reused. No SQL recognition, new map algorithm or execution-default
change was added.

Prepared keys use the existing ProbeView staging buffer, owned and tracked by the
map. Insertion copies encoded keys into the existing append heap; values and the
next entry are addressed from each key's actual length. Growth resumes the first
unconsumed row, and redistribution retains its source byte offset across growth.
Both native and Parquet keys follow OrderedMap's normal copy ownership. NULL,
empty strings, field boundaries and ASCII-independent byte equality are preserved.

500M input rows per run, seven workers plus caller on CPUs 0–7, unchanged server
settings, five warmups and three samples. All cases filter a VARCHAR with
`IS NOT NULL`, group, aggregate and return the largest ten SUMs. Execution includes
merge, Top-K, consumption and cleanup; fixture creation and preparation are
excluded. Exact UTF-8 keys and aggregate values are checked against ordinary SQL.

| Workload | Groups before Top-K | Clean stock median | LLVM median | Stock / LLVM |
| --- | ---: | ---: | ---: | ---: |
| Inline VARCHAR; COUNT/SUM/COUNT(column), wider state | 513 | 982.453 ms | 761.716 ms | 1.29× |
| INT + VARCHAR with shared prefixes, uniform | 131,584 | 6,503.129 ms | 3,251.039 ms | 2.00× |
| Two VARCHAR fields, mixed UTF-8, skewed | 98,560 | 2,604.393 ms | 1,581.414 ms | 1.65× |

The candidate with compilation disabled measured 983.751 ms on the first shape,
matching clean stock's 982.453 ms. Keeping the preceding compiler's eligibility
measured 945.036 ms there: generated aggregate updates but ordinary map and Top-K.
Generating those remaining phases reduces that elapsed time by 19.4%. Preparation
cost 160.5–170.4 ms for LLVM versus 23.8–25.3 ms for Java. Every candidate processed
all 500M filter inputs, all 482,758,621 selected map rows and all result groups in
generated code. The larger two cases used the ordinary 256 shards.

All measured samples had zero major faults and zero storage-read bytes. Host I/O
pressure was high, so these are hot execution results, not storage measurements
or a PMU-based bottleneck/ceiling claim. The unchanged single-VARCHAR control was
578.221 ms [564.104, 581.709] with this change versus a fresh frozen preceding
build's 626.397 ms [570.188, 640.901]; the earlier milestone measured 562.897 ms.
That control is noisy and establishes no small speed change in either direction.

Validation: 209 focused tests passed, four existing skips, across LLVM/AsmJit/C
and both worker modes. After extracting the shared numeric row decoder, all
12 variable-key cases and 57 kernel cases passed again. Coverage includes native
and mixed native/Parquet frames, multiple key orders, integer expressions,
NULL/empty/UTF-8, multi-KiB keys, staging and heap growth, both sharding choices,
DISTINCT, exact full-group output, Top-K, memory-limit failure and reopen.
The actual-schema 43-query audit drains without errors. Strict complete coverage
rises from 5/43 to **7/43: Q8, Q12, Q13, Q14, Q15, Q16 and Q33**. Q16 is additionally
validated against all 14,853 exact groups and the ordered Top-K count multiset,
allowing different valid keys at its tied cutoff. This small audit is a semantic
and coverage check, not a ClickBench performance result. Q17's ordinary unordered
LIMIT and Q34's Java constant projection still exclude them.

Remaining boundaries: computed VARCHAR, SYMBOL keys, string ordering, general
projection and compound string predicates. Filtered Parquet late materialization
retains its safe fallback. Long-key staging and large Parquet workloads have
functional coverage here, not a demonstrated performance gain. Temporary raw
evidence and frozen binaries: `/tmp/query-varordered`; clean stock remains the
unmodified `8da4e347f96563dc4e9d13916e00177e2af708f9` build. See
[FOUNDATIONS_PLAN.md](FOUNDATIONS_PLAN.md) and [TODO.md](TODO.md).

## 2026-10-27 — Seven complete queries validated on actual ClickBench data

Ran the exact Q8, Q12, Q13, Q14, Q15, Q16 and Q33 SQL (zero-based) from the local
ClickBench checkout against `~/.questdb/db/hits~8`: **99,997,497 rows, 22 partitions**.
No replication, filtering of the source dataset or synthetic substitution. Temporary
copy-on-write database views protected the original files from startup migrations;
original table metadata/transaction/column-version and configuration checksums stayed
unchanged. Both benchmark servers were stopped and their views unmounted afterward.

Protocol follows `/home/jara/devel/oss/ClickBench/questdb/run.sh`: start a fresh server
for each variant, run queries sequentially, `sync` and drop OS caches once before
each query, then make three HTTP `/exec?timings=true` requests without extra warmups.
Report `execute`; hot = min(second, third). This local runner uses the historical
**lukewarm** first-run protocol, not a database restart before every query. The
first request includes compilation; hot requests reuse compiled factories, with
no result caching. Plans were collected after timing. Only these seven queries
were run, so their JVM/cache history differs from a full 43-query submission.

Both use the existing server configuration and default 31 query workers, all 32
logical CPUs available, default sharding/frame sizes and JIT setting. Identical
isolation changes enable read-only mode, use a loopback HTTP port and disable unused
ingestion endpoints and telemetry. Stock is the clean `8da4e347f96563dc4e9d13916e00177e2af708f9`
build; LLVM is the frozen preceding milestone candidate. A temporary bootstrap
adapter installs the existing resolved compiler through the engine compiler factory;
the ordinary HTTP processor, worker pools, SQL planner and cursor consumption remain.
Stock ran first, then LLVM; this is one ClickBench-protocol pass per variant.

| Query | Stock runs 1 / 2 / 3 (ms) | LLVM runs 1 / 2 / 3 (ms) | Hot stock / LLVM |
| --- | ---: | ---: | ---: |
| Q8 | 1211.452 / 492.276 / 491.901 | 866.950 / 459.279 / 463.870 | 1.07× |
| Q12 | 954.999 / 183.471 / 184.616 | 649.950 / 172.428 / 165.584 | 1.11× |
| Q13 | 571.130 / 303.241 / 309.975 | 620.070 / 312.449 / 310.797 | 0.98× |
| Q14 | 983.984 / 253.352 / 253.122 | 487.949 / 252.442 / 244.349 | 1.04× |
| Q15 | 914.467 / 287.381 / 287.802 | 458.856 / 208.664 / 202.410 | 1.42× |
| Q16 | 1074.726 / 716.463 / 703.673 | 891.409 / 621.130 / 636.332 | 1.13× |
| Q33 | 2504.525 / 1056.728 / 1009.650 | 2353.187 / 821.213 / 818.412 | 1.23× |

Hot geometric mean across these seven is **1.13×**. Q15 is strongest at 1.42×;
Q13 measures 2.5% slower, and Q14's 3.6% gain is small. The much larger gains from
the synthetic 500M-row fixtures do not carry over uniformly. First-request compiler
time is 62.6–269.4 ms for LLVM versus 1.6–18.1 ms for stock and remains included in
the first-run execute numbers above. This is a modest real-data result, not a broad
performance breakthrough or a hardware-bottleneck diagnosis.

All **42 responses match exactly**: column schemas, ten ordered rows, integer
values, UTF-8 strings and NULLs. No tie relaxation was needed. Native counter
deltas cover all 99,997,497 map rows for unfiltered queries and all 13,172,392
selected map rows for Q12/Q13/Q14; their generated filters examine the full table.
Generated Top-K visits every result group, including 24,070,560 for Q16 and
18,342,019 for Q33; sharded Top-K final merge is generated where used. Plans have
no extra ordinary projection/sort/LIMIT operator. Strict complete coverage remains
**7/43**, now supported by actual-data validation.

Measured hot samples have zero major faults and at most 49 KiB of process storage
reads. Host I/O PSI is high; first-run timings are environment-sensitive, and live
mappings explain why dropping OS caches does not make every query fully cold.
No swap activity, CPU throttling or cgroup OOM events were observed during checks.
Raw HTTP responses, plans, counters, commands, temporary bootstrap adapter and
resource observations are under `/tmp/query-clickbench-real`; no results archive
or compiler implementation change was added.

## 2026-10-27 — Why the real-data speedup shrinks

Investigated all seven actual ClickBench queries on the same frozen stock/candidate
binaries and protected database views. Diagnostic comparisons use five warmups and
five measured full HTTP executions, exact ordered-result checks and unchanged
execution defaults except explicitly labelled worker/affinity counterfactuals.
These warmed medians supplement, rather than replace, the preceding three-run
ClickBench results. Generated-code coverage was already complete; fallback and
compilation are not the explanation.

**Parallelism exposes a memory limit.** Restricting both variants to CPUs 0–7 with
seven query workers recovers a 1.47× seven-query geometric mean; default 31 workers
on all 32 logical CPUs gives 1.13×. Selected stock/LLVM hot speedups:

| Shape | 8 slots | 32 slots |
| --- | ---: | ---: |
| Q12: VARCHAR COUNT | 1.57× | 1.10× |
| Q14: composite VARCHAR COUNT | 1.03× | 1.03× |
| Q15: LONG COUNT | 2.10× | 1.40× |
| Q16: LONG + VARCHAR COUNT | 1.38× | 1.17× |
| Q33: VARCHAR COUNT | 2.00× | 1.27× |

Q33 LLVM takes 1,042 / 755 / 831 ms at 8 / 16 / 32 slots. Stock takes
2,083 / 1,178 / 1,053 ms. Slots 8 and 16 use physical CPUs 0–7 and 0–15;
32 includes SMT. This is a concurrency/topology counterfactual, not a recommendation
to change server defaults. LLVM retires 37.5B instructions per Q33 versus stock's
100.3B, but controller traffic falls only from about 56.7 GB to 49.1 GB per query.
System-wide AMD UMC read/write CAS counters, at 64 bytes per command, show LLVM's
busy 100ms intervals sustaining **63 GB/s**. A separate 3 GiB streaming triad,
10 passes after initialization/warmup, sustains **62.6–63.2 GB/s of actual controller
traffic** with either 16 or 32 threads. Idle traffic is below 0.82 GB/s. The triad's
logical byte rate is only 46–47 GB/s; write allocation explains why it must not be
compared directly with total controller traffic. At this traffic volume Q33 has an
approximately 0.78s bandwidth floor, close to its 0.82–0.83s execution. This establishes
a bandwidth limit for its busy phases, not for every query or a different algorithm.

**The data stresses insertion and merge.** Synthetic composite grouping had
131,584 groups from 482.8M selected rows (~3,669 rows/group). Actual Q14 has
6,474,212 groups from 13,172,392 selected rows (~2 rows/group). Its worker-local
maps hold about 9.2M groups and 1 GiB of append heap before merge, excluding hash
offsets and staging. Temporary phase instrumentation in matched copies of the
cursor measures Q14 scan/update at 148 ms stock versus 142 ms LLVM, merge 46/46 ms,
and Top-K 9/9 ms. Controller traffic is 11.8/11.9 GB per query despite LLVM executing
about 19% fewer instructions. Q16 scan/update improves 438→326 ms, while merge stays
155→155 ms and Top-K 19→19 ms. Generating the same map algorithm does not remove
its state replication, memory transfers or merge work. The real benchmark also
uses COUNT or DISTINCT rather than the synthetic COUNT/SUM combinations.

**DISTINCT has an HTTP checkpoint problem.** CPU profiles attribute 23.6% of Q13
LLVM samples to NetworkSqlExecutionCircuitBreaker, versus approximately zero for
stock. Tracepoint counts establish **4.42M poll syscalls/query versus about 70**.
KernelMemory.checkpoint calls the unthrottled breaker; malloc also checks it, and
NestedDistinct allocation/zeroing and long loops emit checkpoints. Embedded synthetic
runs lack the HTTP connection FD and therefore miss this cost. In a temporary class
only, replacing the direct scan checkpoint with the existing time-throttled method
reduces polls to about 49,700/query and Q13 from 309→279 ms, matching warmed stock
(~280–283 ms). Cancellation/deadline checks still run on every callback; the existing
method throttles only socket probing. Phase callbacks remain unchanged in this
counterfactual. Q8 improves only modestly (roughly 434→419 ms); its large DISTINCT
merge remains ~200 ms. No production checkpoint policy was changed.

Rejected/limited explanations: extra warmup leaves the broad result unchanged;
all hot diagnostic samples have zero major faults and storage reads. Turning off
the existing software-prefetch window worsens Q33 to 894 ms and Q14 to 229 ms.
The HTTP interrupt-on-close configuration flag did not remove breaker socket polls
(checkConnection() remains true), so that ineffective control is excluded. Top-K
is small except Q13 (~50 ms): its aggregate getter is not thread-safe, so the
existing planner uses serial collection. That is a separate read-contract issue.

Hardware evidence covers all seven shapes: separate two-event PMU groups have
100% scheduling, normalized over five queries. An initial four-event group was
unschedulable and is excluded. CPU profiles cover all seven. UMC measurements are
system-wide with a separately measured idle baseline; they are not per-process
counters. Raw system-wide IBS corroborates RAM access weight for Q33; the short
Q14 capture missed its active interval and is excluded, so no Q14 load-latency
claim is made. Host I/O PSI remains high, but process/device observations show no
hot-query storage activity; swap activity, cgroup throttling, OOM and meaningful GC
CPU were absent. No precise latency/port bottleneck is claimed for the remaining
phases. Temporary source copies, profiles, counter logs, calibration and exact
commands are under `/tmp/query-clickbench-diagnosis`.

Next: correct checkpoint policy using existing breaker infrastructure, then target
aggregation/merge traffic and duplicated state with bounded, general experiments.
Retain stock defaults and complete timing. More efficient instruction lowering
alone has limited headroom in the bandwidth-saturated phases. Original database
checksums remain unchanged; diagnostic servers and mounts are cleaned up.

## 2026-10-27: autonomous goal, HTTP checkpoint correction and full-workload control

North Star now includes repeatable >=1.5x end-to-end execution gains on at least
22/43 actual ClickBench queries, with complete generated processing and exact
semantics. The 1.5x threshold is an explicit working definition, not a result.
Use the unchanged clean stock defaults and the local three-request/cache-drop
protocol. Continue one mechanism at a time; unsupported operators do not count.

`KernelMemory` now uses the existing time-throttled breaker for native poll and
allocation callbacks, including the standalone context-owned scope. Timeout and
cancellation still run every callback; only socket probes are time-throttled.
Arbitrary owner-provided phase callbacks retain their contract. A deterministic
real-network-breaker test failed before the change (1001 probes instead of one)
and passes after it, including timeout, explicit cancellation, disconnect at the
window boundary, captured FFM failures and reuse of borrowed allocator scopes.
Nine existing native cancellation/allocation/merge tests also pass across LLVM,
AsmJit and C. Validated bottleneck study and fixed-work syscall counterfactual:
`/tmp/query-clickbench-next/checkpoint-study.json` and prior diagnosis artifacts.

A full 43-query clean stock/candidate HTTP pass completed on the actual
99,997,497-row database. For the seven complete generated shapes, hot stock / LLVM
milliseconds are Q8 494.407/414.893, Q12 182.190/163.155, Q13 286.905/276.445,
Q14 256.431/241.092, Q15 325.270/202.841, Q16 710.433/593.642,
Q33 996.860/774.974. Their ordered outputs match. Q15 reaches 1.60x in this pass,
but the earlier stock control was faster, so do not yet count it as a repeatable
major gain. Most remaining shapes use stock execution or partial generation.
The full pass exposes selective-filter regressions (Q7 4.957/11.855 ms,
Q41 17.292/33.874 ms); isolate those before expanding filter coverage. Unordered
LIMIT and tied sort cutoffs differ on Q17/Q31/Q32/Q39/Q41 and require semantic
comparison, not false exact-order failure or blanket correctness approval.
Raw responses, plans, commands and logs remain temporary under
`/tmp/query-clickbench-next`; canonical database/configuration are unchanged.

## 2026-10-27: remove the generated filter's masked-load dependency chain

The full-workload regression was in predicate execution. With the same LLVM map
and aggregate code, restoring stock filtering changes Q41 from 28.63 to 16.32 ms
(medians of 40 warm executions). The generated filter accounts for about 81% of
process CPU. Disassembly and IBS agree on chained AVX-512 masked compare memory
operands: the later load depends on the earlier mask. The baseline moves about
0.86 GB/query at ~30 GB/s, below the ~63 GB/s calibrated ceiling; 87.3% of
non-complete-retirement cycles involve incomplete loads. IBS samples concentrate
at those exact compare offsets, with zero sample loss. This is a latency/dependency
problem, not evidence of saturated bandwidth. A first multiplexed PMU group was
excluded and replaced with separate 100%-scheduled groups.

Shared `ScanLowering` now splits conjunctions into bounded lane regions and skips
remaining predicate columns when the current mask is empty. It retains SQL NULL
semantics, selection order, existing output buffers and scalar tails. Eight-row
selection batches remain: widening alone only changed Q41 to 25.9 ms, while the
short-circuit change at the original width reaches 11.84 ms. The 32/64-row variants
gave no useful additional gain and were not retained. This also avoids unnecessary
loads for future conjunctions without knowing a query, constant or selectivity.

A 500M-row ordinary-SQL regression experiment varied the first conjunct's threshold
and compared exactly against ordinary QuestDB results. Old/new LLVM medians:
no matches 199.28/31.61 ms; about 0.09% selected 176.39/32.61 ms; about 43% selected
214.11/203.47 ms; about 87% selected 405.20/402.68 ms. These are same-candidate
counterfactuals, not claims of equivalent speedups over clean stock. Fifteen shared
scan/guard-page/projection tests and six resolved SQL predicate tests pass across
LLVM, AsmJit and C (including the existing worker-mode matrix).

Evidence: `/tmp/query-clickbench-next/filter-study.json`, `filter-*-{pmu,umc}`
artifacts and `filter-regression.log` (actual filenames in that directory).
Q41 still uses ordinary encoded sorting and is not counted as a complete LLVM query.
Q7's low-cardinality aggregation needs separate attribution; its single predicate
is unchanged by this fix.

## 2026-10-27: generated numeric and VARCHAR-null conjunctions

The existing numeric mask producer now accepts a generated residual predicate.
Resolved conjunctions place total numeric predicates before direct VARCHAR NULL
checks, using the existing 48-byte frame descriptors only for surviving rows.
No strings are copied; multiple VARCHAR checks, empty values and NULLs retain
ordinary semantics. Unsupported mixed OR and computed string expressions fall
back before mutation. This reuses the same selection-vector owner and safe
column-top/cast/late-materialization gates. Thirty-three focused SQL and shared
scan tests pass across all backends and worker modes.

Q36/Q37 now have complete generated filter/map/merge/Top-K execution: coverage is
9/43. Exact full-group comparison with clean stock passes for Q36 (298,787 groups),
Q37 (42,155), Q38 (13,299), Q39 (426,328) and Q41 (10,948); the differing bounded
outputs are tied cutoffs. Full-protocol execution is not a breakthrough: Q36 is
28.53 ms versus stock 22.50, Q37 16.27 versus 16.53. Their longer warmed diagnostics
are 19.65/14.97 ms. Keep the protocol and warmed measurements distinct. Q41's
numeric-mask change reduces its full-protocol candidate time from 33.87 to 19.61 ms,
still above stock's 17.29 ms. Evidence stays in `/tmp/query-clickbench-next/compound`
and the exact group-validation files. Do not count coverage as a performance win.

## 2026-10-27: direct SYMBOL storage bindings

Direct SYMBOL keys now use their existing dictionary IDs and -1 NULL sentinel in
fixed/composite generated maps. Direct SYMBOL NULL predicates share the numeric
filter path. Java retains dictionary/result ownership; computed/cast bindings keep
the existing fallback gates. Exact result comparison now serializes symbol text,
including empty and NULL values. Six backend/worker-mode tests cover dictionaries,
nulls, composite keys, sharding/merge and native/Parquet frames.

Q10/Q11 now execute the complete generated pipeline: coverage is 11/43. A fresh
full 43-query stock/LLVM protocol pass gives 81.85/84.15 ms and 78.73/77.18 ms:
coverage, not a performance gain. Q15 repeats its larger gain (342.89/202.97 ms,
1.69x; preceding full pass 325.27/202.84 ms, 1.60x). Remaining complete shapes are
mostly near parity or modestly faster; selective compound cases still regress.
Evidence: `/tmp/query-clickbench-next/symbol` and `symbol-classes`.

## 2026-10-27: bounded local state wins for fixed-width aggregation

A measured bandwidth gate motivated bounded preaggregation plus dense partial-state
partitions. Existing maps, slot allocators, memory tracking, merge task queues,
work stealing and final cursors remain the owners. Generated code flushes at batch
boundaries and resumes before the unconsumed row. VARCHAR keys owned by a local
arena are copied before clearing it. Allocating compiled state explicitly requests
exclusive worker allocators even when the resolved Java functions are thread-safe.
Standalone borrowed-map kernels do not opt into this query-lifetime capability.

At 500M rows, three interleaved samples after two warmups compare ordinary Java /
previous LLVM / bounded LLVM (ms): 512 groups 365.7/379.4/382.7; 5,000,003 groups
2461.0/1974.0/1038.0; 90% heavy-hitter skew 702.5/636.6/451.0. SUM results match
exactly, including repeated use. These are same-core mechanism controls, not
clean-stock ClickBench acceptance. Actual Q15 changes 205.64 -> 119.40 ms, with
controller traffic 12.74 -> 6.03 GB/query, over 40 warmed requests.

The same change does not win automatically for strings: Q33 changes 820.72 ->
841.02 ms and 49.15 -> 50.63 GB/query. Repartitioning from the stored 32-bit hash
instead of rehashing key bytes did not help and was discarded. Retain existing
VARCHAR aggregation by default; explicit `query.codegen.bounded=true` tests it.
Nonallocating fixed-width unordered maps use the bounded path by default; `false`
retains a direct A/B control. Other state/layout families keep their existing path.

The string profile also exposed an eight-byte scalar equality loop. Shared byte
comparison now uses the existing typed vector-mask IR for bounded 64/32/16-byte
comparisons, followed by exact narrow tails. This applies to VARCHAR and encoded
variable composite keys. Protected-page checks cover both slice ends, zero length,
every length through 130 and every mismatch position across all three backends.
Without bounded aggregation, Q33 becomes 718.09 ms and 42.52 GB/query; its bounded
variant is slower at 751.84 ms. No new ClickBench major-gain claim yet: run the
three-request protocol before acceptance. Allocation-failure/reopen and injected
post-update failure checks cover the new partition lifetime.

Evidence remains temporary: `/tmp/query-clickbench-next/bandwidth-study.json`,
`bounded-{off,on,profile}`, `bounded-regression.log`, `unbounded-simd` and
`bounded-simd`. Design: `BOUNDED_AGGREGATION.md`.

## 2026-10-27: key-only grouping reaches the existing scalar DISTINCT rewrite

The binding now admits an empty aggregate state only when a physical key probe is
bound. The ordinary planner still chooses the DISTINCT algorithm and injects its
NULL filter. Generated map insertion, bounded fixed-width partitioning and merge
perform all row processing; CountRecordCursorFactory reads the existing map-size
metadata (no row loop). Six backend/worker-mode tests cover LONG/VARCHAR, NULL,
empty strings, empty results, Parquet and repeated execution.

Complete coverage is 13/43. A full-protocol pass gives Q4 97.56 ms against clean
stock 219.19 ms (2.25x), exact distinct count 17,630,976. Q5 is 122.58 against153.02
ms (1.25x), exact count 6,019,102. All admitted input rows are accounted for by
native counters. Q15 repeats a major gain: 156.30 against331.04 ms (2.12x), versus
160.42 ms in the previous candidate pass. Q33 is at the threshold (676.26 against
1019.16 ms, 1.507x; previous pass 680.98 ms, 1.497x); do not yet count it as a
repeatable major gain. Two queries clearly qualify; the 22-query target is distant.

The full 43-query check also shows stock's own run-to-run AVG(UserID) rounding
variation and tied cutoff variation. Those unsupported paths are not evidence of
new LLVM result errors or full validation; compare complete groups for ties.
Evidence: `/tmp/query-clickbench-next/key-only` and the preceding clean-stock
control in `bounded-default`.

## 2026-10-27: floating-point IR and aggregate finalization

The shared IR now has IEEE double arithmetic and integer conversion, lowered by
LLVM, AsmJit and C without fast-math or contraction. AVG(short) uses the existing
LONG sum/count state and merge rules. A generated final-map pass overwrites the
sum slot with the result bits; borrowed DoubleColumn readers avoid per-result
native calls. Original aggregate functions retain their lifecycle ownership.
The map cursor walk is shared with generated Top-K. Bit-exact tests cover all
backends, worker modes, map layouts, bounded/unbounded merge, Parquet and reuse.

Full-protocol Q9 is 433.81 ms against clean stock 570.96 ms (1.32x), with exact
output and 99,997,497 native input rows plus 9,040 finalized groups per query.
Complete coverage reaches 14/43, but this adds no major-gain query. Q4 and Q15
repeat above 2x. Evidence: `/tmp/query-clickbench-next/avg`.

Direct physical key binding now also preserves IPv4 (32 bits, NULL=0), DATE and
both timestamp precisions (64 bits, LONG_NULL) in existing maps. It does not
silently enable numeric arithmetic or casts on these logical types. Six focused
backend/worker tests cover individual/composite keys, NULL, signed bit patterns,
negative timestamps, variable keys, Parquet and repeated execution. Real-data
validation is next.

IPv4/temporal real-data pass: complete coverage reaches 17/43 with Q30/Q31/Q32.
Fresh clean-stock/candidate hot times are respectively 145.15/134.51 ms,
185.29/190.49 ms and 1665.35/1544.12 ms. No major gain. Q31/Q32 ties were checked
against stock by re-aggregating every returned key; every output value and the
ranked count sequence agree. Evidence: `/tmp/query-clickbench-next/physical`.

Q32 diagnosis: 40 hot executions, median 1614.97 ms, zero storage reads/major
faults. System-wide UMC reports 84.43 GB/query and 52.20 GB/s (local perf metric's
64-byte CAS multiplier). CPU samples attribute 27% to the generated merge's
map-offset load and 15% to the scan's map-offset load, with substantial state
stores/loads elsewhere. These samples localize work; they do not alone prove a
hardware bottleneck. Bounded fixed composite-key state is the next counterfactual;
first collect a physical-core scaling control. Evidence: `physical-q32-{umc,profile}`.

## 2026-10-27: generated OFFSET sorting and a larger-K algorithm

Static positive OFFSET windows on an admitted single LONG aggregate now compose
the existing Long Top K and LIMIT operators. Small K retains insertion sorting;
larger K generates EncodedTopKBuffer's bounded sort/truncate strategy using
QuestDB's eight-histogram stable radix algorithm. Scratch uses the query's existing
worker allocators. Nine focused tests compare exact ties, signed extremes, merges,
cancellation, empty windows, both worker modes and repeated SQL execution.

The first implementation re-sorted the accumulated list after every final shard.
Batching all final shards through one generated collector removes that waste:
Q38 21.66 ->14.92 ms, Q41 26.00 ->19.05 ms. Clean stock remains 12.57 and21.08 ms;
no major-gain claim. Complete coverage is19/43 (addsQ38/Q41). Q40 also sorts through
LLVM, but its IN predicate remains a fallback and excludes it from the complete
count. Evidence: `/tmp/query-clickbench-next/{offset,span}`. Wide/other sort shapes
and better selective filtering remain gaps.

## 2026-10-27: bound composite state; generate full LONG sorting

The Q32 16/32-slot control remains near the same memory-traffic floor. Bounded
fixed OrderedMap keys now reuse the existing dense partition pages and merge
workers. Same-code 40-query A/B: 1622.43 ->1047.62 ms, 84.48 ->57.03 GB/query;
controller rate stays about52–54 GB/s. The 500M-row composite COUNT/SUM/AVG control
is unchanged at512 groups (428.00/427.35 ms), improves at5M groups (2687.22/1884.83)
and under90% skew (799.86/580.89). Exact results pass. Fixed composite bounding is
now enabled by default; allocating DISTINCT and variable keys retain their paths.

Proper-protocol repetitions with identical defaults show Q30 87.06/90.40 ms
against clean stock145.15 ms (1.67x/1.61x), and Q32 1079.99/1089.62 against1665.35
(1.54x/1.53x). Every selected Q30–Q32 group in all three requests was re-aggregated
by stock; values and ranked counts agree despite ties. These are two additional
major-gain candidates, alongside Q4/Q15. A fresh clean-stock repeat is in progress.
Evidence: `/tmp/query-clickbench-next/{ordered-off,ordered-on,full-sort,time-parts}`
and `ordered-regression.log`; hardware gate `q32-study.json`.

Single LONG full sorting now generates collection plus QuestDB's stable radix
algorithm into the existing EncodedSort cursor's pair/scratch buffers. Q7 accounts
for all18 output rows in generated sorting, with exact output, but remains slower
than stock. Small constant IN sets bind the resolved constant values; larger or
dynamic sets retain the existing lookup. UTC hour/minute/second bind the resolved
timestamp drivers with exact NULL and negative-epoch semantics for both precisions.
Signed positive-constant division is shared by LLVM/C/AsmJit; LLVM can strength
reduce it. Focused backend/worker tests pass, including Parquet and reuse. Q18/Q40
now account for their input and output in LLVM; final real-data semantic checks
remain pending before claiming22/43 complete coverage. Coverage is not the
22-major-gain target.

The fresh clean-stock repeat confirms Q4/Q15/Q30/Q32 at2.19x/2.11x/1.58x/1.54x.
Q33 remains below the repeatable1.5x threshold. Full-group comparisons pass for
Q36/Q37/Q38/Q40/Q41 (298787/42155/13299/41194/10948 groups); every selected Q18 key
in all three runs re-aggregates exactly under stock with identical ranked counts.
Complete generated coverage is now22/43, but only four clear major gains. Q18 is
1073.65 vs1169.10 ms; Q40 is16.02 vs20.21 ms. Short-query regressions remain.

Q7 warmed profile: about22% of process samples at the generated filter's SHORT
mask test, substantial scheduler/yield/worker selection activity, and little map
work. Native filter uses128-bit loads for8 SHORTs despite512-bit support. Warm
medians are5.79 ms default,5.04 ms without bounding,6.62 ms with capacity64; these
controls do not justify an allocation-only fix. Proper-protocol Q7 is12.62 vs6.09
ms, showing why extra-warm diagnostic results must not replace protocol results.
Wider single-predicate batches are an isolated `/tmp` experiment next.

Wider-mask experiment rejected: the500M-row SHORT/INT/LONG matrix varied no
matches, about0.1%, about50% and all non-NULL rows, with three interleaved measured
runs per ordinary Java/LLVM8/LLVM32/LLVM64 mode. Exact results pass. SHORT sparse
18.41/17.41/18.15 ms and dense195.73/196.28/197.97 ms do not justify a wider default;
INT dense200.72/201.72/206.57 ms regresses. LONG rejection is already about60 ms
for4GB input. The apparent tiny-query LLVM64 gain did not generalize. Production
lowering remains unchanged. Evidence:`/tmp/query-clickbench-next/filter-width.log`.

Cancellation-loop strip mining was also tested only in temporary classes. Removing
the inner initialization poll branch changed Q8 from406.79 to405.35 ms and Q13
from254.29 to253.68 ms. It does not establish a useful end-to-end gain and is not
adopted. DISTINCT still spends most CPU in generated probing/update and set merge;
Q8 moves14.05 GB/query at34.53 GB/s, Q13 13.15 GB at51.45 GB/s. No inference that
both share one bottleneck. Evidence:`distinct-{umc,profile}`,`distinct-chunk-umc`.

Variable composite bounding was tested after a validated traffic/scaling study,
with12 focused backend/worker checks covering Parquet, NULL/empty/non-ASCII keys,
78KB keys, allocation failure and reuse. It is not adopted: Q14 stays220.01/219.93
ms while traffic rises11.74/12.23GB; Q16 stays549.58/550.74ms while traffic rises
27.18/29.37GB. Extra owned-key copies and partition records cancel the locality
benefit. The experiment remains temporary. Next inspect the dependent offset-table
lookup in the existing prefetch stage, which accounts for the largest sampled
locations in variable grouping. Evidence:`variable-{study.json,profile,bound-umc}`.

## 2026-10-27: stage dependent OrderedMap lookups

IBS identifies the scan offset-table demand load at277 mean/704 p90 sampled cycles;
non-multiplexed counters attribute98.48% of incomplete-retirement cycles to pending
loads (not98% of all cycles). Stage key/hash+offset prefetch, candidate+entry prefetch,
then validated update. Reuse hash/slot on insertion; growth rebuilds every unconsumed
candidate. Apply the same staging to existing merge/redistribution. Map layouts,
allocators, shard ownership and default worker configuration are unchanged.

Same-binary40-query controls: Q16 544.23 ->464.18ms; Q14 216.86 ->210.68ms.
All18 focused backend/worker checks pass. Interleaved500M-row controls (old/new ms):
fixed small429.32/430.33,5M groups1914.72/1901.01,skew575.75/577.37;
variable small766.34/758.43,5M groups4573.18/3776.58,skew1228.31/1179.78.
Every output matches ordinary SQL exactly. The new default retains an explicit
`query.codegen.orderedPipeline=false` control. A speculative value-prefetch address
based on incoming key length was neutral/slightly worse and is not adopted.
IBS first-offset demand latency falls277.2->7.5 cycles in scan and237.4->30.6
in merge (sample means; prefetched paths, not whole-query cycle shares). Proper
protocol Q16 is498.18ms andQ18 908.08ms, down from585.90/1073.65. Exact results
pass, including stock re-aggregation of every selected Q30-Q32 key and full-group
checks forQ36/Q37/Q38/Q40/Q41. Four repeatable major gains remain; Q16/Q33 do not
consistently clear1.5x across stock repeats. Evidence:`/tmp/query-clickbench-next/pipeline-study.json`,
`ordered-pipeline-{off,merge}`,`pipeline-{regression,variable-regression}.log`.

DISTINCT diagnosis: Q8's warmed scan/merge medians are148.72/201.41ms. The256
merge tasks total1530.49ms of work, but the largest takes167.30ms versus1.71ms
median (second-largest94.98ms). Cleanup adds roughly45–48ms. Merge-only CPU
sampling puts about28% at the first target-set load and14% at set initialization;
the busiest thread has a similar split. IBS samples the target load at194 mean/
600 p90 cycles. Thus whole-process scan profiles conceal a long merge tail.
A temporary32-slot union-prefetch window is essentially neutral;128 slots reduce
Q8 to about380ms but leaveQ13 near254ms. Neither is enabled. Next test vector
probing over the same set layout, preserving first-match/first-empty order and
allocation rules; changing parallel decomposition requires a separate design.
Evidence:`distinct-merge-study.json`,`distinct-phases`,`merge-only`,`distinct-ibs`.

## 2026-10-27: generated projection and bounded unordered LIMIT

Projection binds the existing pure integer/timestamp/IPv4 expressions to shared IR.
It packs referenced numeric inputs in128-row batches; existing records retain string,
symbol and row-ID ownership. Random A/B lookups have separate output slots; cursor
rewind/skip/reopen and function ownership remain in VirtualRecord. IPv4 subtraction
matches the resolved unsigned/null/underflow rules. Six backend/worker checks pass,
then18 checks including variable aggregation and average regressions. At500M actual
table rows, three interleaved samples show simple arithmetic roughly on par (about
7.0s), while a deeper expression falls22.03->8.80s with exact checksums. A separate
fully scheduled two-counter run observes124.62->47.20 billion cycles and675.85->283.08
billion instructions for that same500M-row deep expression. This is expression
execution evidence, not a memory-bandwidth claim. A simple-expression profile run
regresses6.88->7.47s; removing unnecessary per-row repositioning when every output
is already materialized removes that regression: the500M-row interleaved repeat
measures Java6.917s versus LLVM6.321s at the median, with identical checksums.

Positive constant LIMIT/OFFSET now generates row-ID selection over finalized maps,
with bounded query-tracked storage, early stop and whole-map skips. Large/dynamic/
negative limits keep the streaming implementation. Selection follows actual cursor
order, including the zero key last and encoded shard row IDs. Six backend/worker
checks compare every selected row ID against the same live map cursor, then validate
SQL values, offset, rewind, skip, size, cleanup and reuse across fixed/variable keys.

Full43-query protocol, unchanged defaults and fresh clean stock: Q35 is284.91/77.20ms
(3.69x), with exact output and10 generated projection rows/request. Q34 is999.56/675.90
(1.48x). Q17 is696.43/485.24 (1.44x), with10 generated selections/request; its selected
groups in all three requests now independently match clean-stock re-aggregation.
Q34 and Q35 also match exact complete output in all three requests.
Q16 remains1.41x in this full run despite a1.56x four-query diagnostic, reinforcing
why subset runs cannot replace the acceptance protocol. Q32 is near the cutoff at
1.49x this time versus1.53-1.54x previously. Coverage and major speedups remain
separate. Evidence:`/tmp/query-clickbench-next/{projection-matrix.log,projection-profile.log,
projection-*-deep-perf.csv,limit-protocol,limit-focused.log}`.

The8-lane DISTINCT probe preserves exact set layout but regresses Q8 to416.18ms;
it is not adopted. A64-row OrderedMap prefetch window is also neutral/worse than32.


## 2026-10-27: parallel DISTINCT union, using the existing workers

The native-filter counterfactual is neutral for Q10/Q11. Their scan finishes in
about26ms, but one inner-set merge takes35-40ms. Captured real sets show4-5x union
headroom with8 atomic workers; an isolated C++ helper confirms whole-query benefit.
That helper is not adopted. Shared IR now has aligned relaxed atomic load and strong
compare-exchange, with explicit non-removable memory effects in structured/SSA
optimization and implementations in LLVM, AsmJit and C. Contention checks pass on all
three. The generated path defers large count/payload states, drains outer-map jobs,
then dispatches inner-set ranges through the existing merge queue and acquired slots.
Generated finalization restores ordinary state before output. No new thread pool.

Exact full-group checks pass for actual Q8-Q11. Eighteen focused backend/worker checks
cover combined DISTINCT/SUM/AVG, reopen, injected union-worker failure and query-memory
failure. Warm40-query diagnostics reduce Q8 about411->312ms, Q9 about424->330ms,
Q10 77.02->39.03ms and Q11 64.78->44.97ms. Q10 traffic falls1.843->1.669GB/query while
controller rate rises22.9->39.9GB/s; Q11 traffic stays1.97GB/query while rate rises
28.8->41.1GB/s. This supports exposing independent probes to more workers; it does
not imply lower individual load latency. Q10's longest outer task falls to3.9ms,
then the largest union task is5.3ms. Q8 still spends35.4ms preparing target sets.
Avoiding a finalization scan when no states were deferred restores Q13 from263 to255ms.

The500M-row matrix exposed a45% regression when many independent groups each spawned
eight union jobs. Fanout now follows available worker slots divided by independent
states, and target initialization uses the same worker queue before union begins.
The repeated matrix is neutral for small sets (438.9/439.3ms before adaptation;
437.4/434.2ms after), neutral for many large groups (2806.2/2819.9ms serial/parallel),
and faster for one dominant group (456.2/426.1ms). Every aggregate matches exactly.
Warm actual-data medians are now Q8 284.18ms,Q9 305.91ms,Q10 33.77ms,Q11 38.73ms;
Q13 is253.65ms. Q10/Q11 controller rates reach44.8/46.2GB/s with similar or lower
traffic. All25 generated query shapes pass the actual-data result checks, including
independent full-group validation of tied and unordered selections.

The feature is now enabled by default; `query.codegen.parallelDistinct=false` retains
the ordinary generated serial union for counterfactuals. Presizing from summed source
cardinalities can waste memory on overlapping sets; a256MiB per-target ceiling selects
serial union. In a fresh complete43-query protocol, Q8/Q9/Q10/Q11 improve1.69/1.79/2.02/
1.58x versus clean stock. Nine queries exceed1.5x in that pass; boundary cases vary
between runs, so this is not22 repeatable major wins. Raw evidence stays in
`/tmp/query-clickbench-next/{adaptive-*,deferred-*,union-floor-*,union-matrix*}`.

## 2026-10-27: discard empty selection buffers and compile standalone filters

Short protocol runs exposed allocation work hidden by long warmups. Native filter
output growth copied discarded row IDs through realloc:3757 sampled faults in two
early Q41 requests versus132 when the empty buffer was freed before growth. Three
fresh-process pairs reduce Q41 median16.03->11.00ms and minor faults3325->18/request.
The500M-row zero/sparse/half/dense filter matrix has no meaningful regression.
Stock standalone selection has the same problem:54195 of54570 sampled early Q19
faults occur inside output-buffer realloc. A small shared
`DirectLongList.clearAndEnsureCapacity` now serves both filter paths, retaining
query-close shrinking and memory tracking. Q19 traffic falls1.254->0.841GB/request;
ordinary candidate execution falls23.32->13.02ms without LLVM. This infrastructure
gain must not be attributed to compiler instruction selection.

Standalone numeric selection and filtered COUNT now bind the shared scan IR to the
existing CompiledFilter/CompiledCountOnlyFilter interfaces. They borrow the existing
full column-pointer arrays directly. COUNT reduces predicate masks without row-ID
writes; selection preserves ordered row IDs. Existing scheduling, frame cancellation,
column-top/cast fallback, Parquet decoding and cursor ownership remain in place.
Population count is a typed IR operation, with LLVM intrinsic and portable arithmetic
lowering for C/AsmJit. Eighteen focused backend/worker checks cover mask bits, repeated
SQL, sparse/dense/empty outputs, column tops, Parquet and partial-compilation cleanup.
The500M-row interleaved matrix matches all results: zero/sparse/half/dense COUNT is
32.82/31.58/68.77/106.16ms ordinary versus33.17/31.51/68.53/105.50ms LLVM. Sparse
selection is50.04/52.04ms with overlapping ranges. Simple scans are near the measured
bandwidth floor; generated code itself is neutral here.

Complete generated coverage reaches27/43 with Q1 and Q19. A fresh full protocol
measures Q19 at31.17ms stock /13.02ms LLVM, and Q1 at5.82/5.35ms. Sixteen complete
queries exceed1.5x in this pass, but Q7 and Q38 vary substantially on stock and Q32
is borderline. This is still short of22 repeatable major gains. Do not compare the
earlier17.9ms Q7 stock result with a later5.4ms candidate: clean stock also reaches
5.4ms in a repeat. Exact deterministic outputs and independent group/tie oracles
validate the current actual-data results. Evidence remains under
`/tmp/query-clickbench-next/{selection-*,standalone-*,fresh-*,shared-buffer-stock}`.

The next VARCHAR counterfactual is negative. Raw generated partitioning bypasses
local maps and reuses existing partition pages/merge workers, but Q33 rises from
696.2 to848.0ms while traffic rises43.0 to51.8GB/query. Both sustain about61GB/s.
All results match. The temporary implementation is not adopted. Current profiles
put substantial work in key equality/probes and map initialization; polynomial
hash arithmetic is a small share, so unrolling that hash is not the next bet.

## 2026-10-27: one-query shared-state feasibility study

Focus remains Q33's general VARCHAR COUNT shape until it delivers a substantial
end-to-end gain. Current LLVM moves43.0GB/query and takes696ms; at63.2GB/s,
the traffic alone costs680ms. The physical URL input is10.64GB, including its
auxiliary column. Instruction selection cannot remove the remaining state traffic.

A temporary handwritten shared-table counterfactual reduces traffic to28.0GB and
takes471ms, versus980ms clean stock in the same40-request warmed diagnostic.
It grows seven times from262144 entries on the first request and retains only
capacity knowledge between requests. A second version uses QuestDB's actual final
maps, growth callbacks, memory tracking and existing workers. It takes478ms after
independent maps initialize concurrently; serial initialization takes525ms.
Fault profiling attributes224450of225499 sampled faults to map zeroing. All returned
results match; full-group and500M generality checks remain pending. These are native
algorithm experiments, not accepted LLVM speedups or proper ClickBench results.

The design keeps the ordinary maps as the final state, avoiding both replicated
worker tables and a final materialization pass. Java owns initialization/growth and
query lifetimes; generated code will own hashing, exact comparison, insertion and
COUNT updates. Acquire/release publication and relaxed atomic addition now exist
in the shared IR, LLVM, C and AsmJit; six contention/publication checks pass.
Evidence:`/tmp/query-clickbench-next/{shared-study.json,shared-summary.json,
shared-parallel32,global-grow32,global-stock32,shared-map-faults}`.

The opt-in shared-IR implementation now takes486ms in LLVM, versus696ms with
private generated maps and980ms clean stock; measured traffic falls43.0->28.6GB.
Three fresh-process Q33 checks, each using one cold and two hot HTTP requests,
confirm1.96-2.01x hot speedups. Cold execution regresses:3.32-3.52s versus
2.83-3.09s stock, including0.56-0.59s LLVM compilation. This is a single-query
check, not a new full ClickBench acceptance result. All18,342,019 groups match
stock byte-for-byte and count-for-count; all99,997,497 rows take the shared path.
Six focused backend/worker checks cover growth, native/Parquet mixing, cancellation,
memory-limit failure and reuse. The following matrix closes the pending generality
check; `query.codegen.sharedVarchar=true` remains opt-in. Evidence:`shared-llvm32`,
`shared-short-{0,1,2}`, and`shared-validation` under the same temporary directory.

Unconditional shared atomics fail the500M-row controls:512 groups take1120ms versus
191ms private LLVM;90% hot-key skew takes5151ms versus504ms. A4096-entry private
counter cache now combines repeated updates, flushing before every resize-lock
release. Copies of immutable key headers/pointers avoid rereading a shared counter's
cache line. On the skewed input, cross-CCD demand fills fall9.3x and cycles27.5x,
versus only1.19x fewer instructions. Eight-core/single-CCD versus32-slot scaling
independently confirms contention. Three-event PMU groups run100%; rejected
four-event groups could not schedule and supply no evidence.

After65536 already-aggregated rows, observed small shared state permits private
maps up to8192 groups per slot, approximately384KiB at the normal load factor.
Growing private state returns to sharing. No preliminary scan or cardinality
oracle is used. All final states are actual QuestDB maps merged by existing workers.
The500M-row matrix, using identical core/configuration, gives median milliseconds:

| Distribution | Ordinary Java | Private LLVM | Adaptive shared LLVM |
| --- | ---: | ---: | ---: |
| 512 groups | 419 | 188 | 210 |
| 20M dispersed groups | 7127 | 5305 | 2486 |
| 90% hot key, 2M tail groups | 725 | 506 | 339 |
| 20M clustered groups | 810 | 602 | 553 |

Small-state traffic falls14.1->8.4GB/query, near private LLVM's8.2GB, but latency
remains about10% above private LLVM. Memoizing the small-state decision did not
help and was rejected. These synthetic Java controls use the candidate core;
actual ClickBench comparisons continue to use the clean stock binary.

Final Q33 validation again compares every18,342,019 key/count pair exactly;12
focused backend/worker checks also cover small-to-large phases and reuse. A full
43-query protocol with the shared option measures Q33 at994.4/476.2ms stock/LLVM,
Q34 at997.2/479.6ms and Q12 at182.8/115.3ms. Q33's first execution still regresses
1412.5->1616.4ms, including411ms compilation. Seventeen complete generated queries
exceed1.5x in this pass; volatile short queries and borderline cases prevent a
claim of22 repeatable wins. Coverage remains27/43. Evidence stays in`/tmp` under
`query-clickbench-next/{shared-coherence-study.json,shared-adaptive-matrix.log,
shared-final-test.log,shared-validation,shared-final-protocol}`.

## 2026-10-27: EC2, 96 cores and high memory bandwidth

The frozen prototype runs on c8i.metal-48xl: Xeon6975P-C,96 physical cores,
192 logical CPUs,377GiB RAM and three NUMA domains. LLVM21.1.8 generates code
for the host. The actual99,997,497-row ClickBench table is resident in tmpfs,
with identical input pages balanced across NUMA nodes. This measures resident
execution, not cold-storage ClickBench. Stock uses a clean8da4e347f9 JAR and
an explicit classpath excluding candidate classes. Its JIT,190 query workers,
sharding threshold, frame sizes and unmapping policy retain execution defaults.

Three fresh processes per engine, reversed order,10 warmups and40 requests per
query give median milliseconds (median of process medians):

| Query | Clean stock | LLVM | Speedup |
| --- | ---: | ---: | ---: |
| Q1 | 2.06 | 2.05 | 1.00x |
| Q8 | 298.95 | 191.50 | 1.56x |
| Q12 | 67.60 | 29.36 | 2.30x |
| Q15 | 70.61 | 53.11 | 1.33x |
| Q32 | 503.88 | 427.81 | 1.18x |
| Q35 | 59.40 | 44.37 | 1.34x |
| Q33 | 236.23 | 136.37 | 1.73x |

Only Q8,Q12,Q33 exceed1.5x in every paired process comparison. Q32 stock
process medians range503-738ms; selecting the slowest baseline exaggerates its
gain. This seven-query sample does not establish a full-suite breakthrough.

Isolated Q33 takes230ms stock,146ms private LLVM and134ms shared LLVM. Controller
traffic is59.1,41.6,22.7GB/query respectively; shared LLVM averages164GB/s versus
the host's calibrated525GB/s mixed streaming rate. Its traffic would take43ms
at that streaming rate, an optimistic yardstick rather than an attainable bound.
At96 physical CPUs, private LLVM wins137ms versus182ms shared; SMT reverses the
ordering. Saving traffic is insufficient to choose the best state organization.
The generated shared kernel dominates CPU samples; metadata sharing and memory
access latency need a controlled counterfactual before another implementation.

All18,342,019 URL keys/counts compare exactly with clean stock. Tied Q32 outputs
are independently re-aggregated by stock; native audits cover the required
operators. No engine implementation changed. Initial matched runs inherited
asynchronous unmapping=true; all numbers above were rerun with its default=false.
Timing-source audit also corrects the earlier wording above: HTTP execute time
already includes compiler time; the two must not be added.
In the corrected EC2 sequence, the first Q33 request still regresses:0.81-1.52s
stock versus2.02-2.31s LLVM, including0.48-0.49s compilation. The hot gain does
not establish a first-execution gain.

Detailed evidence and rerun scripts remain in `/tmp/query-ec2` and on the disposable
host at `/home/ec2-user/query-compiler-bench`; primary labels start`defaultconf-`.

Eight-core Q33 follow-up: restricting the entire JVM to eight physical cores,
without SMT siblings, yields 2396ms stock, 1142ms private LLVM (2.10x) and
1352ms shared LLVM (1.77x). These are medians of three fresh-process medians,
each with ten warmups and twelve measured requests. QuestDB automatically
selects eight query workers; the execution configuration otherwise stays default.
Packing the eight cores onto one NUMA domain gives 2279/1107/1241ms in a separate
single-process control, preserving the ordering. Input pages remain interleaved
across all three domains. Private LLVM wins despite moving 29.9GB/query versus
shared's 21.3GB; stock moves 37.6GB. All outputs match, with no measured storage
reads or major faults. This is Q33 only, and restricting affinity retains the
large host's cache and memory system. Evidence: `eight-*` under the same roots.

Eight-core Q33 diagnosis: the remaining cost is a mix of load latency and bad
speculation in variable-length key processing. The same eight spread cores reach
110 GB/s controller reads in a separate streaming calibration (156 GB/s with an
ordinary-store triad), versus approximately 26 GB/s private LLVM and 16 GB/s shared.
Private LLVM's pipeline-slot breakdown is 31% memory stalls, 26% bad speculation,
19% frontend, 13% other backend, 12% retiring (rounding). Stock is 56% memory stalls.
These are slot fractions, not fractions of elapsed time. All counter groups ran
100%; each profile covers twelve complete 99,997,497-row executions after warmup.

Precise retired-branch sampling locates 42% of private LLVM misprediction samples
in scan hashing loops/tails and 27% in the primary string-equality loops/tails.
The hash retains a serial multiply/add chain; SIMD equality still has separate
32/16/8/4/2/1-byte tail branches. Private LLVM executes 291 instructions and 5.79
branch misses per input row, versus stock 622 and 4.01. Load-latency samples point
mainly to URL bytes, with shared-view metadata secondary at eight cores. Shared
LLVM incurs 3.31x private's remote-HITM events and is 18% slower despite 29% less
controller traffic; this supports a coherence cost but does not isolate false
sharing from necessary ownership transfers. Page walks are substantial but their
busy cycles overlap execution and are not a separate additive wall-time cost.

Next experiment: improve general variable-length hash/equality primitives and
measure branch misses, cycles/row, load latency and end-to-end time together.
Test arbitrary lengths, page boundaries, early/late mismatches and different key
distributions; preserve exact hashing and byte equality. Metadata separation
remains an unverified hypothesis, not the first eight-core target. No engine
code changed in this diagnosis. Evidence and limitations: `/tmp/query-ec2/REPORT.md`,
`pmu8-analysis.json`, `mem8-analysis.json`, and `remote-results/diagnose8-*`.

## 2026-10-27: bounded overlapping byte comparisons

Replaced the generated equality remainder cascade with bounded first/last blocks,
using the overlap technique already present in QuestDB's native memcmp. Long
strings retain the 64-byte loop and compare one final block ending exactly at the
slice boundary. Short strings select a width and compare their first/last blocks.
No new IR, backend operation, hash algorithm or query-specific rule is needed.

On EC2's eight physical cores, Q33 improves from 1147 to 984ms (14.2% less time)
in the final ABBA control, ten warmups and twelve timed queries per fresh process.
The compiler directories have identical structure and differ only in this class.
An earlier three-process comparison gives 1160 to 983ms. Fixed-work counters show
about 28% fewer branch misses, 13% fewer instructions and 15% fewer cycles per row.
Equality's precise misprediction samples fall from 17,909 to 3,978; instruction
delivery improves too. Controller traffic stays near 30GB/query. A separate
shared-map control improves from 1362 to 1214ms with traffic near 21GB/query.

Independent 537M-comparison cases, repeated in ABBA order, improve mixed short
lengths 2.61x, mixed equal lengths 1.37x and late mismatches 2.06x. First-byte
mismatches are unchanged. Equal 256-1023-byte strings regress 4.1%; keep that
tradeoff visible. All 18.3M actual URL keys/counts match the retained stock oracle.
The 18 existing scan/backend checks and six focused SQL checks pass; guard pages
now cover both ends of either slice, every length 0-258 and every mismatch position.
EC2 guard checks also pass at LLVM O0/O2/O3.

All 43 queries pass the existing output checks. This screening is not a full-suite
speedup claim: Q32 initially regresses 25%, then about 3% in fresh-process controls,
including equal-structure classpaths. Its native objects are byte-identical and
the cause remains unresolved. Q10's smaller apparent regression does not persist.
The full-machine Q33 controls improve roughly 5-9%, less than at eight cores.
Keep this as a measured prototype improvement, not a universal no-regression claim.
Study, controls and raw evidence: `/tmp/query-ec2/tail-experiment` and
`/tmp/query-ec2/remote-results/tail-*`. Stock binaries/configuration are unchanged.

## 2026-10-27: DuckDB 2.0.0 comparison on the EC2 URL aggregation

Q33 (zero-based), the exact 99,997,497-row URL/COUNT/Top-10 workload, now has a
released DuckDB control. Exported URL bytes through a stock QuestDB SQL cursor,
including all 67,763 NULLs; loaded DuckDB native storage outside timing. Every one
of the 18,342,019 grouped keys and counts matches the retained stock oracle exactly.
DuckDB v2.0.0/d54cbbe3df uses ordinary hash aggregation and Top-N; its default
latest-format VARCHAR storage is DICT_FSST. This is a resident execution comparison,
not an official cold-cache ClickBench run or a comparison across the full suite.

| Engine | Eight physical CPUs, ms | All 192 logical CPUs, ms |
| --- | ---: | ---: |
| Stock QuestDB | 2453.8 | 230.3 |
| Updated private-map LLVM pipeline | 993.2 | 136.5 |
| DuckDB 2.0.0, default compression | 1836.7 | 239.0 |
| DuckDB 2.0.0, separate uncompressed control | 1589.6 | 229.2 |

Each cell is the median of three fresh-process medians, ten warmups and twelve
measurements per process. Primary engine order rotates; all engines get the same
CPU affinity. QuestDB execution defaults and binaries are unchanged. DuckDB uses
8 threads under the eight-CPU restriction and its default 192 on the full host.
LLVM is 1.85x/1.75x faster than default DuckDB, and 1.60x/1.68x faster than its
uncompressed control. The final compressed eight-core repeat is 1822.8ms, supporting
the storage control despite running it after the primary comparison.

At eight cores, LLVM executes about254 instructions/row versus1018 for default
DuckDB and535 uncompressed. DuckDB's default IPC is1.79 versus LLVM0.84: reduced
instruction count does not translate proportionally into elapsed time. Separate
fixed-work controller passes measure29.6/45.8/49.7GB per query for LLVM/default
DuckDB/uncompressed DuckDB, with all counters running100%. Removing compression
reduces DuckDB instructions and time while increasing traffic. Its measured
operator CPU time remains dominated by hash aggregation; this is software
attribution, not a new hardware-bottleneck diagnosis or proof that JIT alone
explains the engine difference. Storage formats, layouts and aggregation algorithms
also differ.

All timed requests have zero physical storage reads/writes and zero major faults;
the primary 100ms USE sampler retains at least275GiB available memory, no memory
pressure and zero cgroup throttling. No new kernel errors or OOM events. DuckDB's
separate profiles report no spilling. DuckDB timings include prepared execution
and fetching all ten results; QuestDB server timings exclude about1.1ms HTTP/client
overhead, too small to explain these gaps. All benchmark processes are stopped.
Temporary harness/evidence: `/tmp/query-ec2/duckdb-experiment`,
`/tmp/query-ec2/remote-results/duck-*`; no engine source changed in this comparison.

## 2026-10-27: Full ClickBench script protocol on EC2

Completed all 43 queries with three executions each in all six requested
configurations: 774 accepted timings, no query errors. Used the repository's
cache-drop/three-run protocol, no extra warmups, one QuestDB server per suite and
one DuckDB CLI process per query. Both full native datasets are disk-backed on the
same EBS filesystem, with 99,997,497 rows. DuckDB 2.0.0 uses verified uncompressed
storage. All cores means 96 physical / 192 logical CPUs; eight means eight physical
CPUs spread across the three NUMA domains. Retained binaries and QuestDB execution
defaults are unchanged; no engine source changed in this comparison.

| Engine | CPUs | First-run sum, s | Warm sum, s | Warm geometric speedup vs stock |
| --- | --- | ---: | ---: | ---: |
| Stock | All | 63.055 | 5.428 | 1.000x |
| LLVM candidate | All | 58.689 | 3.300 | 1.529x |
| DuckDB uncompressed | All | 81.255 | 3.650 | 1.582x |
| Stock | 8 | 73.170 | 28.520 | 1.000x |
| LLVM candidate | 8 | 66.458 | 15.435 | 1.527x |
| DuckDB uncompressed | 8 | 116.082 | 18.608 | 1.473x |

Warm is min(run 2, run 3); geometric scoring uses ClickBench's 10ms additive
constant and 1ms publication precision, against matching-core stock. This is one
protocol pass, not a confidence interval. LLVM and DuckDB have close full-suite
scores; the earlier Q33 win does not generalize to the entire suite. LLVM completely
generates 27 queries; only 20 fully generated queries at either core count reach
1.5x stock warm speedup. The 22/43 major-gain target remains unmet. Generated queries
cover 83.5%/82.5% of stock warm time on all/eight cores. No single query supplies more
than 20% of net warm latency saving. Q27 all-core regresses 47.6 to 176.9ms; Q28 at
eight cores regresses 3056.9 to 3431.3ms. Both are fallback queries and need followup.

First-run sums are not comparable cold-cache tests: QuestDB retains mapped pages
between queries, while DuckDB closes its buffer pool. Native process physical reads
are about 39-40GB versus DuckDB 106-109GB across a suite. LLVM compilation is included
in execution timing and totals 12.5-13.0s on first runs. No memory pressure, cgroup
throttling, OOM or new kernel errors were observed; this suite did not collect a new
PMU/controller profile and establishes no new hardware-bottleneck diagnosis.

Stock/LLVM result checks pass, including independent reaggregation of 1,300
returned tied/unordered rows. The supplied cross-engine SQL differs semantically
for Q5 (empty-string DISTINCT), Q27 (byte/character length) and Q28 (length plus
regex end-anchor handling of trailing newlines); untimed equivalent diagnostics
match stock exactly. Timed SQL remains unchanged. Excluding those three queries,
warm geometric scores are LLVM/DuckDB 1.612x/1.626x all-core and 1.561x/1.513x at eight.
DuckDB all-core Q18 was repeated only after a resource-sampler process-exit race;
the abandoned output is retained and no speed-based retries occurred.

Full per-query report and CSV: `/tmp/query-ec2/full-clickbench/REPORT.md` and
`results.csv`; raw evidence also remains in the remote `full-clickbench` directory.
All benchmark processes are stopped. Keep only this progress note in the prototype.

## 2026-10-27: generated searched-CASE VARCHAR keys and sorted LIMIT windows

Computed VARCHAR keys now bind direct columns, UTF-8 literals and nested searched
CASE branches to shared IR. Conditions reuse resolved integer predicates; string
views are decoded only in the selected branch. Slot descriptors own literal bytes,
and existing OrderedMap staging owns copied keys. No row-level Java callbacks,
new map, allocator or scheduling infrastructure. Simple CASE/SWITCH, dynamic string
construction and unsupported conditions retain fallback. The binder reads the
searched picker's resolved values: the lifecycle argument list alone is insufficient,
and StrConstant deliberately supplies UTF-8 even though its declared type is STRING.

The strict coverage audit found a second composition boundary: earlier Q38/Q40/Q41
coverage counts included generated Top-K but overlooked ordinary Java OFFSET
iteration. Long Top K now advertises a separate sorted-window capability. The
existing LIMIT kernel copies the requested row-ID window from the borrowed sorted
pair buffer into LIMIT's tracked output. Existing polling, workers and cursor
lifetimes remain in charge. Older compiler implementations safely decline this
new capability. Actual Q38-Q41 plans now show compiledSelection, and counters prove
all ten output row IDs per request are generated. This closes the overlooked
boundary and brings cumulative supported shapes to 28/43 with Q39; the full
43-query performance protocol was not rerun in this slice.

On the local Ryzen 9950X, pinned to eight physical cores, a 500M-row mixed UTF-8
fixture gives these warm medians (three interleaved samples after three warmups):

| General grouping shape | Ordinary Java, ms | LLVM, ms | Speedup |
| --- | ---: | ---: | ---: |
| Column-or-empty conditional key | 2762.512 | 941.468 | 2.93x |
| Multiple/nested branches, Unicode literal and NULL | 3794.989 | 1100.999 | 3.45x |
| Existing direct composite-key control | 6712.777 | 1969.615 | 3.41x |

Both modes use current core and server execution defaults, seven workers plus
caller. Every result group matches exactly; every LLVM execution records 500M
native map rows. Timings include scan/aggregation, merge, output and cleanup;
compilation and validation sorting are separate. Native preparation for the two
new shapes is about 163/98ms. All measured executions have zero physical reads
and major faults. The temporary harness initially sorted validation results by
only the first of two grouping keys; it was corrected before accepting timings.
These are local exploratory outcomes, not a new hardware-bottleneck diagnosis or
an EC2 comparison. The EC2 instance has been deleted.

Actual ClickBench Q39, 99,997,497 input rows, eight physical cores: medians of two
fresh-process medians are clean stock 73.240ms, previous prototype 72.114ms and new
pipeline 62.869ms. Process order is stock/before/after/after/before/stock; each uses
three warmups and six timed requests. Warm requests have zero physical reads and
major faults. All 426,328 complete groups match clean stock, including NULL versus
empty-string keys; each query has 722,688 generated updates, 426,328 generated
Top-K visits and ten generated selected row IDs. Q38/Q40/Q41 complete groups also
match stock, with their sorted-window plans and counters checked independently.

Q39's warm gain is 1.17x stock, below the major-gain threshold. First fresh-server
execution is about 656-667ms, including 538-544ms compilation. Keep that cost visible;
more coverage does not establish the 22-query performance target.

Validation: 24 focused backend/worker checks for computed strings and existing
variable keys, 18 for sorted/unordered LIMIT plus storage/literal lifetimes, and
six existing core CASE tests pass. Coverage includes multiple/nested branches,
Unicode, NULL/empty, long literals, column tops, native/Parquet storage, sharding,
merge, cursor reuse, ASC/DESC windows and windows beyond the result size.
Temporary evidence: `/tmp/query-case-keys`; no result archive was added to the repo.
All experiment servers and workers are stopped.
## 2026-10-27: Q39 compilation cost is real; generated Top-K dominates

Checked the original HTTP nanosecond counters: fresh-server compilation is
537.910/543.638ms; later EXPLAIN compilation is 446.188/441.860ms. The HTTP compiler
timer measures SQL preparation plus owner/telemetry bookkeeping, not LLVM alone,
and is included in the HTTP execute timer. Cached exact-SQL requests report zero
compilation. These are local Ryzen 9950X results, not the deleted EC2 host.

Temporary instrumentation outside the checkout split Java lowering/emission and
native parse/initialization/O2 optimization/materialization. A compile-only harness
uses the existing isolated ClickBench overlay and normal server configuration,
pinned to the same eight physical cores; it does not scan/execute the query. Five
recompilations after the first measure 435.867-452.018ms (median442.461ms), consistent
with the uninstrumented EXPLAIN figures. Nine separate native kernels total a
median420.308ms: approximately198.379ms optimization,215.743ms machine-code
materialization/linking and5.589ms parsing/verification. Native thread CPU time is
420.248ms. Thread-scoped perf groups, without multiplexing, measure median2.332B
cycles and5.882B instructions across those calls; scheduler waiting does not
explain the cost.

Top-K alone costs307.919ms native (roughly144ms optimization and161ms codegen),
about73% of native preparation. Its input is218,321bytes of LLVM text /6,295 IR
instructions, growing to13,367 instructions after optimization. It accounts for
roughly1.7B cycles and4.7B retired compiler instructions. Map scan/update is46.540ms,
merge29.929ms, and the remaining six kernels total about36ms. The first instrumented
compile is605.738ms; do not substitute that startup/instrumentation result for the
original HTTP cold measurement.

Source attribution: QuestLongTopK emits small/large algorithms and both sort
orders, selected through runtime state. QuestLargeTopK emits three source modes
(single map, map array, pair list) plus a final sort for each order. Each site
expands QuestRadixSort inline: eight copies of the radix-sort body in one function,
with all eight radix passes explicitly emitted. LLVM receives a large multi-mode
function and runs the full per-module O2 pipeline on every separately built kernel.
The reused native engine itself initializes in about0.5ms on first use and is
negligible afterward. UTF-8 conversion/parsing and FFM are not the main expense.

Diagnostic O1 recompilation remains423.899-435.198ms (median428.837ms), versus
442.461ms atO2; execution quality was not measured, so this is not a recommended
configuration change. Next design target: typed internal helpers / controlled
inlining for shared radix code, plan-derived structural specialization, and reuse
of compatible compiled helpers with explicit ownership. Preserve generated sorting,
existing semantics and algorithm choices. Measure preparation and query execution
together; do not merely lower optimization or add query-specific exceptions.
Diagnostic files are temporary under /tmp/query-compile-cost; no compiler or
execution implementation changed in this investigation.

## 2026-10-27: direction specialization halves Top-K compilation work

Experiment B is implemented as `query.codegen.specializeTopKDirection=true`
(default false). The ordinary LongTopK factory passes resolved ASC/DESC through
its existing Selected/Virtual/Async preparation chain. Shared IR emits one order;
all code remains inline and retains both capacity algorithms and the map/map-array/
worker-list inputs. Older hooks retain generic preparation. A slot declines a
mismatched list order before mutation. No query recognition or execution settings
changed. C (capacity-class specialization) and D/E (helper policies) remain pending.

On the local Ryzen9950X, three fresh processes per arm and five forced warm
recompilations each give these medians, with JVM affinity on the original eight
physical cores and per-thread native compiler counters:

| Preparation metric | Generic A | Specialized B |
| --- | ---: | ---: |
| Whole warm SQL preparation | 446.437ms | 292.627ms |
| Native Top-K compilation | 308.490ms | 156.925ms |
| Top-K compiler cycles | 1.709B | 0.867B |
| Top-K compiler instructions | 4.695B | 2.349B |
| Top-K IR instructions before/after optimization | 6295/13367 | 3178/6801 |
| Top-K machine-code bytes | 61398 | 33635 |

Native compiler CPU time matches wall time, and counter groups did not multiplex.
An initial whole-JVM single-core run caused JVM compiler-thread scheduling delays;
its elapsed timings are excluded. Code dumps were collected separately from timing.

Actual Q39, 99,997,497 rows: A/B/B/A/A/B fresh servers, three warmups and six measured
requests each. Medians of process medians are63.103ms A versus63.241ms B. First
SQL preparation is546.759ms versus384.627ms; first complete request670.060ms versus
506.081ms. All426,328 complete groups match and native work/selection counts are
equal. Complete groups and generated OFFSET selection also validate forQ38/Q40/Q41.
Warm requests show zero physical reads/major faults. These are current-prototype
A/B comparisons, not new stock QuestDB results or a full ClickBench protocol.

General nullable SUM fixture:500M source rows,1,048,576 groups, LIMIT1024, both
orders, unchanged server execution defaults. Eight-core complete-query medians
are1363.861->1362.544ms DESC and1375.053->1373.288ms ASC. One execution-thread
medians are8763.795->8783.700ms DESC and8751.897->8746.002ms ASC; both comparisons
remain within0.3%. Each execution has500M native updates and1,048,576 generated
Top-K visits, exact ordinary-Java result equality, zero physical reads/major faults.
Three warmups and seven alternating samples per arm at8cores; five samples at1.
The one-execution-thread JVM retains eight-core affinity for runtime housekeeping.

Isolated collection/merge over the finalized maps exposes a small tradeoff: a
separate balanced counter run measures7.562->7.193ms DESC (about4.9% faster) and
7.624->7.735ms ASC (about1.5% slower). Non-multiplexed cycles/group are42.066->40.322
and42.009->42.434 respectively; instructions/group increase in both cases. This
supports preparation savings with no material full-query regression on these
workloads, not uniformly improved execution or an instruction-cache diagnosis.
The initial six-event counter run multiplexed and is excluded from conclusions.
Global host I/O PSI remained high; timed process reads/faults were zero and native
compiler wall/CPU time agreed. The unrelated host PSI condition is not explained.

Focused validation covers24 cases across LLVM/AsmJit/C and both dispatcher modes:
exact values/row IDs and ties, K64/65/1024/5000, signed extremes/null, wrong-order
refusal, cancellation/reopen, map-array collection, worker-list merge, composite
keys and OFFSET. Maven method filtering initially selected zero parameterized
cases, so the existing focused JUnit runner was used. An older numeric worker-merge
test failed identically for A/B: bounded small maps bypass its forced sharding
threshold. That test now scopes/restores bounded=false to exercise its intended
merge path; benchmark defaults remain unchanged. No new scheduling/map/lifetime
infrastructure. Build and diff checks pass. Temporary evidence and reusable500M-row
fixture live under /tmp/query-topk-specialize; no results archive was added.

## 2026-10-27: small-capacity specialization cuts Top-K compilation about tenfold

Experiment C passes the retained prefix length through the existing Top-K
preparation chain. The shared IR can select either side of the existing K=64
algorithm boundary; exact K remains a runtime value. OFFSET supplies its upper
bound, not its output row count. Destination capacity/order checks decline an
incompatible kernel before mutation. Collection, merge, storage, cancellation and
ownership remain unchanged. No helpers, new algorithm or query recognition.

The public opt-in `query.codegen.specializeTopKCapacity=true` retains only K<=64
specialization. Larger K keeps the previous kernel because the large-only variant
failed the execution regression gate below. The low-level large-only emitter
remains available for experiments. Both specialization flags default to false.
D/E helper experiments remain pending, with B as their large-K inline control.

Control B has direction specialization enabled; raw C adds capacity-class
specialization. Local Ryzen9950X, LLVM21 O2, JVM affinity0-7, unchanged execution
defaults. Three fresh processes/arm, five forced warm recompiles/process:

| Metric | B | Raw C |
| --- | ---: | ---: |
| LIMIT10 whole warm SQL preparation | 287.569ms | 134.915ms |
| LIMIT10 native Top-K compilation | 157.121ms | 15.412ms |
| LIMIT10 Top-K compiler cycles/instructions | 0.870B/2.349B | 0.085B/0.165B |
| LIMIT10 IR instructions before/after optimization | 3178/6801 | 349/556 |
| LIMIT10 Top-K machine-code bytes | 33635 | 1438 |
| Original OFFSET query, K1010, whole preparation | 292.932ms | 274.918ms |
| K1010 native Top-K compilation | 156.338ms | 142.360ms |

LIMIT10 is a diagnostic variant of the actual-data query shape, not an original
ClickBench result. Compiler counters did not multiplex; native wall and CPU times
agree. These savings remove compiler work without changing the selected algorithm.

For execution isolation, independently built parallel maps proved an imperfect
control because their traversal may differ. A temporary harness now holds one
finalized map set, destinations and scratch fixed, switches B/C compiled kernels
only after workers drain, and validates exact `(value,row ID)` sequences. With
1,048,576 groups and21 alternating measured calls/arm, collection plus merge:

| K/order | B | Raw C | Change |
| --- | ---: | ---: | ---: |
| 10 DESC | 6.950ms | 7.008ms | +0.83% |
| 10 ASC | 7.066ms | 6.977ms | -1.26% |
| 1024 DESC | 7.121ms | 7.388ms | +3.75% |
| 1024 ASC | 7.665ms | 7.292ms | -4.87% |

Large-only DESC exceeds the3% phase regression gate despite only9% compilation
savings. Reject it for both directions; one CPU's direction-dependent outcome is
not a sound planner rule. The hardware cause of the regression remains unproven.
Separate-map PMU runs are retained in temporary diagnostics, but cannot establish
a code-only mechanism. Earlier B's phase measurements have the same traversal
qualification; its compilation and complete-query results are unaffected.

General fixture:500M source rows, nullable SUM,1,048,576 groups. Eight-core full
queries (three warmups, seven alternating samples/arm): K10 DESC1369.573->1371.846ms,
ASC1381.998->1371.209ms; K1024 DESC1362.367->1361.480ms, ASC1369.568->1371.710ms.
All results match ordinary Java, with500M native updates and1,048,576 generated
Top-K visits per query. After retaining only small K, one execution thread with
three warmups and five alternating samples/arm gives K10 DESC8745.203->8749.780ms
and ASC8785.210->8719.754ms, with the same exact results/work and zero reads/faults.
JVM housekeeping retains eight-core affinity. Top-K is only about0.5% of the
eight-core full query here, so the large phase regression is hidden by complete-query
timings. This is a compilation win, not an execution breakthrough.

Original Q39 on99,997,497 actual rows, B/C/C/B/B/C fresh servers, three warmups and
six measured requests/server: median of process medians62.244->63.021ms. All426,328
groups match, with equal native work; Q38/Q40/Q41 complete groups and generated
OFFSET also match. These timings use the rejected large-only variant; the retained
policy generates B for this query. They are not stock comparisons or a new full
ClickBench suite. Timed execution reports zero physical reads/major faults; global
host I/O PSI remains high and unexplained, as in B.

Focused validation:27 cases pass across LLVM/AsmJit/C and both SQL dispatcher modes,
including K1/64/65/1024/5000, both orders, ties/null/extremes, cancellation/reopen,
incompatible-capacity refusal and OFFSET63,64 versus64,65. After restricting the
public flag, the12 numeric/OFFSET SQL cases pass again. No benchmark execution
settings changed. Separate final code dumps confirm that only Top-K IR differs
for LIMIT10, while all generated IR is byte-identical for K1010 with the flag on
or off. Code sizes above come from those dumps, outside the timed runs. Build and
diff checks pass. Temporary evidence is under /tmp/query-topk-capacity and reuses
the existing500M-row fixture; no results archive was added.

## 2026-10-27: remove specialization toggles

Direction and K<=64 specialization are now automatic in resolved Top-K preparation.
Removed both experiment properties; there are no replacement configuration knobs.
Larger K retains the direction-specialized kernel with both algorithms. Earlier
entries describe the measured A/B controls; their opt-in instructions are historical.
Build and12 focused numeric/OFFSET SQL cases pass with no specialization properties
set, across LLVM/AsmJit/C and both dispatcher modes.

## 2026-10-27: retain typed radix helper after the inline/outlined experiment

One direction-specific native radix body now serves large Top-K and full LONG
sort. The algorithm, buffers, polling, O2 and CPU target are unchanged. Typed
helper parameters prevent captured state; nonzero status aborts the caller before
publication. LLVM retains the direct internal call in the query's existing ORC
lifetime. C/AsmJit expand the same typed call before their existing lowering.
Small K has no radix call. No new configuration toggle or runtime cache.

Three fresh processes per arm, five warm forced preparations each, balanced order:

| Actual Q39 preparation | B: inline | D: helper, alwaysinline | E: helper, noinline |
| --- | ---: | ---: | ---: |
| Whole SQL preparation | 293.805ms | 288.250ms | 189.879ms |
| Native Top-K compilation | 156.732ms | 154.324ms | 56.292ms |
| Compiler instructions | 2.348B | 2.299B | 0.698B |
| Final native bytes, including helper | 33,635 | 33,619 | 8,100 |

D's helper calls disappear; E retains four call sites to one radix body. Compiler
thread CPU agrees with wall time and counters do not multiplex. In emitted x86,
the caller's stack reservation shrinks from2,392 to120bytes; the helper reserves
another232bytes, excluding saved registers. This supports reduced compiler/code
work, not an established instruction-cache bottleneck.

Same finalized maps, destination and scratch, 1,048,576 groups, eight slots,
21 alternating calls per arm, exact value/row-ID validation: D reproduces B.
E reduces collection/merge from7.143 to5.965ms for K1024 DESC and7.688 to5.961ms
for ASC. Both directions also improve at K65 and8192. Separate selected-arm PMU
processes report40.8->33.3cycles/group and52.6->35.1instructions/group; those
counter totals use separately built maps, so they are supporting evidence only.
Standalone radix checks at0..16,384 pairs cover ties/extremes, early cancellation,
no caller publication on failure, successful reuse and code lifetime.

Whole-query cost barely moves. With500M rows and1,048,576 groups, eight-slot
nullable SUM at K1024 changes1377.532->1384.446ms DESC and1374.153->1369.044ms ASC.
One execution thread gives8761.329->8750.342ms and8759.943->8762.966ms respectively.
A composite VARCHAR/integer key with tied COUNTs gives2192.782->2175.589ms; a
selective16-group query gives69.763->70.124ms. Preparation is separate; native
work and results match, warmed execution has no reads/major faults. The general
fixtures use at least500M rows; the tiny radix checks are boundary diagnostics.

Original Q39 on99,997,497 actual rows, six balanced fresh servers, three warmups
and six measured requests each: warmed process-median execution63.242->63.767ms;
first request508.481->409.026ms, first compilation392.494->290.404ms. Complete
426,328-group results and OFFSET windows match. No whole-query execution win is
claimed. The preparation benefit is amortized over reuse; high reuse makes it
small relative to aggregation. Global host I/O PSI remains high and unexplained,
but timed warm execution reports zero reads/major faults.

The integrated compiler emits exactly the measured E optimized LLVM IR. Build
and24 focused cases pass across LLVM/AsmJit/C, including helper argument isolation,
failure/cancellation/reuse, numeric Top-K, generated OFFSET and full LONG sort.
Temporary evidence is under /tmp/query-topk-helpers; no results archive was added.

## 2026-10-27: refresh stock comparison after helper integration

Actual99,997,497-row ClickBench data, local9950X, affinity0-7 (eight physical cores
on one CCD). Clean stock and candidate share base8da4e347f9 and byte-identical
server configuration; default worker settings are unchanged. Six fresh-server
suites in stock/LLVM/LLVM/stock/stock/LLVM order; each query gets one cache drop and
three executions, as in the repository scripts. Plans are collected afterward.

Generated coverage remains28/43. Median of each process's minimum of runs2/3 gives
21 generated queries at least1.5x faster;20 survive comparing the fastest stock
process with the slowest LLVM process. Q40 is the boundary case. The22-query target
is not established. ClickBench's10ms-adjusted geometric score at1ms precision is
1.455x warm and1.040x first execution. Summed per-query warm medians fall22.321s to
11.852s; the largest saving, Q32, contributes17.8% of the total. These results
include all accumulated prototype changes; they are not gains from the helper alone.

Representative stock/LLVM warm milliseconds: grouped DISTINCT Q8 812.6/472.0,
Q13 531.0/406.4; composite grouping Q16 1493.5/627.1; wide grouping Q32
3115.7/1252.3; VARCHAR grouping Q33 2095.9/819.3; computed keys Q35 671.4/185.9;
conditional-key OFFSET Q39 77.8/61.8. Q1/Q7 andQ13/Q36-Q39 do not reach1.5x.
Unsupported Q2 regresses7.68->8.77ms andQ20 326.3->348.0ms in these process medians;
no cause is established. Earlier EC2 Q27/Q28 regressions do not reproduce here:
284.9/283.5ms and2104.4/2062.6ms. This does not invalidate those older host results.
Summed per-query first-compilation medians remain160ms stock versus3506ms LLVM.

All387 LLVM responses validate against stock: exact non-floating values, up to
8ULP for parallel floating reductions, SQL-unspecified tie/order differences
checked separately. Every differing selected group inQ17/Q31/Q32/Q41 was
independently re-aggregated by clean stock; ordered count ranks match. Native
counters confirm generated work, including sorted OFFSET. Warm requests report
zero major faults; total process reads across the258 warm requests per mode are
1.27MB stock/3.37MB LLVM. Host-wide I/O PSI remains elevated and unexplained,
so first-run results retain that environmental qualification. No EC2 claim.

Returning to execution cost identified a general scheduling gap: Q13's finalized
DISTINCT reader is held behind the Java aggregate's mutation-safety gate. Temporary
parallel/serial/parallel runs reuse existing Top-K shard jobs: phase49.6->9.6ms,
whole query409.5->368.0/370.4ms, exact results. Scan259-261ms and merge92-93ms are
unchanged. This diagnostic is not part of the suite above or integrated code;
TODO.md records the explicit reader/fallback safety contract and500M-row checks
needed before promotion. No new toggles or executors were added.

Build and diff checks pass. Final emitter cleanup preserves optimized LLVM IR
byte for byte. Raw comparison/validation and diagnostic evidence remain temporary
under /tmp/query-current-clickbench; the repository keeps this breakthrough log.

## 2026-10-27: separate finalized read safety and enable DISTINCT Top-K workers

The gap belongs to the aggregate's read contract, not the LLVM backend.
`GroupByFunction.isReadThreadSafe()` defaults to the existing stronger safety
requirement. LONG DISTINCT opts in because its finalized getter only reads the
count slot; its mutation thread safety remains false. Primary and shared Top-K
cursor paths consult this after build/merge drains. Existing shard jobs, acquired
slots, lists and failure/drain handling are reused. Java getter fallback is safe
under the same contract. No new configuration toggle, algorithm or executor.

Phase CPU accounting confirms the scheduling mechanism: serial collection uses
57.45ms process CPU across50.08ms elapsed, about14% of eight-core capacity. Existing
parallel collection uses74.88ms across9.56ms, about98%. Total CPU increases while
wall time falls; this is not an instruction-count or memory-stall improvement.
The scoped USE/hardware study passes. Hot diagnostic requests have zero reads and
major faults; the unrelated host-wide I/O PSI remains elevated and unexplained.

A focused SQL check fails on the old worker-list admission and passes after the
contract change. Eighteen cases pass across LLVM/AsmJit/C and both worker modes,
covering numeric/Distinct Top-K, both orders/capacities, generated OFFSET, forced
ordinary getter fallback, cancellation/draining and reopen. Core Maven compilation
and diff checks pass. No generated native kernel changes were needed.

Three fixtures contain500M rows each. Uniform and90%-hot-key fixtures each produce
8,388,608 groups; the selective control produces16. The production parallel
threshold remains5,000,000 groups. A temporary serial control raises that threshold
only for alternating measurements; the candidate uses the default. Whole-query
medians after two warmups, three samples per arm, eight execution slots:

| Shape / ordering | Serial | Parallel |
| --- | ---: | ---: |
| Uniform, DESC K10 | 2444.4ms | 2391.6ms |
| Uniform, ASC K1024 | 2463.3ms | 2485.2ms |
| Skewed, DESC K10 | 1159.9ms | 1119.1ms |
| Skewed, ASC K1024 | 1151.7ms | 1107.0ms |
| Selective, DESC K10 | 69.01ms | 69.20ms |
| Selective, ASC K1024 | 69.53ms | 69.14ms |

Every selected aggregate and rank multiset matches an independent ordinary SQL
oracle; native group counts match, and only eligible cases perform worker-list
merge. Timing includes build/output/cleanup, with zero measured reads/major faults.
Uniform large-K is neutral within overlapping samples; this is not a general
large-query breakthrough. The small control retains serial collection. An initial
1M-group fixture was below the real5M threshold and was not used to claim a win.

Actual Q13, unchanged eight-core affinity and server defaults: three fresh complete
LLVM suites, plus three clean-stock and three current-Java runs of the identical
Q0-Q13 repository-protocol prefix. Warm process-minimum medians: clean stock546.24ms,
current Java505.14ms, LLVM366.04ms. LLVM was406.35ms in the preceding three complete
suites, so the integrated change saves9.9%. Current Java includes all existing
shared-core changes; its difference from clean stock is not an isolated codegen
claim. The fresh LLVM/Java ratio is1.38x, LLVM/stock1.49x. Q8/Q12 remain controls;
Q13 native counters now show worker-list merging of all6,019,102 groups.

All387 refreshed LLVM responses validate, with SQL-unspecified ties/unordered
selections independently checked by clean-stock aggregation and up to8ULP allowed
for parallel floating reductions. Against the retained three-pass full-suite stock
baseline, the score is1.465x warm/1.056x first-run, with21/43 median major wins and20
across all process ranges. Only the Q0-Q13 stock prefix was rerun this turn; this is
not a new full stock suite. First-run numbers retain the host I/O qualification.
Small candidate timing increases remain visible: Q7 4.98->5.21ms, Q36 22.12->22.91ms,
Q41 9.79->10.21ms, Q42 4.88->5.06ms; these use no newly admitted DISTINCT Top-K and
no causal regression is established. The22-query target remains unmet.

Evidence and temporary controls: /tmp/query-read-safety. Next work returns to
scan/merge cost; Q13 still spends roughly260/92ms there. Avoid threshold tuning
just to cross a benchmark score boundary.


## 2026-10-27 — compiler cleanup during the profiling pause

Fixed generated-code ownership and compilation accounting; consolidated aggregate
lifecycles; shared the map-view ABI; split planning/execution contracts; removed
the forwarding slot adapter, 13 obsolete switches and the superseded SQL adapter.
35 focused checks pass and 58 captured LLVM kernels are unchanged. Details and
remaining experimental controls: [cleanup plan](CLEANUP_PLAN.md). No new performance
claim: profiling is paused and the interrupted full ClickBench run is excluded.


## 2026-10-27 — fused filter and aggregation experiment

Resumed controlled runs. Added an explicitly selected compiler experiment that
passes predicate masks into existing map-prefetch/update stages. On 500M rows and
eight cores on one CCD, 50% acceptance improves 175.95 -> 148.20 ms; cycles/row
fall 17.7% and controller traffic falls 10.38 -> 8.56 GB/query. The important finding
is cache pressure: live row IDs occupy about 32 MB against a 32 MiB LLC. With
125K-row frames, the materialized arm needs only 4 MB of live row IDs and the gap shrinks to
154.75 / 150.18 ms, with nearly equal traffic. Splitting eight cores across both
CCDs likewise removes most of the gap. DRAM buses are only 54-56% busy; the
streaming roof alone was insufficient to diagnose saturation. IBS points to SUM
and key input loads as the remaining long-latency accesses, not the small map.
Rejected a counter rewrite that lowered stores but regressed sparse execution.
Three focused tests pass; default kernels remain unchanged. No new production
toggle or default policy. Details and evidence: [fusion experiment](FILTER_FUSION_EXPERIMENT.md).
The lesson is to bound intermediate working sets; fusion's benefit depends on
whether materialization fits cache. This is not a whole-engine breakthrough.

## 2026-10-27 — E0: representation matters more than instruction count

Fresh 500M-row experiment, eight physical cores on one Ryzen 9950X CCD, LLVM 21,
JDK 25. Query: LONG key plus `count(), count(v), sum(v), sum(w), sum(s)`, with
LONG/INT/SHORT inputs, nulls and 8,388,608 dispersed groups. Consume every result;
include output allocation, build, merge and cleanup. Clean stock is `8da4e347f9`,
candidate `4cc95d1e96`; same server defaults and worker budget. No canonical DB
changes. Three warmups followed by 3–5 measurements per fresh process.

| Path | Median complete time |
| --- | ---: |
| Clean stock | 11,286 ms |
| Candidate Java | 11,194 ms |
| Current full LLVM, two processes | 2,557 / 2,522 ms |
| Compact input partitions, unchanged LLVM emitter | 1,768 ms |

This is a narrow additional ~1.43x over current LLVM, not a new ClickBench score
or the planned additional 2x. Exact key sets and all 41.9M aggregate values match
the clean-stock oracle on every run. The separate matched handwritten-C / LLVM
probe-update control is 5,399 / 5,414ms: no material code-quality gap at that
boundary. It uses Java growth/merge in both arms, so it is not a full native
algorithm comparison or proof of an optimal execution floor.

Generated-PC CPU samples attribute 28% to local-map updates, 35% to partition
flush, 28% to partition merge and 3% to filtering. The bounded map creates 500M
partial groups: no local combining on this input. Its records contain an 8-byte
key plus five 8-byte states, or 24GB of payload. Merely bypassing that map lowers
instructions/row from 303 to 190 but regresses 2.56s to 2.76s. Fewer instructions
are insufficient: the new scalar schedule initially retains redundant state
operations. Pointer-aware LLVM lowering and loading inputs before initializing
state recover a 2.36s result; that compiler change is not adopted.

The successful exploratory producer stores the three original inputs once,
preserving their native widths and null sentinels, alongside the key: 24 bytes
including padding. Final merge reuses `AggregateUpdates.emit`, the existing
maps, allocator, shards and worker scheduling. It uses no known key range or
SQL identity, and works with the unchanged LLVM emitter. Cycles/row fall
195 -> 129 and instructions/row 303 -> 138. Scan/partition time falls roughly
1.55 -> 0.92s; merge 0.64 -> 0.52s; output consumption stays near 0.24s.

Fresh controller measurements are about 117 -> 76GB/query. Napkin
math: halving the payload saves 12GB of stores and 12GB of rereads; ordinary
write allocation could add another 12GB saved. This is consistent with the
observed traffic reduction, not proof of exact cache-line accounting. Counters
are system-wide: an idle sample contributes ~8GB/s and the shorter query captures
less background traffic. Eight-core streaming controls reach 61–64GB/s raw;
the queries reach roughly 43–47GB/s. DRAM saturation is not established.
Store-queue dispatch stalls remain: 63.6 -> 55.6 cycles/input row, while their
fraction rises from 33% to 44% as other work falls. The remaining limiting
resource needs phase-specific attribution. Failed load/store event groups
reported `not counted` and are excluded; quoted PMU groups ran 100% of enabled
time. Query windows have no GC, major faults or process read I/O; global I/O
activity and desktop interference qualify close timing comparisons.

**Negative control:** another 500M-row fixture with only 1,024 groups takes
474ms on current LLVM and 1,151ms with compact partitions. Local combining wins
there; forcing raw input partitions creates unnecessary work. No execution
default or configuration toggle was added. The next E1 experiment must select
using observed combining effectiveness and record widths, preserve both record
formats across switching, and test changing distributions and lifecycle failures.
The high-cardinality result alone does not justify automatic admission.

Temporary harness, isolated fixtures, exact oracle, objects and raw profiles:
`/tmp/query-compiler-e0-20261027`. The winning exploratory source is retained as
`compact-partitions.patch` there; it applies to the current sources and uses the
unchanged emitter. It is not a production patch: failure semantics, filter and
type coverage and strategy admission remain to be checked. No results archive
or experiment framework was added to the repository.

## 2026-10-27 — adaptive compact partitions retain local combining

Implemented the selection mechanism in the experimental compiler, with no new
toggle or core scheduler/allocator. At the existing full-map exit, compare
`groups * stateRecordBytes` with `acceptedRows * inputRecordBytes`. When compact
inputs are smaller, flush the completed state and partition the rest of the
frame as inputs. The next frame resumes local aggregation. A fixed-prefix
sampler was implemented and rejected: a repeated-key prefix in every frame hid
its high-cardinality remainder. The full-map decision passes that counterexample
and avoids an extra sampling call when the map never fills.

The two formats use separate chains of the existing pages, owned by the same
slot allocator. Shared append code commits each record after its payload is
written. Existing shard owners merge both chains, and query reset clears both
before allocator reuse. Separate chains avoid stranding a nearly empty 64KiB
page in every shard whenever the mode changes. The original generated map IR is
identical in the measured baseline and candidate.

Fresh paired runs, same eight-core affinity and five measured executions after
three warmups, each scanning 500M rows and consuming the complete result:

| Distribution | Previous LLVM median | Adaptive median | Compact input rows/query |
| --- | ---: | ---: | ---: |
| 8,388,608 dispersed groups | 2,553ms | 1,752ms | 498,976,000 |
| 1,024 groups | 492ms | 477ms | 0 |
| Small/large domain alternating every 10M rows | 1,675ms | 1,319ms | ~249,310,000 |

Reversing the small-domain process order gives 480 / 479ms: retain local combining,
do not claim a small-domain speedup. Exact keys and all aggregate values match
clean-stock oracles on every run, including a fresh stock oracle for the changing
fixture. This is synthetic mechanism coverage, not a new ClickBench suite result.

Large-domain cycles/input row fall 193 -> 128; instructions 303 -> 138. The changing
case falls 117 -> 86 cycles and 220 -> 137 instructions. Separate controller runs
measure 118.3 -> 74.8GB/query and 75.9 -> 54.7GB/query respectively. The latter saves
about half as much traffic, matching roughly half the rows taking the compact path.
Napkin math predicts about 36GB saved for the full large-domain input if avoided
writes, rereads and ordinary write allocation each contribute 12GB. System-wide
counts also capture background traffic and other allocation/cache effects; a
fresh idle sample measures 4.46GB/s, versus ~8GB/s earlier. Do not subtract one
idle rate as if background were constant or infer DRAM saturation from these totals.
Quoted PMU groups run 100%; measured query windows have no GC, major faults or
process read I/O. Per-query USE traces accompany the counters.

Fourteen focused checks pass across LLVM/AsmJit/C and legacy/fiber workers: mixed
formats, INT/composite keys, filters, changing distributions, the misleading
prefix, cancellation after compact output, reopen, existing bounded VARCHAR/
Parquet paths, tracked allocation failure and compiled-code ownership. Refreshed
the stale local core artifact; the focused Maven run also passes all 14 with no
skips. Temporary controls,
source identities and the changing fixture are in
`/tmp/query-adaptive-partitions-20261027`; paired counter directories are
`/tmp/query-compiler-e0-20261027/e1{b,c}-*`.

Limits: two extra generated kernels add roughly 11–24ms to preparation in the
paired processes. The byte comparison does not model partly filled pages or all
CPU costs; after entering compact mode it reconsiders only at the next frame.
Next validate aggregate-width, small-frame and short-query first-use costs before
claiming broad admission or moving on to the final-map representation experiment.

## 2026-10-28 — narrow-state controls and a rejected metadata experiment

Same 500M-row dispersed fixture, 8,388,608 groups, eight physical cores on one
9950X CCD. Query: `select k,count(),sum(v) from input where k>=0 group by k`.
The narrower aggregate naturally selects Unordered8Map with local capacity 4,096;
retain that default in both arms. Every execution validates every key, COUNT and
SUM against a clean-stock oracle, including the total count of 500M.

| Control | Previous LLVM | Adaptive LLVM |
| --- | ---: | ---: |
| Default 1M-row frames, process medians in A/B/B/A order | 1,915 / 1,891ms | 1,379 / 1,382ms |
| Matched 125K-row frames, separate diagnostic runs | 1,733ms | 1,204ms |
| Filter rejects all 500M rows, warm execution | 69.8ms | 69.4ms |

Five samples after three warmups per main process. Clean stock, same configuration
and affinity, measures 4,756ms (4,729–4,804ms, five samples after three warmups),
about 3.4x the adaptive latency. Stock is the clean `8da4e347f9` checkout; this is
synthetic mechanism evidence, not a new ClickBench coverage or suite result.

At the default frame size, cycles/input row fall 132–134 -> 92 and instructions
253 -> 102. Scan/partition time falls roughly 1.13 -> 0.68s; merge changes about
0.47 -> 0.42s. Separate intrusive allocation diagnostics verify 500M state records
become 2,048,000 states plus 497,952,000 compact inputs. Payload falls from 12.0GB
to 8.016GB; actual compact page allocation is 8.120GB. Thus page fragmentation
does not erase the narrower-record saving here. Diagnostic traversal timings are
excluded from all performance comparisons.

Napkin math: 8 fewer bytes per compact row saves about 4GB of payload, 8GB including
the reread, and potentially 12GB with ordinary write allocation. Separate
system-wide UMC passes observe 68.1 -> 53.9GB/query. Background traffic, allocation
and cache effects prevent attributing that entire 14.2GB difference to RFOs.
With 125K-row frames, controller traffic falls to 53.9 / 41.5GB/query respectively,
even though more repeated local-map sampling creates fewer compact inputs. Eight
1M-row selection buffers occupy 64MB; at 125K rows they occupy 8MB, on a CCD with
32MB LLC. This supports testing bounded intermediates, not changing the product
frame default or claiming a precise cache-miss attribution from traffic alone.

Cold empty-filter control: prepare 193.5 -> 203.0ms, first execution 80.42 / 80.41ms.
Both actually test all 500M rows and select none. Two extra kernels have a visible
cost without benefit in this case. These are one fresh process per arm and separate
preparation/execution phases, not complete service-request latency.

Generated-object attribution puts about 42% of cycle samples in compact partition
production, 37% in compact merge and 13% in vector filtering. Load samples expose
the producer's dependent tail-pointer -> page-count chain. An isolated experiment
moved active counts into a 2KiB slot-local array and published page headers only
at page/frame completion, preserving records, hashing, merge and allocation.
It passes every 500M-row oracle comparison but does not improve execution:
controls 1,387 / 1,395ms, candidate 1,395 / 1,389ms; scan phases also overlap.
Disassembly confirms the changed accesses. Separate PMU passes show load-queue
stall cycles/input row falling 4.59 -> 0.22 while store-queue stalls rise
23.71 -> 30.00; total cycles stay about 92–93. Removing that dependency does not
remove the throughput limit. Reject the change; it is not in repository sources.
Sampled load-miss weights are not fractions of query wall time.

All quoted PMU groups ran at 100%. Query windows report no GC, major faults or
process storage reads. Host I/O pressure and desktop activity remain present;
do not interpret sub-percent differences as wins. DRAM saturation is not proven.
The existing 14 focused lifecycle/backend checks remain the correctness gate for
the retained implementation; no runtime source changed in this control study.
Temporary harness, source counterfactuals, exact objects and raw measurements:
`/tmp/query-narrow-partitions-20261028`. Next: E3's bounded filter/consumer boundary,
keeping frame scheduling, adaptive record selection and final maps fixed.

## 2026-10-28 — bound filter intermediates inside existing frames

Retained a 16K-row filter/aggregate stage for eligible bounded fixed-width keys
and numeric predicates. The core filtered-update hook lends its existing selection
buffer; generated selection retains absolute frame row IDs. No new pool,
scheduler, owner, configuration switch or frame-size change. Compact-input mode
and observed combining span all stages within a frame, then resample on the next
frame. Stage continuation is local to the call. Existing fused experiments and
unsupported frame/predicate paths retain their contracts.

Final implementation, 500M dispersed rows, COUNT/SUM, same eight physical cores:
two fresh processes per arm in A/B/B/A order, five samples after three warmups.
Process medians are 1,404 / 1,378ms before and 1,158 / 1,181ms after. Cycles/input
row fall 92–93 -> 75; instructions remain about 102. Exact stock-oracle comparison
passes on every execution. The compact-row count remains exactly 497,952,000:
this experiment changes the intermediate boundary, not the aggregate algorithm.

Separate PMU passes support the mechanism: system-wide controller traffic falls
54.3 -> 38.7GB/query. Eight complete 1M-row selection buffers touch 64MB, versus
1MB for eight 16K stages, on a 32MB-LLC CCD. At full selection the IDs alone imply
8GB of logical writes/rereads; ordinary write allocation can add 4GB, and reusing
the just-filtered key column can avoid another 4GB read. That is a useful scale
for the observed 15.6GB reduction, not exact attribution of system-wide traffic.
Same-period cycle sampling drops from 17.4K -> 4.8K samples in the filter and
56.2K -> 44.0K in partition production; merge stays 49.1K / 49.2K. IBS attributes
less exposed latency to filter and producer input loads. Load-queue stalls fall
4.59 -> 1.12 cycles/input row; store-queue stalls fall 23.71 -> 20.50. PMU groups
run at 100%, measured windows have no GC, major faults or storage reads, and
memory PSI remains zero. Background activity still qualifies small differences.

Controls, also 500M rows with full stock-oracle checks:

| Workload | Whole-frame selection | Bounded stages |
| --- | ---: | ---: |
| 1,024 groups, 0.94% selected | 36.8–38.6ms | 37.1–37.6ms |
| 1,024 groups, 47.06% selected | 138–139ms | 123–124ms |
| 1,024 groups, 94.11% selected | 314–315ms | 214ms |
| Empty filter | 70.25ms | 70.35ms |
| Wide aggregate, 8.39M groups | 1,720ms | 1,549ms |
| Wide aggregate, alternating 1,024 / 8.39M domains | 1,262ms | 1,094ms |

Small-domain entries use two processes per arm; wide/changing controls use one
per arm. These controls were collected on the initial implementation; the final
integration removes repeated frame setup and uses call-local continuation state.
A 64K-stage control reaches 1,190ms and offers no advantage over the smaller stage.
Twenty-three focused checks pass: range coordinates, odd tails, empty stages,
nullable predicates, INT/composite keys, cancellation, reuse, allocation failure,
existing fused execution and code ownership, across C/AsmJit/LLVM and both worker
modes where parameterized. No broad test expansion or result archive was added.
Evidence and exploratory variants: `/tmp/query-bounded-stages-20261028`.

The remaining narrow-query merge is about 0.42s and did not improve. Further
large gains require reducing state/partition work or improving its ownership and
access schedule; repeatedly tuning the removed selection boundary will not do it.

### Actual-data transfer check: bounded/adaptive pipeline, 2026-10-28

Actual ClickBench data: 99,997,497 rows, eight physical cores on one CCD,
identical server execution settings. Each isolated server gets one cache drop
and three requests, following the repository protocol for one query at a time.
These are query subsets, not complete-suite passes. Clean stock is the unchanged
`8da4e347f9` checkout. The earlier LLVM control predates adaptive input partitions.

Q15 (UserID/COUNT/top-10), two process blocks per arm: warm fastest times are
540–571ms stock, 245–257ms earlier LLVM, 218–219ms current. Only about 36M rows
choose compact records: actual data has much more local reuse than the dispersed
synthetic fixture. A separate balanced PMU pass gives 236–237 / 98 / 83–84
cycles per input row, respectively, with 100% counter running time. First-use
LLVM remains about 0.55s; the warm improvement does not remove compilation and
cold-read costs. Results match stock exactly, including generated top-k counters.

Q32 (two keys, COUNT/SUM/AVG/top-10): warm fastest times are 2,532–2,546ms stock,
1,363–1,374ms earlier LLVM, 1,210–1,223ms current. There are 99,997,493 groups;
99.63M rows choose compact records. ORDER BY COUNT has legitimate ties, even
between stock runs. Validation compares the complete ranking and independently
recomputes every returned group's aggregates with stock SQL; all pass.

Phase/profile evidence puts the next Q32 opportunity in final-map construction:
roughly 0.85s merging versus 0.13–0.15s scanning/partitioning, with 61% of cycle
samples in compact-record merge. That loop probes serially, unlike the existing
staged scan/ordinary-map merge paths. Its directory load has appreciable IBS
latency; the source and optimized object agree. A staged-merge counterfactual is
under evaluation, not yet integrated. Temporary evidence and commands live in
`/tmp/query-moonshot-actual-20261028`; no benchmark-specific engine rule was added.

### Retained: stage compact-record merge probes, 2026-10-28

Compact records now use the existing probe prepare/finish operations and the
existing slot scratch. Independent directory lookups precede dependent entry
accesses; consumption revalidates candidates after earlier rows in the batch.
A capacity exit resumes at the unconsumed row and rebuilds every candidate after
growth. The existing prefetch batch size controls this path; no setting was added.

Actual composite-key COUNT/SUM/AVG/top-10, balanced two-process blocks per arm:
warm Q32 is 1,217–1,253ms before and 1,094–1,113ms after in the final integration.
An earlier matched PMU pass gives 351 -> 261–264 cycles/input row while instructions
rise 194–196 -> 244–246. Merge falls about 0.78 -> 0.65s; scan stays 0.13–0.14s.
IBS directory demand loads change from mean 142 / p90 568 reported miss cycles
to roughly 80–92 mean at the main prepared/fallback lookup sites. Store-queue
dispatch stalls fall 40.22 -> 35.73 cycles/input row; load-queue stalls remain
small (0.74 -> 1.13). The extra instructions buy overlap, not less logical work.
The initial four-event PMU group failed scheduling and is excluded; replacement
groups run at 100%. Exact independent stock checks pass, including legal top-k ties.

The 500M-row, 8.39M-group single-key COUNT/SUM control is neutral: process medians
1,188 / 1,174ms before and 1,177 / 1,166ms after, with every output checked against
the stock oracle. Twelve focused parameterized tests pass across C/AsmJit/LLVM
and both worker modes, exercising composite keys, nulls, growth, cancellation,
reopen and partition allocation failures. The USE/hardware study validates at
`/tmp/query-moonshot-actual-20261028/merge-study.json`.

Limits: native state allocation still incurs about 1.75M minor faults per Q32;
both arms pay this cost. Warm storage reads/major faults are zero. Server workers
and the owner share the same eight CPUs in both arms; scheduler competition and
background activity remain. First-use performance is not materially established
as improved. This is a family-level improvement, not the full moonshot target.

Rejected separate experiment: move initialization and the first aggregate update
into the new-entry branch, following Java's computeFirst/computeNext split.
Instructions fall only about 1%, with overlapping 1.07–1.10s query times. Leave it
out; inspect address/alias information before adding another aggregate lifecycle.

Two further isolated counterfactuals, 2026-10-28:

- Constant-offset LLVM GEP lowering changes generated instructions but leaves
  Q32 at about 1.09–1.11s and 263 cycles/input row. It adds no measured benefit;
  retain the existing address semantics. No alias or alignment promises were added.
- Late aggregate payload: compute COUNT/top-10 first, then rescan for SUM/AVG of
  selected keys. A native coarse key filter followed by exact composite-key lookup
  reaches 818–838ms including both requests and 119–121ms of fresh second-query
  compilation, versus 1,090–1,104ms for the complete current pipeline. The second
  scan itself takes 15–16ms; only ten AVG states are finalized. All output/ranking
  checks pass. An initial composite-predicate variant used an ordinary filter and
  spent roughly 285ms rescanning, erasing most of the win. Native coverage counters
  distinguish these cases. This is an immutable-data, two-query diagnostic, not an
  integrated snapshot-safe rewrite or a default optimization.

The next architectural experiment fuses complete partition aggregation with
local top-k, copies surviving states into ordinary output maps, and reuses each
worker's working map. Disjoint hash partitions make local top-k pruning exact
without skew assumptions or an extra scan. It must preserve allocation tracking,
failure cleanup, tie semantics and row-address lifetime before retention.

### Retained: fuse complete partition aggregation with Top-K, 2026-10-28

Small, statically prepared Top-K can now consume each complete fixed-width hash
partition before its merge workspace is reused. The existing worker fragment is
the workspace; generated code copies selected raw states into ordinary output
maps, then finalizes those states once. Output row addresses remain valid. The
same disjoint partitions, map algorithms, allocator, worker slots and cancellation
contracts apply. No skew assumption, extra scan or setting was introduced. Large
or unknown K, HAVING, variable/allocating states and ranking fields requiring
finalization keep their existing paths.

Final balanced Q32 integration: 1,089–1,099ms before -> 664–671ms after; clean
stock 2,603–2,814ms. First requests are 1,796–1,806 -> 1,080–1,103ms, including
compilation and the repository cache drop. All 99,997,497 rows still reach the
aggregate, but only 2,560 rather than 99,997,493 final groups survive. Independent
stock recomputation verifies selected payloads and legal tied rankings.

The win is mainly state lifetime and native allocation, not cheaper arithmetic:
all-mode cycles fall 381–382 -> 286–287 per input row, while user CPU rises slightly.
Kernel CPU falls about 2.3 -> 0.31s/query and minor faults 1.7M -> 0.13–0.17M.
An earlier matched controller pass shows 48.0 -> 35.7GB/query; kernel samples in
anonymous faults, page initialization and release shrink sharply. Warm storage
reads and major faults are zero. The working map capacity is reused within a query;
this is not a cross-query result or memory cache.

Non-ClickBench controls use 500M rows and exact stock results. Unique composite
groups improve from process medians 6,245/6,267 -> 4,410/4,396ms; all-mode cycles
487 -> 374–375/row. At 8.39M repeated groups the gain is only 2–3%
(1,280/1,281 -> 1,238/1,254ms). At 1,024 groups the unpartitioned path is neutral
(350/352 -> 352/348ms). Six focused parameterized tests exercise keys, nulls,
ordering, fallback, cancellation after copying and reopening; 18 existing
Top-K/DISTINCT/bounded-state checks also pass. The hardware study validates at
`/tmp/query-moonshot-actual-20261028/partition-topk-study.json`; final integration
evidence uses `retained-topk-*` there. A fresh matched full suite is next; these
family-level results do not establish the 22-query performance target.

Fresh complete suite, same eight physical cores and identical server configuration:
four process passes in stock/LLVM/LLVM/stock order, one cache drop and three
requests per query. Median process-minimum warm times sum to 22.654s stock and
11.140s LLVM. Twenty-two queries reach 1.5x by medians, but only 21 clear it across
both process ranges (Q40 is borderline); this does not establish 22 robust wins.
Q32 supplies 21.6% of the summed saving, not a majority. First-request sums are
33.966/23.244s, including compilation and the protocol's cache state. Host-wide
I/O pressure still limits interpretation of those first requests.

All 516 responses validate: exact non-floating values, at most 8 ULP for parallel
floating reductions, matching ordered rank values and independent clean-stock
re-aggregation for differing tied/unordered outputs. Aggregate/filter/sort counters
confirm the changed paths; generated shape coverage remains at the prior 28.
Regressions above 3% are Q0, Q1, Q7, Q20, Q22 and Q26. Most are sub-millisecond
differences; Q20 is material (313 -> 365ms) despite an unchanged ordinary Java
plan, and needs a separate causal check. Q22 is 253 -> 272ms. No stock settings
were altered. Evidence: `/tmp/query-moonshot-actual-20261028/full-suite`.

Next experiment: cache-sized final aggregation, still outside the checkout.
On 500M unique composite groups, current execution moves about 245GB/query at
55GB/s. A fresh eight-core streaming calibration measures 60.4GB/s reads and
63.5GB/s mixed controller traffic. Increasing global fan-out from 256 to 4,096
reduces execution 4.44 -> 2.95s, traffic to 151GB, and all-mode cycles 377 -> 250
per input row. Merge drops 3.26 -> 1.62s while scanning grows 0.99 -> 1.20s.
But 8.39M repeated groups regress 4–7%; reject a blanket fan-out increase.

Following the runtime adaptation in [Cache-Efficient Aggregation: Hashing Is
Sorting](https://d-nb.info/1259890538/34), the isolated follow-up subdivides only
after actual merge state exceeds an experimental cache budget. Deciding after a
whole worker contribution is too late and leaves execution unchanged at 4.45s.
Deciding at existing page boundaries reaches 3.52s with 16 subpartitions and
2.57s with 64, retaining exact stock results. The 16-way variant reaches
345–357ms on actual Q32 versus 657–680ms, but regresses Q31 about 10%: it partitions
too much for the work left. A remaining-record-based fan-out is under evaluation.
These are diagnostic results, not a retained policy or new user settings.

The remaining-record policy fails a held-out 500M-row, 16.77M-group input:
1.336/1.342s current -> 1.711/1.710s candidate. Every input row runs generated
code and results match clean stock. Unread rows are not a distinct-group estimate;
repartitioning repeated keys pays another materialization pass without enough
cache benefit. Reject that policy despite its 4.42 -> 2.57s unique-group result.
The initial modulo-expression control silently used Java and is excluded; the
replacement materializes the key and the harness now asserts native coverage.

A follow-up estimates each partition's group count during existing partition
writes, using sampled keys and QuestDB's existing dense HLL estimator. This is
execution metadata only; exact aggregation remains unchanged. The first version
adds little measured time but overestimates dense scalar domains by 44–54%:
QuestDB's cheap map hash does not supply independent enough register/rank bits
under this conditioning. A Murmur finalizer on sampled hashes removes the bias
in a focused generated-code check, including comparison of every register with
QuestDB's HLL. The [HLL++ paper](https://stefanheule.com/papers/edbt13-hyperloglog.pdf)
explains the uniform-hash requirement and mergeable register representation.
Sketch-guided subdivision remains an isolated experiment pending accuracy,
held-out controls, hardware counters and a portable cache-budget policy.


Retained sketch-guided cache subdivision after the hardware gate. Initial
partitioning and the extra pass now share page reservation; all storage and work
remain under existing query allocators/worker slots. A startup cache/affinity
snapshot supplies the per-worker budget, with the existing path on missing
metadata. No setting or query recognition was added. Exact groups are completed
before selection, raw AVG states are copied, and only destination maps finalize.

Balanced hardware evidence on 500M unique composite groups: L3 misses
4.815/4.834 -> 2.611/2.586 per row; controller traffic 244.3 -> 120.2GB/query;
compact-merge demand-load miss cost at the same instruction 194 -> 93 reported
cycles. Extra instructions buy locality. Fresh streaming ceilings are 60.4/63.5GB/s;
this establishes cache-capacity pressure, not a universal bandwidth-saturation
claim. Warm paging and process reads are zero; host I/O PSI still qualifies cold
results. Gate: `/tmp/query-moonshot-actual-20261028/cache-study.json`.

Integrated actual Q32: 653-677 -> 328-334ms, clean stock 2,618-2,699ms by process
warm minima; first requests 1,082-1,118 -> 776-793ms including compilation.
Q31 stays roughly neutral at 100-102 versus 98-101ms. The separate 500M/67.1M-group
holdout improves 2.25-2.28 -> 1.73-1.75s; four-core unique groups 5.36 -> 3.76-3.78s.
Six helper cases verify unbiased dense-key sketches and recycled page chains;
twelve SQL cases cover state copies, cancellation/reopen, ordering and fallback
across all three backends and both worker modes. The first SQL method selector
matched zero parameterized cases; only the corrected 12-case run is counted.
Final integrated 500M controls: unique groups 4.410/4.419 -> 2.585/2.630s;
8.39M repeated groups 1.255/1.246 -> 1.256/1.251s. The fresh full suite
validates all 516 responses, including independent stock re-aggregation of tied
outputs. Warm per-query process-minimum medians sum to 22.494s stock / 10.793s
LLVM. Coverage remains 28 generated shapes, with 22 median 1.5x wins and 21
across process ranges; Q40 remains borderline. Actual Q32 is 3.096s / 0.310s.
Regressions above 3% are Q1, Q2, Q7, Q20, Q24 and Q42; only Q20 is material.
Q20 uses ordinary Java in both arms; isolated balanced runs do not reproduce a
consistent difference. Its causal explanation remains open. No stock settings
changed. Evidence: `/tmp/query-moonshot-actual-20261028/cache-full-suite`.

## 2026-10-28: generated bounded UTF-8 substring search

The standalone LLVM compiler was behind the legacy serializer, so unsupported
LIKE predicates never reached it. Resolved compilation now runs first. The exact
existing constant, case-sensitive 1–8-byte substring function exposes its literal;
shared binding/lowering serves selection, count-only and aggregate residuals.
Borrowed frame metadata distinguishes native split values from Parquet slices.
Existing callers, frame ownership, Java fallback and cancellation remain in charge.

Actual 99,997,497-row substring/count: integrated warm minima 192–193ms versus
326–337ms stock. Every input row is accounted for natively. Baseline is about
344 instructions and 2.7 branch misses/input row; generated code about 91 and 1.1.
AMD dispatch/retirement counters show 24–26% bad-speculation slots. Controller
traffic changes only 11.55 -> 11.24GB/query while throughput rises 35 -> 57–58GB/s,
near the measured 60.4GB/s read ceiling. This supports the CPU-work explanation
for the original loop and a memory-throughput limit on further gains here.
First-process SQL compilation is 120ms versus 7ms stock; after prior service work
it is about 28ms versus 4ms. These are SQL compilation timings, not isolated LLVM
optimization time. Gate: `/tmp/query-moonshot-actual-20261028/string-study.json`.

Two policies failed independent 500M-row controls. Blind first/last-byte probes
regressed a repetitive pattern to 1.15s from 0.83–0.98s; selecting a different
literal byte gives 0.506s. Keeping SWAR for all-equal literals also regressed,
1.057s versus 0.781–0.787s. Exact contiguous-run masks remove candidate enumeration
and reach 0.517s. Retain neither failed policy. Other integrated controls range
from neutral at ~0.515s for simple/Unicode cases to 1.21 -> 0.515s for an eight-byte
literal. No query names, data-specific byte-frequency tables, or settings enter
selection. Small per-frame descriptor allocation is included in timings.

Twenty-four SQL cases pass across three backends and both worker modes, covering
selection/count, aggregation, NULLs, Parquet, reopen and failed compilation cleanup.
The final repeated-literal change passes 67,680 protected-page checks across the
three backends. A full protocol run was interrupted after the repeated-literal
regression; it is excluded. Final balanced 500M controls give 777/775 -> 516/513ms
for a seven-byte repeated literal and neutral 519 -> 516ms for an independent
eight-byte repeated literal. Every generated execution accounts for 500M inputs.

The fresh full protocol validates all 516 responses. Process-minimum warm medians
sum to 22.673s stock / 10.393s LLVM; 24/43 queries clear 1.5x even comparing the
fastest stock process with the slowest LLVM process. The largest saving is Q32,
23% of total savings, so the suite result is not dominated by one case. Q20 is
322–403 -> 189–190ms; Q21 is 317–335 -> 188–189ms. Only Q0/Q26 regress over 3%,
by 0.02/0.12ms. Stock configuration is unchanged. First-run server sums are
34.127/22.708s; the HTTP execution timer starts before compilation, so its
`compiler` field must not be added again. Physical I/O and host activity still
qualify first-run comparisons. LLVM SQL compilation totals 3.942s across the suite.

A conservative coverage audit corrects the historical 28 to 27 complete shapes:
filtered COUNT's frame counts are still summed by Java calculateSize. Q20 similarly
keeps that reduction; Q21 generates filtering but retains Java MIN(VARCHAR), map
updates/merge and Top-K; Q23 retains Java early LIMIT. These new wins therefore do
not increase complete generated coverage. There are 21 robust 1.5x winners within
the 27 complete shapes, still short of 22. This distinction prevents native filter
row counters from being mistaken for whole-query compilation.
Evidence: `/tmp/query-moonshot-actual-20261028/string-final-suite`.

Next: instrument compilation before changing it. First-use SQL compilation is
roughly 230–247ms for grouped DISTINCT, versus 475–498ms warm execution for two
substantial cases. Separate IR construction, SSA/text, native initialization,
parse/verify, optimization, emission/linking and wait time. A phase profile must
precede a tiering or cache design; repeated code and unused alternatives are
testable hypotheses, not established causes.

## 2026-10-28: measure compilation and remove duplicated merge lowering

Fresh warmed Q11 preparation spends ~114ms in LLVM optimization, ~110ms in
emission/linking, 4.5ms parsing/verifying and 10ms constructing SSA/text. Mutex wait
is 0.012ms in this serial experiment. Native compiler CPU and wall time agree.
Replaying its largest merge module gives ~60ms, 334M cycles and 746M instructions
per compilation. Zen 5 counters show 54% frontend underdelivery, including 36%
frontend latency; instruction-cache misses corroborate fetch pressure. This is a
compiler-phase diagnosis, not a query-execution bottleneck or proof that one LLVM
pass dominates. Five warmups precede each 120-compilation counter window.

Retained one small representation change: choose the existing size-directed set
merge operands before emitting its probe/growth loops. The algorithm and state
ownership are unchanged. That module shrinks 2,172 -> 1,937 IR instructions and
391 -> 345 blocks; compile time falls 59.8–60.1 -> 52.5ms, cycles ~334 -> 292M,
instructions 746 -> 639M. Complete warmed SQL preparation falls ~244–249 ->
229–233ms. Outlining the whole serial merge with the existing helper contract
saved much less and is not retained. No cache or tiering machinery was added.

500M-row controls match clean stock: 31 medium DISTINCT sets take ~1.06s and 31
8.39M-value sets ~5.6s in both versions. One medium candidate process spiked;
alternating both compiled versions in one JVM gives 1.073/1.076s, within 0.4%.
Every execution accounts for 500M native inputs. Eighteen focused SQL cases pass
across all backends and both worker modes, including deferred unions, reopen,
allocation failure and cancellation. No new full-suite speedup is claimed from
this preparation change. Evidence and the validated hardware study:
`/tmp/query-moonshot-actual-20261028/compile-study`.

Post-integration full protocol: all 516 responses validate again. Warm sums are
22.707/10.387s stock/LLVM, 25 median and 24 robust 1.5x wins; strict generated
coverage stays 27, with 21 robust winners inside it. First-run sums are
34.119/22.562s and LLVM SQL compilation totals 3.837s. Q1/Q2/Q42 regress
1.01/0.58/0.42ms; the rest stay within 3% or improve. These small-case variations
were not consistent across the earlier suite. Stock settings remain unchanged.
Evidence: `/tmp/query-moonshot-actual-20261028/merge-final-suite`.

Fast-tier diagnostics expose a larger first-use opportunity, with opposite
outcomes by family. Fresh grouped DISTINCT requests take ~302ms with LLVM O2,
~125ms with O0 and ~93ms with AsmJit. Short-literal count takes ~225ms with O2,
~289ms with O0 and ~723ms with AsmJit. Forced worker-driven O2 promotion reaches
~215ms for the substring case but ~268ms for DISTINCT from O0: unnecessary
optimization of short phases costs more than it saves. Do not retain this policy
or make a global backend switch. The concrete next admission experiment is in
[FOUNDATIONS_PLAN.md](FOUNDATIONS_PLAN.md#e6--make-specialization-economical-on-first-use).

### Retained: native byte masks in the fast compiler, 2026-10-28

AsmJit scalarized the shared vector-mask IR. On the 100M-row substring/count
control it retired ~2,034 instructions/row, with 59% retiring, 29% frontend and
3% backend dispatch slots; generated scalar lane loops held 97.7% of CPU samples.
System memory traffic was ~17GB/s versus the measured 60.4GB/s read ceiling.

SSE2 byte equality/inequality masks and conjunctions now lower directly; unsupported
predicates retain portable lowering. Exact signedness, lane bounds and read effects
are preserved. No SQL recognition, new setting or default backend switch. Balanced
runs fall 708–718 -> 199–204ms, ~2,033 -> 264–266 instructions/row and ~294 -> 84
cycles/row. Controller traffic is 12.4 -> 11.2GB/query and throughput ~56GB/s.
Eight independent 500M-row string controls improve from 1.07–1.51s to 0.512–0.515s,
including absent, Unicode, false-candidate and repeated-byte patterns; all results
match stock and account for every input row. Warm windows have zero major faults
and disk reads. Focused vector/scan/bridge checks pass (86 cases), plus 60 SSA cases.

Fresh substring requests now take 198–202ms with ~3.7–3.8ms AsmJit preparation,
versus ~221ms with ~28.5ms LLVM preparation. Forced AsmJit→LLVM promotion takes
205–208ms: the optimized code has too little remaining execution advantage to
repay the lost compiler worker and interference. Grouped DISTINCT still favors
fast compilation (~92–104ms versus ~303–344ms O2); SIMD changes no relevant
instruction in that numeric family, and process warmup variation remains visible.
Do not retain unconditional tiering or claim an LLVM-only coverage gain. The earlier
500M forced-handoff experiment verified code lifetime and exact tier row accounting,
but saved only 2–3% first-use time. Evidence: `/tmp/query-moonshot-actual-20261028/asm-vector`
and `compile-study/filter-matrix`. The bottleneck study passes the USE/hardware gate.

### Retained: generated VARCHAR MIN/MAX, 2026-10-28

The string aggregate control spends 55% of dispatch slots in backend-memory stalls;
controller throughput is only 31GB/s against the measured 60GB/s read ceiling.
IBS attributes 54% of sampled miss weights to Java MIN construction/update, with
316–366-cycle misses. This is a general MIN/MAX/COUNT grouping control over the
actual 100M rows, not a ClickBench suite member.

Generate the existing Java algorithm over its exact GroupByUtf8Sink layout.
Winning values copy into the existing query allocator; growth uses its realloc,
and merge adopts source pointers while the factory retains their allocators.
Shared UTF-8 descriptors now serve aggregate arguments as well as keys. NULL,
empty strings, unsigned byte order, column tops, Parquet, failure and reopening
checks pass across all three native backends. The seven-argument native ABI and
reallocation IR pass focused tests; a balanced 500M-row numeric control stays
neutral at ~1.25s.

Balanced actual-data execution improves ~995–999 -> 708–715ms, with instructions
~800–811 -> 455–457/input row and cycles ~425 -> 301–302. Traffic changes only
31.7 -> 30.6GB/query; backend-memory slots fall to 44%. Removing the generated
state prefetch slows execution 7–8% despite fewer instructions. Demand IBS samples
exclude software-prefetch instructions; miss weights are not wall-time fractions.

Independent 500M-row controls match stock exactly: 8.39M dispersed groups improve
12.2 -> 4.7s. For 31 groups, a further counterfactual rejects auxiliary-prefix
decoding: bounded payload-prefix reads improve ~0.88 -> 0.59s despite 15% more
instructions, versus 1.31–1.34s stock. Backend-memory slots fall 56% -> 30%; traffic
slightly increases. IBS shows the hot aggregate state stays in L1, and the faster
version has more store-forwarding conflicts, rejecting that explanation. Retain
the simpler payload view; exact dependency-chain attribution remains incomplete.
The large-domain control is neutral to this simplification. Sharing decoded
arguments across MIN/MAX saves instructions but does not improve elapsed time;
leave it out. On the actual aggregate control, payload-prefix reads improve a
further 2–3% while retiring 6% more instructions. No new settings.

The final 500M large-domain profile moves 283 -> 238GB/query and raises controller
throughput 23 -> 51GB/s; backend-memory slots fall 82% -> 68%. At the calibrated
63.5GB/s mixed-streaming ceiling, the current traffic alone takes ~3.74s, versus
~4.7s measured. Another 2x requires less traffic, not just better instruction
selection. Native build/merge hold 59%/20% of CPU samples, while JVM upcall entry
holds 18%: allocator transition frequency is a concrete next experiment, with
less headroom than changing state representation. This attribution is a CPU
profile, not a claim that every upcall sample is allocator execution.

Actual Q21 now accounts for all 99,997,497 filter inputs, 1,038 selected aggregate
updates and 677 Top-K groups per execution; it remains ~190–191ms versus
324–328ms stock. A separate call audit confirms 27 generated merge invocations
over three executions; plan labels alone were not used as proof.

The clean full-suite repeat validates all 516 responses and records 24 robust
1.5x warm wins, summed warm times 22.635/10.502s and first-request sums
34.288/23.149s. Q0/Q2/Q26/Q42 regress by 0.012/0.367/0.099/0.210ms.
Strict generated coverage is 26/43, including the new Q21 but correcting Q4/Q5:
their outer COUNT sums shard cardinalities in Java. Only 20 robust wins meet the
complete-pipeline criterion. Q32 accounts for 23.4% of total saved time. An entire
preceding suite was excluded after external host activity contaminated one arm;
the repeat passes the recorded host CPU gate. Evidence: `extrema-clean-suite`.

A separate intrusive allocation diagnostic records roughly 134M malloc and 13M
realloc callbacks per 500M-row large-domain query, versus 0.63M cancellation polls
and no frees. This identifies the source of transition frequency; diagnostic times
are not acceptance measurements. Next test borrowing the existing allocator's
bump cursor across a native call, preserving its tracked chunk allocation and
subclass contracts. The unchanged-traffic bandwidth floor bounds likely gains;
this is a transition experiment, not a forecast of another 2x.
Evidence: `/tmp/query-moonshot-actual-20261028/extrema-study`.


### Exploratory: borrowing the allocator cursor, 2026-10-28

An isolated LLVM arm borrows FastGroupByAllocator's existing cursor/limit for one
native invocation, committing before every Java callback and on return. Tracked
chunk allocation, realloc/free and custom/subclass allocators stay in Java.
This diagnostic uses private-field access and is **not retained production code**.
The full query ABI is otherwise unchanged, isolating transition frequency.

On the same 500M-row, 8.39M-group string MIN/MAX control, malloc upcalls fall
134M -> 45K; realloc remains ~13.1M. Balanced process medians improve
4696/4698 -> 4505/4502ms. Instructions fall ~464–469 -> 373–374/input row,
but cycles fall only 410 -> 395; branch misses rise ~1.08 -> 1.12/row.
JVM upcall-entry samples fall 18% -> 2.1%, with no lost samples. Controller bytes
stay ~237–238GB/query, bandwidth rises 50.7 -> 53.0GB/s, and backend-memory
slots rise 67.4% -> 71.9%. The profile's original 18% was not an attainable
elapsed saving: the remaining memory schedule dominates after calls disappear.
The 31-group control is neutral at ~0.587s; actual grouped DISTINCT improves
roughly 342 -> 333ms, with some overlap among individual warm requests.
All compared results match and measured warm windows have zero major faults
and disk reads. Intrusive callback-count timings are excluded.

Keeping the borrowed malloc helper out of line is neutral (~4.50–4.52s), rejecting
inlining disruption as a material explanation of the limited gain. Do not add an
ABI/lifetime mechanism solely on the strength of sample percentages. Next inspect
the dependent map-entry/state loads: the ordered-map preparation loop immediately
loads aggregate pointers after prefetching their entries, whereas another map
layout already separates those stages. Test that existing schedule generally
before deciding whether the allocator optimization earns its integration cost.
Evidence: `/tmp/query-moonshot-actual-20261028/allocator-study`.


### Retained: typed string arguments in adaptive input partitions, 2026-10-28

The allocator experiment above saves only 4%; separating aggregate-pointer
prefetches into another preparation stage saves only 1.6%. Neither is retained.
The remaining 237GB/query and ~67M worker-local groups argue for less replicated
state, using the existing bounded maps and partition scheduler.

Resolved string arguments can now be materialized as a typed length/null/ASCII
header plus payload pointer. Stable frame bytes are borrowed; transient Parquet
or expression bytes use the existing allocator-backed partition blobs. The same
MIN/MAX update reads either frames or these arguments. Fixed-key string-extremum
pipelines qualify only when the compact input record is narrower than the partial
state; the observed combining ratio still selects the representation per window.
No new allocator, map, scheduler, query recognition or configuration switch.

A failed arm establishes why byte reduction alone is insufficient: compact records
without payload prefetch take 5.05–5.20s despite reducing traffic to ~170GB/query.
Demand IBS locates ~97.7% of sampled miss weight in input-partition merge, mostly
bounded input-prefix loads, with ~374-cycle misses. Prefetching nonempty string
payloads in the existing preparation stage changes balanced execution to
3.08–3.15s with roughly unchanged instruction counts. Software-prefetch PCs are
excluded from demand-load analysis; weights are not elapsed-time percentages.

The final balanced 500M-row, 8.39M-group control improves 4662/4667 -> 3119/3165ms.
Cycles fall 406–408 -> 262–263/input row; instructions 464 -> 335–339. Branch
misses rise 1.08 -> 1.15/row. Controller traffic falls 236.7 -> 153.1GB/query,
while throughput stays 50.7 -> 48.5GB/s. Backend-memory slots remain ~66%.
Demand IBS sampled misses fall 8835 -> 2190 over the same work/period and their
mean weight falls 305 -> 222 cycles. At the calibrated 63.5GB/s mixed ceiling,
153GB alone requires ~2.41s; another 2x requires fewer bytes or a different
schedule. Intrusive diagnostics count ~18.8M malloc and 1.65M realloc callbacks,
versus 134M/13M before; their timings are excluded.

The 31-group control remains locally combined and improves ~584–588 -> 558ms;
that small-case improvement has not been isolated to a single mechanism. LLVM
preparation rises ~245 -> 342–357ms, including unused partition alternatives:
first-use compilation is the next cost to remove, not work to hide from timing.
Every 500M-row result matches stock; all eight acceptance processes pass the host
gate, with no warm major faults, disk reads, GC or swapping. Eighteen focused
SQL/lifecycle cases and six partition-page cases pass across native backends,
including Parquet/transient literals, column tops, memory failure and reopening.

The full repository-protocol refresh validates all 516 responses: summed warm
times 22.455/10.398s, first-request sums 33.938/27.194s, and compiler sums
0.160/4.278s for stock/LLVM. There are 23 robust 1.5x warm wins (24 at medians),
of which 19 currently have every processing operator generated. Q0's regression
is 0.005ms; Q32 contributes 23.0% of saved time. All four host gates pass.
Evidence: `/tmp/query-moonshot-actual-20261028/string-partitions` and
`string-partitions-suite`; the bottleneck study passes the hardware/USE validator.


### Retained: generated final COUNT reduction, 2026-10-28

COUNT over async filters and grouped DISTINCT now uses the existing cursor's bulk
Counter contract to feed a bounded 256-value generated LONG reduction. Core owns
cursor/cancellation/yield and factory lifetime as before; metadata counts and LIMIT
cursors retain their current paths. No row enumeration, new worker or SQL matching.
Twelve focused cases pass across C/AsmJit/LLVM and legacy/fiber workers, covering
empty input, multiple batches, shard reduction, wrapping arithmetic and reopening.

Actual-data audits consume 180 frame partials for Q1, 177 for Q20 and 256 shard
partials each for Q4/Q5. All 144 responses match stock in balanced six-process
controls. Warm execution is neutral: Q4 ~189–195 -> 187–195ms; Q5 ~170–172 ->
171–172ms; Q20 ~193–198 -> 195–197ms. Normalized PMU counts overlap process
variation; this is a coverage correction, not a speedup claim. Preparation adds
roughly 7–10ms. All host gates pass; two Q20 windows record 8KiB process reads,
with zero major faults, so do not call every warm window entirely I/O-free.
The source/call audit raises strict coverage from 26 to 30/43; a new full-suite
refresh is still needed before claiming the combined major-speedup target.
Evidence: `/tmp/query-moonshot-actual-20261028/final-count`.


### Retained: compile optional partition kernels on demand, 2026-10-28

The 500M-row / 31-group string control compiles twelve kernels but calls only
three. Seven optional partition kernels alone cost ~98–101ms Java+native time.
A fixed replay of their exact LLVM input takes ~82ms per seven-module batch,
458M cycles and 850M instructions; frontend-unused slots are ~61%, with ~44%
of cycles having all dispatch slots unused for frontend reasons. Instruction-cache
miss events and system line fills corroborate compiler frontend pressure. Replay
CPU/wall agree; no measured major faults or swapping, and no compiler queue.
The compiler-work USE/hardware study passes; this is not a query-loop diagnosis.

Optional flush, input/state merge, selection and subdivision kernels now retain
pure program generators and compile once at first use. The existing factory
owns generated code; workers join before close. Publication is synchronized and
backend calls remain serial. No global cache, worker pool or setting. An unused
operator never generates a program. Failed compilation does not replay a row;
accounting includes later compilation and remains available after close.

Balanced 500M-row 31-group cold-process first-use totals fall ~986–993 ->
880–892ms; warm execution stays ~558–563ms with unchanged normalized instructions
and cycles. Fresh factories in warmed JVMs fall ~778–785 -> 683–690ms, including
all compilation needed for the result. The 8.39M-group case must compile five of
the seven optional kernels during execution: warm times remain ~3.1s, and fresh
factory totals overlap (~3270–3381 -> 3255–3290ms). One initial lazy process takes
3994ms versus 3860–3877ms eager; later fresh-process samples overlap or favor lazy.
Do not claim a universal cold-start win or hide deferred work in execution timing.
All sixteen host gates pass, every 500M-row result matches stock, and measured
warm windows have zero major faults, reads and GC. Twenty-four SQL/lifecycle
cases plus five ownership/publication/failure cases pass.

The clean repository-protocol refresh validates all 516 responses and establishes
30/43 complete generated pipelines, 23 robust 1.5x warm wins, and **22 satisfying
both criteria**. Warm sums: 22.533/10.437s stock/LLVM. First-request sums:
34.076/27.106s; compiler-timer sums: 0.168/3.959s, with deferred compilation
included in execution. Q2/Q7/Q24/Q25/Q42 regress by
0.264/0.709/0.039/2.607/0.180ms. Q32 contributes 22.9% of saved time.
All four host gates pass. The entire preceding refresh is excluded because
JetBrains Toolbox consumed ~14 CPU-seconds during a two-second interval of the
final stock pass. Correctness remained valid; contaminated performance did not.
Evidence: `/tmp/query-moonshot-actual-20261028/lazy-partitions` and
`lazy-clean-suite`. Next investigate active state footprint and cache residency;
the original coverage target is met, but the architectural stretch target remains.

### Rejected: aggressive subdivision for external string state, 2026-10-28

The partition budget counts inline map fields but excludes MIN/MAX sink capacity.
An intrusive audit of 8.39M groups measures 91.5 additional allocated bytes/group
for the two sinks, versus 15.9 bytes of final string payload. Eight simultaneous
partitions can exceed this host's 32MiB LLC. This is a footprint observation,
not proof that cache capacity limits execution.

Temporary builds reduced the existing cache allowance without changing maps or
algorithms. Balanced 500M-row processes: baseline 3.103/3.109s, eight-way
subdivision 3.564/3.565s, sixteen-way 3.818/3.780s. A smaller allowance that
did not trigger subdivision stayed at 3.049/3.105s. Eight-way subdivision adds
~32–37 instructions and ~42 cycles/input despite fewer branch misses. Controller
traffic rises 152.6 -> 172.1GB/query, at nearly unchanged 48.6/48.1GB/s. The extra
19.5GB takes about 0.40s at that rate, consistent with much of the 0.46s regression.
Demand IBS samples also expose new loads in the copy pass; sample weights are
not elapsed-time fractions. Warm windows have zero measured reads, major faults
and GC, and the process host gates pass. Results match clean stock.

No cache-policy change is retained. The existing overshoot allowance protects
against an extra pass costing more than smaller maps save. Next test a smaller
physical aggregate representation, preserving ordinary result layout at the
output boundary. Evidence: `/tmp/query-moonshot-actual-20261028/state-cache`.

### Rejected: borrowed winning-string state, 2026-10-28

An isolated generated working-state layout replaces each growable sink with a
24-byte pointer/length/prefix view, materializing ordinary sinks only for output.
Twelve focused checks pass across the three backends and both worker modes,
including transient Parquet/literal inputs. The 500M-row / 8.39M-group case gets
worse: balanced process medians 3.149–3.160 -> 3.692–3.768s, cycles/input
265 -> 315–320, instructions/input 330–335 -> 339. Tiny-state execution remains
~560ms because it does not enter partition merge. All matrix host gates pass;
warm reads, major faults and GC are zero; exact stock outputs match.

The smaller allocation footprint does not produce substantially less total
traffic: the controller comparison is 158.2 -> 155.7GB/query, while achieved
bandwidth falls 50.1 -> 41.7GB/s. Generated code adds a dependent pointer load
before comparing old winning payloads. IBS attributes prominent sampled waits
to those loads, but total sampled miss latency does not rise; this is evidence
about code location, not a complete resource diagnosis. A 32-byte view retaining
a longer prefix recovers part of the loss (3.300–3.372s), but still loses to the
3.125–3.131s matched control. It moves 147.4GB/query and executes ~365–368
instructions/input. Fewer bytes alone are insufficient. No representation
change is retained and no other-machine claim follows from this rejection.

Next test a larger reduction in work: exact ranking first, then replay existing
partition records to evaluate independent output aggregates only for surviving
groups. Evidence: `/tmp/query-moonshot-actual-20261028/borrowed-state` and
`borrowed-prefix`.

### Retained: exact ranking before independent string aggregation, 2026-10-28

For bounded fixed-width group keys ordered by a known small LONG Top-K, compute
numeric ranks exactly, select candidates, then replay existing partition records
to fill their VARCHAR MIN/MAX. Ordinary maps, sink layouts, workers and source
lifetimes are unchanged. No base-table rescan or new toggle. The existing sketch
provides a conservative distinct-group lower bound; small partitions retain
ordinary merge. Post-aggregation filters and other pre-selection consumers block the pushdown.

Accepted eight-core profiles of 500M rows / 8.39M groups show ~3.1 -> ~1.7s.
Controller traffic falls 150.3 -> 86.4GB/query at similar 48.3/50.8GB/s; avoiding
64GB corresponds to ~1.28s at 50GB/s, consistent with the elapsed reduction.
Backend-memory slots fall 66.5% -> 52.5%; matched IBS demand-miss samples fall
680 -> 428 per 500M rows and their mean reported latency 210 -> 112 cycles.
These are statistical samples, not elapsed-time shares or proof of saturated
DRAM. The USE/hardware study passes. Guarded one/four-core medians improve
16.60 -> 10.02s and 4.80 -> 2.71s. Ordinary tiny-state controls stay neutral.

Later cross-cache, 16-core and balanced refreshes include background interference
and are excluded where the host gate fails. The user then paused wall-clock
profiling; the final interrupted controls are also excluded. Exact stock checks
pass, including the completed 500M-row controls, but no final-build timing claim
is made. This adds no ClickBench coverage. Skew and other architectures remain
unverified. Evidence: `/tmp/query-moonshot-actual-20261028/late-aggregates`.

Checkpoint validation: native bridges, touched core classes, and all prototype
main/test sources build; 24 focused SQL cases and 130 ownership/IR/backend/page
checks pass. Full-suite evidence above predates this final scheduling change;
final-build controlled timing remains pending while profiling is paused.

## 2026-10-28: integer MIN/MAX use the existing generated aggregate lifecycle

Bind the exact LONG, INT and SHORT MIN/MAX implementations through one typed
update/merge emitter. Preserve LONG's 8-byte state, INT/SHORT's 4-byte state,
NULL/empty semantics and the existing map/partition/join owners. No core changes
or new switches. LONG results retain generated sort/Top-K; INT ordering remains
ordinary. Generic aggregation without GROUP BY still lacks an update hook.

The previous build rejects the added MIN/MAX kernel check. The new build passes
9 kernel, 24 SQL aggregate and 4 SQL join checks across LLVM/AsmJit/C, including
both worker modes. Cases cover explicit NULL flags, packed mixed-width state,
Java/native merging, bounded partitions, VARCHAR/composite keys, Parquet, column
tops, ordering and reopening. All prototype main sources and changed tests compile.
The extreme-value fixture also exposes the existing LONG SUM sentinel/merge-order
issue, recorded separately in TODO.md. Evidence: `/tmp/query-integer-extrema-20261028`.
These are small correctness fixtures, not performance experiments; profiling
remains paused and no speedup or additional ClickBench coverage is claimed.

## 2026-10-28: generated scalar aggregation reaches ordinary SQL

The async unkeyed factory now binds COUNT and integer SUM/MIN/MAX on its
row-update paths. Existing vector/metadata choices remain authoritative, including
the optimizer's rewrite of SUM(column + constant). Kernels borrow original frame
or selection-vector coordinates and the existing 32-byte-spaced SimpleMapValue
layout. Workers, locks, empty-state handling and merge order remain engine-owned.
Native copying/merging accepts ordinary or generated partials. Unsupported frame
representations decline before mutation; execution failure drains and resets.

The compiler promotes the existing aggregate update IR to scalar accumulators;
it does not duplicate their definitions. A four-aggregate optimized LLVM sample
has four final state stores and none inside its row loops. Cancellation polls
occur between 1024-row chunks. This establishes code structure, not throughput.

The old compiler fails the six new SQL coverage checks. The implementation passes
30 focused SQL checks across all backends/both worker modes, three direct kernel
checks, and three existing unkeyed memory/lifecycle checks. These cover native row
counts, selected/repeated rows, mixed-width values in actual scalar slots, empty
inputs, column tops, lazy conversion, Parquet, cancellation, reopen and partial
slot-construction cleanup. Main sources and changed tests compile. Evidence and
LLVM dumps: `/tmp/query-scalar-aggregate-20261028`. Profiling remains paused;
no new performance or ClickBench coverage claim.

## 2026-10-28: shared integer multiplication

INT/LONG multiplication now binds the exact QuestDB function implementations and
uses the existing arithmetic emitter for wrapping, NULL propagation and INT-to-LONG
widening. It composes in predicates, projections, grouping keys and aggregate
arguments, including unkeyed reductions. Scan substitution, column tracking and
expression limits understand the new node. No backend changes or toggles.

The old compiler fails the six new SQL coverage cases. The implementation passes
12 focused SQL checks across LLVM/AsmJit/C and both worker modes, plus three scan
checks that each exercise all lowering modes. Cases include NULL operands, zero
multipliers, overflow into the NULL sentinel, widening before/after multiplication,
native row counts and repeated execution. Evidence: `/tmp/query-integer-multiply-20261028`.
Profiling remains paused; this is a coverage gain, not a measured speedup.

## 2026-10-28: string negation and ordered filter LIMIT

Supported VARCHAR predicates now bind through NOT, preserving the resolved
functions' NULL behavior. Forward finite filter LIMIT uses generated frame-prefix
admission in the existing ordered collector. Native VARCHAR selection stops after
K matches per frame; workers share an immutable cap, not a racing global quota.
No copied row lists, scheduler changes or new switches. Numeric-only selection
keeps its full-frame SIMD path; Parquet stays full-frame for selectivity statistics
and retains its existing late-materialization/column-top fallback.

Original Q22–Q24 SQL matches stock on a 4,096-row ClickBench-schema fixture,
including all 105 Q23 output columns in order. Q22 filters 4,096 rows natively and
updates 2,253. Q23/Q24 evaluate 24/10 rows across dispatched frames and each admit
ten; the full-frame LLVM control evaluates 262 for each at identical settings.
A separate dense fixture checks 10–20 evaluated rows at LIMIT 10 with two in-flight
frames. Work includes scheduling slack; this does not establish lower elapsed time.

42 focused SQL, nine scan and four existing core checks pass: LLVM/AsmJit/C, both
worker modes, later-frame-first completion, cursor operations, runtime bound
changes, zero dispatch, tails and preparation-failure recovery. Evidence:
`/tmp/query-filter-limit-20261028`. Full-data coverage remains 30/43 pending the
planned audit; controlled profiling remains paused. No speedup claim.

## 2026-10-28: generated VARCHAR Top-K batch build

Single-VARCHAR ordering now generates the existing encoded-buffer build: six-byte
split-prefix rejection, normalized key encoding, full threshold comparison and
admission, in either direction. It borrows actual worker buffers and returns only
for growth/compaction, retaining existing budgets, threshold copies, scheduling
and row IDs. LLVM recognizes the byte permutations as bswap intrinsics; its only
external call site in this kernel is the throttled cancellation poll.

The old build fails all six added SQL coverage cases. The implementation passes
12 SQL checks across LLVM/AsmJit/C and both worker modes, three guard-page checks,
and the existing Top-K partial-filter-compilation cleanup check. Cases include
NULL/empty/long-prefix strings, encoding boundaries, Parquet, ordered ties,
compaction, reopen, preparation/worker failure and memory-limit recovery. A losing
split-prefix guard test proves no payload read. Original Q25 matches stock on the
4,096-row ClickBench-schema fixture; native filter and build counters both cover
4,096 rows. Evidence: `/tmp/query-varchar-topk-20261028`.

Compaction, merge and final sorting remain ordinary, so Q25 is not yet a complete
LLVM pipeline. Q26's partially ordered composite path remains untouched. Profiling
is paused; no performance or full-data qualification claim. No new toggles.

## 2026-10-28: generated encoded Top-K merge and heap packing

Worker-result admission and compaction's survivor selection/heap packing now use
shared typed IR over QuestDB's normalized variable-key entries. These stages are
compiled lazily, retain the existing threshold and compaction algorithm, and
resume after ordinary buffer growth. Scan and merge share one full-key comparison.
FIRST N avoids copying entries already in place; LAST N safely moves overlapping
ranges. Scratch packing remains necessary because survivors reference arbitrary
positions in the old heap. Sorting and threshold publication still use stock code.

21 focused checks pass across LLVM/AsmJit/C: 12 SQL checks, six buffer checks and
three guard-page checks. Buffer contents and packed heaps match stock byte for
byte, including a capped compaction trigger, repeated heap growth, FIRST/LAST N,
both directions and recovery after failure midway through packing. Source buffers
remain unchanged; compiled code and scratch allocations are released. Original
Q25 matches stock on the 4,096-row fixture, with 4,096 generated build rows, 3,965
merge input entries and one generated packing pass. Optimized LLVM merge/packing
have only cancellation polls as runtime call sites. Evidence:
`/tmp/query-encoded-topk-20261028`.

Generated sorting (inside compaction and for final output) is still missing;
Q25 remains partial and full-data verified coverage stays 30/43. No controlled
performance runs or speedup claim; desktop profiling remains paused.

## 2026-10-28: generated normalized variable-key sorting

Compaction and final Top-K sorting now execute typed-IR ports of the existing
natural-run detection, in-place MSD radix partitioning and non-branchless pdqsort.
Insertion sort, pattern breaking, equal-key handling and worst-case heapsort remain.
Bounded explicit stacks replace recursion; pdqsort processes the smaller partition
first. Sorting uses normalized 40-byte entries, without SQL names, column types or
hardware-specific tuning. The 16-byte prefix avoids heap reads when it decides the
order; entries already in their radix bucket avoid copying. Workspace reuses the
buffer's tracked DirectLongList and only grows. Small sorts need no radix workspace.

The final cursor's Java LIMIT truncation was another uncovered boundary. It now
consumes the prefix count from generated sorting, using the existing row-limit
expression. The EXPLAIN marker is `compiledTopK`. Stock execution remains available
through the ordinary factory; lazy-conversion fallback remains explicit.

Validation: 12 existing SQL checks pass across all three backends and both worker
modes. Six further SQL checks cover empty/singleton/short results, changing binds
and cached-factory reopening. Six guarded-sort checks cover eight distributions,
0/1/23/24/127/128 boundaries, long shared prefixes, arbitrary normalized bytes,
unsigned row IDs, both guard edges and forced heapsort, plus query-budget failure
and cancellation recovery. Six buffer checks and three encoding guard checks pass.
The first edge-fixture attempt omitted its bind service; matching existing test
setup fixed the fixture. No production workaround was added.

Original Q25 matches stock on 4,096 fixture rows: filter/build process all rows,
merge consumes 262 worker entries, and generated sorts process 4,096 entries at
compaction plus ten for final output. The LLVM module defines its own sort helpers;
external runtime calls are cancellation polls, with compiler intrinsics for primitive
operations. Evidence: `/tmp/query-encoded-sort-20261028`.

Q25 now has a complete generated path on the fixture, joining Q22–Q24 awaiting
full-data qualification (34 provisional shapes, 30/43 full-data verified). This
establishes correctness coverage. Profiling stays paused;
compilation cost, long-tail comparison cost and end-to-end gains still need measured
validation. Composite/partially ordered SQL binding remains the next separate gap.
