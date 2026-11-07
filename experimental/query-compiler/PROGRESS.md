# Query compiler breakthrough log

Retained changes, measured effects and important counterexamples. Measurements
below belong to their dated builds and protocols; they are not a current score.
Current coverage and entrypoints: [README](README.md). Open work: [TODO](TODO.md).
Detailed earlier entries remain in Git history; raw evidence stays at its existing
location rather than being copied into another archive.

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
[Contracts and research](DEPENDENT_JOIN_FILTER_PLAN.md).
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
Contracts, controls and limitations: [ASOF_SELECTION_PLAN.md](ASOF_SELECTION_PLAN.md).

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
  Main design records: [PIPELINE_DESIGN.md](PIPELINE_DESIGN.md),
  [BOUNDED_AGGREGATION.md](BOUNDED_AGGREGATION.md),
  [AGGREGATE_DESIGN.md](AGGREGATE_DESIGN.md).
