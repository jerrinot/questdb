# General generated joins

Historical plan. The maintained design and open work are in [ARCHITECTURE](../ARCHITECTURE.md)
and [TODO](../TODO.md); this document records its original context.

Follow-up plan (2026-11-07): [selective transfer through dependent joins](DEPENDENT_JOIN_FILTER_PLAN.md)
connects the typed fused machinery below to ordinary runtime filters and closes
composite-producer membership reduction. The completed milestones below remain
the implementation baseline, not a request to rebuild their infrastructure.

## Next milestones: validate generality and portability

Plan, 2026-11-07; implementation baseline `9100ce3eb6`. Goal: establish which
properties make the retained optimizations profitable across types, distributions
and machines, then fix the largest demonstrated failure. Start with the latest
inner-join/filter/grouping work. Wider benchmark and ASOF controls follow; do not
infer that all earlier results apply to this commit. Finish one milestone and
reassess its assumptions before starting the next.

Execution update (2026-11-07): G0 is complete. G1/G2 establish an applicability
envelope with 500M-row controls on AMD and the supplied Apple M1 `fedora` host.
The unchanged native module builds on ARM; the same 79 focused tests pass on both.
The high-group same-plan control gains at least 9.2x AMD / 7.5x M1; independent
no-join aggregation gains at least 2.00x / 1.74x across process median ranges.
Skew, small-state, scalar, typed-key, scaling and natural admission boundaries are
recorded in [the evidence report](../target/generality/REPORT.md). M1 aggregation has
an unresolved effective-clock accounting discrepancy; do not present it as a
fixed-frequency comparison. ARM large-run typed-key coverage remains narrower
than AMD; Intel/NUMA hardware is unavailable.

G3 remains open after a bounded exploration experiment. Short waves improve AMD
uniform first use by about 27%, with lower load-wait cycles at similar memory
traffic, but scalar/skew regressions reject blanket shortening. Initial-minimum,
CPU-clock and finalist-confirmation overlays do not establish reliable choices.
Confirmation can discard the eventual faster kernel: one M1 uniform pair regresses
19% warm with 16% more instructions/row. No production chooser change is retained.
[The new experiment](../target/exploration-budget/REPORT.md) includes cumulative
1/2/5/10 costs, full-output checks, hardware evidence and warmed alternating-kernel
controls. First validate sampled ranking under matched concurrency; only then add
remaining-work/compilation budgeting. Preserve unknown probe counts behind
residual filters and do not assume query-cache reuse.

G4 refresh completed the 20-query RTAB control, original SF100 Q1/Q3/Q5 and the
full official ClickBench protocol. All complete responses validate within the
previously documented cross-engine semantic boundaries. The fresh
[ClickBench HTML](../target/generality/clickbench/clickbench.html) gives 22.007 ->
7.767 s best-hot totals, 28/43 queries at least 1.5x faster, and 34 fully generated
plus four partial queries. Historical H2O/SYMBOL 1BRC datasets are missing; those
sentinels remain unrun. ASOF is unchanged and no shared policy was retained.
This validation pass establishes transfer and counterexamples, not completion
of the remaining admission/first-use work.

### G0. Make the existing measurement path trustworthy

Reuse `target/dependent-transfer/run.py`, the RTABench `RtaMain`/`java.py` harness,
existing SQL/oracles and `run-griffin-tests.sh`. Keep one small reusable runner;
do not build another benchmark framework. Before transferring it:

- Replace scratch assumptions about CPUs `0..N-1`, `nvme0n1`, cpufreq availability,
  absolute paths and AMD events with explicit host inputs or discovered metadata.
  Capture actual allowed CPUs, sockets/cache sharing, NUMA placement, SMT, memory,
  quotas, JVM/LLVM versions, frequency and competing work. Establish affinity before
  `CacheSizing` initializes; it reads the process's allowed CPUs once.
- Update existing diagnostic chooser inspection for `Programs.choices[][]`.
  Scratch telemetry still reflects the deleted `exactChoices`/`expandedChoices`
  fields. Verify diagnostics against known alternatives before trusting profiles.
  Keep forced choices in the diagnostic harness, with no new production toggles.
- Freeze source and binary identities for stock `df37676583`, current branch Java,
  current LLVM, and only the mechanism counterfactual needed for the case.
  Disabling the compiler can change planning: an attribution control must verify
  the same factory/worker/layout path, or explicitly include the plan difference.
  Old class overlays are evidence, not portable builds; reconstruct source for
  any retained counterfactual and rebuild it on each architecture.
- Verify complete results, native input rows, selected paths and compilation
  counts. Plan labels and native call counts alone do not prove row/operator
  coverage. Exercise the existing focused lifecycle checks before large runs.

Exit: one known 500M-row control runs with correct output and explained execution
coverage under each applicable arm. The harness identifies missing host metrics
without inventing zeroes. Reproduce the committed cleanup baseline before using
older performance numbers as its score.

### G1. Find the workload boundaries on the current host

First repeat the existing 262K-group control, isolating destination reuse and
prefetch from parallel-plan admission. Then examine one no-rejection transfer
case and one cache-resident grouping case. Extend only the factor that explains
a loss or uncertainty; avoid a Cartesian product of every option.

| Mechanism | Prediction and falsifier | Controls to cover across the sequence |
| --- | --- | --- |
| Selective transfer | Avoided exact probes/build insertions must repay preparation and membership checks. All-match/no-match cases expose overhead and empty-state behavior. | INT, high-bit LONG, differently numbered SYMBOL dictionaries, mixed tuples; chains and duplicate-bearing branches; 0%, selective, 50% and 100% survival. |
| Destination reuse | Expensive group lookup approaches first encounters per build row per slot; updates still scale with surviving joined rows. Low reuse can lose. | Mixed COUNT/SUM/MIN/MAX/AVG and DOUBLE state; VARCHAR/Unicode/NULL groups; uniform, shuffled and hot-key inputs; reuse just below/above the existing admission bound. |
| Group prefetch | At unchanged results, additional instructions should reduce load-wait cycles for larger states; cache-local states may gain nothing. | Actual state footprints below/near/above the discovered cache budget; one versus several workers; plain and join-only alternatives retained. |

Include a deliberately ineligible case: grouping inputs from multiple builds,
probe-derived groups, an outer join or build-side Parquet VARCHAR. Verify correct
fallback and its cost; do not count it as generated coverage. Existing correctness
tests cover wider NULL/overflow/reopen combinations without another huge test suite.

For each primary case use at least 500M input rows. Compare complete executions:
preparation, scan/probe, aggregation, merge and result consumption. Record SQL
compilation, lazy native compilation, the first ten cached executions and a
separate settled series. The first process retains ten initial executions;
subsequent portable repeat processes use three warmups and five measured runs
after traces established that training completed within two executions. Extend
this when a case has not settled. Preserve training executions rather than discarding
their cost. Report cumulative cost at 1/2/5/10 reuses and the observed break-even;
only use `(compile_candidate - compile_control)/(warm_control - warm_candidate)`
as a rough model when warmed per-query costs are stable and the denominator is positive.

Exit: a small coverage table with per-case gains/losses and an explanation for
where each mechanism pays. Keep small-state variance and cold-use losses visible.
No admission or scheduling change until a failing case and hardware evidence
identify what needs changing.

### G2. Transfer the unchanged workload to different hardware

Run the bounded G1 control set on ARM64 using the supplied host; a different-vendor
x86 control remains desirable but no such host is currently supplied.
Prefer a server with meaningfully different cache/core/bandwidth balance. Build
from the same source with JDK 25 and the CMake-required LLVM 21.1; use host-native
LLVM targeting and record versions/features. Never copy native binaries across ISAs.

ARM requires a build smoke test first: `LlvmBackend` accepts 64-bit Linux and
`llvm_backend.cpp` detects the host, but `build-native.sh` builds all targets and
`asmjit_backend.cpp` explicitly uses x86. Verify the LLVM/regex target build and
QuestDB native dependencies; make only a necessary build separation if blocked.
An AVX2 target on the current AMD CPU remains an instruction-set control, not
another machine's result.

Use 1, up to 8 physical cores, and all physical cores with fixed total work.
Compare SMT separately. On a NUMA host, compare a node-local placement first,
then multiple nodes with explicit, equal memory policy and measured locality.
The M1's performance cores are CPUs 4–7 and efficiency cores are 0–3. Compare
four performance cores first, then one performance core and the mixed clusters.
Discover physical CPUs through sibling sets: core IDs alone repeat across M1
clusters. Both hosts use the identical compact fixture in `bench/fixtures`,
which reduces the probe table to 6 GB without changing keys or aggregate values.
The original wider local control is a separate measurement, not a cross-host pair.

Exit: per-machine correctness, first-use/warm distributions, generated coverage,
normalized counters and resource limits. If the machine is unavailable, mark
portability unverified; do not substitute another local affinity configuration.

### G3. Fix one demonstrated portability or admission failure

Pick the largest repeatable absolute-time loss. Possible targets are preparation
that cannot amortize, poor candidate selection or an unsuitable prefetch schedule;
none is a predetermined patch. Derive a hardware/workload explanation, state the
predicted counter and elapsed-time changes, and test the smallest general change.
Reuse current memory tracking, frame scheduling and chooser boundaries. Reject
query-name rules, CPU-model thresholds and new user-facing switches. Keep a change
only if the failing case improves and its nearest typed/distribution controls hold
on both available machines; otherwise re-plan or retain the previous mechanism.

The first local control exposes exploration cost, but does not justify fixing a
choice globally: a forced group-lookahead alternative has lower cumulative cost
through ten executions, while automatic selection sometimes has a better warm
time. Relevant experiments, if this remains the largest loss after the boundary
controls, are progressive elimination of slow alternatives and budgeting compilation
against remaining work. The former is evaluated in Schmidt's
[adaptive compiled execution study, section 6](https://db.in.tum.de/people/sites/schmidt/papers/master-thesis.pdf);
the latter follows the decision problem in
[Kohn, Leis and Neumann, ICDE 2018](https://db.in.tum.de/~leis/papers/adaptiveexecution.pdf).
These are alternatives to test individually, not a request for another compiler
backend or a reason to remove choices from this one query's result.

### G4. Confirm transfer to real workloads

Refresh the 20-query RTABench control and relevant original-query TPC-H cases;
retain failed/unsupported cases with reasons. Use SYMBOL 1BRC and one H2O aggregate
as independent sentinels. If shared chooser/runtime changes, include the existing
ASOF runner with INT/LONG/SYMBOL, both forward and lookup regimes. Finally run the
full stock/LLVM ClickBench repository protocol and standard HTML with generated
coverage badges. Different machines receive separate reports, not a blended score.
DuckDB is an optional fixed-version control after the primary comparison stabilizes.

Report per-query and absolute-time-weighted effects, largest-case contribution,
first-use costs, peak memory and regressions. Distinguish full generation, partial
generation and fallback. A few large wins cannot establish whole-suite generality.
For a portable breakthrough claim, target repeatable >=1.5x complete-query gains
in more than one independent workload family on two hardware families. Keep the
strongest applicable same-plan control beside stock to separate compiler effects
from planning and parallel admission. Smaller gains remain useful but get that label.

### Measurement and retention gates

- Clear or control systemic USE concerns before interpreting CPU counters. Calibrate
  read and mixed read/write ceilings on each host/placement; discover PMU events
  locally, check scheduling/multiplexing and profile generated symbols. Use at least
  two independent signals plus a counterfactual for a hardware bottleneck claim.
  Classify CPU/off-CPU/I/O work for every case; detailed profiles must cover at
  least 80% of baseline time and one representative of each operator family.
  Validate the existing bottleneck-study format before any production optimization.
- Use at least three balanced fresh-process pairs and five measured warm queries
  per primary case; keep all first-use results. A repeatable >5% regression beyond
  process variation triggers investigation; smaller unresolved differences remain
  reported. This is an investigation threshold, not permission to accept regressions.
- Preserve complete-result checks, including all warmups. Keep output serialization
  inside the same timing boundary for every arm and canonical comparison outside it.
  Reuse current data/oracles, estimate disk/memory needs before creating fixtures,
  and remove only owned scratch data after validation. Retain commands, hashes,
  counters and enough result evidence without duplicate full-output archives.
- Write only accepted findings and explicit limits into PROGRESS.md. Completion
  means a measured applicability envelope and resolved or clearly bounded failures,
  not a promise that every query becomes faster. No new operators in this sequence
  unless an observed correctness or portability prerequisite requires them.

## Historical implementation milestones

2026-11-02 implementation sequence from WHOLE_QUERY_PLAN.md. SSB is a
control, not the specification. Narrow prototypes do not satisfy completion.

## 1. Generalize the existing join optimization

Required key coverage: INT, full-width LONG and SYMBOL with different dictionaries.
Use composite fixed-width keys to validate the representation contract. Preserve the
existing INT fast path and existing map/build/worker ownership. Do not add a second
hash table or a type-dispatch call to every generated row.

Starting constraints verified before implementation (historical baseline):

- HashJoinGroupByCandidate.analyseMultiway admits only independent single-INT joins.
- CompiledJoin passes IntKeyed builds and an untyped single probe-column index.
- ResolvedJoinKernel emits INT-specific loads/hash/probes. ResolvedJoinFilter also
  accepts INT only. The ordinary core already supports SYMBOL translation and uses
  MapHashJoinBuild with Unordered8Map/OrderedMap for other keys.
- Frozen builds own payload lifetime and duplicate chains. Generated consumers must
  retain every resume position across aggregation-map growth.

Execution order:

1. Freeze current source/classes. Reuse the existing harness with matched 500M-row
   INT/LONG/SYMBOL workloads and independent result oracles. Capture native execution,
   complete first/warm requests, phase times, memory, USE and fixed-work PMU evidence.
   LONG values must exercise high bits; equal SYMBOL texts must have different IDs.
2. Establish the affected execution boundary and validate the hardware study. Use
   ordinary/current LLVM controls with identical data and resources; retain all setup
   costs. Existing historical INT profiles do not establish LONG/SYMBOL bottlenecks.
3. Describe join keys using resolved types/source columns and existing frozen lookup
   storage. Reuse NativeMapView and generated map hashing/equality where possible.
   Keep compile-time representation choices separate from execution-owned addresses.
4. Extend generated probing and applicable membership rejection across required
   types. SYMBOL translation retains the engine's dictionary/NULL/reopen contracts.
   Wide ranges use an appropriate representation or direct generated probes; never
   truncate LONG or compare dictionary IDs from unrelated symbol tables.
5. Extend planner admission only for implemented contracts. Exercise single/multi
   joins, reordered projections, mixed key types and composite fixed keys. Ordinary
   fallback alone does not count as optimization coverage for a required type.
6. Focused semantic tests: duplicates, NULL, collisions, empty inputs, map growth,
   partitioned builds, cancellation and reopen. Balanced large A/B controls verify
   results and the predicted work/counter changes at multiple worker counts.

Completion requires actual generated execution and validated complete-query results
for all required representations. Report per-type performance and regressions;
neither one INT win nor one grand-average score proves generality.

Milestone 1 status: implemented. The final typed `CompiledJoin.Build` describes key
columns/types in the ordinary RecordSink order. INT/SYMBOL retain the narrow table;
LONG and composites borrow the existing map views. SYMBOL reads the existing
translation cache, returning to its ordinary translator only for unresolved entries.
Single and independent multiway joins share this lowering and the existing frame
workers. Integral membership bounds/bitmaps use full-width arithmetic and bounded
construction; composite keys use exact map probes. All-composite plans compile no
unused membership alternative.

Validation and performance, including limits: [typed-join evidence](../target/join-generalization.md).
No new setting or hash table. This completes representation generalization, not
arbitrary join graphs, generated build/merge/order, or the next two milestones.

## 2. Generalize operator dependencies

Represent where a key/value is produced, so a later lookup can use an earlier joined
payload. Reuse the same typed lookup and consumer contracts for independent joins and
dependent chains. Preserve duplicate nesting and resumes. Extend composition into
existing aggregate merge/ranking only when phase measurements justify the boundary.
Outer and temporal joins need their own semantic proofs; do not admit them by analogy.

Milestone 2 execution slice (2026-11-02):

- Freeze the actual installed milestone-1 classes; use 500M-row chains with INT,
  full-width LONG and differently numbered SYMBOL dictionaries, both all-match and
  selective. Profile the ordinary plan before changing production. Include a
  prejoined build-side control with its construction cost, not merely a faster
  per-fact-row nested loop. Independent joins remain regression controls.
- Preserve the optimizer's existing left-deep order when no eligible common probe
  exists. Require every key input to be produced by the scan or an earlier build;
  admit mixed-source composite keys without treating a chain as independent.
- Keys retain source identity during analysis and bind into the existing joined
  column namespace. Prune payloads only after retaining later key dependencies.
  Reuse existing build/probe maps, symbols, row heaps, records and frame workers.
- Independent lookups keep early rejection. Dependent lookups execute inside their
  producer's duplicate loop. Java fallback follows the same nesting. Save prior
  duplicate positions across dictionary resolution and aggregate-map growth.
- Bind translation caches to the key's real source dictionary, and clear consumers
  before producers. Validate NULLs, changed dictionaries, missing downstream rows,
  duplicate fanout, composites and reordered projections before large A/B runs.
- A large gain versus the ordinary plan includes parallel-plan admission. Use the
  same fused Java plan and one-core generated runs to separate mechanisms. If
  resolving the small build chain once wins, prioritize that physical strategy
  before adding more per-row operators. This milestone does not add a new optimizer
  framework, arbitrary join reordering or outer/temporal join semantics.

The compilation structure follows data-centric produce/consume lowering
([Neumann 2011](https://www.vldb.org/pvldb/vol4/p539-neumann.pdf)).
The prejoined/cached-dependency control tests the separate opportunity to share
repeated subcomputations described by [factorized databases](https://www.cs.ox.ac.uk/dan.olteanu/papers/os-sigrec16.pdf).
These motivate experiments; neither substitutes for QuestDB measurements.

Milestone 2 status: implemented. INT, full-width LONG, SYMBOL and mixed-source fixed
composites can read earlier joined payloads. Duplicate nesting, dictionary resumes,
aggregate-map growth and Java fallback share the existing joined-prefix contract.
60 compiler and 84 core tests pass; all large controls validate complete results.
Eight-core 500M-row chains improve 12.2-24.2x over the previous ordinary plans,
including parallel admission, and 1.49-3.18x over the same fused Java factory.
One/sixteen-core controls and native-symbol PMU attribution confirm the mechanism.
Independent single/multiway regressions are not repeatable above the retention gate.
See [dependency evidence](../target/join-dependencies.md).

## 3. Reduce unnecessary joined work

Revised from milestone 2 measurements: test build-only dependency work before broader
multiplicity aggregation. Selective prejoined controls, including 41ms construction,
take 159ms INT / 811ms sparse LONG / 154ms SYMBOL versus generated chains at
561 / 1404 / 668ms. All-match materialization costs 3.1s and loses overall. The
faster scan alone does not justify unconditional prejoining.

1. Profile the selective chain at fixed work, then compare typed build-side membership
   pruning with bounded reuse of resolved dependency links. Trace the ordinary INT
   build-scan filter and existing frozen-build/payload preparation before designing.
   Reuse those owners and maps. Separate physical strategy from compiler improvements.
2. Prove when a dependency is build-only. A mixed-source key that also reads the fact
   row cannot be cached by producer row alone. Preserve SYMBOL dictionaries, duplicate
   multiplicity, NULLs and execution-local snapshot lifetime across all required types.
3. Include construction, memory and first-request costs. Vary fanout, skew, rejection,
   thin probes and all-match cases; do not materialize an unbounded duplicate product.
   Retain exact execution when work cannot be amortized. Use no CPU/dataset-name rule.
4. Target at least 2x over the newly generated chain on two independent selective
   representations, with the existing regression gate, before retaining a strategy.
   Refresh the hardware gate first. A rejected experiment is a valid outcome.

Membership-transfer experiment: retained. Owner-side preparation walks existing frozen
producer keys and ordinary child probes, publishing conservative necessary conditions
before workers run. Compact sets use the existing bitmap; sparse LONG and sparse INT
use the existing split-block Bloom layout with generated probes. SYMBOL uses the
ordinary dictionary translator. Exact lookups still decide payloads and multiplicity.
At this measured checkpoint, composite child keys are supported when all components
share one producer; composite producer keys and mixed-source conditions did not use
this reduction. The 2026-11-07 follow-up implements composite producers, with performance
validation pending. Mixed-source conditions remain excluded. Known probe work
must amortize construction, and scratch/filter memory must fit the query budget.

500M-row, eight-core selective INT/LONG/SYMBOL: **562/1414/679 -> 198/346/249ms**,
including preparation. Gains hold at one/sixteen cores and with skew/duplicates.
LONG drops **122 -> 29 cycles/input** and **1.27 -> 0.13 branch misses/input**.
64 compiler and 84 core tests pass; 740 complete large outputs match their oracles.
All-match SYMBOL showed a 7.3% loss in individual process pairs, but six further
balanced process pairs average +0.5%; unchanged exact machine code itself varies.
Forced alternatives and fixed code-address diagnostics do not establish a systematic
regression or a layout cause. Preserve this uncertainty; do not tune to a lucky layout.
See [membership-transfer evidence](../target/join-dependency-filter.md).

Bounded dependency-link experiment: retained. Each producer row can retain one
four-byte compressed head into its child's existing duplicate chain. Ordinary typed
probes and SYMBOL translation prepare these heads; generated consumers share the
lookup result while retaining exact duplicate iteration. Composite producers and
children are supported; mixed-source child keys retain ordinary probing. Known work,
query budget and frozen-heap capabilities gate preparation. No new map or setting.

500M rows, eight cores, two balanced process pairs: all-match INT **917 -> 614ms**,
LONG **1355 -> 784ms**, SYMBOL **1059 -> 668ms**. Selective cases gain another 18-22%.
LONG drops **115 -> 65 cycles/input**, **0.91 -> 0.48 branch misses/input**; memory
traffic stays near 8.5GB. Gains hold at one/sixteen cores and on mixed LONG+SYMBOL
composite keys. Preparation takes 1.3-2.1ms and 1MiB for 262,144 producer rows.
No-root-match input wastes ~1.8ms, averaging +2% over six process pairs; known scanned
rows do not guarantee useful reuse. 64 compiler / 155 core tests and 752 complete
large outputs pass. See [dependency-link evidence](../target/join-dependency-links.md).

Multiplicity-aware COUNT experiment: implemented for unobserved leaves. A bounded,
query-owned copy of the existing backward-link chain counts preserves ordinary
probes and Java fallback. Resolved predicates, grouping and later key inputs exclude
observed builds. COUNT uses its existing state and wrapping arithmetic; zero/negative
wrapped products do not mean an empty join. No inference about SUM/AVG/COUNT(value).

500M rows, eight cores: two-leaf branching INT **1954 -> 1032ms**, LONG **2338 ->
1424ms**, SYMBOL **2292 -> 1085ms**. The original goal of 2x on two representations
was missed. With an additional leaf, unchanged code gains **3.35/2.91/3.52x**;
grouped COUNT gains **1.70x**. This is a fanout-sensitive improvement, not a universal
join win. Composite probing gains only 7%; observed producers still iterate.
One/sixteen-core branching controls gain 1.55-1.86x. LONG drops **199 -> 120
cycles/input**, **483 -> 265 instructions/input**. Withholding count publication
from the same emitted code restores the instruction cost.

The first version regressed a unique-child control 5-8%, with 16% more instructions.
It was rejected. Frame-level dispatch now keeps executions without count arrays on
the ordinary loop; this control gains 3%. Unique/thin controls remain stable in
longer balanced runs. Compilation grows ~20-65ms in eligible examples; no-match
compile-plus-first execution regresses ~12%. These are real costs, not hidden setup.
67 compiler and 155 core checks pass. See [multiplicity evidence](../target/join-multiplicities.md).

Closed COUNT forests: retained. The compiler proves that predicate/grouping inputs
come only from the probe and every dependent key has one producer. Reverse-order
sums/products reuse cached heads and the existing heap recurrence. Workers probe
roots and consume their weights. Temporary tracked arrays publish only after all
roots succeed; insufficient memory, unsupported heaps and subtree overflow preserve
the existing paths. Zero weights reject before group creation; COUNT still wraps.

500M rows, eight cores, balanced comparisons against leaf folding: chains gain
**1.84-1.98x**, branches **2.76-2.99x**, grouped COUNT **2.08x**, composite keys **1.24x**.
A deeper LONG -> SYMBOL -> LONG forest with an independent INT root gains **3.83x**.
LONG branching drops **117 -> 41 cycles/input**, **265 -> 82 instructions/input**
and **68 -> 26 load-wait cycles/input**. Same-lowering publication bypass restores
the old instruction cost. One/sixteen-core direction agrees on this CPU.

The unique-key first-run loss does not survive balanced repetition; no special-case
patch was added. Excluded controls remain within the regression gate (non-COUNT
LONG loses 3.7%, with unchanged instructions/input). Main/deep compilation adds
~9-24ms, and no-root compile-plus-first loses 4.9%. Warm preparation varies ~2-13ms
in diagnostics, with first preparation ~21-25ms. Build preparation remains Java.
69 compiler / 155 core checks, 761 complete outputs and installed hashes pass.
See [subtree evidence](../target/join-subtrees.md).

Aggregate generality now extends beyond COUNT. Existing multiplicities feed weighted
COUNT(value), idempotent MIN/MAX and ordered SUM/AVG replay, including mixed lists and
grouping. `countsRowsOnly()` now distinguishes wrapping row-count arithmetic; it no
longer gates reduction. SUM/AVG reuse actual state updates, not multiplication, and
peel initialization from subsequent updates. Non-COUNT leaf reduction is restricted
to the unobserved suffix after the last observed build, preserving input order.
Cross-root run-length overflow declines publication and keeps ordinary expansion.

Three process pairs on the same 500M-row typed branches give SUM **2.86-3.21x** and
AVG **2.52-2.67x** over the frozen COUNT-capable compiler. COUNT(value), extrema,
mixed/grouped aggregates and DOUBLE SUM also benefit. No-match preparation and
ordinary AVG retain small costs; compilation and generated code grow. See
[repeated-input evidence](../target/join-repeated-inputs.md) for controls and limits.

MIN and MAX also pass independent primary comparisons: three 500M-row process pairs
per INT/LONG/SYMBOL join key give MIN **4.99-6.67x** and MAX **4.91-7.10x**. INT and
DOUBLE aggregate values, grouped/mixed lists and observed build values are controls.
Exact state checks cross prior/current floating values, including replaceable infinite
states and signed-zero ties. SYMBOL here describes join keys, not extrema over symbol
text. See [extrema evidence](../target/join-extrema.md); the compiler is unchanged.

Aggregate integration now has explicit typed scalar state and result-access contracts.
Ordinary KSUM validates a new three-cell family across scalar, grouped and typed
joins without adding a backend or scalar-promotion class case. Its original getter
remains required; vector/Rosti KSUM and native derived-result expressions are separate
work. Generic first-update peeling also improves the existing mixed scalar control.
See [KSUM evidence](../target/aggregate-ksum.md) for exact-state and 500M-row checks.

Next distinguish remaining root lookup from ordered aggregate arithmetic. Observed
build values permit only safe suffix folding; mixed-source dependencies, DISTINCT,
outer joins and further aggregate pushdown need separate proofs. Choose the next
mechanism from fresh fixed-work profiles, not an SSB query quota.

## Retention rules

One mechanism per comparison. Refresh the hardware-first gate before production
optimization. Keep source-matched profiles, complete output checks and a compact
report; clean only owned temporary data. No query-name, dataset-range or CPU-count
heuristics, new toggles, worker pool or optimizer framework. Reuse existing tests and
add only cases sensitive to the changed contract. No commit until requested.
