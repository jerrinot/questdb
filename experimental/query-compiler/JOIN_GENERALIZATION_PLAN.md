# General generated joins

2026-11-02. This is the active execution slice of WHOLE_QUERY_PLAN.md. SSB is a
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

Validation and performance, including limits: [typed-join evidence](target/join-generalization.md).
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
See [dependency evidence](target/join-dependencies.md).

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
Composite child keys are supported when all components share one producer; composite
producer keys and mixed-source conditions do not use this reduction. Known probe work
must amortize construction, and scratch/filter memory must fit the query budget.

500M-row, eight-core selective INT/LONG/SYMBOL: **562/1414/679 -> 198/346/249ms**,
including preparation. Gains hold at one/sixteen cores and with skew/duplicates.
LONG drops **122 -> 29 cycles/input** and **1.27 -> 0.13 branch misses/input**.
64 compiler and 84 core tests pass; 740 complete large outputs match their oracles.
All-match SYMBOL showed a 7.3% loss in individual process pairs, but six further
balanced process pairs average +0.5%; unchanged exact machine code itself varies.
Forced alternatives and fixed code-address diagnostics do not establish a systematic
regression or a layout cause. Preserve this uncertainty; do not tune to a lucky layout.
See [membership-transfer evidence](target/join-dependency-filter.md).

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
large outputs pass. See [dependency-link evidence](target/join-dependency-links.md).

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
67 compiler and 155 core checks pass. See [multiplicity evidence](target/join-multiplicities.md).

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
See [subtree evidence](target/join-subtrees.md).

Next separate remaining root lookup cost from grouping/aggregate work. Closed
subtrees remove repeated build work but do not remove irregular root probes.
Broader aggregate placement requires null/overflow/rounding proofs; observed build
values and mixed-source dependency keys retain ordinary processing. Choose the next
mechanism from fresh fixed-work profiles, not an SSB query quota.

## Retention rules

One mechanism per comparison. Refresh the hardware-first gate before production
optimization. Keep source-matched profiles, complete output checks and a compact
report; clean only owned temporary data. No query-name, dataset-range or CPU-count
heuristics, new toggles, worker pool or optimizer framework. Reuse existing tests and
add only cases sensitive to the changed contract. No commit until requested.
