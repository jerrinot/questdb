# Selective filter transfer through dependent joins

Historical plan. The maintained design and open work are in [ARCHITECTURE](../ARCHITECTURE.md)
and [TODO](../TODO.md); this document records its original context.

Implementation checkpoint, 2026-11-07. Results and evidence are in
[PROGRESS.md](../PROGRESS.md); this file keeps the contract and remaining decisions.

## Objective

Carry necessary membership conditions through required scans/builds before exact
probes and duplicate expansion. Transfer must generalize across INT, full-width
LONG, translated SYMBOL and fixed tuples, independently of the final aggregate.
For example, surviving customer IDs filter orders; surviving order IDs then
filter items. Copying a customer-ID filter cannot perform that key transformation.

## Implemented

- `QueryCodeGenerator.JoinFilterSource` describes resolved probe columns, physical
  key types and a deferred completed-map binding. Ordinary keys use the actual
  probe dictionary domain, including after runtime orientation changes.
- `HashJoinLightRecordCursorFactory.prepareRuntimeFilter` reuses outer-before-inner
  build order to constrain intermediate build scans and the final probe scan.
  Their ordinary completed maps supply outbound membership. No independent scan,
  scheduler or mutable-map sharing with frame workers is introduced.
- `ResolvedJoinFilter` and `IntegralMembership` use bounded ranges, bitmaps and
  split-block Bloom filters. `JoinMembershipPreparation` emits completed-map
  traversal through `QuestMapScan`; Java preparation remains available. Composite
  filters can use explicit indexed SIMD reads; single-key filters retain scalar
  loops. Exact joins always validate surviving candidates.
- `JoinDependencyFilter` reduces completed fused builds, including tuple producers.
  All child conditions must match the same producer duplicate. It reuses frozen
  probes, row handles, translator views and query memory accounting.
- `HashJoinGraph` shares connected equality-region analysis with
  `SqlOptimiser.chooseHashJoinProbeRoot`. The optimizer can avoid a large
  unfiltered build when maximum-distinct alternative storage fits a conservative
  bound. Existing order categories preserve observable timestamp ordering.
  This is a storage-risk guard, not a selectivity or whole-query cost model.

## Contracts that must survive further work

- Preserve tuple order, signed widths, NULL semantics and current SYMBOL domains.
  Never compare untranslated IDs from different dictionaries. Rebind after reopen.
- Membership reduces candidates, not bag multiplicity. A surviving producer key
  still needs exact duplicate/payload checks. Approximate filters permit false
  positives, never false negatives.
- An unavailable or unaffordable producer is universal membership; a completed
  empty producer rejects everything. Cancellation and allocation failures propagate
  normally. Publish only completed state and release it after workers drain.
- Ordinary mutable maps and frozen fused builds retain different ownership
  contracts. Share encoding/allocation decisions where they match; do not force
  both into a new join framework.
- Respect query trackers, runtime swaps, cursor reset, suspension/retry and fallback.
  Do not introduce a dependency cycle between filtered sizing and producer builds.
- Cross only proven inner-equality boundaries. Outer/anti/temporal joins, LIMIT,
  windows, aggregation and volatile expressions require separate semantic proofs.
  Composite dependency keys drawn from multiple producers need a joined-prefix
  representation; an individual row cache is insufficient.

## Remaining experiments, one at a time

1. Profile cases where preparation or ineffective filtering dominates. Compare
   no transfer, existing filtering and dependent transfer on complete queries,
   including low selectivity and false-positive inflation.
2. If measured benefit warrants it, admit or abandon filters at existing frame
   boundaries using construction cost, rejection and critical-path delay. Reuse
   current adaptive execution; bound variants and preserve first-use costs.
3. Where readiness is the obstacle, compare legal reordering of existing builds
   before adding an extra key-only pass. Cost every scan and materialization.
   Filtered-input boundaries and mixed-source dependencies remain explicit gaps.

A full reducer, SafeSubjoin implementation or backtracking engine is not implied
by the retained work. Any such change needs a new counterfactual and ownership proof.

Validation uses ordinary SQL and independent oracles, native-row assertions,
500M-row performance controls, typed/composite keys, duplicate branches, skew,
NULLs, dictionary changes, COUNT/SUM/MIN/MAX/AVG and ordinary DECIMAL consumers.
Existing tests cover ordering barriers, memory failure, cancellation, reopen and
Parquet fallback. Gains must survive preparation and stronger existing strategies.

## Research informing the design

| Primary source | Relevant result and design choice |
| --- | --- |
| [Looking Ahead Makes Query Plans Robust, PVLDB 2017](https://www.vldb.org/pvldb/vol10/p889-zhu.pdf) | Looking-ahead information passing supplies the common-scan baseline and adaptive filter ordering. Its star-join setting does not solve transfer through a different key column. |
| [Predicate Transfer, CIDR 2024](https://www.vldb.org/cidrdb/papers/2024/p22-yang.pdf) | Propagate necessary membership conditions through multiple relations, extending a one-hop Bloom join. This is the model for customer-key to order-key transfer. |
| [Robust Predicate Transfer, SIGMOD 2025](https://arxiv.org/abs/2502.15181) | LargestRoot and SafeSubjoin connect reduction scheduling with acyclic-query robustness. A few safe transfer edges do not inherit the paper's guarantees; non-gamma-acyclic cases require additional join-order reasoning. |
| [RPT+: Robust Predicate Transfer with Dynamic Execution, PVLDB 2026](https://yimingqiao.github.io/files/rpt_plus.pdf) | Asymmetric transfer plans, cascaded filters and runtime abandonment address redundant filter construction and scan/materialization overhead. Adopt those cost concerns; do not copy its fixed thresholds or implement mandatory two-pass reduction. |
| [Efficiently Compiling Dynamic Code for Adaptive Query Processing, 2022](https://www-db.cs.tum.edu/~schmidt/papers/dynamic-blocks.pdf) | Umbra adapts compiled execution without repeatedly compiling whole queries. Bind execution data separately and reuse our existing batch-level selection mechanism before considering new dynamic-code machinery. |
| [TreeTracker Join](https://arxiv.org/abs/2403.01631) | Backtracking and removal of failing tuples offer an alternative to eager reduction. Mutation of shared frozen builds conflicts with our present ownership model; retain this as a later counterfactual, not a second implementation in this milestone. |

RPT+ was implemented in a research fork of DuckDB v1.3.0. Our DuckDB v2.0.0
profiles demonstrate dynamic filtering, not that the installed binary runs RPT+.
