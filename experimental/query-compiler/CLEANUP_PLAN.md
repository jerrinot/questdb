# Compiler cleanup

Wall-clock profiling and benchmark suites remain paused while the host is in use.
Preserve the measured algorithms, generated memory accesses and existing QuestDB
ownership/scheduling. Validate structure changes with focused correctness checks
and before/after generated IR; make no new performance claim.

1. Give prepared code one resource owner. Validate before allocation; preserve
   the primary failure, close every adopted resource, and count every kernel.
2. Consolidate aggregate lifecycle lowering. Keep initialization, update, merge,
   prefetch and finalization with each bound aggregate. Separate binding/emission
   from acquired-slot execution without adding native indirection.
3. Bind map variants explicitly and share named native-view offsets with core.
4. Remove retired behavior switches. Retain only documented controls used by current counterfactual
   harnesses and instrumentation; keep normal decisions fixed during compilation.
5. Separate the compiler entry point from aggregate execution, projection and
   join contracts. Remove forwarding that adds no boundary semantics.
6. Retire the superseded SQL adapter; preserve controls still used to measure
   native execution floors and shared IR/backend behavior.

Complete one item at a time. Record checks and any deferred work here; keep raw
comparison artifacts outside the repository.

## Completed

- Prepared code now adopts each resource immediately, validates capacity before
  allocation, closes in reverse order without masking the original failure, and
  includes every generated kernel in compilation accounting. Two fault-injection
  tests reproduced the cleanup/accounting defects before the fix and pass now.
- `AggregateUpdates` owns each aggregate's initialization/update/merge/prefetch/
  finalization emission. `AggregateSlot` implements the acquired-slot contract
  directly; the forwarding `SlotUpdater` is gone. The slot entry cancellation
  check is preserved. Its direct-call test now accounts for that entry check.
- Named map factories select an explicit layout. `NativeMapView` constants are
  shared by core map writers/publishers and generated probes/scans.
- Removed 13 established-path/redundant switches. Bounded-state capacity, shared
  VARCHAR, batching/hint counterfactuals and profiling remain documented in README.
  No replacement configuration framework was added.
- `QueryCodeGenerator` is the planning entry point. `GroupByKernel`, `CompiledJoin`
  and `CompiledProjection` own their execution contracts. Registration is now
  `setQueryCodeGenerator(new ResolvedQueryCompiler(...))`; temporary external
  harnesses using the previous names must be recompiled before benchmarking resumes.
- Removed `ExperimentalSqlCompiler`, `SqlFrontend`, their adapter-only tests and
  `ReuseDemo`/`run-reuse.sh`. Kept the standalone DSL executors and handwritten
  controls used by current backend/algorithm experiments.

Validation: core Maven compilation and all prototype main/test sources compile.
35 focused checks pass: 2 ownership, 12 kernel cases across LLVM/AsmJit/C,
18 resolved SQL cases including legacy/fiber workers, and 3 join cases. The tests
exercise actual native execution, cleanup failures, mutation, cancellation/reopen,
merge/finalization, bounded/shared maps, sorting/LIMIT, selection and projection.
All 58 captured kernels from nine SQL shapes emit byte-identical LLVM IR before
and after cleanup. This is bounded equivalence evidence, not a throughput claim.

Wall-clock profiling and ClickBench remain paused. The interrupted full-suite run
is excluded from performance conclusions; the earlier staged VARCHAR prefetch
candidate is preserved. Raw checks are outside the repository in
`/tmp/query-code-cleanup`; no result archive was added here.
