# Subquery cardinality recovery: staged implementation

## Evidence and scope

TPC-H SF 0.1 at `eff7c00` has nine measurements whose subtrees contain four joins.
Four severe errors belong to q18; three repeat the final grouping error through
aggregation/sort/output. Its HAVING filter estimates 38,493 groups instead of five;
the marker CASE filter independently applies 25% selectivity, estimating 150,143
rows instead of 35. The final grouping estimates 150,143 instead of five.
These are estimation measurements, not controlled latency measurements.

The canonical pipeline already has `MarkJoinToSemiJoin`, but that rule recognizes
only an immediately adjacent selection with a bare marker conjunct. More
importantly, `SubqueryToJoin` currently recognizes direct semi/anti semantics only
when the *whole* selection predicate is a subquery. An IN conjunct inside a larger
AND is lowered unnecessarily to two marks plus CASE. Avoid that information loss
at the producer before adding more complex downstream mark elimination.

Keep phases independently reviewable. Preserve append-only IR and unrelated work.
Do not extend null-sensitive rewrites based on statistical estimates.

## Implementation checkpoint

Phase 1 producer lowering is implemented in `SubqueryToJoin`; no pipeline reorder
or new cardinality model is introduced. Two new shape regressions failed before
implementation and pass afterward. The core suites pass with default features
(287 tests) and without default features (275 tests). All 18 release feature SLT
files and all 22 TPC-H result SLT files pass. The feature fixture asserts semi/anti
plan shapes, and covers nullable/empty inputs, duplicates, OR, correlation, and a
grouped-IN/HAVING query. The direct-physical-path unit fixture was changed from
nullable VALUES (which hits an existing typed-NULL import limitation) to Arrow
MemTables; its recheck passes and explicitly verifies direct physical execution.
`cargo clippy --workspace --all-targets --locked -- -D warnings` also passes.
The debug connector test link emits a macOS compact-unwind-size warning, but the
test executes successfully.
Changed-file Rust formatting passes; workspace formatting still reports unrelated
existing import formatting in `runtime_statistics.rs` and `optd/core/src/lib.rs`.
No new q-error or latency claim is made for this phase.

The separate literature comparison is recorded in
[`2026-11-17-fent-aggregate-estimation-vs-lpbound.md`](../../investigations/2026-11-17-fent-aggregate-estimation-vs-lpbound.md).
Phase 1b, the remaining Phase 2 domain work, and Phases 3–4 remain planned.
The functional-dependency slice of Phase 2 is implemented and validated below.

## Phase 1 — Preserve filter-context semi/anti semantics (implemented)

1. Recursively flatten only top-level AND in `SubqueryToJoin` selections.
2. Lower each whole conjunct using the existing direct-subquery logic:
   - positive IN and EXISTS -> LeftSemi;
   - NOT EXISTS -> LeftAnti;
   - NOT IN -> LeftAnti only with the existing two-sided non-null proof.
3. Retain ordinary predicates as residual selections. Lower genuine expression
   contexts (OR, CASE, projected Boolean values) using the existing mark logic.
   Keep unsupported nullable NOT IN behavior; never equate NOT IN with NOT EXISTS.
4. Thread the input through multiple eligible conjuncts without changing their
   scalar expressions or original arena payloads. Return Unchanged at fixpoint.
5. Do not introduce nullable marker truth-table rewrites or move predicates across
   outer joins, limits, or aggregations in this increment.

Acceptance / verification:
- Core shape tests for nested AND, positive IN, EXISTS, NOT EXISTS, non-null NOT IN,
  multiple subqueries, residual predicates, unchanged scalar predicates and fixpoint.
- Negative tests for nullable NOT IN and IN under OR/projection.
- SQL fixtures covering duplicates, NULL LHS/RHS, empty RHS, mixed conjunctions,
  correlated subqueries, and a small grouped-IN/HAVING q18 analogue.
- Assert the positive conjunct actually becomes LeftSemi with no marker CASE;
  passing results through DataFusion fallback alone are not coverage.
- Run focused core tests before/after, core tests without default features,
  release-mode feature SLTs on direct physical execution, formatting of changed
  Rust files, and workspace Clippy. Existing unrelated formatting defects stay out
  of this change.

Phase 1b, separately: eliminate already-materialized marker filters through proven
row-preserving wrappers, using liveness and exact SQL truth tables. Retain marks
when their values remain observable. This requires dedicated parent/liveness tests;
it is not needed to prevent the q18 producer from manufacturing marks.

## Phase 2 — Propagate surviving key domains and grouping dependencies

Checkpoint baseline: Phase 1 is committed as `d56bb72`, on top of `cbb9b15`'s
PostgreSQL-style semi/anti policy. That newer policy intentionally avoids deriving
matched/complementary key domains from semi/anti selectivity. Preserve it: do not
reinstate the former coverage/fanout or HLL-intersection policy as part of this work.
The first Phase 2 slice instead tracks logical functional dependencies from
provider-asserted unique keys (per scan instance) and aggregation grouping keys.
Dependencies survive subsets/duplicates and safe aliases but are dropped from
null-supplying outer-join sides. Group estimation can then remove FD-dependent
keys without changing emitted columns or join-row selectivity. Observed NDV equal
to row count is not a key proof. Nullable-key semantics remain the catalog
provider's responsibility under the existing `UniqueKey` contract.

### Phase 2a checkpoint — logical dependencies implemented

- Added `ValueFunctionalDependency` and equality-aware transitive closure to logical
  facts. Only declared catalog keys and grouping semantics create dependencies.
- Added estimator-only grouping-key reduction in `analysis/grouping_dependencies.rs`;
  output columns and PostgreSQL-style semi/anti selectivity are unchanged.
- The new before/after regression changes a declared-key grouping estimate from
  300 to 100 after threefold duplication; the undeclared-key control stays at 300.
- Safety tests cover composite determinants, partial scan keys, self-join instances,
  filter/projection/rename/direct-alias propagation, inner equality, outer/single
  joins, aggregation outputs, cyclic/transitive dependencies and old serialized facts.
- Verification: 297 core tests, 284 without default features, 22 release TPC-H result
  SLTs, 18 release feature SLTs, and workspace Clippy all pass. Changed analysis
  modules pass rustfmt; existing unrelated workspace formatting differences remain.
- Limitation: catalog key dependencies require provider-asserted metadata. The runtime
  collector does not infer keys from measured uniqueness, so no q18 gain is claimed
  without verifying the necessary key metadata actually reaches the estimator.
- No new q-error or controlled query-runtime benchmark was run for this checkpoint.

### Phase 2b checkpoint — exact semi-join predicate consequences implemented

A positive semi join on same-typed column equality can transfer RHS-proven literal,
range, and non-null constraints onto the matching preserved-side key. This is a
logical consequence of a match, not an inferred NDV domain or a change to the
PostgreSQL semi-selectivity formula. It must not apply to anti/mark/outer joins,
OR predicates, ambiguous column ownership, or cross-type coercions without proof.
Known literal values also belong to the dependency closure as constants, allowing
grouping to recognize that such keys add no independent domain.

The new before/after reproducer initially estimated 10 semi rows and then 1.53534
rows after a redundant LHS equality. The regression now asserts the invariant that
repeating the proven filter preserves the semi estimate, rather than hard-coding
a selectivity model's point. Concurrent work changed PostgreSQL variable-NDV lookup;
that work and its extracted `semi_anti_tests.rs` are separate from this checkpoint.
Tests cover both equality orientations, null-safe equality, exact literals,
conjunctive ranges, contradictions, OR/computed-key refusal, cross-type/non-finite
literal refusal, and anti/mark/outer/single negative boundaries. Grouping the known
constant key has an upper bound of one, not an unconditional exact one-row claim.

Verification on the combined working tree: 301 core tests, 288 without default
features, 22 release TPC-H result SLTs, 18 release feature SLTs, and workspace Clippy
pass. Earlier runs caught the old hard-coded point assertion and two concurrent
floating-point test assertions; the latest full rerun passes after those test
expectations were corrected. Analysis modules pass rustfmt. No new q-error or
controlled latency benchmark was run, and HAVING estimation remains unchanged.

### Remaining Phase 2 work

1. Audit semi-join key coverage and equality-class NDVs before introducing new
   metadata. A semi join restricts the join-key *domain*, not just independent rows.
2. Cap surviving join-key NDV by both participating domains/intersection; keep
   non-key occupancy estimates separate. Preserve null rejection and composite
   key semantics. Do not multiply independent NDVs for equivalent columns.
3. Propagate the restricted domain through projection, rename, lossless aliases,
   and subsequent equality joins using logical value identities.
4. Extend grouping estimation to reduce grouping tuples by proven functional
   dependencies: e.g. order key determines date/price/customer attributes. Distinguish
   semantic dependencies from catalog point estimates; retain population-validity
   checks for FK/unique statistics and null-supplying joins.
5. Prefer existing equality/domain structures over adding a parallel lineage system.

Acceptance:
- Known k surviving unique RHS keys constrain the downstream matching-key NDV to k,
  even if the fact side has many duplicates; test skew and multi-column keys.
- Adding FD-dependent grouping columns does not inflate group estimates.
- Counterexamples across outer joins, nullable keys, lossy casts, and unrelated
  domains retain conservative fallback.
- Focused core CE tests, connector coverage for grouped IN, and q18/q21 regression
  measurements. Fixing domain propagation alone does not fix the HAVING estimate.

## Phase 3 — Estimate aggregate-result predicates

1. Use the delegated Fent/Birler/Neumann vs LpBound comparison to record the model
   boundary: fitted distributions produce selectivity point estimates; norm/degree
   bounds are not a distribution of grouped SUM values.
2. Add explicit numerical-moment statistics (sample count, first/second/third moments)
   and group-size estimates with compatibility/provenance metadata. Decide storage
   ownership in catalog vs derived profiles before adding fields.
3. Start with a narrow numeric COUNT/SUM/AVG estimator. Derive random-sum moments
   with documented independence/correlation assumptions; account for group-size
   variability, NULL exclusion and empty scalar aggregates. Do not approximate a
   random sum by multiplying moments by a single average group size.
4. Fit/evaluate the chosen distribution (Umbra skew-normal is a candidate), then
   estimate supported HAVING thresholds using its CDF. Keep unsupported types,
   unstable fits, and insufficient samples on explicit fallback. Normalize safe
   literal casts without inventing statistics.
5. Tail risk matters: q18 selects five of 150,000 groups. Include rare-tail and
   bounded/discrete distributions, not just average q-error. Ordinary row samples
   contain incomplete groups; do not evaluate their SUM as a population SUM.
6. Keep model estimates distinct from logical proofs and population-relative bounds;
   historical statistics never become logical Exact solely because sample coverage
   equals the historical population.

Acceptance:
- Synthetic known-distribution tests, variable group sizes, skew/correlation,
  all-NULL/empty input, threshold extremes, unsupported/failing fits.
- Independent analytic or exact-data oracle for formulas and tail probabilities.
- q18 inner HAVING and root q-error, broader TPC-H/JOB summaries, and measured model
  overhead. Report regressions rather than promising improvement on all distributions.

## Phase 4 — Truth distributions for marks that must remain

1. Record P(TRUE), P(FALSE), P(UNKNOWN) (or equivalent counts) separately from marker
   NDV and non-null frequency, using the join match model and exact null semantics.
2. Give derived markers an explicit producer identity. Preserve/invalidate their
   statistics as population-changing operators demand; do not key behavior by names.
3. Consume these statistics in bare-marker filters, NOT, IS TRUE/IS FALSE/IS NULL,
   and recognized generated CASE forms. Respect shared-event correlation between
   match/unknown markers: multiplying independent marginals is unsound.
4. Retain the existing full SQL expression semantics for projected IN, NOT IN and OR.

Acceptance:
- Exhaustive TRUE/FALSE/UNKNOWN truth-table tests, empty/nonempty RHS, NULL LHS/RHS,
  duplicates, projected values and shared marker dependencies.
- Non-eliminable mark estimates match equivalent semi/anti formulations where those
  formulations are semantically valid, without charging selectivity twice.

## References

- Fent, Birler, Neumann, *Practical Planning and Execution of Groupjoin and Nested
  Aggregates*, VLDB Journal 2023, §5: https://mediatum.ub.tum.de/doc/1763824/1763824.pdf
- Moerkotte, *Cardinality Estimation for Having-Clauses*, PVLDB 18(1), 2024:
  https://www.vldb.org/pvldb/vol18/p28-moerkotte.pdf
- Neumann, Leis, Kemper, *The Complete Story of Joins (in HyPer)*, BTW 2017,
  §§3.3/4.4: https://15721.courses.cs.cmu.edu/spring2019/papers/23-optimizer2/hyperjoins-btw2017.pdf
- Calcite `RelMdUtil.computeSemiJoinSelectivity`, `RelMdDistinctRowCount`;
  CockroachDB `statistics_builder.go` (semi-join/grouping/equivalence propagation).
