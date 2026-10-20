# Continuous Benchmarking and Optimizer Health

Status: local emitter implementation; benchmark publishing/nightly workflows are not connected.
See [benchmark_emitter.md](benchmark_emitter.md) for supported commands and actual measurement
boundaries. Unchecked tasks below are outstanding, not claims of implemented functionality.

## First delivery: TPC-H/JOB planning and execution

Use existing table registration and query fixtures with a small structured collector around production
optimizer/direct-physical execution helpers. Keep `profile_passes` unchanged as a developer debugging
tool, not the headline benchmark. Emit planning/execution medians for `customSmallerIsBetter`, raw
samples, source/configuration metadata, explicit coverage/failures, and per-pass diagnostics. Do not
silently fall back to a different execution path. Criterion and richer diagnostics remain later passes.

The local emitter's total planning includes statistics/import, with a separately recorded combined
statistics/import phase. Data is hashed before measurement and warm-ups execute queries, so label
results warm-cache. Semantic correctness remains explicitly unverified by the collector; existing SLT
correctness CI remains authoritative. No statistical significance is inferred from the initial samples.

Repository ownership is decided: benchmark workflows run in `optd`; the separate `optd-web`
repository owns the frontend, benchmark history, and Pages deployment. Source attribution and warning
comments stay in `optd`. Cross-repository publication requires a narrowly scoped credential.

**First-pass acceptance:** two successful nightly-style runs cover explicit TPC-H and JOB query
manifests, append correctly attributed comparable planning/execution points to private-repository
history, and produce a dashboard viewable on that repository's Pages with explicitly approved visibility. Record unsupported queries and timeouts explicitly; do not claim full-suite success when
coverage is partial. A profiler/parser/publisher error fails visibly; performance changes do not block
CI. Criterion, flamegraphs, and richer diagnostics must not block reaching this milestone.

## Goals

Run TPC-H and Join Order Benchmark (JOB) nightly against a recorded optd source revision. Track
planning and execution performance over time, retain enough optimizer evidence to explain changes,
and distinguish correctness failures from optimizer-health warnings and measured runtime regressions.
Use [github-action-benchmark](https://github.com/benchmark-action/github-action-benchmark) for numeric
history and charts. Store history, reports, and dashboard files in a separate private repository and
host the dashboard using that repository's GitHub Pages.

Do not replace the existing correctness CI or treat a different plan as proof of a regression. Do not
use subtree cardinality-regression execution as a proxy for end-to-end query runtime.

## Repository architecture

**Decision: source-owned execution, with frontend and deployment in `optd-web`.**

| Location | Responsibility |
| --- | --- |
| `optd` | Collectors, query fixtures, nightly/manual benchmark workflows, source attribution, CI summaries and warning comments |
| `optd-web` / `main` | Shared web-interface and benchmark frontend source, site build, Pages deployment workflow |
| `optd-web` / `benchmark-results` | Action-managed numeric history and approved compact reports |

The `optd` workflow resolves `main` to one immutable SHA for all suites. Initial manual runs are
restricted to trusted revisions on `main`. Never execute arbitrary fork/PR code with publishing
credentials or on a trusted self-hosted benchmark runner. Future read-only PR measurements are a
separate path and do not receive destination write access.

Separate measurement from reporting/publication. Measurement jobs use read-only permissions and
checkouts with persisted credentials disabled. A publisher receives validated result artifacts and is
the only job granted a destination write credential. Do not execute uploaded scripts or interpolate
query names, commit messages, or other report fields into shell commands.

### Credentials, reporting, and deployment handoff

There are two permission domains; do not grant a single broad token access to both unnecessarily:

- Source reporting uses `optd`'s `GITHUB_TOKEN` with the minimum permission required for the selected
  comment API (commit comments require Contents write). Keep write permission out of measurement
  jobs. The action's built-in comments are commit comments, not PR review comments.
- Destination history writes use a GitHub App installation token or fine-grained PAT scoped to
  `optd-web` Contents read/write. `optd`'s `GITHUB_TOKEN` cannot write to `optd-web`.
- The action exposes one `github-token` input, so a destination-only credential must not be assumed
  capable of also commenting on `optd`. Prefer a source-token comparison/reporting step using external
  history, followed by a destination-token history-write step with comments disabled. Verify both
  steps compare against the same prior history and append only once. If that cannot be done cleanly
  with the pinned release, use a small explicit reporting adapter rather than broadening token scope.

Use the action's `gh-repository` to point to `github.com/cmu-db/optd-web`, `gh-pages-branch` to select
`benchmark-results`, and `benchmark-data-dir-path` to select the history directory. Keep maintained
frontend source on `optd-web/main`; the action-generated default HTML is not the shared web app.

After a successful history push, the `optd` publisher sends `repository_dispatch` with event type
`benchmark-updated` to `optd-web`, using the destination-scoped App/PAT (Contents write). This runs
the current deployment workflow on default branch `main`, avoiding a stale workflow copy on the
history branch. Changes to frontend source and manual dispatch on `main` also trigger site builds.
Resolve the latest history to an immutable SHA per build; dispatch payloads do not select arbitrary
code or data revisions. Build-time validation parses the action's `data.js` JSON envelope without
execution and emits content-addressed JSON plus an availability manifest for the custom frontend.
Fail visibly if history publication or dispatch fails. Serialize deployments and record both frontend
and history revisions; rapid history updates may coalesce into one build. Verify the handoff in a
real Actions pilot. This dispatch is explicit integration plumbing, not automatic action behavior.

### Source attribution and visibility

Since the benchmark action executes in `optd`, its repository context naturally targets `optd` for
metadata and comments even when history is written elsewhere. Preserve the source repository URL,
SHA, commit URL/message/timestamp, workflow run URL, and measurement timestamp. Verify attribution
for scheduled/manual runs and pinned checkouts; do not assume `ref` overrides a push payload's
`head_commit`. No fabricated event payload is required for this architecture.

Logs, summaries, raw artifacts, and source comments in public `optd` may be publicly accessible even
though `optd-web` is private. Treat publication privacy as an end-to-end decision, not just a destination
repository setting; never include secrets or private-only diagnostics in source reporting.

## GitHub Pages and privacy

The requested hosting target is the private benchmark repository's GitHub Pages. Repository privacy
and website visibility are separate settings:

- A public Pages site may intentionally be served from a private repository on a supported plan.
  Everything in the published site is public, including embedded query names, source metadata,
  histories, and downloadable reports.
- Access-controlled private Pages project sites require an organization using GitHub Enterprise Cloud
  and an eligible private/internal project repository. A private repository alone is not sufficient.
- Confirm the owner's plan and desired audience before enabling deployment. Do not silently publish
  publicly if private hosting is unavailable. Keep generated files in the private repo until the
  hosting decision is resolved.

See GitHub's [Pages visibility documentation](https://docs.github.com/en/pages/getting-started-with-github-pages/changing-the-visibility-of-your-github-pages-site).
For a public site, publish an explicit allowlist of dashboard files, not all raw logs or job artifacts.
Prefer GitHub's Pages deployment workflow with a dedicated publishing job and narrowly scoped Pages
permissions. Verify that publication triggers work with the selected authentication/deployment method;
do not assume a branch push made with `GITHUB_TOKEN` triggers another workflow or Pages build.

## Existing infrastructure and gaps

- `optd/connectors/datafusion/src/setup.rs` registers local TPC-H SF 0.1 and JOB Parquet tables.
  Existing download scripts and `.github/workflows/scheduled.yml` provide data-cache examples.
- `src/profiling.rs` and `src/bin/profile_passes.rs` in that connector provide synthetic workloads,
  warm-up, repeated runs, and pass-duration TSV output. These are reusable examples, not a TPC-H/JOB
  performance harness.
- `optd/core/src/optimize/mod.rs` exposes `PassProfile` (iteration, pass index/name, changed/unchanged
  or error, duration) and `PassTrace` snapshots. These do not establish a join-search counter API.
- `optd/connectors/datafusion/src/runner.rs::default_pass_manager` defines the canonical pipeline.
  The benchmark must exercise the real supported execution path, not a duplicate optimizer pipeline.
- The CLI exposes direct and logical physical-plan inspection. The collector needs structured phase
  timing, chosen-plan output, and explicit path reporting rather than parsing CLI display output.
- [The cardinality regression harness](cardinality_regression.md) already supports isolated queries,
  timeouts, checkpointing, and estimator evaluation. Its subtree executions/full-scan sketch collection
  are expensive diagnostic operations and must remain outside normal execution timing.
- No dedicated Cargo benchmark targets or Criterion/Divan dependencies were found during inspection.
  Existing TSV is not compatible with the action's libtest `cargo` parser. Emit its supported custom
  JSON format instead; no nightly Rust dependency is needed for this integration.

## Workloads and measurement contract

Nightly jobs should run both TPC-H and JOB against one recorded source revision. Use the existing
TPC-H SF 0.1 data initially; record the exact JOB snapshot, query manifest, and data identities.
Inventory the repository query sets before promising complete suite coverage. Every selected query
must end in an explicit success, unsupported, mismatch, crash, error, timeout, or resource-limit status.
Maintain a reviewed unsupported-query manifest; new unsupported cases are not successful benchmarks.

Use per-query child processes with deadlines, memory limits where available, checkpoints, and a suite
wall-clock budget. Preserve completed results if a later query crashes. Establish runner capacity and
query coverage with a pilot before choosing nightly deadlines. Expensive suites should not overlap on
the same machine; run suites sequentially or on separate equivalent dedicated runners.

### Timing boundaries

| Metric | Definition |
| --- | --- |
| SQL planning/import | Parse/analyze SQL and import into optd, excluding separately measured statistics preparation |
| Statistics preparation | Statistics acquisition or loading; record cache/snapshot mode explicitly |
| Optimizer time | Wall-clock canonical pass-manager execution plus final root resolution; exclude plan rendering |
| Per-pass time | Each invocation's duration, iteration, pass identity, and outcome; aggregate by pass for drill-down |
| Physical planning | Convert the chosen optimized plan into the executable DataFusion physical plan |
| Total planning | Wall-clock complete planning path; publish the included phases and stats policy explicitly |
| Execution runtime | Execute a fresh instance of the selected physical plan through full result consumption, without replanning or display formatting |

Do not label the sum of pass durations as total planning time. Repeated planning must start with fresh
query/optimizer state; repeated execution must not reuse an already-consumed execution stream. Keep
statistics state equivalent across compared samples. Record partition count, thread count, execution
path, and any fallback; never combine direct-physical and logical-fallback timings in the same series.

Perform correctness comparison separately from timed samples using existing SLT/reference machinery
where applicable. Preserve SQL ordering semantics, duplicate rows, null handling, and documented
floating-point tolerances. Row counts alone do not establish correctness. Do not report correctness as
verified for queries without an applicable oracle. Consume results during timing but keep expensive
canonicalization, comparisons, plan serialization, and full tracing outside the timed region.

Use warm-up and repeated samples, retaining raw observations. Select sample counts and minimum
measurement duration during a pilot, with smaller bounded counts for expensive queries. Distinguish
warm-cache runs from cold-cache experiments; do not call an ordinary fresh process a cold OS cache.
Record CPU, memory, OS, runner image, Rust version, dependency lock hash, build profile, dataset/query
hashes, statistics snapshot, benchmark schema version, and effective configuration.

## Join-ordering Criterion microbenchmarks

Add a separate Criterion benchmark target in `optd-core` for join-ordering performance. This is a
complement to nightly TPC-H/JOB planning and execution, not a substitute. Keep it independent of
DataFusion, SQL parsing, dataset downloads, and query execution so changes in join search can be
isolated. Criterion is a proposed dev-dependency; it is not currently installed in this workspace.

Build deterministic IR/catalog fixtures using the existing join-ordering tests and design as the
starting point (see [join ordering](join_ordering_design.md) and [query hypergraphs](query_hypergraph.md)).
Cover chain, star, cycle, and dense join graphs with increasing relation counts, plus selected
predicate/selectivity and disconnected-graph cases where supported. Bound the dense cases and record
search limits so the suite does not accidentally benchmark unbounded combinatorial explosion.
Version fixture definitions, cost model, statistics, and algorithm settings alongside metric identity.

Define two distinct measurement families:

- **Join-ordering pass:** run the real pass on a prepared optimizer context, including search setup,
  enumeration/costing, selection, and plan reconstruction inside the pass.
- **Search-only:** add only if an existing or deliberately introduced API exposes a defensible search
  boundary. State exactly which preparation and reconstruction costs are excluded; do not duplicate
  the production algorithm merely to obtain a benchmark entry point.

Use Criterion batched setup to create fresh equivalent mutable state outside each measured operation.
Account explicitly for teardown and ensure setup does not cache the answer across samples. Consume the
result with `std::hint::black_box`; check expected plan validity and fixture properties outside timed
loops. Retain search counters and selected-plan fingerprints in companion diagnostics to distinguish
faster code from less search or changed optimizer behavior. Disable full tracing during measurement.

Use stable benchmark IDs, for example `join_ordering/pass/chain/08`, and retain Criterion raw estimates,
samples, and baseline comparison reports. Verify the pinned Criterion version's machine-readable
output and implement a tested conversion to `customSmallerIsBetter` with explicit time units and
uncertainty metadata. The action's `cargo` parser is not a Criterion parser; do not parse unstable
human-readable console summaries. Criterion's local statistical comparison complements, but does not
replace, environment matching, practical effect-size thresholds, and repeated-run confirmation.

Run a bounded microbenchmark matrix nightly and on manual dispatch. Larger scaling experiments can
run separately. Expose join-ordering trends as a distinct dashboard group rather than mixing their
numbers into TPC-H/JOB runtime aggregates. Initially report microbenchmark slowdowns as warnings;
calibrate before introducing a performance gate.

### Flamegraphs for microbenchmarks

Support opt-in CPU flamegraphs for a single selected Criterion benchmark using `cargo-flamegraph`
(or an explicitly configured Criterion profiler). Run these as separate diagnostic invocations, never
use profiled timings as regression samples. Offer manual dispatch by trusted benchmark ID and optional
follow-up profiling for a reproducible warning; avoid profiling every case every night.

Build the benchmark with usable debug symbols in an optimized bench profile. Record profiler version,
sampling frequency, OS/CPU, source SHA, fixture ID, and exact command. On Linux, audit `perf` availability
and runner permissions first; do not grant broad privileged access or change host-wide security policy
as an automatic workflow side effect. Prefer a controlled profiling runner where necessary. Provide a
platform-appropriate local alternative when CI cannot profile.

The intended command shape, to validate once the target exists, is:

```sh
cargo flamegraph -p optd-core --bench join_ordering -- \
  --bench 'join_ordering/pass/chain/08' --profile-time 30
```

Confirm filtering and profiling-mode arguments with the pinned tool versions. Ensure the recording
is dominated by the selected workload rather than Criterion startup/setup; batched fixture creation
may still appear in a whole-process flamegraph even when excluded from Criterion timings. Label that
scope honestly or use profiler hooks around the intended region. Retain raw stack samples where
supported and generate an SVG with a sanitized, stable filename. Treat flamegraph widths as sampled
CPU attribution, not per-pass wall-clock time or proof of a slowdown.

Link flamegraphs from the per-benchmark drill-down with explicit source/baseline provenance. Keep
large raw recordings in expiring private artifacts; publish only approved sanitized SVGs to Pages.
Check symbol/source-path disclosure against the site's visibility, and never let profiling failure
mask correctness failures or invalidate otherwise valid unprofiled measurements.

## Optimizer-health evidence

Retain structured per-query diagnostics alongside scalar measurements:

- Pass invocation counts, changed/unchanged/error counts, iteration counts, and pass timings. Define
  "pass fired" as a changed invocation; report invocations separately. Rule-match counts need separate
  instrumentation and must not be inferred from `PassResult`.
- Join-search counters, with a documented definition: candidate states considered, alternatives
  costed, retained states, and search-limit/fallback events where supported. These require an API audit
  and likely instrumentation; do not invent values or confuse invocation counts with explored orders.
- Selected logical join tree and final executable physical plan, in readable and structured forms.
  Compute versioned normalized fingerprints, removing unstable arena IDs but retaining physical
  operators, join orientation, predicates, and other meaningful choices.
- Estimated cost with cost-model identity/version and units; per-operator estimated cardinalities.
  Missing estimates are null with a reason, not zero. Cost-model/statistics changes make raw costs
  incomparable unless explicitly rebaselined.
- Query status, output row count, correctness evidence, timeouts, resource limits, and failure logs.

Full pass snapshots and subtree actual-cardinality/q-error collection are diagnostic modes. Enable
these on selected queries or follow-up runs; do not let their overhead contaminate headline timing.
A final-plan actual cardinality from execution metrics can be retained when its semantics are clear.

## CI signal classification

| Class | Examples | Initial policy |
| --- | --- | --- |
| Hard failure | Semantic mismatch, crash, verifier/invariant failure, unexpected query failure or unsupported-path regression | Fail correctness/health status; retain diagnostics |
| Regression warning | Optimizer-time explosion, comparable estimated-cost increase, join-order/physical-plan fingerprint change, search-limit event | Report and link evidence; plan changes alone do not fail CI |
| Benchmark regression | Statistically supported execution slowdown exceeding a practical effect-size threshold | Warn initially; gate only after runner and detection policy are calibrated |
| Infrastructure / inconclusive | Missing dataset, runner failure, insufficient samples, incompatible baseline | Separate explicit status; never green or a fabricated performance regression |

A timeout needs attribution: an optimizer deadline can indicate a planning explosion, an execution
limit may require a follow-up runtime investigation, and runner termination can be infrastructure.
Do not convert censored/failed measurements to zero or to successful numeric timing samples. Always
publish a coverage/status manifest so missing queries cannot make suite aggregates look better.

Keep correctness failure distinct from a calibrated performance gate. Upload diagnostics and publish
valid evidence even when queries fail, then propagate the hard-failure status; do not mask failure with
`continue-on-error` or overwrite it with a successful dashboard job.

## Baselines and regression statistics

The benchmark action's built-in percentage alert compares a scalar to prior history. It is useful for
charts and provisional warnings, but is not a statistical-significance test. For example, `150%` means
1.5 times the reference measurement, not a 150% increase.

Store raw samples separately and implement a versioned comparison policy. Compare only matching
query/data/configuration/environment identities. Keep a designated accepted baseline, rather than
automatically accepting a regression because it was the most recent run. Failed correctness runs
must not replace that baseline. Changing hardware, data, execution path, measurement boundaries, or
cost models requires an explicit new comparison cohort or rebaseline.

Calibrate an effect-size threshold and confidence procedure from repeated pilot runs. A candidate is
an interval for the candidate/baseline median-runtime ratio plus a minimum practical slowdown, with
minimum sample requirements and explicit treatment of multiple-query comparisons. Same-machine,
interleaved baseline/candidate reruns are preferable for confirming warnings from nightly historical
comparisons. Do not promise statistical significance from three samples or noisy hosted-runner
measurements. Preserve sample counts, uncertainty, and inconclusive results in the report.

## History, dashboard, and retention

Use `customSmallerIsBetter` for planning and execution durations, with stable names such as
`tpch/sf0.1/q01/direct/optimizer_ms` and `tpch/sf0.1/q01/direct/execution_ms`, partitioned by a versioned
comparison cohort. Validate finite nonnegative values, expected names, units, and coverage before
publication. Diagnostic counts and estimated costs need context and should not all become standalone
regression charts. Physical plans are artifacts, not scalar benchmark values.

Keep the overview compact: suite coverage, correctness status, planning/execution trends, aggregate
relative performance on a fixed successful query set, and the largest changes. Provide per-query
charts and drill-down links for pass breakdowns, join-search counters, costs/cardinalities, plan diffs,
and raw samples. A suite aggregate must identify its query set and omissions; do not sum per-query
medians and call the result measured suite wall time.

The action generates numeric history/dashboard assets; richer status tables, significance decisions,
and plan comparisons require a small companion report/index. Verify compatibility with its generated
`index.html` before customization. Record a unique run ID (source SHA plus workflow run/attempt) so
reruns are explicit and retries do not silently duplicate points.

Serialize publication to a single history branch; use non-force pushes and bounded fetch/reapply
retries. A concurrency group only coordinates writers within one repository and can replace pending
runs; do not promise an every-commit archive. Distinct suite artifacts must be associated with the
same run and their statuses retained even if one suite fails.

Keep compact numeric history/manifests and selected normalized plans in Git. Prefer compressed raw
samples and bulky diagnostics as private workflow artifacts with documented expiry initially. Choose
retention limits before enabling full tracing; artifacts expiring must not leave the dashboard claiming
permanent evidence availability. Never publish secrets, tokens, environment dumps, or data extracts.

## Delivery checklist

### Pass 1: local TPC-H/JOB emitter → benchmark history → GitHub Pages

#### Local implementation

- [x] Add a structured worker around shared production optimizer and direct-physical helpers; retain
  `profile_passes` as a debugging tool, unchanged.
- [x] Reuse existing TPC-H/JOB registration and SQL/SLT fixtures; reject ambiguous multi-query/setup
  SLT inputs and unsupported direct paths rather than silently using only part of a test or fallback.
- [x] Record warm-up/repeated samples, planning subphases, execution, per-pass outcomes, and final
  physical plans. Mark semantic correctness `not_checked` instead of inferring it from row counts.
- [x] Add per-query subprocess deadlines, checkpoints, failure statuses, and continued collection after
  failures. Refuse scalar exports for an incomplete/failed selected suite.
- [x] Emit validated planning/execution medians in `customSmallerIsBetter` JSON; retain raw samples and
  source/binary/data/query/configuration provenance with versioned comparison-cohort metric names.
- [x] Add local Python tests for medians, malformed/incomplete samples, duplicate names, failure
  retention, no partial-suite publication, and stale-output refusal.
- [x] Structure Python configuration, provenance, worker records, outcomes, manifests, and action
  metrics with dataclasses/string enums; validate JSON explicitly at ingestion and preserve v1 output.
- [x] Verify three focused Rust benchmark tests, 23 runner regression tests, 12 Python emitter
  tests, formatting, and workspace all-target Clippy. Run a release pilot for TPC-H q1 and JOB 1a
  with one warm-up and three measured samples each.
- [ ] Audit broader query coverage and establish runtime/resource budgets before promising full-suite
  support; the two-query pilot is not a full-suite correctness or performance certification.
- [ ] Add an `optd` workflow for nightly and manual runs of trusted source revisions. Use the pinned Rust toolchain, `protoc`, build caching, an explicit repetition
  count, job timeout, and non-overlapping execution.
- [ ] Schedule TPC-H and JOB nightly without competing for the same runner resources; use separate
  suite identities and record both suites against the same resolved source SHA.
- [ ] Upload the emitter's manifest/raw reports/logs even on failures; do not let publication or shell
  pipelines hide a failed benchmark run.
- [ ] Bind benchmark artifacts to the trusted source/build job before publishing commit attribution.
  The local emitter records collector-checkout metadata and a binary hash but explicitly labels
  executable build provenance unknown; arbitrary stale/custom binaries are not source-attested.
- [ ] Add the benchmark action pinned to a verified immutable release commit, with custom JSON input,
  stable suite identity, `summary-always: true`, and `fail-on-alert: false`. Configure source warning
  comments separately from destination writes; do not expect a destination-only token to do both.
- [ ] Separate measurement from credential-bearing publication. Configure least-privilege checkout,
  history writes, serialized publication, and no force pushes.
- [ ] Verify source attribution for the chosen workflow location, for both scheduled and manual runs.
  Record reruns explicitly and prevent accidental duplicate publication on retries.
- [ ] Supply an `optd-web` deployment workflow/template combining frontend from `main` and data from
  `benchmark-results`; verify history-update and frontend-update triggers, revision pinning, and
  deployment ordering. Keep bulky artifacts outside the site and existing correctness CI unchanged.
- [ ] Document exact local reproduction, generated files, required variables/secrets, branch bootstrap,
  permissions, first-run behavior, and troubleshooting.
- [ ] Run representative release emitter pilots and Rust formatting/tests/Clippy; run `actionlint`
  when publishing workflows are added.

#### Manual GitHub setup (requires owner action/approval)

- [x] Choose ownership: benchmark execution/reporting in `optd`; frontend/history/Pages in `optd-web`.
- [x] Identify the destination repository: [cmu-db/optd-web](https://github.com/cmu-db/optd-web).
- [ ] Verify repository access and initialize `benchmark-results` with owner approval.
- [ ] Confirm whether its Pages site may be public or must be access-controlled; verify plan support.
- [ ] Configure `BENCHMARK_RESULTS_TOKEN` (or App credentials) scoped to `optd-web` Contents read/write;
  verify metadata access and destination workflow triggering without granting source write access.
- [ ] Allow source reporting via `optd`'s job-scoped `GITHUB_TOKEN` and verify a warning comment targets
  the measured `optd` commit, not a destination history commit.
- [ ] Configure Pages and its deployment environment/permissions; do not publish before visibility is
  explicitly approved.
- [ ] Run twice on GitHub; verify history appends, source commit links, workflow summaries, and chart
  rendering. Verify a deliberately invalid result fails without corrupting existing history.
- [ ] Check site access as the intended audience, including an unauthenticated check for privacy.
- [ ] Enable the nightly schedule after the end-to-end pilot succeeds.

### Pass 2: expand coverage and correctness/resource controls

- [ ] Audit all-suite direct-physical coverage; maintain an explicit reviewed supported-query selection.
- [ ] Add an appropriate semantic correctness oracle outside timed regions; keep mismatch/crash/
  invariant failures distinct from warning-only performance changes.
- [ ] Split statistics collection from import more finely if needed; version timing changes explicitly.
- [ ] Add memory limits, suite-wide budgets, richer hardware identity, and reproducibility controls.
- [ ] Publish per-query status and final-plan evidence alongside numeric charts; do not infer missing
  queries or semantic correctness from a successful timing chart.

### Pass 3: Criterion join-ordering microbenchmarks and flamegraphs

- [ ] Add an `optd-core` Criterion target with deterministic chain/star/cycle/clique fixtures and fresh
  optimizer state; validate results outside timed regions.
- [ ] Add bounded seeded random-tree fixtures and shape/size scaling coverage inspired by
  [Neumann and Radke, SIGMOD 2018](https://db.in.tum.de/~radke/papers/hugejoins.pdf). Keep large timeout
  experiments separate from repeated Criterion sampling.
- [ ] Record optimization time alongside plan-cost quality; distinguish exact-optimum references from
  best-known references under matching cost models and search restrictions.
- [ ] Add tested Criterion-to-custom-JSON conversion and a separate dashboard group.
- [ ] Add opt-in single-case flamegraphs; verify symbols, profiler permissions, selected workload scope,
  sanitized SVG artifacts, and links. Never ingest profiled timings into regression history.

### Pass 4: optimizer-health diagnostics

- [ ] Define and expose search counters: connected subsets/DP states, candidate partitions, alternatives
  costed, and budget/fallback events where supported; avoid an ambiguous "orders explored" metric.
- [ ] Add pass counts/outcomes and per-pass timing drill-downs.
- [ ] Retain selected join trees, normalized physical-plan fingerprints, costs, and cardinalities.
- [ ] Add plan diffs and warnings for search explosions, comparable cost increases, and plan changes.
- [ ] Keep expensive full snapshots and subtree cardinality collection opt-in and outside timing.

### Pass 5: calibrated performance regression detection

- [ ] Establish accepted baselines and compatible environment/configuration cohorts.
- [ ] Measure repeated-run noise and calibrate sample counts, practical effect-size thresholds, and
  confidence procedures, including multiple-query comparisons.
- [ ] Classify hard correctness failures, optimizer-health warnings, benchmark regressions, and
  infrastructure/inconclusive results separately.
- [ ] Add same-machine baseline/candidate confirmation where practical.
- [ ] Decide which calibrated signals should gate changes; do not treat the action's scalar percentage
  alert as statistical significance.

## Verification strategy

Verification must include schema/aggregation tests, malformed and partial outputs, missing queries,
source attribution for schedule/manual events, timing boundaries, deterministic fingerprints, crash
and timeout retention, incompatible baselines, and retry/idempotency behavior. For microbenchmarks,
verify fixture invariants, fresh-state setup, unit conversion, Criterion output compatibility, and a
representative unprofiled run. For flamegraphs, verify the selected case executes, symbols resolve,
and profiling outputs remain separate from timing history. Run focused Rust tests,
formatting and Clippy for Rust changes, and `actionlint` for workflow changes. A real private-repository
Actions pilot must validate permissions, source checkout, dataset capacity, artifact transfer,
concurrent history updates, and Pages access/visibility. Local tests cannot prove those properties.

## Decisions and scope boundaries

Workflow ownership and destination (`cmu-db/optd-web`) are settled. Repository access, authentication,
runner budget, Pages visibility, and initial TPC-H/JOB query manifests/statistics policy still need resolution
for the first delivery. Execution
correctness references, physical execution paths, Criterion fixture sizes, search instrumentation, and
statistical gates belong to later passes and must not delay the initial profiler-backed integration. Choose a short raw-artifact retention
for the pilot and document it; calibrate performance thresholds before adding blocking policies.
