# Fent/Birler/Neumann aggregate estimation versus LpBound

Read-only comparison delegated to `openai-codex/gpt-6.1-sol`; conclusions checked
against the returned primary-source citations. No comparative benchmarks were run.

## Primary references

- Philipp Fent, Altan Birler, Thomas Neumann, *Practical planning and execution of
  groupjoin and nested aggregates*, VLDB Journal 32, 1165–1190 (2023), §§5–6 and
  Appendix A: https://mediatum.ub.tum.de/doc/1763824/1763824.pdf
  DOI: https://doi.org/10.1007/s00778-022-00765-x
- Haozhe Zhang, Christoph Mayer, Mahmoud Abo Khamis, Dan Olteanu, Dan Suciu,
  *LpBound: Pessimistic Cardinality Estimation using ℓp-Norms of Degree Sequences*,
  SIGMOD/PACMMOD 2025. Section/equation references here use arXiv v1:
  https://arxiv.org/pdf/2502.05912v1
  Institutional listing: https://www.ifi.uzh.ch/en/dast/research/LpBound.html

## Different estimation targets

| Aspect | Fent/Birler/Neumann | LpBound |
| --- | --- | --- |
| Target | Distribution of computed values; predicate selectivity | Pessimistic join/projected-group cardinality |
| Statistics | Numerical moments and group-size estimates | Norms of population degree sequences |
| Mechanism | Moment propagation; skew-normal CDF; aggregate approximations | Entropy/Shannon linear program |
| Output | Model-dependent point estimate | One-sided upper bound for databases satisfying supplied constraints |
| HAVING SUM tail | Directly addressed, including q18 | No aggregate-value/HAVING model found |
| Independence assumptions | Present in distribution/group-size approximations | Not needed for the population upper-bound guarantee |

## Aggregate estimation and its limits

Fent et al. §5 fits skew-normal distributions using mean, variance and skewness.
Arithmetic transforms these moments; CDF evaluation estimates threshold selectivity.
For aggregation, §5.3 models variable group sizes using an i.i.d./binomial
approximation and estimates SUM via multiplication of value and group-size
random variables. This is a fitted approximation, not an exact law for a random
sum of independent values. COUNT uses group sizes; MIN/MAX also use an extreme-
value model. Group-size quality remains an input limitation (§6).

The paper explicitly examines q18's rare qualifying order sums. It improves
estimation but does not guarantee rare-tail accuracy: skew-normal cannot fit
kurtosis and has limited attainable skewness; the evaluation acknowledges very
selective outliers. Appendix A notes that several thousand qualifying sums may
still be estimated, causing missed index-access opportunities. Do not promise
that adopting the model alone will recover five qualifying orders in our fixture.

## What LpBound guarantees

For a degree sequence d, ℓ1 is relation size and ℓ∞ is maximum frequency. Finite-p
norms supply additional constraints. LpBound §3 builds an entropy LP; Theorem 3.1
bounds output cardinality for databases satisfying those population constraints.
The two-relation bounds in equations (11)–(14) include Cartesian, max-degree and
Cauchy–Schwarz bounds. Worst-case tightness is not closeness for every actual data
set. GROUP BY support counts projected distinct groups; it does not model the
values of their SUMs. The paper's single-block scope excludes subqueries.

Dropping a HAVING predicate leaves a valid pre-HAVING upper bound, but cannot
predict whether five or 50,000 groups pass a rare threshold. Likewise, the paper
does not establish semantics/estimation support for nullable SQL mark joins or
NOT IN. A semijoin can sometimes be modeled as existential projection, but SQL
bag multiplicity and NULL semantics require explicit treatment.

## Mapping to optd

`optd/sampling/src/evaluate.rs::lpbound_equijoin_upper` is a max-degree special
case using sample-scaled frequencies; it is not the paper's finite-p/entropy-LP
estimator. A scaled sample maximum can miss a heavy hitter and underestimate the
population maximum, even before historical population drift. The paper's hard
bound guarantee therefore does not transfer to that helper automatically.

Our contract deliberately reuses past samples as model-relative evidence. Keep
that contract explicit: historical coverage or sample-derived moments do not
become unconditional logical proofs about a changed current population.

Recommendation: separate selectivity point estimates from bound provenance.
Use Fent-style aggregate statistics for HAVING estimation, with explicit fit and
tail diagnostics; use validated degree constraints for join/group risk bounds.
First avoid needless marks and propagate surviving key domains, so improved
HAVING estimates are not subsequently discarded by unrelated marker fallbacks.
