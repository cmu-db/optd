# CE: Match Probability (Leis/Rusu) + Group NDV

**Date:** 2026-11-12  
**Status:** Implemented

## Match probability (semi / anti / NOT EXISTS)

### Literature

- **Leis et al., PVLDB 2015** — Postgres-style assumptions: uniformity, independence, **inclusion** (`|T1 ▷◁ T2| = |T1||T2|/max(dom)` for inner; semi/anti use existence fraction of the outer).
- **Rusu et al., TODS 2015** — same inclusion baseline for antijoin `|R ▷ S| ≈ (dR−dS)·|R|/dR` (`dR≥dS`); documents catastrophic errors when inclusion fails; recommends **sampling** for antijoin.

### Design

1. **Inclusion baseline (Leis/Postgres):**  
   `P(match) = left_nn * min(dL', dR') / dL'`  
   with NDVs **clamped** to non-null frequency / row count (Postgres `eqjoinsel_semi` clamps inner NDV to inner rows).

2. **Sample presence (Rusu-inspired):** when both join keys resolve to base tables with catalog samples, estimate  
   `P(match) ≈ (# left sample rows whose key appears in right sample key set) / |left sample|`.  
   Prefer this over inclusion when available (fixes NOT EXISTS underestimation when domains “include” but sets do not).

3. Semi rows = `|L| * P(match)`; anti = `|L| * (1 - P(match))` (existing profile helpers).

## Group-by NDV

When aggregating, prefer **HLL sketch NDV** on a grouping column when the input profile still carries sketches; otherwise keep joint_ndv / unique-key / product-of-distincts. Document that post-join sketch invalidation still limits this until sketches or joint NDVs are retained more carefully.

## Out of scope

Adaptive CE; string/LIKE pass; full Theta/KMV intersection.
