# Catalog-corrected comparison with pre-paper implementation

Both sides use identical deterministic random trees, release-mode timed repetitions, and
populated benchmark catalog entries. The baseline is commit `7a98797` with only the
catalog-fixture correction applied; no baseline optimizer code was changed.

| Relations | Baseline median | Current median | Current / baseline | Change |
|---:|---:|---:|---:|---:|
| 10 | 0.266 ms | 0.274 ms | 1.029x | +2.9% |
| 20 | 17.630 ms | 20.889 ms | 1.185x | +18.5% |
| 30 | 2.592 ms | 8.985 ms | 3.466x | +246.6% |
| 40 | 3.424 ms | 9.149 ms | 2.672x | +167.2% |
| 70 | 7.923 ms | 13.116 ms | 1.655x | +65.5% |
| 100 | 13.161 ms | 18.919 ms | 1.437x | +43.7% |
| 128 | 111.691 ms | 19.513 ms | 0.175x | -82.5% |
| 192 | 610.952 ms | 27.135 ms | 0.044x | -95.6% |
| 256 | 2223.087 ms | 40.373 ms | 0.018x | -98.2% |
