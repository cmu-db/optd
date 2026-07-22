# Adaptive random-tree timing summary

Times are milliseconds for one complete `JoinOrdering::run` invocation.

| Relations | Selected algorithms (of 10) | Min | P10 | Median | P90 | Max | Median candidates |
|---:|:---|---:|---:|---:|---:|---:|---:|
| 10 | DpHyp=10 | 0.157 | 0.209 | 0.329 | 0.473 | 0.564 | 445 |
| 20 | DpHyp=7; LinearizedDp=3 | 1.918 | 2.092 | 27.917 | 42.415 | 60.361 | 47844 |
| 30 | LinearizedDp=10 | 2.699 | 2.708 | 2.819 | 2.977 | 3.055 | 52 |
| 40 | LinearizedDp=10 | 3.460 | 3.512 | 3.684 | 3.750 | 3.882 | 62 |
| 70 | LinearizedDp=10 | 6.587 | 6.788 | 8.030 | 8.958 | 9.152 | 95 |
| 100 | LinearizedDp=10 | 12.864 | 13.129 | 13.487 | 14.013 | 14.387 | 125 |
| 128 | GooDp=10 | 110.371 | 112.422 | 113.867 | 116.132 | 116.608 | 10778 |
| 192 | GooDp=10 | 608.216 | 627.476 | 638.923 | 661.924 | 664.074 | 22838 |
| 256 | GooDp=10 | 2247.534 | 2267.145 | 2339.453 | 2388.013 | 2407.145 | 38570 |
