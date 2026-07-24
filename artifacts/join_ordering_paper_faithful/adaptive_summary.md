# Adaptive random-tree timing summary

Times are milliseconds for one complete `JoinOrdering::run` invocation.

| Relations | Selected algorithms | Min | P10 | Median | P90 | Max | Exact CSGs | DP states | Repairs |
|---:|:---|---:|---:|---:|---:|---:|---:|---:|---:|
| 10 | dphyp=9 | 0.145 | 0.147 | 0.274 | 0.443 | 0.448 |  | 115 | 0 |
| 20 | dphyp=6; linearized_dp=3 | 6.234 | 6.243 | 20.889 | 30.331 | 30.422 | 3360 | 2897 | 0 |
| 30 | linearized_dp=9 | 7.117 | 7.679 | 8.985 | 9.532 | 10.738 |  | 70 | 0 |
| 40 | linearized_dp=9 | 8.118 | 8.118 | 9.149 | 9.359 | 9.506 |  | 90 | 0 |
| 70 | linearized_dp=9 | 11.980 | 12.120 | 13.116 | 14.038 | 14.155 |  | 152 | 0 |
| 100 | linearized_dp=9 | 18.741 | 18.757 | 18.919 | 19.837 | 19.924 |  | 213 | 0 |
| 128 | goo_linearized_dp=9 | 18.637 | 18.773 | 19.513 | 21.265 | 23.512 |  | 277 | 2 |
| 192 | goo_linearized_dp=9 | 26.501 | 26.546 | 27.135 | 35.776 | 35.896 |  | 408 | 3 |
| 256 | goo_linearized_dp=9 | 37.929 | 38.315 | 40.373 | 43.102 | 43.231 |  | 550 | 3 |
| 512 | goo_linearized_dp=9 | 69.392 | 69.401 | 77.020 | 79.454 | 80.465 |  | 1141 | 7 |
| 1024 | goo_linearized_dp=9 | 150.304 | 151.342 | 157.357 | 196.822 | 197.086 |  | 2285 | 15 |
| 2000 | goo_linearized_dp=9 | 444.224 | 447.351 | 466.027 | 485.693 | 490.482 |  | 4510 | 28 |
| 5000 | goo_linearized_dp=9 | 2000.353 | 2000.488 | 2037.878 | 2118.885 | 2125.976 |  | 10057 | 66 |
