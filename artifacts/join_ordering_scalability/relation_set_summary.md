# RelationSet timing summary

Times are nanoseconds per workload iteration; rows show selected representation boundaries.

| Workload | Variant | Relations | Median | P10 | P90 |
|:---|:---|---:|---:|---:|---:|
| mixed_set_ops | allocating_assign | 64 | 5.4 | 5.4 | 5.8 |
| mixed_set_ops | allocating_assign | 65 | 23.4 | 22.8 | 26.6 |
| mixed_set_ops | allocating_assign | 256 | 27.1 | 26.8 | 29.3 |
| mixed_set_ops | allocating_assign | 1024 | 38.6 | 38.4 | 41.5 |
| mixed_set_ops | borrowed_union | 64 | 2.5 | 2.5 | 2.8 |
| mixed_set_ops | borrowed_union | 65 | 23.8 | 23.3 | 26.2 |
| mixed_set_ops | borrowed_union | 256 | 28.4 | 27.0 | 31.6 |
| mixed_set_ops | borrowed_union | 1024 | 42.8 | 41.9 | 45.7 |
| mixed_set_ops | in_place_assign | 64 | 2.6 | 2.5 | 2.8 |
| mixed_set_ops | in_place_assign | 65 | 2.9 | 2.9 | 2.9 |
| mixed_set_ops | in_place_assign | 256 | 6.6 | 5.9 | 6.7 |
| mixed_set_ops | in_place_assign | 1024 | 12.3 | 11.5 | 12.8 |
| set_build | from_iter | 64 | 65.0 | 64.8 | 71.1 |
| set_build | from_iter | 65 | 126.6 | 122.8 | 129.5 |
| set_build | from_iter | 256 | 155.5 | 153.4 | 171.4 |
| set_build | from_iter | 1024 | 642.6 | 629.5 | 672.8 |
| set_build | incremental_with | 64 | 171.7 | 171.3 | 196.9 |
| set_build | incremental_with | 65 | 210.6 | 210.3 | 218.8 |
| set_build | incremental_with | 256 | 6481.0 | 6338.6 | 6651.9 |
| set_build | incremental_with | 1024 | 34595.7 | 34500.2 | 35120.4 |
