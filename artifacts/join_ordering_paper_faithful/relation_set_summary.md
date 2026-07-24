# RelationSet timing summary

Times are nanoseconds per workload iteration; rows show selected representation boundaries.

| Workload | Variant | Universe | Representation | Median | P10 | P90 |
|:---|:---|---:|:---|---:|---:|---:|
| mixed_set_ops | allocating_assign | 64 | Inline64 | 6.8 | 6.8 | 6.8 |
| mixed_set_ops | allocating_assign | 65 | Inline128 | 7.9 | 7.9 | 7.9 |
| mixed_set_ops | allocating_assign | 128 | Inline128 | 7.9 | 7.9 | 7.9 |
| mixed_set_ops | allocating_assign | 129 | Dense | 30.9 | 30.9 | 30.9 |
| mixed_set_ops | allocating_assign | 256 | Dense | 32.4 | 32.3 | 32.4 |
| mixed_set_ops | allocating_assign | 1024 | Dense | 44.5 | 44.4 | 44.5 |
| mixed_set_ops | borrowed_union | 64 | Inline64 | 8.0 | 7.9 | 8.0 |
| mixed_set_ops | borrowed_union | 65 | Inline128 | 8.5 | 8.5 | 8.9 |
| mixed_set_ops | borrowed_union | 128 | Inline128 | 8.3 | 8.2 | 8.3 |
| mixed_set_ops | borrowed_union | 129 | Dense | 32.0 | 32.0 | 32.1 |
| mixed_set_ops | borrowed_union | 256 | Dense | 32.9 | 32.9 | 33.0 |
| mixed_set_ops | borrowed_union | 1024 | Dense | 45.0 | 44.9 | 45.0 |
| mixed_set_ops | in_place_assign | 64 | Inline64 | 5.6 | 5.6 | 5.8 |
| mixed_set_ops | in_place_assign | 65 | Inline128 | 6.5 | 6.5 | 6.5 |
| mixed_set_ops | in_place_assign | 128 | Inline128 | 6.2 | 6.2 | 6.2 |
| mixed_set_ops | in_place_assign | 129 | Dense | 7.4 | 7.3 | 7.4 |
| mixed_set_ops | in_place_assign | 256 | Dense | 10.0 | 10.0 | 10.0 |
| mixed_set_ops | in_place_assign | 1024 | Dense | 21.4 | 18.6 | 21.7 |
| set_build | from_iter | 64 | Inline64 | 106.3 | 105.9 | 106.8 |
| set_build | from_iter | 65 | Inline128 | 110.4 | 109.8 | 110.7 |
| set_build | from_iter | 128 | Inline128 | 178.2 | 177.7 | 178.6 |
| set_build | from_iter | 129 | Dense | 181.3 | 180.9 | 184.7 |
| set_build | from_iter | 256 | Dense | 287.6 | 285.7 | 288.6 |
| set_build | from_iter | 1024 | Dense | 1109.6 | 1107.5 | 1112.0 |
| set_build | incremental_with | 64 | Inline64 | 158.8 | 158.3 | 160.1 |
| set_build | incremental_with | 65 | Inline128 | 165.7 | 165.5 | 166.8 |
| set_build | incremental_with | 128 | Inline128 | 346.9 | 346.4 | 347.7 |
| set_build | incremental_with | 129 | Dense | 462.8 | 462.0 | 464.0 |
| set_build | incremental_with | 256 | Dense | 7428.0 | 7421.3 | 7449.6 |
| set_build | incremental_with | 1024 | Dense | 54082.4 | 54002.1 | 54211.2 |
| sparse_set_ops | allocating_assign | 16385 | Sparse | 48.6 | 48.6 | 48.8 |
| sparse_set_ops | borrowed_union | 16385 | Sparse | 45.6 | 45.6 | 46.0 |
| sparse_set_ops | in_place_assign | 16385 | Sparse | 48.3 | 48.2 | 48.7 |
