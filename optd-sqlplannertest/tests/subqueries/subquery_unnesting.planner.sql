-- (no id or description)
create table t1(t1v1 int, t1v2 int);
create table t2(t2v1 int, t2v3 int);
create table t3(t3v2 int, t3v4 int);

/*

*/

-- Test whether the optimizer can unnest correlated subqueries.
select * from t1 where (select sum(t2v3) from t2 where t2v1 = t1v1) > 100;

/*
LogicalProjection { exprs: [ #0, #1 ] }
└── LogicalFilter
    ├── cond:Gt
    │   ├── #2
    │   └── 100(i64)
    └── RawDependentJoin { join_type: Cross, cond: true, extern_cols: [ Extern(#0) ] }
        ├── LogicalScan { table: t1 }
        └── LogicalProjection { exprs: [ #0 ] }
            └── LogicalAgg
                ├── exprs:Agg(Sum)
                │   └── [ Cast { cast_to: Int64, expr: #1 } ]
                ├── groups: []
                └── LogicalFilter
                    ├── cond:Eq
                    │   ├── #0
                    │   └── Extern(#0)
                    └── LogicalScan { table: t2 }
LogicalProjection { exprs: [ #0, #1 ] }
└── LogicalFilter
    ├── cond:Gt
    │   ├── #2
    │   └── 100(i64)
    └── LogicalProjection { exprs: [ #0, #1, #3 ] }
        └── LogicalJoin
            ├── join_type: Inner
            ├── cond:Eq
            │   ├── #0
            │   └── #2
            ├── LogicalScan { table: t1 }
            └── LogicalProjection { exprs: [ #0, #1 ] }
                └── LogicalAgg
                    ├── exprs:Agg(Sum)
                    │   └── [ Cast { cast_to: Int64, expr: #2 } ]
                    ├── groups: [ #1 ]
                    └── LogicalFilter
                        ├── cond:Eq
                        │   ├── #1
                        │   └── #0
                        └── LogicalJoin { join_type: Inner, cond: true }
                            ├── LogicalAgg { exprs: [], groups: [ #0 ] }
                            │   └── LogicalScan { table: t1 }
                            └── LogicalScan { table: t2 }
PhysicalProjection { exprs: [ #2, #3 ], cost: {compute=9147.220000000001,io=3000}, stat: {row_cnt=1} }
└── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ], cost: {compute=9139.2,io=3000}, stat: {row_cnt=1} }
    ├── PhysicalAgg
    │   ├── aggrs:Agg(Sum)
    │   │   └── [ Cast { cast_to: Int64, expr: #2 } ]
    │   ├── groups: [ #1 ]
    │   ├── cost: {compute=8133.180000000001,io=2000}
    │   ├── stat: {row_cnt=1}
    │   └── PhysicalProjection { exprs: [ #2, #0, #1 ], cost: {compute=8045.06,io=2000}, stat: {row_cnt=1} }
    │       └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ], cost: {compute=8033.04,io=2000}, stat: {row_cnt=1} }
    │           ├── PhysicalFilter
    │           │   ├── cond:Gt
    │           │   │   ├── #0
    │           │   │   └── 100(i64)
    │           │   ├── cost: {compute=5005,io=1000}
    │           │   ├── stat: {row_cnt=1}
    │           │   └── PhysicalScan { table: t2, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
    │           └── PhysicalAgg { aggrs: [], groups: [ #0 ], cost: {compute=2022.0199999999995,io=1000}, stat: {row_cnt=1000} }
    │               └── PhysicalScan { table: t1, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
    └── PhysicalScan { table: t1, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
*/

-- Test whether the optimizer can unnest correlated subqueries.
select * from t1 where (select sum(t2v3) from (select * from t2, t3 where t2v1 = t1v1 and t2v3 = t3v2)) > 100;

/*
LogicalProjection { exprs: [ #0, #1 ] }
└── LogicalFilter
    ├── cond:Gt
    │   ├── #2
    │   └── 100(i64)
    └── RawDependentJoin { join_type: Cross, cond: true, extern_cols: [ Extern(#0) ] }
        ├── LogicalScan { table: t1 }
        └── LogicalProjection { exprs: [ #0 ] }
            └── LogicalAgg
                ├── exprs:Agg(Sum)
                │   └── [ Cast { cast_to: Int64, expr: #1 } ]
                ├── groups: []
                └── LogicalProjection { exprs: [ #0, #1, #2, #3 ] }
                    └── LogicalFilter
                        ├── cond:And
                        │   ├── Eq
                        │   │   ├── #0
                        │   │   └── Extern(#0)
                        │   └── Eq
                        │       ├── #1
                        │       └── #2
                        └── LogicalJoin { join_type: Cross, cond: true }
                            ├── LogicalScan { table: t2 }
                            └── LogicalScan { table: t3 }
LogicalProjection { exprs: [ #0, #1 ] }
└── LogicalFilter
    ├── cond:Gt
    │   ├── #2
    │   └── 100(i64)
    └── LogicalProjection { exprs: [ #0, #1, #3 ] }
        └── LogicalJoin
            ├── join_type: Inner
            ├── cond:Eq
            │   ├── #0
            │   └── #2
            ├── LogicalScan { table: t1 }
            └── LogicalProjection { exprs: [ #0, #1 ] }
                └── LogicalAgg
                    ├── exprs:Agg(Sum)
                    │   └── [ Cast { cast_to: Int64, expr: #2 } ]
                    ├── groups: [ #1 ]
                    └── LogicalProjection { exprs: [ #0, #1, #2, #3, #4 ] }
                        └── LogicalFilter
                            ├── cond:And
                            │   ├── Eq
                            │   │   ├── #1
                            │   │   └── #0
                            │   └── Eq
                            │       ├── #2
                            │       └── #3
                            └── LogicalJoin { join_type: Inner, cond: true }
                                ├── LogicalAgg { exprs: [], groups: [ #0 ] }
                                │   └── LogicalScan { table: t1 }
                                └── LogicalJoin { join_type: Cross, cond: true }
                                    ├── LogicalScan { table: t2 }
                                    └── LogicalScan { table: t3 }
PhysicalProjection { exprs: [ #2, #3 ], cost: {compute=10153.240000000002,io=4000}, stat: {row_cnt=1} }
└── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ], cost: {compute=10145.220000000001,io=4000}, stat: {row_cnt=1} }
    ├── PhysicalAgg
    │   ├── aggrs:Agg(Sum)
    │   │   └── [ Cast { cast_to: Int64, expr: #2 } ]
    │   ├── groups: [ #1 ]
    │   ├── cost: {compute=9139.2,io=3000}
    │   ├── stat: {row_cnt=1}
    │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #2 ], right_keys: [ #0 ], cost: {compute=9051.080000000002,io=3000}, stat: {row_cnt=1} }
    │       ├── PhysicalProjection { exprs: [ #2, #0, #1 ], cost: {compute=8045.06,io=2000}, stat: {row_cnt=1} }
    │       │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ], cost: {compute=8033.04,io=2000}, stat: {row_cnt=1} }
    │       │       ├── PhysicalFilter
    │       │       │   ├── cond:Gt
    │       │       │   │   ├── #0
    │       │       │   │   └── 100(i64)
    │       │       │   ├── cost: {compute=5005,io=1000}
    │       │       │   ├── stat: {row_cnt=1}
    │       │       │   └── PhysicalScan { table: t2, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
    │       │       └── PhysicalAgg { aggrs: [], groups: [ #0 ], cost: {compute=2022.0199999999995,io=1000}, stat: {row_cnt=1000} }
    │       │           └── PhysicalScan { table: t1, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
    │       └── PhysicalScan { table: t3, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
    └── PhysicalScan { table: t1, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
*/

