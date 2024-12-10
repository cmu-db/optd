-- (no id or description)
create table t1(t1v1 int, t1v2 int);
create table t2(t2v1 int, t2v3 int);
create table t3(t3v2 int, t3v4 int);

/*

*/

-- Test whether the optimizer can unnest correlated subqueries with (scalar op agg)
select * from t1 where (select sum(t2v3) from t2 where t2v1 = t1v1) > 100;

/*
LogicalProjection { exprs: [ #0, #1 ] }
└── LogicalFilter
    ├── cond:Gt
    │   ├── #2
    │   └── 100(i64)
    └── RawDependentJoin { sq_type: Scalar, cond: true, extern_cols: [ Extern(#0) ] }
        ├── LogicalScan { table: t1 }
        └── LogicalProjection { exprs: [ #0 ] }
            └── LogicalAgg
                ├── exprs:Agg(Sum)
                │   └── [ Cast { cast_to: Int64, child: #1 } ]
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
                └── LogicalProjection { exprs: [ #0, #2 ] }
                    └── LogicalJoin
                        ├── join_type: LeftOuter
                        ├── cond:And
                        │   └── Eq
                        │       ├── #0
                        │       └── #1
                        ├── LogicalAgg { exprs: [], groups: [ #0 ] }
                        │   └── LogicalScan { table: t1 }
                        └── LogicalAgg
                            ├── exprs:Agg(Sum)
                            │   └── [ Cast { cast_to: Int64, child: #2 } ]
                            ├── groups: [ #0 ]
                            └── LogicalFilter
                                ├── cond:Eq
                                │   ├── #1
                                │   └── #0
                                └── LogicalJoin { join_type: Inner, cond: true }
                                    ├── LogicalAgg { exprs: [], groups: [ #0 ] }
                                    │   └── LogicalScan { table: t1 }
                                    └── LogicalScan { table: t2 }
PhysicalProjection { exprs: [ #0, #1 ], cost: {compute=4033003,io=4000}, stat: {row_cnt=1} }
└── PhysicalFilter
    ├── cond:Gt
    │   ├── #4
    │   └── 100(i64)
    ├── cost: {compute=4033000,io=4000}
    ├── stat: {row_cnt=1}
    └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ], cost: {compute=4030000,io=4000}, stat: {row_cnt=1000} }
        ├── PhysicalScan { table: t1, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
        └── PhysicalNestedLoopJoin
            ├── join_type: LeftOuter
            ├── cond:And
            │   └── Eq
            │       ├── #0
            │       └── #1
            ├── cost: {compute=4018000,io=3000}
            ├── stat: {row_cnt=10000}
            ├── PhysicalAgg { aggrs: [], groups: [ #0 ], cost: {compute=3000,io=1000}, stat: {row_cnt=1000} }
            │   └── PhysicalScan { table: t1, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
            └── PhysicalAgg
                ├── aggrs:Agg(Sum)
                │   └── [ Cast { cast_to: Int64, child: #2 } ]
                ├── groups: [ #0 ]
                ├── cost: {compute=14000,io=2000}
                ├── stat: {row_cnt=1000}
                └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ], cost: {compute=6000,io=2000}, stat: {row_cnt=1000} }
                    ├── PhysicalAgg { aggrs: [], groups: [ #0 ], cost: {compute=3000,io=1000}, stat: {row_cnt=1000} }
                    │   └── PhysicalScan { table: t1, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
                    └── PhysicalScan { table: t2, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
*/

-- Test whether the optimizer can unnest correlated subqueries with (scalar op group agg)
select * from t1 where (select sum(sumt2v3) from (select t2v1, sum(t2v3) as sumt2v3 from t2 where t2v1 = t1v1 group by t2v1)) > 100;

/*
LogicalProjection { exprs: [ #0, #1 ] }
└── LogicalFilter
    ├── cond:Gt
    │   ├── #2
    │   └── 100(i64)
    └── RawDependentJoin { sq_type: Scalar, cond: true, extern_cols: [ Extern(#0) ] }
        ├── LogicalScan { table: t1 }
        └── LogicalProjection { exprs: [ #0 ] }
            └── LogicalAgg
                ├── exprs:Agg(Sum)
                │   └── [ #1 ]
                ├── groups: []
                └── LogicalProjection { exprs: [ #0, #1 ] }
                    └── LogicalAgg
                        ├── exprs:Agg(Sum)
                        │   └── [ Cast { cast_to: Int64, child: #1 } ]
                        ├── groups: [ #0 ]
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
                └── LogicalProjection { exprs: [ #0, #2 ] }
                    └── LogicalJoin
                        ├── join_type: LeftOuter
                        ├── cond:And
                        │   └── Eq
                        │       ├── #0
                        │       └── #1
                        ├── LogicalAgg { exprs: [], groups: [ #0 ] }
                        │   └── LogicalScan { table: t1 }
                        └── LogicalAgg
                            ├── exprs:Agg(Sum)
                            │   └── [ #2 ]
                            ├── groups: [ #0 ]
                            └── LogicalProjection { exprs: [ #0, #1, #2 ] }
                                └── LogicalProjection { exprs: [ #0, #2, #3 ] }
                                    └── LogicalJoin
                                        ├── join_type: LeftOuter
                                        ├── cond:And
                                        │   └── Eq
                                        │       ├── #0
                                        │       └── #1
                                        ├── LogicalAgg { exprs: [], groups: [ #0 ] }
                                        │   └── LogicalScan { table: t1 }
                                        └── LogicalAgg
                                            ├── exprs:Agg(Sum)
                                            │   └── [ Cast { cast_to: Int64, child: #2 } ]
                                            ├── groups: [ #0, #1 ]
                                            └── LogicalFilter
                                                ├── cond:Eq
                                                │   ├── #1
                                                │   └── #0
                                                └── LogicalJoin { join_type: Inner, cond: true }
                                                    ├── LogicalAgg { exprs: [], groups: [ #0 ] }
                                                    │   └── LogicalScan { table: t1 }
                                                    └── LogicalScan { table: t2 }
PhysicalProjection { exprs: [ #0, #1 ], cost: {compute=44228003,io=5000}, stat: {row_cnt=1} }
└── PhysicalFilter
    ├── cond:Gt
    │   ├── #4
    │   └── 100(i64)
    ├── cost: {compute=44228000,io=5000}
    ├── stat: {row_cnt=1}
    └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ], cost: {compute=44225000,io=5000}, stat: {row_cnt=1000} }
        ├── PhysicalScan { table: t1, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
        └── PhysicalNestedLoopJoin
            ├── join_type: LeftOuter
            ├── cond:And
            │   └── Eq
            │       ├── #0
            │       └── #1
            ├── cost: {compute=44123000,io=4000}
            ├── stat: {row_cnt=100000}
            ├── PhysicalAgg { aggrs: [], groups: [ #0 ], cost: {compute=3000,io=1000}, stat: {row_cnt=1000} }
            │   └── PhysicalScan { table: t1, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
            └── PhysicalAgg
                ├── aggrs:Agg(Sum)
                │   └── [ #2 ]
                ├── groups: [ #0 ]
                ├── cost: {compute=4119000,io=3000}
                ├── stat: {row_cnt=10000}
                └── PhysicalProjection { exprs: [ #0, #2, #3 ], cost: {compute=4059000,io=3000}, stat: {row_cnt=10000} }
                    └── PhysicalNestedLoopJoin
                        ├── join_type: LeftOuter
                        ├── cond:And
                        │   └── Eq
                        │       ├── #0
                        │       └── #1
                        ├── cost: {compute=4019000,io=3000}
                        ├── stat: {row_cnt=10000}
                        ├── PhysicalAgg { aggrs: [], groups: [ #0 ], cost: {compute=3000,io=1000}, stat: {row_cnt=1000} }
                        │   └── PhysicalScan { table: t1, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
                        └── PhysicalAgg
                            ├── aggrs:Agg(Sum)
                            │   └── [ Cast { cast_to: Int64, child: #2 } ]
                            ├── groups: [ #0, #1 ]
                            ├── cost: {compute=15000,io=2000}
                            ├── stat: {row_cnt=1000}
                            └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ], cost: {compute=6000,io=2000}, stat: {row_cnt=1000} }
                                ├── PhysicalAgg { aggrs: [], groups: [ #0 ], cost: {compute=3000,io=1000}, stat: {row_cnt=1000} }
                                │   └── PhysicalScan { table: t1, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
                                └── PhysicalScan { table: t2, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
*/

-- Test whether the optimizer can unnest correlated subqueries with scalar agg in select list
select t1v1, (select sum(t2v3) from t2 where t2v1 = t1v1) as sum from t1;

/*
LogicalProjection { exprs: [ #0, #2 ] }
└── RawDependentJoin { sq_type: Scalar, cond: true, extern_cols: [ Extern(#0) ] }
    ├── LogicalScan { table: t1 }
    └── LogicalProjection { exprs: [ #0 ] }
        └── LogicalAgg
            ├── exprs:Agg(Sum)
            │   └── [ Cast { cast_to: Int64, child: #1 } ]
            ├── groups: []
            └── LogicalFilter
                ├── cond:Eq
                │   ├── #0
                │   └── Extern(#0)
                └── LogicalScan { table: t2 }
LogicalProjection { exprs: [ #0, #2 ] }
└── LogicalProjection { exprs: [ #0, #1, #3 ] }
    └── LogicalJoin
        ├── join_type: Inner
        ├── cond:Eq
        │   ├── #0
        │   └── #2
        ├── LogicalScan { table: t1 }
        └── LogicalProjection { exprs: [ #0, #1 ] }
            └── LogicalProjection { exprs: [ #0, #2 ] }
                └── LogicalJoin
                    ├── join_type: LeftOuter
                    ├── cond:And
                    │   └── Eq
                    │       ├── #0
                    │       └── #1
                    ├── LogicalAgg { exprs: [], groups: [ #0 ] }
                    │   └── LogicalScan { table: t1 }
                    └── LogicalAgg
                        ├── exprs:Agg(Sum)
                        │   └── [ Cast { cast_to: Int64, child: #2 } ]
                        ├── groups: [ #0 ]
                        └── LogicalFilter
                            ├── cond:Eq
                            │   ├── #1
                            │   └── #0
                            └── LogicalJoin { join_type: Inner, cond: true }
                                ├── LogicalAgg { exprs: [], groups: [ #0 ] }
                                │   └── LogicalScan { table: t1 }
                                └── LogicalScan { table: t2 }
PhysicalProjection { exprs: [ #0, #4 ], cost: {compute=4033000,io=4000}, stat: {row_cnt=1000} }
└── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ], cost: {compute=4030000,io=4000}, stat: {row_cnt=1000} }
    ├── PhysicalScan { table: t1, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
    └── PhysicalNestedLoopJoin
        ├── join_type: LeftOuter
        ├── cond:And
        │   └── Eq
        │       ├── #0
        │       └── #1
        ├── cost: {compute=4018000,io=3000}
        ├── stat: {row_cnt=10000}
        ├── PhysicalAgg { aggrs: [], groups: [ #0 ], cost: {compute=3000,io=1000}, stat: {row_cnt=1000} }
        │   └── PhysicalScan { table: t1, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
        └── PhysicalAgg
            ├── aggrs:Agg(Sum)
            │   └── [ Cast { cast_to: Int64, child: #2 } ]
            ├── groups: [ #0 ]
            ├── cost: {compute=14000,io=2000}
            ├── stat: {row_cnt=1000}
            └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ], cost: {compute=6000,io=2000}, stat: {row_cnt=1000} }
                ├── PhysicalAgg { aggrs: [], groups: [ #0 ], cost: {compute=3000,io=1000}, stat: {row_cnt=1000} }
                │   └── PhysicalScan { table: t1, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
                └── PhysicalScan { table: t2, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
*/

-- Test whether the optimizer can unnest correlated subqueries.
select * from t1 where (select sum(t2v3) from (select * from t2, t3 where t2v1 = t1v1 and t2v3 = t3v2)) > 100;

/*
LogicalProjection { exprs: [ #0, #1 ] }
└── LogicalFilter
    ├── cond:Gt
    │   ├── #2
    │   └── 100(i64)
    └── RawDependentJoin { sq_type: Scalar, cond: true, extern_cols: [ Extern(#0) ] }
        ├── LogicalScan { table: t1 }
        └── LogicalProjection { exprs: [ #0 ] }
            └── LogicalAgg
                ├── exprs:Agg(Sum)
                │   └── [ Cast { cast_to: Int64, child: #1 } ]
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
                └── LogicalProjection { exprs: [ #0, #2 ] }
                    └── LogicalJoin
                        ├── join_type: LeftOuter
                        ├── cond:And
                        │   └── Eq
                        │       ├── #0
                        │       └── #1
                        ├── LogicalAgg { exprs: [], groups: [ #0 ] }
                        │   └── LogicalScan { table: t1 }
                        └── LogicalAgg
                            ├── exprs:Agg(Sum)
                            │   └── [ Cast { cast_to: Int64, child: #2 } ]
                            ├── groups: [ #0 ]
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
PhysicalProjection { exprs: [ #0, #1 ], cost: {compute=4036003,io=5000}, stat: {row_cnt=1} }
└── PhysicalFilter
    ├── cond:Gt
    │   ├── #4
    │   └── 100(i64)
    ├── cost: {compute=4036000,io=5000}
    ├── stat: {row_cnt=1}
    └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ], cost: {compute=4033000,io=5000}, stat: {row_cnt=1000} }
        ├── PhysicalScan { table: t1, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
        └── PhysicalNestedLoopJoin
            ├── join_type: LeftOuter
            ├── cond:And
            │   └── Eq
            │       ├── #0
            │       └── #1
            ├── cost: {compute=4021000,io=4000}
            ├── stat: {row_cnt=10000}
            ├── PhysicalAgg { aggrs: [], groups: [ #0 ], cost: {compute=3000,io=1000}, stat: {row_cnt=1000} }
            │   └── PhysicalScan { table: t1, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
            └── PhysicalAgg
                ├── aggrs:Agg(Sum)
                │   └── [ Cast { cast_to: Int64, child: #2 } ]
                ├── groups: [ #0 ]
                ├── cost: {compute=17000,io=3000}
                ├── stat: {row_cnt=1000}
                └── PhysicalHashJoin { join_type: Inner, left_keys: [ #2 ], right_keys: [ #0 ], cost: {compute=9000,io=3000}, stat: {row_cnt=1000} }
                    ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ], cost: {compute=6000,io=2000}, stat: {row_cnt=1000} }
                    │   ├── PhysicalAgg { aggrs: [], groups: [ #0 ], cost: {compute=3000,io=1000}, stat: {row_cnt=1000} }
                    │   │   └── PhysicalScan { table: t1, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
                    │   └── PhysicalScan { table: t2, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
                    └── PhysicalScan { table: t3, cost: {compute=0,io=1000}, stat: {row_cnt=1000} }
*/

