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
PhysicalProjection { exprs: [ #0, #1 ] }
└── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
    ├── PhysicalScan { table: t1 }
    └── PhysicalFilter
        ├── cond:Gt
        │   ├── #1
        │   └── 100(i64)
        └── PhysicalAgg
            ├── aggrs:Agg(Sum)
            │   └── [ Cast { cast_to: Int64, expr: #2 } ]
            ├── groups: [ #1 ]
            └── PhysicalFilter
                ├── cond:Eq
                │   ├── #1
                │   └── #0
                └── PhysicalNestedLoopJoin { join_type: Inner, cond: true }
                    ├── PhysicalAgg { aggrs: [], groups: [ #0 ] }
                    │   └── PhysicalScan { table: t1 }
                    └── PhysicalScan { table: t2 }
*/

