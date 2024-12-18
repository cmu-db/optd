-- TPC-H Q20
select
    s_name,
    s_address
from
    supplier,
    nation
where
    s_suppkey in (
        select
            ps_suppkey
        from
            partsupp
        where
            ps_partkey in (
                select
                    p_partkey
                from
                    part
                where
                    p_name like 'indian%'
            )
            and ps_availqty > (
                select
                    0.5 * sum(l_quantity)
                from
                    lineitem
                where
                    l_partkey = ps_partkey
                    and l_suppkey = ps_suppkey
                    and l_shipdate >= date '1996-01-01'
                    and l_shipdate < date '1996-01-01' + interval '1' year
            )
    )
    and s_nationkey = n_nationkey
    and n_name = 'IRAQ'
order by
    s_name;

/*
LogicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── LogicalProjection { exprs: [ #1, #2 ] }
    └── LogicalFilter
        ├── cond:And
        │   ├── #11
        │   ├── Eq
        │   │   ├── #3
        │   │   └── #7
        │   └── Eq
        │       ├── #8
        │       └── "IRAQ"
        └── RawDependentJoin { sq_type: Any { pred: PredNode { typ: ColumnRef, children: [], data: Some(UInt64(0)) }, op: Eq }, cond: true, extern_cols: [] }
            ├── LogicalJoin { join_type: Inner, cond: true }
            │   ├── LogicalScan { table: supplier }
            │   └── LogicalScan { table: nation }
            └── LogicalProjection { exprs: [ #1 ] }
                └── LogicalFilter
                    ├── cond:And
                    │   ├── #5
                    │   └── Gt
                    │       ├── Cast { cast_to: Float64, child: #2 }
                    │       └── #6
                    └── RawDependentJoin { sq_type: Scalar, cond: true, extern_cols: [ Extern(#0), Extern(#1) ] }
                        ├── RawDependentJoin { sq_type: Any { pred: PredNode { typ: ColumnRef, children: [], data: Some(UInt64(0)) }, op: Eq }, cond: true, extern_cols: [] }
                        │   ├── LogicalScan { table: partsupp }
                        │   └── LogicalProjection { exprs: [ #0 ] }
                        │       └── LogicalFilter { cond: Like { expr: #1, pattern: "indian%", negated: false, case_insensitive: false } }
                        │           └── LogicalScan { table: part }
                        └── LogicalProjection
                            ├── exprs:Mul
                            │   ├── 0.5(float)
                            │   └── Cast { cast_to: Float64, child: #0 }
                            └── LogicalAgg
                                ├── exprs:Agg(Sum)
                                │   └── [ #4 ]
                                ├── groups: []
                                └── LogicalFilter
                                    ├── cond:And
                                    │   ├── Eq
                                    │   │   ├── #1
                                    │   │   └── Extern(#0)
                                    │   ├── Eq
                                    │   │   ├── #2
                                    │   │   └── Extern(#1)
                                    │   ├── Geq
                                    │   │   ├── #10
                                    │   │   └── Cast { cast_to: Date32, child: "1996-01-01" }
                                    │   └── Lt
                                    │       ├── #10
                                    │       └── Add
                                    │           ├── Cast { cast_to: Date32, child: "1996-01-01" }
                                    │           └── INTERVAL_MONTH_DAY_NANO (12, 0, 0)
                                    └── LogicalScan { table: lineitem }
PhysicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── PhysicalProjection { exprs: [ #1, #2 ] }
    └── PhysicalFilter
        ├── cond:And
        │   ├── #11
        │   ├── Eq
        │   │   ├── #3
        │   │   └── #7
        │   └── Eq
        │       ├── #8
        │       └── "IRAQ"
        └── PhysicalNestedLoopJoin
            ├── join_type: LeftMark
            ├── cond:Eq
            │   ├── #0
            │   └── #11
            ├── PhysicalNestedLoopJoin { join_type: Inner, cond: true }
            │   ├── PhysicalScan { table: supplier }
            │   └── PhysicalScan { table: nation }
            └── PhysicalProjection { exprs: [ #1 ] }
                └── PhysicalProjection { exprs: [ #3, #4, #5, #6, #7, #8, #0, #1, #2 ] }
                    └── PhysicalFilter
                        ├── cond:And
                        │   ├── #8
                        │   └── Gt
                        │       ├── Cast { cast_to: Float64, child: #5 }
                        │       └── #2
                        └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0, #1 ], right_keys: [ #0, #1 ] }
                            ├── PhysicalProjection
                            │   ├── exprs:
                            │   │   ┌── #0
                            │   │   ├── #1
                            │   │   └── Mul
                            │   │       ├── 0.5(float)
                            │   │       └── Cast { cast_to: Float64, child: #2 }
                            │   └── PhysicalProjection { exprs: [ #0, #1, #4 ] }
                            │       └── PhysicalNestedLoopJoin
                            │           ├── join_type: LeftOuter
                            │           ├── cond:And
                            │           │   ├── Eq
                            │           │   │   ├── #0
                            │           │   │   └── #2
                            │           │   └── Eq
                            │           │       ├── #1
                            │           │       └── #3
                            │           ├── PhysicalAgg { aggrs: [], groups: [ #0, #1 ] }
                            │           │   └── PhysicalNestedLoopJoin
                            │           │       ├── join_type: LeftMark
                            │           │       ├── cond:Eq
                            │           │       │   ├── #0
                            │           │       │   └── #5
                            │           │       ├── PhysicalScan { table: partsupp }
                            │           │       └── PhysicalProjection { exprs: [ #0 ] }
                            │           │           └── PhysicalFilter { cond: Like { expr: #1, pattern: "indian%", negated: false, case_insensitive: false } }
                            │           │               └── PhysicalScan { table: part }
                            │           └── PhysicalAgg
                            │               ├── aggrs:Agg(Sum)
                            │               │   └── [ #6 ]
                            │               ├── groups: [ #0, #1 ]
                            │               └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0, #1 ], right_keys: [ #1, #2 ] }
                            │                   ├── PhysicalAgg { aggrs: [], groups: [ #0, #1 ] }
                            │                   │   └── PhysicalNestedLoopJoin
                            │                   │       ├── join_type: LeftMark
                            │                   │       ├── cond:Eq
                            │                   │       │   ├── #0
                            │                   │       │   └── #5
                            │                   │       ├── PhysicalScan { table: partsupp }
                            │                   │       └── PhysicalProjection { exprs: [ #0 ] }
                            │                   │           └── PhysicalFilter { cond: Like { expr: #1, pattern: "indian%", negated: false, case_insensitive: false } }
                            │                   │               └── PhysicalScan { table: part }
                            │                   └── PhysicalFilter
                            │                       ├── cond:And
                            │                       │   ├── Geq
                            │                       │   │   ├── #10
                            │                       │   │   └── Cast { cast_to: Date32, child: "1996-01-01" }
                            │                       │   └── Lt
                            │                       │       ├── #10
                            │                       │       └── Add
                            │                       │           ├── Cast { cast_to: Date32, child: "1996-01-01" }
                            │                       │           └── INTERVAL_MONTH_DAY_NANO (12, 0, 0)
                            │                       └── PhysicalScan { table: lineitem }
                            └── PhysicalNestedLoopJoin
                                ├── join_type: LeftMark
                                ├── cond:Eq
                                │   ├── #0
                                │   └── #5
                                ├── PhysicalScan { table: partsupp }
                                └── PhysicalProjection { exprs: [ #0 ] }
                                    └── PhysicalFilter { cond: Like { expr: #1, pattern: "indian%", negated: false, case_insensitive: false } }
                                        └── PhysicalScan { table: part }
*/

