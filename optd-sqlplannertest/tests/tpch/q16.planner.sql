-- TPC-H Q16
select
    p_brand,
    p_type,
    p_size,
    count(distinct ps_suppkey) as supplier_cnt
from
    partsupp,
    part
where
    p_partkey = ps_partkey
    and p_brand <> 'Brand#45'
    and p_type not like 'MEDIUM POLISHED%'
    and p_size in (49, 14, 23, 45, 19, 3, 36, 9)
    and ps_suppkey not in (
        select
            s_suppkey
        from
            supplier
        where
            s_comment like '%Customer%Complaints%'
    )
group by
    p_brand,
    p_type,
    p_size
order by
    supplier_cnt desc,
    p_brand,
    p_type,
    p_size;

/*
LogicalSort
├── exprs:
│   ┌── SortOrder { order: Desc }
│   │   └── #3
│   ├── SortOrder { order: Asc }
│   │   └── #0
│   ├── SortOrder { order: Asc }
│   │   └── #1
│   └── SortOrder { order: Asc }
│       └── #2
└── LogicalProjection { exprs: [ #0, #1, #2, #3 ] }
    └── LogicalAgg
        ├── exprs:Agg(Count)
        │   └── [ #1 ]
        ├── groups: [ #8, #9, #10 ]
        └── LogicalFilter
            ├── cond:And
            │   ├── Eq
            │   │   ├── #5
            │   │   └── #0
            │   ├── Neq
            │   │   ├── #8
            │   │   └── "Brand#45"
            │   ├── Like { expr: #9, pattern: "MEDIUM POLISHED%", negated: true, case_insensitive: false }
            │   ├── InList { expr: Cast { cast_to: Int64, child: #10 }, list: [ 49(i64), 14(i64), 23(i64), 45(i64), 19(i64), 3(i64), 36(i64), 9(i64) ], negated: false }
            │   └── Not
            │       └── [ #14 ]
            └── RawDependentJoin { sq_type: Any { pred: PredNode { typ: ColumnRef, children: [], data: Some(UInt64(1)) }, op: Eq }, cond: true, extern_cols: [] }
                ├── LogicalJoin { join_type: Cross, cond: true }
                │   ├── LogicalScan { table: partsupp }
                │   └── LogicalScan { table: part }
                └── LogicalProjection { exprs: [ #0 ] }
                    └── LogicalFilter { cond: Like { expr: #6, pattern: "%Customer%Complaints%", negated: false, case_insensitive: false } }
                        └── LogicalScan { table: supplier }
PhysicalSort
├── exprs:
│   ┌── SortOrder { order: Desc }
│   │   └── #3
│   ├── SortOrder { order: Asc }
│   │   └── #0
│   ├── SortOrder { order: Asc }
│   │   └── #1
│   └── SortOrder { order: Asc }
│       └── #2
└── PhysicalAgg
    ├── aggrs:Agg(Count)
    │   └── [ #1 ]
    ├── groups: [ #8, #9, #10 ]
    └── PhysicalFilter
        ├── cond:And
        │   ├── Eq
        │   │   ├── #5
        │   │   └── #0
        │   ├── Neq
        │   │   ├── #8
        │   │   └── "Brand#45"
        │   ├── Like { expr: #9, pattern: "MEDIUM POLISHED%", negated: true, case_insensitive: false }
        │   ├── InList { expr: Cast { cast_to: Int64, child: #10 }, list: [ 49(i64), 14(i64), 23(i64), 45(i64), 19(i64), 3(i64), 36(i64), 9(i64) ], negated: false }
        │   └── Not
        │       └── [ #14 ]
        └── PhysicalNestedLoopJoin
            ├── join_type: LeftMark
            ├── cond:Eq
            │   ├── #1
            │   └── #14
            ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
            │   ├── PhysicalScan { table: partsupp }
            │   └── PhysicalScan { table: part }
            └── PhysicalProjection { exprs: [ #0 ] }
                └── PhysicalFilter { cond: Like { expr: #6, pattern: "%Customer%Complaints%", negated: false, case_insensitive: false } }
                    └── PhysicalScan { table: supplier }
*/

