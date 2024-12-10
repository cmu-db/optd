-- TPC-H Q4
select
    o_orderpriority,
    count(*) as order_count
from
    orders
where
    o_orderdate >= date '1993-07-01'
    and o_orderdate < date '1993-07-01' + interval '3' month
    and exists (
        select
            *
        from
            lineitem
        where
            l_orderkey = o_orderkey
            and l_commitdate < l_receiptdate
    )
group by
    o_orderpriority
order by
    o_orderpriority;

/*
LogicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── LogicalProjection { exprs: [ #0, #1 ] }
    └── LogicalAgg
        ├── exprs:Agg(Count)
        │   └── [ 1(i64) ]
        ├── groups: [ #5 ]
        └── LogicalFilter
            ├── cond:And
            │   ├── Geq
            │   │   ├── #4
            │   │   └── Cast { cast_to: Date32, child: "1993-07-01" }
            │   ├── Lt
            │   │   ├── #4
            │   │   └── Add
            │   │       ├── Cast { cast_to: Date32, child: "1993-07-01" }
            │   │       └── INTERVAL_MONTH_DAY_NANO (3, 0, 0)
            │   └── #9
            └── RawDependentJoin { sq_type: Exists, cond: true, extern_cols: [ Extern(#0) ] }
                ├── LogicalScan { table: orders }
                └── LogicalProjection { exprs: [ #0, #1, #2, #3, #4, #5, #6, #7, #8, #9, #10, #11, #12, #13, #14, #15 ] }
                    └── LogicalFilter
                        ├── cond:And
                        │   ├── Eq
                        │   │   ├── #0
                        │   │   └── Extern(#0)
                        │   └── Lt
                        │       ├── #11
                        │       └── #12
                        └── LogicalScan { table: lineitem }
PhysicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── PhysicalAgg
    ├── aggrs:Agg(Count)
    │   └── [ 1(i64) ]
    ├── groups: [ #5 ]
    └── PhysicalFilter
        ├── cond:And
        │   ├── Geq
        │   │   ├── #4
        │   │   └── Cast { cast_to: Date32, child: "1993-07-01" }
        │   ├── Lt
        │   │   ├── #4
        │   │   └── Add
        │   │       ├── Cast { cast_to: Date32, child: "1993-07-01" }
        │   │       └── INTERVAL_MONTH_DAY_NANO (3, 0, 0)
        │   └── #9
        └── PhysicalNestedLoopJoin
            ├── join_type: LeftMark
            ├── cond:And
            │   └── Eq
            │       ├── #0
            │       └── #9
            ├── PhysicalScan { table: orders }
            └── PhysicalProjection { exprs: [ #16, #0, #1, #2, #3, #4, #5, #6, #7, #8, #9, #10, #11, #12, #13, #14, #15 ] }
                └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
                    ├── PhysicalFilter
                    │   ├── cond:Lt
                    │   │   ├── #11
                    │   │   └── #12
                    │   └── PhysicalScan { table: lineitem }
                    └── PhysicalAgg { aggrs: [], groups: [ #0 ] }
                        └── PhysicalScan { table: orders }
*/

