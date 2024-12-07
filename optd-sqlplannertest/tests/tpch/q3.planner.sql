-- TPC-H Q3
SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority 
FROM
    customer,
    orders,
    lineitem 
WHERE
    c_mktsegment = 'FURNITURE' 
    AND c_custkey = o_custkey 
    AND l_orderkey = o_orderkey 
    AND o_orderdate < DATE '1995-03-29' 
    AND l_shipdate > DATE '1995-03-29' 
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority 
ORDER BY
    revenue DESC,
    o_orderdate LIMIT 10;

/*
LogicalLimit { skip: 0(i64), fetch: 10(i64) }
└── LogicalSort
    ├── exprs:
    │   ┌── SortOrder { order: Desc }
    │   │   └── #1
    │   └── SortOrder { order: Asc }
    │       └── #2
    └── LogicalProjection { exprs: [ #0, #3, #1, #2 ] }
        └── LogicalAgg
            ├── exprs:Agg(Sum)
            │   └── Mul
            │       ├── #22
            │       └── Sub
            │           ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
            │           └── #23
            ├── groups: [ #17, #12, #15 ]
            └── LogicalFilter
                ├── cond:And
                │   ├── Eq
                │   │   ├── #6
                │   │   └── "FURNITURE"
                │   ├── Eq
                │   │   ├── #0
                │   │   └── #9
                │   ├── Eq
                │   │   ├── #17
                │   │   └── #8
                │   ├── Lt
                │   │   ├── #12
                │   │   └── Cast { cast_to: Date32, child: "1995-03-29" }
                │   └── Gt
                │       ├── #27
                │       └── Cast { cast_to: Date32, child: "1995-03-29" }
                └── LogicalJoin { join_type: Cross, cond: true }
                    ├── LogicalJoin { join_type: Cross, cond: true }
                    │   ├── LogicalScan { table: customer }
                    │   └── LogicalScan { table: orders }
                    └── LogicalScan { table: lineitem }
PhysicalLimit { skip: 0(i64), fetch: 10(i64) }
└── PhysicalSort
    ├── exprs:
    │   ┌── SortOrder { order: Desc }
    │   │   └── #1
    │   └── SortOrder { order: Asc }
    │       └── #2
    └── PhysicalProjection { exprs: [ #0, #3, #1, #2 ] }
        └── PhysicalAgg
            ├── aggrs:Agg(Sum)
            │   └── Mul
            │       ├── #22
            │       └── Sub
            │           ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
            │           └── #23
            ├── groups: [ #17, #12, #15 ]
            └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #1 ] }
                ├── PhysicalFilter
                │   ├── cond:Eq
                │   │   ├── #6
                │   │   └── "FURNITURE"
                │   └── PhysicalScan { table: customer }
                └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
                    ├── PhysicalFilter
                    │   ├── cond:Lt
                    │   │   ├── #4
                    │   │   └── Cast { cast_to: Date32, child: "1995-03-29" }
                    │   └── PhysicalScan { table: orders }
                    └── PhysicalFilter
                        ├── cond:Gt
                        │   ├── #10
                        │   └── Cast { cast_to: Date32, child: "1995-03-29" }
                        └── PhysicalScan { table: lineitem }
*/

