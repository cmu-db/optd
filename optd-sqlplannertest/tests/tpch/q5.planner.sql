-- TPC-H Q5
SELECT
    n_name AS nation,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'Asia' -- Specified region
    AND o_orderdate >= DATE '2023-01-01'
    AND o_orderdate < DATE '2024-01-01'
GROUP BY
    n_name
ORDER BY
    revenue DESC;

/*
LogicalSort
├── exprs:SortOrder { order: Desc }
│   └── #1
└── LogicalProjection { exprs: [ #0, #1 ] }
    └── LogicalAgg
        ├── exprs:Agg(Sum)
        │   └── Mul
        │       ├── #22
        │       └── Sub
        │           ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
        │           └── #23
        ├── groups: [ #41 ]
        └── LogicalFilter
            ├── cond:And
            │   ├── Eq
            │   │   ├── #0
            │   │   └── #9
            │   ├── Eq
            │   │   ├── #17
            │   │   └── #8
            │   ├── Eq
            │   │   ├── #19
            │   │   └── #33
            │   ├── Eq
            │   │   ├── #3
            │   │   └── #36
            │   ├── Eq
            │   │   ├── #36
            │   │   └── #40
            │   ├── Eq
            │   │   ├── #42
            │   │   └── #44
            │   ├── Eq
            │   │   ├── #45
            │   │   └── "Asia"
            │   ├── Geq
            │   │   ├── #12
            │   │   └── Cast { cast_to: Date32, child: "2023-01-01" }
            │   └── Lt
            │       ├── #12
            │       └── Cast { cast_to: Date32, child: "2024-01-01" }
            └── LogicalJoin { join_type: Inner, cond: true }
                ├── LogicalJoin { join_type: Inner, cond: true }
                │   ├── LogicalJoin { join_type: Inner, cond: true }
                │   │   ├── LogicalJoin { join_type: Inner, cond: true }
                │   │   │   ├── LogicalJoin { join_type: Inner, cond: true }
                │   │   │   │   ├── LogicalScan { table: customer }
                │   │   │   │   └── LogicalScan { table: orders }
                │   │   │   └── LogicalScan { table: lineitem }
                │   │   └── LogicalScan { table: supplier }
                │   └── LogicalScan { table: nation }
                └── LogicalScan { table: region }
PhysicalSort
├── exprs:SortOrder { order: Desc }
│   └── #1
└── PhysicalAgg
    ├── aggrs:Agg(Sum)
    │   └── Mul
    │       ├── #22
    │       └── Sub
    │           ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
    │           └── #23
    ├── groups: [ #41 ]
    └── PhysicalHashJoin { join_type: Inner, left_keys: [ #42 ], right_keys: [ #0 ] }
        ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #36 ], right_keys: [ #0 ] }
        │   ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #19, #3 ], right_keys: [ #0, #3 ] }
        │   │   ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #8 ], right_keys: [ #0 ] }
        │   │   │   ├── PhysicalProjection { exprs: [ #9, #10, #11, #12, #13, #14, #15, #16, #0, #1, #2, #3, #4, #5, #6, #7, #8 ] }
        │   │   │   │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #1 ], right_keys: [ #0 ] }
        │   │   │   │       ├── PhysicalFilter
        │   │   │   │       │   ├── cond:And
        │   │   │   │       │   │   ├── Geq
        │   │   │   │       │   │   │   ├── #4
        │   │   │   │       │   │   │   └── Cast { cast_to: Date32, child: "2023-01-01" }
        │   │   │   │       │   │   └── Lt
        │   │   │   │       │   │       ├── #4
        │   │   │   │       │   │       └── Cast { cast_to: Date32, child: "2024-01-01" }
        │   │   │   │       │   └── PhysicalScan { table: orders }
        │   │   │   │       └── PhysicalScan { table: customer }
        │   │   │   └── PhysicalScan { table: lineitem }
        │   │   └── PhysicalScan { table: supplier }
        │   └── PhysicalScan { table: nation }
        └── PhysicalFilter
            ├── cond:Eq
            │   ├── #1
            │   └── "Asia"
            └── PhysicalScan { table: region }
*/

