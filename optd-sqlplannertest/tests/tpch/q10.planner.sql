-- TPC-H Q10
SELECT
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM
    customer,
    orders,
    lineitem,
    nation
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate >= DATE '1993-07-01'
    AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH
    AND l_returnflag = 'R'
    AND c_nationkey = n_nationkey
GROUP BY
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
ORDER BY
    revenue DESC
LIMIT 20;

/*
LogicalLimit { skip: 0(i64), fetch: 20(i64) }
└── LogicalSort
    ├── exprs:SortOrder { order: Desc }
    │   └── #2
    └── LogicalProjection { exprs: [ #0, #1, #7, #2, #4, #5, #3, #6 ] }
        └── LogicalAgg
            ├── exprs:Agg(Sum)
            │   └── Mul
            │       ├── #22
            │       └── Sub
            │           ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
            │           └── #23
            ├── groups: [ #0, #1, #5, #4, #34, #2, #7 ]
            └── LogicalFilter
                ├── cond:And
                │   ├── Eq
                │   │   ├── #0
                │   │   └── #9
                │   ├── Eq
                │   │   ├── #17
                │   │   └── #8
                │   ├── Geq
                │   │   ├── #12
                │   │   └── Cast { cast_to: Date32, child: "1993-07-01" }
                │   ├── Lt
                │   │   ├── #12
                │   │   └── Add
                │   │       ├── Cast { cast_to: Date32, child: "1993-07-01" }
                │   │       └── INTERVAL_MONTH_DAY_NANO (3, 0, 0)
                │   ├── Eq
                │   │   ├── #25
                │   │   └── "R"
                │   └── Eq
                │       ├── #3
                │       └── #33
                └── LogicalJoin { join_type: Inner, cond: true }
                    ├── LogicalJoin { join_type: Inner, cond: true }
                    │   ├── LogicalJoin { join_type: Inner, cond: true }
                    │   │   ├── LogicalScan { table: customer }
                    │   │   └── LogicalScan { table: orders }
                    │   └── LogicalScan { table: lineitem }
                    └── LogicalScan { table: nation }
PhysicalLimit { skip: 0(i64), fetch: 20(i64) }
└── PhysicalSort
    ├── exprs:SortOrder { order: Desc }
    │   └── #2
    └── PhysicalProjection { exprs: [ #0, #1, #7, #2, #4, #5, #3, #6 ] }
        └── PhysicalAgg
            ├── aggrs:Agg(Sum)
            │   └── Mul
            │       ├── #22
            │       └── Sub
            │           ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
            │           └── #23
            ├── groups: [ #0, #1, #5, #4, #34, #2, #7 ]
            └── PhysicalHashJoin { join_type: Inner, left_keys: [ #3 ], right_keys: [ #0 ] }
                ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #8 ], right_keys: [ #0 ] }
                │   ├── PhysicalProjection { exprs: [ #9, #10, #11, #12, #13, #14, #15, #16, #0, #1, #2, #3, #4, #5, #6, #7, #8 ] }
                │   │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #1 ], right_keys: [ #0 ] }
                │   │       ├── PhysicalFilter
                │   │       │   ├── cond:And
                │   │       │   │   ├── Geq
                │   │       │   │   │   ├── #4
                │   │       │   │   │   └── Cast { cast_to: Date32, child: "1993-07-01" }
                │   │       │   │   └── Lt
                │   │       │   │       ├── #4
                │   │       │   │       └── Add
                │   │       │   │           ├── Cast { cast_to: Date32, child: "1993-07-01" }
                │   │       │   │           └── INTERVAL_MONTH_DAY_NANO (3, 0, 0)
                │   │       │   └── PhysicalScan { table: orders }
                │   │       └── PhysicalScan { table: customer }
                │   └── PhysicalFilter
                │       ├── cond:Eq
                │       │   ├── #8
                │       │   └── "R"
                │       └── PhysicalScan { table: lineitem }
                └── PhysicalScan { table: nation }
*/

