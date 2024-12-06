-- TPC-H Q12
SELECT
    l_shipmode,
    sum(case when o_orderpriority = '1-URGENT'
             or o_orderpriority = '2-HIGH'
             then 1 else 0 end) as high_priority_orders,
    sum(case when o_orderpriority <> '1-URGENT'
             and o_orderpriority <> '2-HIGH'
             then 1 else 0 end) as low_priority_orders
FROM
    orders,
    lineitem
WHERE
    o_orderkey = l_orderkey
    AND l_shipmode in ('MAIL', 'SHIP')
    AND l_commitdate < l_receiptdate
    AND l_shipdate < l_commitdate
    AND l_receiptdate >= DATE '1994-01-01'
    AND l_receiptdate < DATE '1995-01-01'
GROUP BY
    l_shipmode
ORDER BY
    l_shipmode;

/*
LogicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── LogicalProjection { exprs: [ #0, #1, #2 ] }
    └── LogicalAgg
        ├── exprs:
        │   ┌── Agg(Sum)
        │   │   └── Case
        │   │       └── 
        │   │           ┌── Or
        │   │           │   ├── Eq
        │   │           │   │   ├── #5
        │   │           │   │   └── "1-URGENT"
        │   │           │   └── Eq
        │   │           │       ├── #5
        │   │           │       └── "2-HIGH"
        │   │           ├── 1(i64)
        │   │           └── 0(i64)
        │   └── Agg(Sum)
        │       └── Case
        │           └── 
        │               ┌── And
        │               │   ├── Neq
        │               │   │   ├── #5
        │               │   │   └── "1-URGENT"
        │               │   └── Neq
        │               │       ├── #5
        │               │       └── "2-HIGH"
        │               ├── 1(i64)
        │               └── 0(i64)
        ├── groups: [ #23 ]
        └── LogicalFilter
            ├── cond:And
            │   ├── Eq
            │   │   ├── #0
            │   │   └── #9
            │   ├── InList { expr: #23, list: [ "MAIL", "SHIP" ], negated: false }
            │   ├── Lt
            │   │   ├── #20
            │   │   └── #21
            │   ├── Lt
            │   │   ├── #19
            │   │   └── #20
            │   ├── Geq
            │   │   ├── #21
            │   │   └── Cast { cast_to: Date32, child: "1994-01-01" }
            │   └── Lt
            │       ├── #21
            │       └── Cast { cast_to: Date32, child: "1995-01-01" }
            └── LogicalJoin { join_type: Cross, cond: true }
                ├── LogicalScan { table: orders }
                └── LogicalScan { table: lineitem }
PhysicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── PhysicalAgg
    ├── aggrs:
    │   ┌── Agg(Sum)
    │   │   └── Case
    │   │       └── 
    │   │           ┌── Or
    │   │           │   ├── Eq
    │   │           │   │   ├── #5
    │   │           │   │   └── "1-URGENT"
    │   │           │   └── Eq
    │   │           │       ├── #5
    │   │           │       └── "2-HIGH"
    │   │           ├── 1(i64)
    │   │           └── 0(i64)
    │   └── Agg(Sum)
    │       └── Case
    │           └── 
    │               ┌── And
    │               │   ├── Neq
    │               │   │   ├── #5
    │               │   │   └── "1-URGENT"
    │               │   └── Neq
    │               │       ├── #5
    │               │       └── "2-HIGH"
    │               ├── 1(i64)
    │               └── 0(i64)
    ├── groups: [ #23 ]
    └── PhysicalProjection { exprs: [ #16, #17, #18, #19, #20, #21, #22, #23, #24, #0, #1, #2, #3, #4, #5, #6, #7, #8, #9, #10, #11, #12, #13, #14, #15 ] }
        └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
            ├── PhysicalFilter
            │   ├── cond:And
            │   │   ├── InList { expr: #14, list: [ "MAIL", "SHIP" ], negated: false }
            │   │   ├── Lt
            │   │   │   ├── #11
            │   │   │   └── #12
            │   │   ├── Lt
            │   │   │   ├── #10
            │   │   │   └── #11
            │   │   ├── Geq
            │   │   │   ├── #12
            │   │   │   └── Cast { cast_to: Date32, child: "1994-01-01" }
            │   │   └── Lt
            │   │       ├── #12
            │   │       └── Cast { cast_to: Date32, child: "1995-01-01" }
            │   └── PhysicalScan { table: lineitem }
            └── PhysicalScan { table: orders }
*/

