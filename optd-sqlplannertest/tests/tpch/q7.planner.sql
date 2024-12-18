-- TPC-H Q7
SELECT
    supp_nation,
    cust_nation,
    l_year,
    SUM(volume) AS revenue
FROM
    (
        SELECT
            n1.n_name AS supp_nation,
            n2.n_name AS cust_nation,
            EXTRACT(YEAR FROM l_shipdate) AS l_year,
            l_extendedprice * (1 - l_discount) AS volume
        FROM
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2
        WHERE
            s_suppkey = l_suppkey
            AND o_orderkey = l_orderkey
            AND c_custkey = o_custkey
            AND s_nationkey = n1.n_nationkey
            AND c_nationkey = n2.n_nationkey
            AND (
                (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
            )
            AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
    ) AS shipping
GROUP BY
    supp_nation,
    cust_nation,
    l_year
ORDER BY
    supp_nation,
    cust_nation,
    l_year;

/*
LogicalSort
├── exprs:
│   ┌── SortOrder { order: Asc }
│   │   └── #0
│   ├── SortOrder { order: Asc }
│   │   └── #1
│   └── SortOrder { order: Asc }
│       └── #2
└── LogicalProjection { exprs: [ #0, #1, #2, #3 ] }
    └── LogicalAgg
        ├── exprs:Agg(Sum)
        │   └── [ #3 ]
        ├── groups: [ #0, #1, #2 ]
        └── LogicalProjection
            ├── exprs:
            │   ┌── #41
            │   ├── #45
            │   ├── Scalar(DatePart)
            │   │   └── [ "YEAR", #17 ]
            │   └── Mul
            │       ├── #12
            │       └── Sub
            │           ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
            │           └── #13
            └── LogicalFilter
                ├── cond:And
                │   ├── Eq
                │   │   ├── #0
                │   │   └── #9
                │   ├── Eq
                │   │   ├── #23
                │   │   └── #7
                │   ├── Eq
                │   │   ├── #32
                │   │   └── #24
                │   ├── Eq
                │   │   ├── #3
                │   │   └── #40
                │   ├── Eq
                │   │   ├── #35
                │   │   └── #44
                │   ├── Or
                │   │   ├── And
                │   │   │   ├── Eq
                │   │   │   │   ├── #41
                │   │   │   │   └── "FRANCE"
                │   │   │   └── Eq
                │   │   │       ├── #45
                │   │   │       └── "GERMANY"
                │   │   └── And
                │   │       ├── Eq
                │   │       │   ├── #41
                │   │       │   └── "GERMANY"
                │   │       └── Eq
                │   │           ├── #45
                │   │           └── "FRANCE"
                │   └── Between { child: #17, lower: Cast { cast_to: Date32, child: "1995-01-01" }, upper: Cast { cast_to: Date32, child: "1996-12-31" } }
                └── LogicalJoin { join_type: Inner, cond: true }
                    ├── LogicalJoin { join_type: Inner, cond: true }
                    │   ├── LogicalJoin { join_type: Inner, cond: true }
                    │   │   ├── LogicalJoin { join_type: Inner, cond: true }
                    │   │   │   ├── LogicalJoin { join_type: Inner, cond: true }
                    │   │   │   │   ├── LogicalScan { table: supplier }
                    │   │   │   │   └── LogicalScan { table: lineitem }
                    │   │   │   └── LogicalScan { table: orders }
                    │   │   └── LogicalScan { table: customer }
                    │   └── LogicalScan { table: nation }
                    └── LogicalScan { table: nation }
PhysicalSort
├── exprs:
│   ┌── SortOrder { order: Asc }
│   │   └── #0
│   ├── SortOrder { order: Asc }
│   │   └── #1
│   └── SortOrder { order: Asc }
│       └── #2
└── PhysicalAgg
    ├── aggrs:Agg(Sum)
    │   └── [ #3 ]
    ├── groups: [ #0, #1, #2 ]
    └── PhysicalProjection
        ├── exprs:
        │   ┌── #41
        │   ├── #45
        │   ├── Scalar(DatePart)
        │   │   └── [ "YEAR", #17 ]
        │   └── Mul
        │       ├── #12
        │       └── Sub
        │           ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
        │           └── #13
        └── PhysicalNestedLoopJoin
            ├── join_type: Inner
            ├── cond:And
            │   ├── Eq
            │   │   ├── #35
            │   │   └── #44
            │   └── Or
            │       ├── And
            │       │   ├── Eq
            │       │   │   ├── #41
            │       │   │   └── "FRANCE"
            │       │   └── Eq
            │       │       ├── #45
            │       │       └── "GERMANY"
            │       └── And
            │           ├── Eq
            │           │   ├── #41
            │           │   └── "GERMANY"
            │           └── Eq
            │               ├── #45
            │               └── "FRANCE"
            ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #3 ], right_keys: [ #0 ] }
            │   ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #24 ], right_keys: [ #0 ] }
            │   │   ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #7 ], right_keys: [ #0 ] }
            │   │   │   ├── PhysicalProjection { exprs: [ #16, #17, #18, #19, #20, #21, #22, #0, #1, #2, #3, #4, #5, #6, #7, #8, #9, #10, #11, #12, #13, #14, #15 ] }
            │   │   │   │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #2 ], right_keys: [ #0 ] }
            │   │   │   │       ├── PhysicalFilter { cond: Between { child: #10, lower: Cast { cast_to: Date32, child: "1995-01-01" }, upper: Cast { cast_to: Date32, child: "1996-12-31" } } }
            │   │   │   │       │   └── PhysicalScan { table: lineitem }
            │   │   │   │       └── PhysicalScan { table: supplier }
            │   │   │   └── PhysicalScan { table: orders }
            │   │   └── PhysicalScan { table: customer }
            │   └── PhysicalScan { table: nation }
            └── PhysicalScan { table: nation }
*/

