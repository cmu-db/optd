-- TPC-H Q14
SELECT
    100.00 * sum(case when p_type like 'PROMO%'
                    then l_extendedprice * (1 - l_discount)
                    else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
FROM
    lineitem,
    part
WHERE
    l_partkey = p_partkey
    AND l_shipdate >= DATE '1995-09-01'
    AND l_shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH;

/*
LogicalProjection
├── exprs:Div
│   ├── Mul
│   │   ├── 100(float)
│   │   └── Cast { cast_to: Float64, child: #0 }
│   └── Cast { cast_to: Float64, child: #1 }
└── LogicalAgg
    ├── exprs:
    │   ┌── Agg(Sum)
    │   │   └── Case
    │   │       └── 
    │   │           ┌── Like { expr: #20, pattern: "PROMO%", negated: false, case_insensitive: false }
    │   │           ├── Mul
    │   │           │   ├── #5
    │   │           │   └── Sub
    │   │           │       ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
    │   │           │       └── #6
    │   │           └── Cast { cast_to: Decimal128(38, 4), child: 0(i64) }
    │   └── Agg(Sum)
    │       └── Mul
    │           ├── #5
    │           └── Sub
    │               ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
    │               └── #6
    ├── groups: []
    └── LogicalFilter
        ├── cond:And
        │   ├── Eq
        │   │   ├── #1
        │   │   └── #16
        │   ├── Geq
        │   │   ├── #10
        │   │   └── Cast { cast_to: Date32, child: "1995-09-01" }
        │   └── Lt
        │       ├── #10
        │       └── Add
        │           ├── Cast { cast_to: Date32, child: "1995-09-01" }
        │           └── INTERVAL_MONTH_DAY_NANO (1, 0, 0)
        └── LogicalJoin { join_type: Inner, cond: true }
            ├── LogicalScan { table: lineitem }
            └── LogicalScan { table: part }
PhysicalProjection
├── exprs:Div
│   ├── Mul
│   │   ├── 100(float)
│   │   └── Cast { cast_to: Float64, child: #0 }
│   └── Cast { cast_to: Float64, child: #1 }
└── PhysicalAgg
    ├── aggrs:
    │   ┌── Agg(Sum)
    │   │   └── Case
    │   │       └── 
    │   │           ┌── Like { expr: #20, pattern: "PROMO%", negated: false, case_insensitive: false }
    │   │           ├── Mul
    │   │           │   ├── #5
    │   │           │   └── Sub
    │   │           │       ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
    │   │           │       └── #6
    │   │           └── Cast { cast_to: Decimal128(38, 4), child: 0(i64) }
    │   └── Agg(Sum)
    │       └── Mul
    │           ├── #5
    │           └── Sub
    │               ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
    │               └── #6
    ├── groups: []
    └── PhysicalHashJoin { join_type: Inner, left_keys: [ #1 ], right_keys: [ #0 ] }
        ├── PhysicalFilter
        │   ├── cond:And
        │   │   ├── Geq
        │   │   │   ├── #10
        │   │   │   └── Cast { cast_to: Date32, child: "1995-09-01" }
        │   │   └── Lt
        │   │       ├── #10
        │   │       └── Add
        │   │           ├── Cast { cast_to: Date32, child: "1995-09-01" }
        │   │           └── INTERVAL_MONTH_DAY_NANO (1, 0, 0)
        │   └── PhysicalScan { table: lineitem }
        └── PhysicalScan { table: part }
*/

