-- TPC-H Q1
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
FROM
    lineitem
WHERE
    l_shipdate <= date '1998-12-01' - interval '90' day
GROUP BY
    l_returnflag, l_linestatus
ORDER BY
    l_returnflag, l_linestatus;

/*
LogicalSort
├── exprs:
│   ┌── SortOrder { order: Asc }
│   │   └── #0
│   └── SortOrder { order: Asc }
│       └── #1
└── LogicalProjection { exprs: [ #0, #1, #2, #3, #4, #5, #6, #7, #8, #9 ] }
    └── LogicalAgg
        ├── exprs:
        │   ┌── Agg(Sum)
        │   │   └── [ #4 ]
        │   ├── Agg(Sum)
        │   │   └── [ #5 ]
        │   ├── Agg(Sum)
        │   │   └── Mul
        │   │       ├── #5
        │   │       └── Sub
        │   │           ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
        │   │           └── #6
        │   ├── Agg(Sum)
        │   │   └── Mul
        │   │       ├── Mul
        │   │       │   ├── #5
        │   │       │   └── Sub
        │   │       │       ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
        │   │       │       └── #6
        │   │       └── Add
        │   │           ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
        │   │           └── #7
        │   ├── Agg(Avg)
        │   │   └── [ #4 ]
        │   ├── Agg(Avg)
        │   │   └── [ #5 ]
        │   ├── Agg(Avg)
        │   │   └── [ #6 ]
        │   └── Agg(Count)
        │       └── [ 1(i64) ]
        ├── groups: [ #8, #9 ]
        └── LogicalFilter
            ├── cond:Leq
            │   ├── #10
            │   └── Sub
            │       ├── Cast { cast_to: Date32, child: "1998-12-01" }
            │       └── INTERVAL_MONTH_DAY_NANO (0, 90, 0)
            └── LogicalScan { table: lineitem }
PhysicalSort
├── exprs:
│   ┌── SortOrder { order: Asc }
│   │   └── #0
│   └── SortOrder { order: Asc }
│       └── #1
└── PhysicalAgg
    ├── aggrs:
    │   ┌── Agg(Sum)
    │   │   └── [ #4 ]
    │   ├── Agg(Sum)
    │   │   └── [ #5 ]
    │   ├── Agg(Sum)
    │   │   └── Mul
    │   │       ├── #5
    │   │       └── Sub
    │   │           ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
    │   │           └── #6
    │   ├── Agg(Sum)
    │   │   └── Mul
    │   │       ├── Mul
    │   │       │   ├── #5
    │   │       │   └── Sub
    │   │       │       ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
    │   │       │       └── #6
    │   │       └── Add
    │   │           ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
    │   │           └── #7
    │   ├── Agg(Avg)
    │   │   └── [ #4 ]
    │   ├── Agg(Avg)
    │   │   └── [ #5 ]
    │   ├── Agg(Avg)
    │   │   └── [ #6 ]
    │   └── Agg(Count)
    │       └── [ 1(i64) ]
    ├── groups: [ #8, #9 ]
    └── PhysicalFilter
        ├── cond:Leq
        │   ├── #10
        │   └── Sub
        │       ├── Cast { cast_to: Date32, child: "1998-12-01" }
        │       └── INTERVAL_MONTH_DAY_NANO (0, 90, 0)
        └── PhysicalScan { table: lineitem }
*/

