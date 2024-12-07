-- TPC-H Q6
SELECT
    SUM(l_extendedprice * l_discount) AS revenue_loss
FROM
    lineitem
WHERE
    l_shipdate >= DATE '2023-01-01'
    AND l_shipdate < DATE '2024-01-01'
    AND l_discount BETWEEN 0.05 AND 0.07
    AND l_quantity < 24;

/*
LogicalProjection { exprs: [ #0 ] }
└── LogicalAgg
    ├── exprs:Agg(Sum)
    │   └── Mul
    │       ├── #5
    │       └── #6
    ├── groups: []
    └── LogicalFilter
        ├── cond:And
        │   ├── Geq
        │   │   ├── #10
        │   │   └── Cast { cast_to: Date32, child: "2023-01-01" }
        │   ├── Lt
        │   │   ├── #10
        │   │   └── Cast { cast_to: Date32, child: "2024-01-01" }
        │   ├── Between { child: Cast { cast_to: Decimal128(30, 15), child: #6 }, lower: Cast { cast_to: Decimal128(30, 15), child: 0.05(float) }, upper: Cast { cast_to: Decimal128(30, 15), child: 0.07(float) } }
        │   └── Lt
        │       ├── Cast { cast_to: Decimal128(22, 2), child: #4 }
        │       └── Cast { cast_to: Decimal128(22, 2), child: 24(i64) }
        └── LogicalScan { table: lineitem }
PhysicalAgg
├── aggrs:Agg(Sum)
│   └── Mul
│       ├── #5
│       └── #6
├── groups: []
└── PhysicalFilter
    ├── cond:And
    │   ├── Geq
    │   │   ├── #10
    │   │   └── Cast { cast_to: Date32, child: "2023-01-01" }
    │   ├── Lt
    │   │   ├── #10
    │   │   └── Cast { cast_to: Date32, child: "2024-01-01" }
    │   ├── Between { child: Cast { cast_to: Decimal128(30, 15), child: #6 }, lower: Cast { cast_to: Decimal128(30, 15), child: 0.05(float) }, upper: Cast { cast_to: Decimal128(30, 15), child: 0.07(float) } }
    │   └── Lt
    │       ├── Cast { cast_to: Decimal128(22, 2), child: #4 }
    │       └── Cast { cast_to: Decimal128(22, 2), child: 24(i64) }
    └── PhysicalScan { table: lineitem }
*/

