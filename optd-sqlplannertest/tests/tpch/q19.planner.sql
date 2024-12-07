-- TPC-H Q19
SELECT
    sum(l_extendedprice* (1 - l_discount)) as revenue
FROM
    lineitem,
    part
WHERE
    (
        p_partkey = l_partkey
        AND p_brand = 'Brand#12'
        AND p_container IN ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        AND l_quantity >= 1 AND l_quantity <= 11
        AND p_size BETWEEN 1 AND 5
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    ) OR (
        p_partkey = l_partkey
        AND p_brand = 'Brand#23'
        AND p_container IN ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        AND l_quantity >= 10 AND l_quantity <= 20
        AND p_size BETWEEN 1 AND 10
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    ) OR (
        p_partkey = l_partkey
        AND p_brand = 'Brand#34'
        AND p_container IN ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        AND l_quantity >= 20 AND l_quantity <= 30
        AND p_size BETWEEN 1 AND 15
        AND l_shipmode IN ('AIR', 'AIR REG')
        AND l_shipinstruct = 'DELIVER IN PERSON'
    )

/*
LogicalProjection { exprs: [ #0 ] }
└── LogicalAgg
    ├── exprs:Agg(Sum)
    │   └── Mul
    │       ├── #5
    │       └── Sub
    │           ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
    │           └── #6
    ├── groups: []
    └── LogicalFilter
        ├── cond:Or
        │   ├── And
        │   │   ├── Eq
        │   │   │   ├── #16
        │   │   │   └── #1
        │   │   ├── Eq
        │   │   │   ├── #19
        │   │   │   └── "Brand#12"
        │   │   ├── InList { expr: #22, list: [ "SM CASE", "SM BOX", "SM PACK", "SM PKG" ], negated: false }
        │   │   ├── Geq
        │   │   │   ├── Cast { cast_to: Decimal128(22, 2), child: #4 }
        │   │   │   └── Cast { cast_to: Decimal128(22, 2), child: 1(i64) }
        │   │   ├── Leq
        │   │   │   ├── Cast { cast_to: Decimal128(22, 2), child: #4 }
        │   │   │   └── Cast { cast_to: Decimal128(22, 2), child: 11(i64) }
        │   │   ├── Between { child: Cast { cast_to: Int64, child: #21 }, lower: 1(i64), upper: 5(i64) }
        │   │   ├── InList { expr: #14, list: [ "AIR", "AIR REG" ], negated: false }
        │   │   └── Eq
        │   │       ├── #13
        │   │       └── "DELIVER IN PERSON"
        │   ├── And
        │   │   ├── Eq
        │   │   │   ├── #16
        │   │   │   └── #1
        │   │   ├── Eq
        │   │   │   ├── #19
        │   │   │   └── "Brand#23"
        │   │   ├── InList { expr: #22, list: [ "MED BAG", "MED BOX", "MED PKG", "MED PACK" ], negated: false }
        │   │   ├── Geq
        │   │   │   ├── Cast { cast_to: Decimal128(22, 2), child: #4 }
        │   │   │   └── Cast { cast_to: Decimal128(22, 2), child: 10(i64) }
        │   │   ├── Leq
        │   │   │   ├── Cast { cast_to: Decimal128(22, 2), child: #4 }
        │   │   │   └── Cast { cast_to: Decimal128(22, 2), child: 20(i64) }
        │   │   ├── Between { child: Cast { cast_to: Int64, child: #21 }, lower: 1(i64), upper: 10(i64) }
        │   │   ├── InList { expr: #14, list: [ "AIR", "AIR REG" ], negated: false }
        │   │   └── Eq
        │   │       ├── #13
        │   │       └── "DELIVER IN PERSON"
        │   └── And
        │       ├── Eq
        │       │   ├── #16
        │       │   └── #1
        │       ├── Eq
        │       │   ├── #19
        │       │   └── "Brand#34"
        │       ├── InList { expr: #22, list: [ "LG CASE", "LG BOX", "LG PACK", "LG PKG" ], negated: false }
        │       ├── Geq
        │       │   ├── Cast { cast_to: Decimal128(22, 2), child: #4 }
        │       │   └── Cast { cast_to: Decimal128(22, 2), child: 20(i64) }
        │       ├── Leq
        │       │   ├── Cast { cast_to: Decimal128(22, 2), child: #4 }
        │       │   └── Cast { cast_to: Decimal128(22, 2), child: 30(i64) }
        │       ├── Between { child: Cast { cast_to: Int64, child: #21 }, lower: 1(i64), upper: 15(i64) }
        │       ├── InList { expr: #14, list: [ "AIR", "AIR REG" ], negated: false }
        │       └── Eq
        │           ├── #13
        │           └── "DELIVER IN PERSON"
        └── LogicalJoin { join_type: Cross, cond: true }
            ├── LogicalScan { table: lineitem }
            └── LogicalScan { table: part }
PhysicalAgg
├── aggrs:Agg(Sum)
│   └── Mul
│       ├── #5
│       └── Sub
│           ├── Cast { cast_to: Decimal128(20, 0), child: 1(i64) }
│           └── #6
├── groups: []
└── PhysicalFilter
    ├── cond:Or
    │   ├── And
    │   │   ├── Eq
    │   │   │   ├── #16
    │   │   │   └── #1
    │   │   ├── Eq
    │   │   │   ├── #19
    │   │   │   └── "Brand#12"
    │   │   ├── InList { expr: #22, list: [ "SM CASE", "SM BOX", "SM PACK", "SM PKG" ], negated: false }
    │   │   ├── Geq
    │   │   │   ├── Cast { cast_to: Decimal128(22, 2), child: #4 }
    │   │   │   └── Cast { cast_to: Decimal128(22, 2), child: 1(i64) }
    │   │   ├── Leq
    │   │   │   ├── Cast { cast_to: Decimal128(22, 2), child: #4 }
    │   │   │   └── Cast { cast_to: Decimal128(22, 2), child: 11(i64) }
    │   │   ├── Between { child: Cast { cast_to: Int64, child: #21 }, lower: 1(i64), upper: 5(i64) }
    │   │   ├── InList { expr: #14, list: [ "AIR", "AIR REG" ], negated: false }
    │   │   └── Eq
    │   │       ├── #13
    │   │       └── "DELIVER IN PERSON"
    │   ├── And
    │   │   ├── Eq
    │   │   │   ├── #16
    │   │   │   └── #1
    │   │   ├── Eq
    │   │   │   ├── #19
    │   │   │   └── "Brand#23"
    │   │   ├── InList { expr: #22, list: [ "MED BAG", "MED BOX", "MED PKG", "MED PACK" ], negated: false }
    │   │   ├── Geq
    │   │   │   ├── Cast { cast_to: Decimal128(22, 2), child: #4 }
    │   │   │   └── Cast { cast_to: Decimal128(22, 2), child: 10(i64) }
    │   │   ├── Leq
    │   │   │   ├── Cast { cast_to: Decimal128(22, 2), child: #4 }
    │   │   │   └── Cast { cast_to: Decimal128(22, 2), child: 20(i64) }
    │   │   ├── Between { child: Cast { cast_to: Int64, child: #21 }, lower: 1(i64), upper: 10(i64) }
    │   │   ├── InList { expr: #14, list: [ "AIR", "AIR REG" ], negated: false }
    │   │   └── Eq
    │   │       ├── #13
    │   │       └── "DELIVER IN PERSON"
    │   └── And
    │       ├── Eq
    │       │   ├── #16
    │       │   └── #1
    │       ├── Eq
    │       │   ├── #19
    │       │   └── "Brand#34"
    │       ├── InList { expr: #22, list: [ "LG CASE", "LG BOX", "LG PACK", "LG PKG" ], negated: false }
    │       ├── Geq
    │       │   ├── Cast { cast_to: Decimal128(22, 2), child: #4 }
    │       │   └── Cast { cast_to: Decimal128(22, 2), child: 20(i64) }
    │       ├── Leq
    │       │   ├── Cast { cast_to: Decimal128(22, 2), child: #4 }
    │       │   └── Cast { cast_to: Decimal128(22, 2), child: 30(i64) }
    │       ├── Between { child: Cast { cast_to: Int64, child: #21 }, lower: 1(i64), upper: 15(i64) }
    │       ├── InList { expr: #14, list: [ "AIR", "AIR REG" ], negated: false }
    │       └── Eq
    │           ├── #13
    │           └── "DELIVER IN PERSON"
    └── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
        ├── PhysicalScan { table: lineitem }
        └── PhysicalScan { table: part }
*/

