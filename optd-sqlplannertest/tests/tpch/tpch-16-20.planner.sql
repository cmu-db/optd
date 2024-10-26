-- TPC-H schema
CREATE TABLE NATION  (
    N_NATIONKEY  INT NOT NULL,
    N_NAME       CHAR(25) NOT NULL,
    N_REGIONKEY  INT NOT NULL,
    N_COMMENT    VARCHAR(152)
);

CREATE TABLE REGION  (
    R_REGIONKEY  INT NOT NULL,
    R_NAME       CHAR(25) NOT NULL,
    R_COMMENT    VARCHAR(152)
);

CREATE TABLE PART  (
    P_PARTKEY     INT NOT NULL,
    P_NAME        VARCHAR(55) NOT NULL,
    P_MFGR        CHAR(25) NOT NULL,
    P_BRAND       CHAR(10) NOT NULL,
    P_TYPE        VARCHAR(25) NOT NULL,
    P_SIZE        INT NOT NULL,
    P_CONTAINER   CHAR(10) NOT NULL,
    P_RETAILPRICE DECIMAL(15,2) NOT NULL,
    P_COMMENT     VARCHAR(23) NOT NULL
);

CREATE TABLE SUPPLIER (
    S_SUPPKEY     INT NOT NULL,
    S_NAME        CHAR(25) NOT NULL,
    S_ADDRESS     VARCHAR(40) NOT NULL,
    S_NATIONKEY   INT NOT NULL,
    S_PHONE       CHAR(15) NOT NULL,
    S_ACCTBAL     DECIMAL(15,2) NOT NULL,
    S_COMMENT     VARCHAR(101) NOT NULL
);

CREATE TABLE PARTSUPP (
    PS_PARTKEY     INT NOT NULL,
    PS_SUPPKEY     INT NOT NULL,
    PS_AVAILQTY    INT NOT NULL,
    PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
    PS_COMMENT     VARCHAR(199) NOT NULL
);

CREATE TABLE CUSTOMER (
    C_CUSTKEY     INT NOT NULL,
    C_NAME        VARCHAR(25) NOT NULL,
    C_ADDRESS     VARCHAR(40) NOT NULL,
    C_NATIONKEY   INT NOT NULL,
    C_PHONE       CHAR(15) NOT NULL,
    C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
    C_MKTSEGMENT  CHAR(10) NOT NULL,
    C_COMMENT     VARCHAR(117) NOT NULL
);

CREATE TABLE ORDERS (
    O_ORDERKEY       INT NOT NULL,
    O_CUSTKEY        INT NOT NULL,
    O_ORDERSTATUS    CHAR(1) NOT NULL,
    O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
    O_ORDERDATE      DATE NOT NULL,
    O_ORDERPRIORITY  CHAR(15) NOT NULL,  
    O_CLERK          CHAR(15) NOT NULL, 
    O_SHIPPRIORITY   INT NOT NULL,
    O_COMMENT        VARCHAR(79) NOT NULL
);

CREATE TABLE LINEITEM (
    L_ORDERKEY      INT NOT NULL,
    L_PARTKEY       INT NOT NULL,
    L_SUPPKEY       INT NOT NULL,
    L_LINENUMBER    INT NOT NULL,
    L_QUANTITY      DECIMAL(15,2) NOT NULL,
    L_EXTENDEDPRICE DECIMAL(15,2) NOT NULL,
    L_DISCOUNT      DECIMAL(15,2) NOT NULL,
    L_TAX           DECIMAL(15,2) NOT NULL,
    L_RETURNFLAG    CHAR(1) NOT NULL,
    L_LINESTATUS    CHAR(1) NOT NULL,
    L_SHIPDATE      DATE NOT NULL,
    L_COMMITDATE    DATE NOT NULL,
    L_RECEIPTDATE   DATE NOT NULL,
    L_SHIPINSTRUCT  CHAR(25) NOT NULL,
    L_SHIPMODE      CHAR(10) NOT NULL,
    L_COMMENT       VARCHAR(44) NOT NULL
);

/*

*/

-- TPC-H Q17
SELECT
    ROUND(SUM(l_extendedprice) / 7.0, 16) AS avg_yearly 
FROM
    lineitem,
    part 
WHERE
    p_partkey = l_partkey 
    AND p_brand = 'Brand#13' 
    AND p_container = 'JUMBO PKG' 
    AND l_quantity < ( 
        SELECT
            0.2 * AVG(l_quantity) 
        FROM
            lineitem 
        WHERE
            l_partkey = p_partkey 
    );

/*
LogicalProjection
├── exprs:Scalar(Round)
│   └── 
│       ┌── Div
│       │   ├── Cast { cast_to: Float64, expr: #0 }
│       │   └── 7(float)
│       └── 16(i64)
└── LogicalAgg
    ├── exprs:Agg(Sum)
    │   └── [ #0 ]
    ├── groups: []
    └── LogicalProjection { exprs: [ #1 ] }
        └── LogicalJoin
            ├── join_type: Inner
            ├── cond:And
            │   ├── Eq
            │   │   ├── #2
            │   │   └── #4
            │   └── Lt
            │       ├── Cast { cast_to: Decimal128(30, 15), expr: #0 }
            │       └── #3
            ├── LogicalProjection { exprs: [ #1, #2, #3 ] }
            │   └── LogicalJoin
            │       ├── join_type: Inner
            │       ├── cond:Eq
            │       │   ├── #0
            │       │   └── #3
            │       ├── LogicalProjection { exprs: [ #1, #4, #5 ] }
            │       │   └── LogicalScan { table: lineitem }
            │       └── LogicalProjection { exprs: [ #0 ] }
            │           └── LogicalFilter
            │               ├── cond:And
            │               │   ├── Eq
            │               │   │   ├── #1
            │               │   │   └── "Brand#13"
            │               │   └── Eq
            │               │       ├── #2
            │               │       └── "JUMBO PKG"
            │               └── LogicalProjection { exprs: [ #0, #3, #6 ] }
            │                   └── LogicalScan { table: part }
            └── LogicalProjection
                ├── exprs:
                │   ┌── Cast
                │   │   ├── cast_to: Decimal128(30, 15)
                │   │   ├── expr:Mul
                │   │   │   ├── 0.2(float)
                │   │   │   └── Cast { cast_to: Float64, expr: #1 }

                │   └── #0
                └── LogicalAgg
                    ├── exprs:Agg(Avg)
                    │   └── [ #1 ]
                    ├── groups: [ #0 ]
                    └── LogicalProjection { exprs: [ #1, #4 ] }
                        └── LogicalScan { table: lineitem }
PhysicalProjection
├── exprs:Scalar(Round)
│   └── 
│       ┌── Div
│       │   ├── Cast { cast_to: Float64, expr: #0 }
│       │   └── 7(float)
│       └── 16(i64)
└── PhysicalAgg
    ├── aggrs:Agg(Sum)
    │   └── [ #0 ]
    ├── groups: []
    └── PhysicalProjection { exprs: [ #1 ] }
        └── PhysicalNestedLoopJoin
            ├── join_type: Inner
            ├── cond:And
            │   ├── Eq
            │   │   ├── #2
            │   │   └── #0
            │   └── Lt
            │       ├── Cast { cast_to: Decimal128(30, 15), expr: #0 }
            │       └── #3
            ├── PhysicalProjection { exprs: [ #1, #2, #3 ] }
            │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
            │       ├── PhysicalProjection { exprs: [ #1, #4, #5 ] }
            │       │   └── PhysicalScan { table: lineitem }
            │       └── PhysicalProjection { exprs: [ #0 ] }
            │           └── PhysicalProjection { exprs: [ #0, #3, #6 ] }
            │               └── PhysicalFilter
            │                   ├── cond:And
            │                   │   ├── Eq
            │                   │   │   ├── #3
            │                   │   │   └── "Brand#13"
            │                   │   └── Eq
            │                   │       ├── #6
            │                   │       └── "JUMBO PKG"
            │                   └── PhysicalScan { table: part }
            └── PhysicalProjection
                ├── exprs:
                │   ┌── Cast
                │   │   ├── cast_to: Decimal128(30, 15)
                │   │   ├── expr:Mul
                │   │   │   ├── 0.2(float)
                │   │   │   └── Cast { cast_to: Float64, expr: #1 }

                │   └── #0
                └── PhysicalAgg
                    ├── aggrs:Agg(Avg)
                    │   └── [ #1 ]
                    ├── groups: [ #0 ]
                    └── PhysicalProjection { exprs: [ #1, #4 ] }
                        └── PhysicalScan { table: lineitem }
*/

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
    │           ├── Cast { cast_to: Decimal128(20, 0), expr: 1(i64) }
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
        │   │   │   ├── Cast { cast_to: Decimal128(22, 2), expr: #4 }
        │   │   │   └── Cast { cast_to: Decimal128(22, 2), expr: 1(i64) }
        │   │   ├── Leq
        │   │   │   ├── Cast { cast_to: Decimal128(22, 2), expr: #4 }
        │   │   │   └── Cast { cast_to: Decimal128(22, 2), expr: 11(i64) }
        │   │   ├── Between { expr: Cast { cast_to: Int64, expr: #21 }, lower: 1(i64), upper: 5(i64) }
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
        │   │   │   ├── Cast { cast_to: Decimal128(22, 2), expr: #4 }
        │   │   │   └── Cast { cast_to: Decimal128(22, 2), expr: 10(i64) }
        │   │   ├── Leq
        │   │   │   ├── Cast { cast_to: Decimal128(22, 2), expr: #4 }
        │   │   │   └── Cast { cast_to: Decimal128(22, 2), expr: 20(i64) }
        │   │   ├── Between { expr: Cast { cast_to: Int64, expr: #21 }, lower: 1(i64), upper: 10(i64) }
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
        │       │   ├── Cast { cast_to: Decimal128(22, 2), expr: #4 }
        │       │   └── Cast { cast_to: Decimal128(22, 2), expr: 20(i64) }
        │       ├── Leq
        │       │   ├── Cast { cast_to: Decimal128(22, 2), expr: #4 }
        │       │   └── Cast { cast_to: Decimal128(22, 2), expr: 30(i64) }
        │       ├── Between { expr: Cast { cast_to: Int64, expr: #21 }, lower: 1(i64), upper: 15(i64) }
        │       ├── InList { expr: #14, list: [ "AIR", "AIR REG" ], negated: false }
        │       └── Eq
        │           ├── #13
        │           └── "DELIVER IN PERSON"
        └── LogicalJoin { join_type: Cross, cond: true }
            ├── LogicalScan { table: lineitem }
            └── LogicalScan { table: part }
PhysicalProjection { exprs: [ #0 ] }
└── PhysicalAgg
    ├── aggrs:Agg(Sum)
    │   └── Mul
    │       ├── #5
    │       └── Sub
    │           ├── Cast { cast_to: Decimal128(20, 0), expr: 1(i64) }
    │           └── #6
    ├── groups: []
    └── PhysicalNestedLoopJoin
        ├── join_type: Inner
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
        │   │   │   ├── Cast { cast_to: Decimal128(22, 2), expr: #4 }
        │   │   │   └── Cast { cast_to: Decimal128(22, 2), expr: 1(i64) }
        │   │   ├── Leq
        │   │   │   ├── Cast { cast_to: Decimal128(22, 2), expr: #4 }
        │   │   │   └── Cast { cast_to: Decimal128(22, 2), expr: 11(i64) }
        │   │   ├── Between { expr: Cast { cast_to: Int64, expr: #21 }, lower: 1(i64), upper: 5(i64) }
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
        │   │   │   ├── Cast { cast_to: Decimal128(22, 2), expr: #4 }
        │   │   │   └── Cast { cast_to: Decimal128(22, 2), expr: 10(i64) }
        │   │   ├── Leq
        │   │   │   ├── Cast { cast_to: Decimal128(22, 2), expr: #4 }
        │   │   │   └── Cast { cast_to: Decimal128(22, 2), expr: 20(i64) }
        │   │   ├── Between { expr: Cast { cast_to: Int64, expr: #21 }, lower: 1(i64), upper: 10(i64) }
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
        │       │   ├── Cast { cast_to: Decimal128(22, 2), expr: #4 }
        │       │   └── Cast { cast_to: Decimal128(22, 2), expr: 20(i64) }
        │       ├── Leq
        │       │   ├── Cast { cast_to: Decimal128(22, 2), expr: #4 }
        │       │   └── Cast { cast_to: Decimal128(22, 2), expr: 30(i64) }
        │       ├── Between { expr: Cast { cast_to: Int64, expr: #21 }, lower: 1(i64), upper: 15(i64) }
        │       ├── InList { expr: #14, list: [ "AIR", "AIR REG" ], negated: false }
        │       └── Eq
        │           ├── #13
        │           └── "DELIVER IN PERSON"
        ├── PhysicalScan { table: lineitem }
        └── PhysicalScan { table: part }
*/

