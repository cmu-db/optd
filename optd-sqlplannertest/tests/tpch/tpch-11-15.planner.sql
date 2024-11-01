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

-- TPC-H Q11
select
    ps_partkey,
    sum(ps_supplycost * ps_availqty) as value
from
    partsupp,
    supplier,
    nation
where
    ps_suppkey = s_suppkey
    and s_nationkey = n_nationkey
    and n_name = 'CHINA'
group by
    ps_partkey having
        sum(ps_supplycost * ps_availqty) > (
            select
                sum(ps_supplycost * ps_availqty) * 0.0001000000
            from
                partsupp,
                supplier,
                nation
            where
                ps_suppkey = s_suppkey
                and s_nationkey = n_nationkey
                and n_name = 'CHINA'
        )
order by
    value desc;

/*
LogicalSort
├── exprs:SortOrder { order: Desc }
│   └── #1
└── LogicalProjection { exprs: [ #0, #1 ] }
    └── LogicalJoin
        ├── join_type: Inner
        ├── cond:Gt
        │   ├── Cast { cast_to: Decimal128(38, 15), expr: #1 }
        │   └── #2
        ├── LogicalAgg
        │   ├── exprs:Agg(Sum)
        │   │   └── Mul
        │   │       ├── #2
        │   │       └── Cast { cast_to: Decimal128(10, 0), expr: #1 }
        │   ├── groups: [ #0 ]
        │   └── LogicalProjection { exprs: [ #0, #1, #2 ] }
        │       └── LogicalJoin
        │           ├── join_type: Inner
        │           ├── cond:Eq
        │           │   ├── #3
        │           │   └── #4
        │           ├── LogicalProjection { exprs: [ #0, #2, #3, #5 ] }
        │           │   └── LogicalJoin
        │           │       ├── join_type: Inner
        │           │       ├── cond:Eq
        │           │       │   ├── #1
        │           │       │   └── #4
        │           │       ├── LogicalProjection { exprs: [ #0, #1, #2, #3 ] }
        │           │       │   └── LogicalScan { table: partsupp }
        │           │       └── LogicalProjection { exprs: [ #0, #3 ] }
        │           │           └── LogicalScan { table: supplier }
        │           └── LogicalProjection { exprs: [ #0 ] }
        │               └── LogicalFilter
        │                   ├── cond:Eq
        │                   │   ├── #1
        │                   │   └── "CHINA"
        │                   └── LogicalProjection { exprs: [ #0, #1 ] }
        │                       └── LogicalScan { table: nation }
        └── LogicalProjection
            ├── exprs:Cast
            │   ├── cast_to: Decimal128(38, 15)
            │   ├── expr:Mul
            │   │   ├── Cast { cast_to: Float64, expr: #0 }
            │   │   └── 0.0001(float)

            └── LogicalAgg
                ├── exprs:Agg(Sum)
                │   └── Mul
                │       ├── #1
                │       └── Cast { cast_to: Decimal128(10, 0), expr: #0 }
                ├── groups: []
                └── LogicalProjection { exprs: [ #0, #1 ] }
                    └── LogicalJoin
                        ├── join_type: Inner
                        ├── cond:Eq
                        │   ├── #2
                        │   └── #3
                        ├── LogicalProjection { exprs: [ #1, #2, #4 ] }
                        │   └── LogicalJoin
                        │       ├── join_type: Inner
                        │       ├── cond:Eq
                        │       │   ├── #0
                        │       │   └── #3
                        │       ├── LogicalProjection { exprs: [ #1, #2, #3 ] }
                        │       │   └── LogicalScan { table: partsupp }
                        │       └── LogicalProjection { exprs: [ #0, #3 ] }
                        │           └── LogicalScan { table: supplier }
                        └── LogicalProjection { exprs: [ #0 ] }
                            └── LogicalFilter
                                ├── cond:Eq
                                │   ├── #1
                                │   └── "CHINA"
                                └── LogicalProjection { exprs: [ #0, #1 ] }
                                    └── LogicalScan { table: nation }
PhysicalSort
├── exprs:SortOrder { order: Desc }
│   └── #1
└── PhysicalNestedLoopJoin
    ├── join_type: Inner
    ├── cond:Gt
    │   ├── Cast { cast_to: Decimal128(38, 15), expr: #1 }
    │   └── #0
    ├── PhysicalAgg
    │   ├── aggrs:Agg(Sum)
    │   │   └── Mul
    │   │       ├── #2
    │   │       └── Cast { cast_to: Decimal128(10, 0), expr: #1 }
    │   ├── groups: [ #0 ]
    │   └── PhysicalProjection { exprs: [ #11, #13, #14 ] }
    │       └── PhysicalHashJoin { join_type: Inner, left_keys: [ #4 ], right_keys: [ #1 ] }
    │           ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #3 ] }
    │           │   ├── PhysicalFilter
    │           │   │   ├── cond:Eq
    │           │   │   │   ├── #1
    │           │   │   │   └── "CHINA"
    │           │   │   └── PhysicalScan { table: nation }
    │           │   └── PhysicalScan { table: supplier }
    │           └── PhysicalScan { table: partsupp }
    └── PhysicalProjection
        ├── exprs:Cast
        │   ├── cast_to: Decimal128(38, 15)
        │   ├── expr:Mul
        │   │   ├── Cast { cast_to: Float64, expr: #0 }
        │   │   └── 0.0001(float)

        └── PhysicalAgg
            ├── aggrs:Agg(Sum)
            │   └── Mul
            │       ├── #1
            │       └── Cast { cast_to: Decimal128(10, 0), expr: #0 }
            ├── groups: []
            └── PhysicalProjection { exprs: [ #0, #1 ] }
                └── PhysicalHashJoin { join_type: Inner, left_keys: [ #2 ], right_keys: [ #0 ] }
                    ├── PhysicalProjection { exprs: [ #1, #2, #4 ] }
                    │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
                    │       ├── PhysicalProjection { exprs: [ #1, #2, #3 ] }
                    │       │   └── PhysicalScan { table: partsupp }
                    │       └── PhysicalProjection { exprs: [ #0, #3 ] }
                    │           └── PhysicalScan { table: supplier }
                    └── PhysicalProjection { exprs: [ #0 ] }
                        └── PhysicalFilter
                            ├── cond:Eq
                            │   ├── #1
                            │   └── "CHINA"
                            └── PhysicalScan { table: nation }
*/

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
            │   │   └── Cast { cast_to: Date32, expr: "1994-01-01" }
            │   └── Lt
            │       ├── #21
            │       └── Cast { cast_to: Date32, expr: "1995-01-01" }
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
            │   │   │   └── Cast { cast_to: Date32, expr: "1994-01-01" }
            │   │   └── Lt
            │   │       ├── #12
            │   │       └── Cast { cast_to: Date32, expr: "1995-01-01" }
            │   └── PhysicalScan { table: lineitem }
            └── PhysicalScan { table: orders }
*/

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
│   │   └── Cast { cast_to: Float64, expr: #0 }
│   └── Cast { cast_to: Float64, expr: #1 }
└── LogicalAgg
    ├── exprs:
    │   ┌── Agg(Sum)
    │   │   └── Case
    │   │       └── 
    │   │           ┌── Like { expr: #20, pattern: "PROMO%", negated: false, case_insensitive: false }
    │   │           ├── Mul
    │   │           │   ├── #5
    │   │           │   └── Sub
    │   │           │       ├── Cast { cast_to: Decimal128(20, 0), expr: 1(i64) }
    │   │           │       └── #6
    │   │           └── Cast { cast_to: Decimal128(38, 4), expr: 0(i64) }
    │   └── Agg(Sum)
    │       └── Mul
    │           ├── #5
    │           └── Sub
    │               ├── Cast { cast_to: Decimal128(20, 0), expr: 1(i64) }
    │               └── #6
    ├── groups: []
    └── LogicalFilter
        ├── cond:And
        │   ├── Eq
        │   │   ├── #1
        │   │   └── #16
        │   ├── Geq
        │   │   ├── #10
        │   │   └── Cast { cast_to: Date32, expr: "1995-09-01" }
        │   └── Lt
        │       ├── #10
        │       └── Add
        │           ├── Cast { cast_to: Date32, expr: "1995-09-01" }
        │           └── INTERVAL_MONTH_DAY_NANO (1, 0, 0)
        └── LogicalJoin { join_type: Cross, cond: true }
            ├── LogicalScan { table: lineitem }
            └── LogicalScan { table: part }
PhysicalProjection
├── exprs:Div
│   ├── Mul
│   │   ├── 100(float)
│   │   └── Cast { cast_to: Float64, expr: #0 }
│   └── Cast { cast_to: Float64, expr: #1 }
└── PhysicalAgg
    ├── aggrs:
    │   ┌── Agg(Sum)
    │   │   └── Case
    │   │       └── 
    │   │           ┌── Like { expr: #20, pattern: "PROMO%", negated: false, case_insensitive: false }
    │   │           ├── Mul
    │   │           │   ├── #5
    │   │           │   └── Sub
    │   │           │       ├── Cast { cast_to: Decimal128(20, 0), expr: 1(i64) }
    │   │           │       └── #6
    │   │           └── Cast { cast_to: Decimal128(38, 4), expr: 0(i64) }
    │   └── Agg(Sum)
    │       └── Mul
    │           ├── #5
    │           └── Sub
    │               ├── Cast { cast_to: Decimal128(20, 0), expr: 1(i64) }
    │               └── #6
    ├── groups: []
    └── PhysicalHashJoin { join_type: Inner, left_keys: [ #1 ], right_keys: [ #0 ] }
        ├── PhysicalFilter
        │   ├── cond:And
        │   │   ├── Geq
        │   │   │   ├── #10
        │   │   │   └── Cast { cast_to: Date32, expr: "1995-09-01" }
        │   │   └── Lt
        │   │       ├── #10
        │   │       └── Add
        │   │           ├── Cast { cast_to: Date32, expr: "1995-09-01" }
        │   │           └── INTERVAL_MONTH_DAY_NANO (1, 0, 0)
        │   └── PhysicalScan { table: lineitem }
        └── PhysicalScan { table: part }
*/

-- TPC-H Q15
WITH revenue0 (supplier_no, total_revenue) AS 
(
    SELECT
        l_suppkey,
        SUM(l_extendedprice * (1 - l_discount)) 
    FROM
        lineitem 
    WHERE
        l_shipdate >= DATE '1993-01-01' 
        AND l_shipdate < DATE '1993-01-01' + INTERVAL '3' MONTH 
    GROUP BY
        l_suppkey 
)
SELECT
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue 
FROM
    supplier,
    revenue0 
WHERE
    s_suppkey = supplier_no 
    AND total_revenue = 
    (
        SELECT
            MAX(total_revenue) 
        FROM
            revenue0 
    )
ORDER BY
    s_suppkey;

/*
LogicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── LogicalProjection { exprs: [ #0, #1, #2, #3, #4 ] }
    └── LogicalJoin
        ├── join_type: Inner
        ├── cond:Eq
        │   ├── #4
        │   └── #5
        ├── LogicalProjection { exprs: [ #0, #1, #2, #3, #5 ] }
        │   └── LogicalJoin
        │       ├── join_type: Inner
        │       ├── cond:Eq
        │       │   ├── #0
        │       │   └── #4
        │       ├── LogicalProjection { exprs: [ #0, #1, #2, #4 ] }
        │       │   └── LogicalScan { table: supplier }
        │       └── LogicalProjection { exprs: [ #0, #1 ] }
        │           └── LogicalAgg
        │               ├── exprs:Agg(Sum)
        │               │   └── Mul
        │               │       ├── #1
        │               │       └── Sub
        │               │           ├── 1(float)
        │               │           └── #2
        │               ├── groups: [ #0 ]
        │               └── LogicalProjection { exprs: [ #0, #1, #2 ] }
        │                   └── LogicalFilter
        │                       ├── cond:And
        │                       │   ├── Geq
        │                       │   │   ├── #3
        │                       │   │   └── 8401(i64)
        │                       │   └── Lt
        │                       │       ├── #3
        │                       │       └── 8491(i64)
        │                       └── LogicalProjection { exprs: [ #2, #5, #6, #10 ] }
        │                           └── LogicalScan { table: lineitem }
        └── LogicalAgg
            ├── exprs:Agg(Max)
            │   └── [ #0 ]
            ├── groups: []
            └── LogicalProjection { exprs: [ #1 ] }
                └── LogicalAgg
                    ├── exprs:Agg(Sum)
                    │   └── Mul
                    │       ├── #1
                    │       └── Sub
                    │           ├── 1(float)
                    │           └── #2
                    ├── groups: [ #0 ]
                    └── LogicalProjection { exprs: [ #0, #1, #2 ] }
                        └── LogicalFilter
                            ├── cond:And
                            │   ├── Geq
                            │   │   ├── #3
                            │   │   └── 8401(i64)
                            │   └── Lt
                            │       ├── #3
                            │       └── 8491(i64)
                            └── LogicalProjection { exprs: [ #2, #5, #6, #10 ] }
                                └── LogicalScan { table: lineitem }
PhysicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── PhysicalProjection { exprs: [ #3, #4, #5, #7, #2 ] }
    └── PhysicalHashJoin { join_type: Inner, left_keys: [ #1 ], right_keys: [ #0 ] }
        ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #1 ] }
        │   ├── PhysicalAgg
        │   │   ├── aggrs:Agg(Max)
        │   │   │   └── [ #0 ]
        │   │   ├── groups: []
        │   │   └── PhysicalProjection { exprs: [ #1 ] }
        │   │       └── PhysicalAgg
        │   │           ├── aggrs:Agg(Sum)
        │   │           │   └── Mul
        │   │           │       ├── #1
        │   │           │       └── Sub
        │   │           │           ├── 1(float)
        │   │           │           └── #2
        │   │           ├── groups: [ #0 ]
        │   │           └── PhysicalProjection { exprs: [ #2, #5, #6 ] }
        │   │               └── PhysicalFilter
        │   │                   ├── cond:And
        │   │                   │   ├── Geq
        │   │                   │   │   ├── #10
        │   │                   │   │   └── 8401(i64)
        │   │                   │   └── Lt
        │   │                   │       ├── #10
        │   │                   │       └── 8491(i64)
        │   │                   └── PhysicalScan { table: lineitem }
        │   └── PhysicalAgg
        │       ├── aggrs:Agg(Sum)
        │       │   └── Mul
        │       │       ├── #1
        │       │       └── Sub
        │       │           ├── 1(float)
        │       │           └── #2
        │       ├── groups: [ #0 ]
        │       └── PhysicalProjection { exprs: [ #2, #5, #6 ] }
        │           └── PhysicalFilter
        │               ├── cond:And
        │               │   ├── Geq
        │               │   │   ├── #10
        │               │   │   └── 8401(i64)
        │               │   └── Lt
        │               │       ├── #10
        │               │       └── 8491(i64)
        │               └── PhysicalScan { table: lineitem }
        └── PhysicalScan { table: supplier }
*/

