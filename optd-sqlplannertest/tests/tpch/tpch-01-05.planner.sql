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
        │   │           ├── Cast { cast_to: Decimal128(20, 0), expr: 1(i64) }
        │   │           └── #6
        │   ├── Agg(Sum)
        │   │   └── Mul
        │   │       ├── Mul
        │   │       │   ├── #5
        │   │       │   └── Sub
        │   │       │       ├── Cast { cast_to: Decimal128(20, 0), expr: 1(i64) }
        │   │       │       └── #6
        │   │       └── Add
        │   │           ├── Cast { cast_to: Decimal128(20, 0), expr: 1(i64) }
        │   │           └── #7
        │   ├── Agg(Avg)
        │   │   └── [ #4 ]
        │   ├── Agg(Avg)
        │   │   └── [ #5 ]
        │   ├── Agg(Avg)
        │   │   └── [ #6 ]
        │   └── Agg(Count)
        │       └── [ 1(u8) ]
        ├── groups: [ #8, #9 ]
        └── LogicalFilter
            ├── cond:Leq
            │   ├── #10
            │   └── Sub
            │       ├── Cast { cast_to: Date32, expr: "1998-12-01" }
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
    │   │           ├── Cast { cast_to: Decimal128(20, 0), expr: 1(i64) }
    │   │           └── #6
    │   ├── Agg(Sum)
    │   │   └── Mul
    │   │       ├── Mul
    │   │       │   ├── #5
    │   │       │   └── Sub
    │   │       │       ├── Cast { cast_to: Decimal128(20, 0), expr: 1(i64) }
    │   │       │       └── #6
    │   │       └── Add
    │   │           ├── Cast { cast_to: Decimal128(20, 0), expr: 1(i64) }
    │   │           └── #7
    │   ├── Agg(Avg)
    │   │   └── [ #4 ]
    │   ├── Agg(Avg)
    │   │   └── [ #5 ]
    │   ├── Agg(Avg)
    │   │   └── [ #6 ]
    │   └── Agg(Count)
    │       └── [ 1(u8) ]
    ├── groups: [ #8, #9 ]
    └── PhysicalFilter
        ├── cond:Leq
        │   ├── #10
        │   └── Sub
        │       ├── Cast { cast_to: Date32, expr: "1998-12-01" }
        │       └── INTERVAL_MONTH_DAY_NANO (0, 90, 0)
        └── PhysicalScan { table: lineitem }
*/

-- TPC-H Q2
select
        s_acctbal,
        s_name,
        n_name,
        p_partkey,
        p_mfgr,
        s_address,
        s_phone,
        s_comment
from
        part,
        supplier,
        partsupp,
        nation,
        region
where
        p_partkey = ps_partkey
        and s_suppkey = ps_suppkey
and p_size = 4
and p_type like '%TIN'
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'AFRICA'
        and ps_supplycost = (
                select
                        min(ps_supplycost)
                from
                        partsupp,
                        supplier,
                        nation,
                        region
                where
                        p_partkey = ps_partkey
                        and s_suppkey = ps_suppkey
                        and s_nationkey = n_nationkey
                        and n_regionkey = r_regionkey
                        and r_name = 'AFRICA'
        )
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
limit 100;

/*
LogicalLimit { skip: 0(u64), fetch: 100(u64) }
└── LogicalSort
    ├── exprs:
    │   ┌── SortOrder { order: Desc }
    │   │   └── #0
    │   ├── SortOrder { order: Asc }
    │   │   └── #2
    │   ├── SortOrder { order: Asc }
    │   │   └── #1
    │   └── SortOrder { order: Asc }
    │       └── #3
    └── LogicalProjection { exprs: [ #14, #10, #22, #0, #2, #11, #13, #15 ] }
        └── LogicalFilter
            ├── cond:And
            │   ├── Eq
            │   │   ├── #0
            │   │   └── #16
            │   ├── Eq
            │   │   ├── #9
            │   │   └── #17
            │   ├── Eq
            │   │   ├── Cast { cast_to: Int64, expr: #5 }
            │   │   └── 4(i64)
            │   ├── Like { expr: #4, pattern: "%TIN", negated: false, case_insensitive: false }
            │   ├── Eq
            │   │   ├── #12
            │   │   └── #21
            │   ├── Eq
            │   │   ├── #23
            │   │   └── #25
            │   ├── Eq
            │   │   ├── #26
            │   │   └── "AFRICA"
            │   └── Eq
            │       ├── #19
            │       └── #28
            └── RawDependentJoin { join_type: Cross, cond: true, extern_cols: [ Extern(#0) ] }
                ├── LogicalJoin { join_type: Cross, cond: true }
                │   ├── LogicalJoin { join_type: Cross, cond: true }
                │   │   ├── LogicalJoin { join_type: Cross, cond: true }
                │   │   │   ├── LogicalJoin { join_type: Cross, cond: true }
                │   │   │   │   ├── LogicalScan { table: part }
                │   │   │   │   └── LogicalScan { table: supplier }
                │   │   │   └── LogicalScan { table: partsupp }
                │   │   └── LogicalScan { table: nation }
                │   └── LogicalScan { table: region }
                └── LogicalProjection { exprs: [ #0 ] }
                    └── LogicalAgg
                        ├── exprs:Agg(Min)
                        │   └── [ #3 ]
                        ├── groups: []
                        └── LogicalFilter
                            ├── cond:And
                            │   ├── Eq
                            │   │   ├── Extern(#0)
                            │   │   └── #0
                            │   ├── Eq
                            │   │   ├── #5
                            │   │   └── #1
                            │   ├── Eq
                            │   │   ├── #8
                            │   │   └── #12
                            │   ├── Eq
                            │   │   ├── #14
                            │   │   └── #16
                            │   └── Eq
                            │       ├── #17
                            │       └── "AFRICA"
                            └── LogicalJoin { join_type: Cross, cond: true }
                                ├── LogicalJoin { join_type: Cross, cond: true }
                                │   ├── LogicalJoin { join_type: Cross, cond: true }
                                │   │   ├── LogicalScan { table: partsupp }
                                │   │   └── LogicalScan { table: supplier }
                                │   └── LogicalScan { table: nation }
                                └── LogicalScan { table: region }
PhysicalLimit { skip: 0(u64), fetch: 100(u64) }
└── PhysicalSort
    ├── exprs:
    │   ┌── SortOrder { order: Desc }
    │   │   └── #0
    │   ├── SortOrder { order: Asc }
    │   │   └── #2
    │   ├── SortOrder { order: Asc }
    │   │   └── #1
    │   └── SortOrder { order: Asc }
    │       └── #3
    └── PhysicalProjection { exprs: [ #14, #10, #22, #0, #2, #11, #13, #15 ] }
        └── PhysicalHashJoin { join_type: Inner, left_keys: [ #19, #0 ], right_keys: [ #1, #0 ] }
            ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #23 ], right_keys: [ #0 ] }
            │   ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #12 ], right_keys: [ #0 ] }
            │   │   ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #0, #9 ], right_keys: [ #0, #1 ] }
            │   │   │   ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
            │   │   │   │   ├── PhysicalFilter
            │   │   │   │   │   ├── cond:And
            │   │   │   │   │   │   ├── Eq
            │   │   │   │   │   │   │   ├── Cast { cast_to: Int64, expr: #5 }
            │   │   │   │   │   │   │   └── 4(i64)
            │   │   │   │   │   │   └── Like { expr: #4, pattern: "%TIN", negated: false, case_insensitive: false }
            │   │   │   │   │   └── PhysicalScan { table: part }
            │   │   │   │   └── PhysicalScan { table: supplier }
            │   │   │   └── PhysicalScan { table: partsupp }
            │   │   └── PhysicalScan { table: nation }
            │   └── PhysicalFilter
            │       ├── cond:Eq
            │       │   ├── #1
            │       │   └── "AFRICA"
            │       └── PhysicalScan { table: region }
            └── PhysicalAgg
                ├── aggrs:Agg(Min)
                │   └── [ #4 ]
                ├── groups: [ #1 ]
                └── PhysicalFilter
                    ├── cond:And
                    │   ├── Eq
                    │   │   ├── #0
                    │   │   └── #1
                    │   ├── Eq
                    │   │   ├── #6
                    │   │   └── #2
                    │   ├── Eq
                    │   │   ├── #9
                    │   │   └── #13
                    │   ├── Eq
                    │   │   ├── #15
                    │   │   └── #17
                    │   └── Eq
                    │       ├── #18
                    │       └── "AFRICA"
                    └── PhysicalNestedLoopJoin { join_type: Inner, cond: true }
                        ├── PhysicalAgg { aggrs: [], groups: [ #0 ] }
                        │   └── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                        │       ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                        │       │   ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                        │       │   │   ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                        │       │   │   │   ├── PhysicalScan { table: part }
                        │       │   │   │   └── PhysicalScan { table: supplier }
                        │       │   │   └── PhysicalScan { table: partsupp }
                        │       │   └── PhysicalScan { table: nation }
                        │       └── PhysicalScan { table: region }
                        └── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                            ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                            │   ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                            │   │   ├── PhysicalScan { table: partsupp }
                            │   │   └── PhysicalScan { table: supplier }
                            │   └── PhysicalScan { table: nation }
                            └── PhysicalScan { table: region }
*/

-- TPC-H Q3
SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority 
FROM
    customer,
    orders,
    lineitem 
WHERE
    c_mktsegment = 'FURNITURE' 
    AND c_custkey = o_custkey 
    AND l_orderkey = o_orderkey 
    AND o_orderdate < DATE '1995-03-29' 
    AND l_shipdate > DATE '1995-03-29' 
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority 
ORDER BY
    revenue DESC,
    o_orderdate LIMIT 10;

/*
LogicalLimit { skip: 0(u64), fetch: 10(u64) }
└── LogicalSort
    ├── exprs:
    │   ┌── SortOrder { order: Desc }
    │   │   └── #1
    │   └── SortOrder { order: Asc }
    │       └── #2
    └── LogicalProjection { exprs: [ #0, #3, #1, #2 ] }
        └── LogicalAgg
            ├── exprs:Agg(Sum)
            │   └── Mul
            │       ├── #22
            │       └── Sub
            │           ├── Cast { cast_to: Decimal128(20, 0), expr: 1(i64) }
            │           └── #23
            ├── groups: [ #17, #12, #15 ]
            └── LogicalFilter
                ├── cond:And
                │   ├── Eq
                │   │   ├── #6
                │   │   └── "FURNITURE"
                │   ├── Eq
                │   │   ├── #0
                │   │   └── #9
                │   ├── Eq
                │   │   ├── #17
                │   │   └── #8
                │   ├── Lt
                │   │   ├── #12
                │   │   └── Cast { cast_to: Date32, expr: "1995-03-29" }
                │   └── Gt
                │       ├── #27
                │       └── Cast { cast_to: Date32, expr: "1995-03-29" }
                └── LogicalJoin { join_type: Cross, cond: true }
                    ├── LogicalJoin { join_type: Cross, cond: true }
                    │   ├── LogicalScan { table: customer }
                    │   └── LogicalScan { table: orders }
                    └── LogicalScan { table: lineitem }
PhysicalLimit { skip: 0(u64), fetch: 10(u64) }
└── PhysicalSort
    ├── exprs:
    │   ┌── SortOrder { order: Desc }
    │   │   └── #1
    │   └── SortOrder { order: Asc }
    │       └── #2
    └── PhysicalProjection { exprs: [ #0, #3, #1, #2 ] }
        └── PhysicalAgg
            ├── aggrs:Agg(Sum)
            │   └── Mul
            │       ├── #22
            │       └── Sub
            │           ├── Cast { cast_to: Decimal128(20, 0), expr: 1(i64) }
            │           └── #23
            ├── groups: [ #17, #12, #15 ]
            └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #1 ] }
                ├── PhysicalFilter
                │   ├── cond:Eq
                │   │   ├── #6
                │   │   └── "FURNITURE"
                │   └── PhysicalScan { table: customer }
                └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
                    ├── PhysicalFilter
                    │   ├── cond:Lt
                    │   │   ├── #4
                    │   │   └── Cast { cast_to: Date32, expr: "1995-03-29" }
                    │   └── PhysicalScan { table: orders }
                    └── PhysicalFilter
                        ├── cond:Gt
                        │   ├── #10
                        │   └── Cast { cast_to: Date32, expr: "1995-03-29" }
                        └── PhysicalScan { table: lineitem }
*/

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
        │           ├── Cast { cast_to: Decimal128(20, 0), expr: 1(i64) }
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
            │   │   └── Cast { cast_to: Date32, expr: "2023-01-01" }
            │   └── Lt
            │       ├── #12
            │       └── Cast { cast_to: Date32, expr: "2024-01-01" }
            └── LogicalJoin { join_type: Cross, cond: true }
                ├── LogicalJoin { join_type: Cross, cond: true }
                │   ├── LogicalJoin { join_type: Cross, cond: true }
                │   │   ├── LogicalJoin { join_type: Cross, cond: true }
                │   │   │   ├── LogicalJoin { join_type: Cross, cond: true }
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
    │           ├── Cast { cast_to: Decimal128(20, 0), expr: 1(i64) }
    │           └── #23
    ├── groups: [ #41 ]
    └── PhysicalHashJoin { join_type: Inner, left_keys: [ #42 ], right_keys: [ #0 ] }
        ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #36 ], right_keys: [ #0 ] }
        │   ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #19, #3 ], right_keys: [ #0, #3 ] }
        │   │   ├── PhysicalProjection { exprs: [ #25, #26, #27, #28, #29, #30, #31, #32, #0, #1, #2, #3, #4, #5, #6, #7, #8, #9, #10, #11, #12, #13, #14, #15, #16, #17, #18, #19, #20, #21, #22, #23, #24 ] }
        │   │   │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #1 ], right_keys: [ #0 ] }
        │   │   │       ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
        │   │   │       │   ├── PhysicalFilter
        │   │   │       │   │   ├── cond:And
        │   │   │       │   │   │   ├── Geq
        │   │   │       │   │   │   │   ├── #4
        │   │   │       │   │   │   │   └── Cast { cast_to: Date32, expr: "2023-01-01" }
        │   │   │       │   │   │   └── Lt
        │   │   │       │   │   │       ├── #4
        │   │   │       │   │   │       └── Cast { cast_to: Date32, expr: "2024-01-01" }
        │   │   │       │   │   └── PhysicalScan { table: orders }
        │   │   │       │   └── PhysicalScan { table: lineitem }
        │   │   │       └── PhysicalScan { table: customer }
        │   │   └── PhysicalScan { table: supplier }
        │   └── PhysicalScan { table: nation }
        └── PhysicalFilter
            ├── cond:Eq
            │   ├── #1
            │   └── "Asia"
            └── PhysicalScan { table: region }
*/

