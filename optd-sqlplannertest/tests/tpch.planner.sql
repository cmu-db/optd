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
└── PhysicalProjection { exprs: [ #0, #1, #2, #3, #4, #5, #6, #7, #8, #9 ] }
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
    └── LogicalProjection { exprs: [ #5, #2, #8, #0, #1, #3, #4, #6 ] }
        └── LogicalJoin
            ├── join_type: Inner
            ├── cond:And
            │   ├── Eq
            │   │   ├── #0
            │   │   └── #10
            │   └── Eq
            │       ├── #7
            │       └── #9
            ├── LogicalProjection { exprs: [ #0, #1, #2, #3, #4, #5, #6, #7, #8 ] }
            │   └── LogicalJoin
            │       ├── join_type: Inner
            │       ├── cond:Eq
            │       │   ├── #9
            │       │   └── #10
            │       ├── LogicalProjection { exprs: [ #0, #1, #2, #3, #5, #6, #7, #8, #10, #11 ] }
            │       │   └── LogicalJoin
            │       │       ├── join_type: Inner
            │       │       ├── cond:Eq
            │       │       │   ├── #4
            │       │       │   └── #9
            │       │       ├── LogicalProjection { exprs: [ #0, #1, #5, #6, #7, #8, #9, #10, #3 ] }
            │       │       │   └── LogicalJoin
            │       │       │       ├── join_type: Inner
            │       │       │       ├── cond:Eq
            │       │       │       │   ├── #2
            │       │       │       │   └── #4
            │       │       │       ├── LogicalProjection { exprs: [ #0, #1, #3, #4 ] }
            │       │       │       │   └── LogicalJoin
            │       │       │       │       ├── join_type: Inner
            │       │       │       │       ├── cond:Eq
            │       │       │       │       │   ├── #0
            │       │       │       │       │   └── #2
            │       │       │       │       ├── LogicalProjection { exprs: [ #0, #1 ] }
            │       │       │       │       │   └── LogicalFilter
            │       │       │       │       │       ├── cond:And
            │       │       │       │       │       │   ├── Eq
            │       │       │       │       │       │   │   ├── #3
            │       │       │       │       │       │   │   └── 4(i32)
            │       │       │       │       │       │   └── Like { expr: #2, pattern: "%TIN", negated: false, case_insensitive: false }
            │       │       │       │       │       └── LogicalProjection { exprs: [ #0, #2, #4, #5 ] }
            │       │       │       │       │           └── LogicalScan { table: part }
            │       │       │       │       └── LogicalProjection { exprs: [ #0, #1, #3 ] }
            │       │       │       │           └── LogicalScan { table: partsupp }
            │       │       │       └── LogicalProjection { exprs: [ #0, #1, #2, #3, #4, #5, #6 ] }
            │       │       │           └── LogicalScan { table: supplier }
            │       │       └── LogicalProjection { exprs: [ #0, #1, #2 ] }
            │       │           └── LogicalScan { table: nation }
            │       └── LogicalProjection { exprs: [ #0 ] }
            │           └── LogicalFilter
            │               ├── cond:Eq
            │               │   ├── #1
            │               │   └── "AFRICA"
            │               └── LogicalProjection { exprs: [ #0, #1 ] }
            │                   └── LogicalScan { table: region }
            └── LogicalProjection { exprs: [ #1, #0 ] }
                └── LogicalAgg
                    ├── exprs:Agg(Min)
                    │   └── [ #1 ]
                    ├── groups: [ #0 ]
                    └── LogicalProjection { exprs: [ #0, #1 ] }
                        └── LogicalJoin
                            ├── join_type: Inner
                            ├── cond:Eq
                            │   ├── #2
                            │   └── #3
                            ├── LogicalProjection { exprs: [ #0, #1, #4 ] }
                            │   └── LogicalJoin
                            │       ├── join_type: Inner
                            │       ├── cond:Eq
                            │       │   ├── #2
                            │       │   └── #3
                            │       ├── LogicalProjection { exprs: [ #0, #2, #4 ] }
                            │       │   └── LogicalJoin
                            │       │       ├── join_type: Inner
                            │       │       ├── cond:Eq
                            │       │       │   ├── #1
                            │       │       │   └── #3
                            │       │       ├── LogicalProjection { exprs: [ #0, #1, #3 ] }
                            │       │       │   └── LogicalScan { table: partsupp }
                            │       │       └── LogicalProjection { exprs: [ #0, #3 ] }
                            │       │           └── LogicalScan { table: supplier }
                            │       └── LogicalProjection { exprs: [ #0, #2 ] }
                            │           └── LogicalScan { table: nation }
                            └── LogicalProjection { exprs: [ #0 ] }
                                └── LogicalFilter
                                    ├── cond:Eq
                                    │   ├── #1
                                    │   └── "AFRICA"
                                    └── LogicalProjection { exprs: [ #0, #1 ] }
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
    └── PhysicalProjection { exprs: [ #5, #2, #8, #0, #1, #3, #4, #6 ] }
        └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0, #7 ], right_keys: [ #1, #0 ] }
            ├── PhysicalProjection { exprs: [ #0, #1, #2, #3, #4, #5, #6, #7, #8 ] }
            │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #9 ], right_keys: [ #0 ] }
            │       ├── PhysicalProjection { exprs: [ #0, #1, #2, #3, #5, #6, #7, #8, #10, #11 ] }
            │       │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #4 ], right_keys: [ #0 ] }
            │       │       ├── PhysicalProjection { exprs: [ #0, #1, #5, #6, #7, #8, #9, #10, #3 ] }
            │       │       │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #2 ], right_keys: [ #0 ] }
            │       │       │       ├── PhysicalProjection { exprs: [ #0, #1, #3, #4 ] }
            │       │       │       │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
            │       │       │       │       ├── PhysicalProjection { exprs: [ #0, #1 ] }
            │       │       │       │       │   └── PhysicalProjection { exprs: [ #0, #2, #4, #5 ] }
            │       │       │       │       │       └── PhysicalFilter
            │       │       │       │       │           ├── cond:And
            │       │       │       │       │           │   ├── Eq
            │       │       │       │       │           │   │   ├── #5
            │       │       │       │       │           │   │   └── 4(i32)
            │       │       │       │       │           │   └── Like { expr: #4, pattern: "%TIN", negated: false, case_insensitive: false }
            │       │       │       │       │           └── PhysicalScan { table: part }
            │       │       │       │       └── PhysicalProjection { exprs: [ #0, #1, #3 ] }
            │       │       │       │           └── PhysicalScan { table: partsupp }
            │       │       │       └── PhysicalProjection { exprs: [ #0, #1, #2, #3, #4, #5, #6 ] }
            │       │       │           └── PhysicalScan { table: supplier }
            │       │       └── PhysicalProjection { exprs: [ #0, #1, #2 ] }
            │       │           └── PhysicalScan { table: nation }
            │       └── PhysicalProjection { exprs: [ #0 ] }
            │           └── PhysicalProjection { exprs: [ #0, #1 ] }
            │               └── PhysicalFilter
            │                   ├── cond:Eq
            │                   │   ├── #1
            │                   │   └── "AFRICA"
            │                   └── PhysicalScan { table: region }
            └── PhysicalProjection { exprs: [ #1, #0 ] }
                └── PhysicalAgg
                    ├── aggrs:Agg(Min)
                    │   └── [ #1 ]
                    ├── groups: [ #0 ]
                    └── PhysicalProjection { exprs: [ #0, #1 ] }
                        └── PhysicalHashJoin { join_type: Inner, left_keys: [ #2 ], right_keys: [ #0 ] }
                            ├── PhysicalProjection { exprs: [ #0, #1, #4 ] }
                            │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #2 ], right_keys: [ #0 ] }
                            │       ├── PhysicalProjection { exprs: [ #0, #2, #4 ] }
                            │       │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #1 ], right_keys: [ #0 ] }
                            │       │       ├── PhysicalProjection { exprs: [ #0, #1, #3 ] }
                            │       │       │   └── PhysicalScan { table: partsupp }
                            │       │       └── PhysicalProjection { exprs: [ #0, #3 ] }
                            │       │           └── PhysicalScan { table: supplier }
                            │       └── PhysicalProjection { exprs: [ #0, #2 ] }
                            │           └── PhysicalScan { table: nation }
                            └── PhysicalProjection { exprs: [ #0 ] }
                                └── PhysicalProjection { exprs: [ #0, #1 ] }
                                    └── PhysicalFilter
                                        ├── cond:Eq
                                        │   ├── #1
                                        │   └── "AFRICA"
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
            │       ├── #3
            │       └── Sub
            │           ├── 1(float)
            │           └── #4
            ├── groups: [ #2, #0, #1 ]
            └── LogicalProjection { exprs: [ #1, #2, #3, #4, #5 ] }
                └── LogicalJoin
                    ├── join_type: Inner
                    ├── cond:Eq
                    │   ├── #0
                    │   └── #3
                    ├── LogicalProjection { exprs: [ #1, #3, #4 ] }
                    │   └── LogicalJoin
                    │       ├── join_type: Inner
                    │       ├── cond:Eq
                    │       │   ├── #0
                    │       │   └── #2
                    │       ├── LogicalProjection { exprs: [ #0 ] }
                    │       │   └── LogicalFilter
                    │       │       ├── cond:Eq
                    │       │       │   ├── #1
                    │       │       │   └── "FURNITURE"
                    │       │       └── LogicalProjection { exprs: [ #0, #6 ] }
                    │       │           └── LogicalScan { table: customer }
                    │       └── LogicalFilter
                    │           ├── cond:Lt
                    │           │   ├── #2
                    │           │   └── 9218(i64)
                    │           └── LogicalProjection { exprs: [ #0, #1, #4, #7 ] }
                    │               └── LogicalScan { table: orders }
                    └── LogicalProjection { exprs: [ #0, #1, #2 ] }
                        └── LogicalFilter
                            ├── cond:Gt
                            │   ├── #3
                            │   └── 9218(i64)
                            └── LogicalProjection { exprs: [ #0, #5, #6, #10 ] }
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
            │       ├── #3
            │       └── Sub
            │           ├── 1(float)
            │           └── #4
            ├── groups: [ #2, #0, #1 ]
            └── PhysicalProjection { exprs: [ #1, #2, #3, #4, #5 ] }
                └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
                    ├── PhysicalProjection { exprs: [ #1, #3, #4 ] }
                    │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #1 ] }
                    │       ├── PhysicalProjection { exprs: [ #0 ] }
                    │       │   └── PhysicalProjection { exprs: [ #0, #6 ] }
                    │       │       └── PhysicalFilter
                    │       │           ├── cond:Eq
                    │       │           │   ├── #6
                    │       │           │   └── "FURNITURE"
                    │       │           └── PhysicalScan { table: customer }
                    │       └── PhysicalProjection { exprs: [ #0, #1, #4, #7 ] }
                    │           └── PhysicalFilter
                    │               ├── cond:Lt
                    │               │   ├── #4
                    │               │   └── 9218(i64)
                    │               └── PhysicalScan { table: orders }
                    └── PhysicalProjection { exprs: [ #0, #1, #2 ] }
                        └── PhysicalProjection { exprs: [ #0, #5, #6, #10 ] }
                            └── PhysicalFilter
                                ├── cond:Gt
                                │   ├── #10
                                │   └── 9218(i64)
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
└── PhysicalProjection { exprs: [ #0, #1 ] }
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
            │   │   ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #1 ] }
            │   │   │   ├── PhysicalScan { table: customer }
            │   │   │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
            │   │   │       ├── PhysicalFilter
            │   │   │       │   ├── cond:And
            │   │   │       │   │   ├── Geq
            │   │   │       │   │   │   ├── #4
            │   │   │       │   │   │   └── Cast { cast_to: Date32, expr: "2023-01-01" }
            │   │   │       │   │   └── Lt
            │   │   │       │   │       ├── #4
            │   │   │       │   │       └── Cast { cast_to: Date32, expr: "2024-01-01" }
            │   │   │       │   └── PhysicalScan { table: orders }
            │   │   │       └── PhysicalScan { table: lineitem }
            │   │   └── PhysicalScan { table: supplier }
            │   └── PhysicalScan { table: nation }
            └── PhysicalFilter
                ├── cond:Eq
                │   ├── #1
                │   └── "Asia"
                └── PhysicalScan { table: region }
*/

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
        │   │   └── Cast { cast_to: Date32, expr: "2023-01-01" }
        │   ├── Lt
        │   │   ├── #10
        │   │   └── Cast { cast_to: Date32, expr: "2024-01-01" }
        │   ├── Between { expr: Cast { cast_to: Decimal128(30, 15), expr: #6 }, lower: Cast { cast_to: Decimal128(30, 15), expr: 0.05(float) }, upper: Cast { cast_to: Decimal128(30, 15), expr: 0.07(float) } }
        │   └── Lt
        │       ├── Cast { cast_to: Decimal128(22, 2), expr: #4 }
        │       └── Cast { cast_to: Decimal128(22, 2), expr: 24(i64) }
        └── LogicalScan { table: lineitem }
PhysicalProjection { exprs: [ #0 ] }
└── PhysicalAgg
    ├── aggrs:Agg(Sum)
    │   └── Mul
    │       ├── #5
    │       └── #6
    ├── groups: []
    └── PhysicalFilter
        ├── cond:And
        │   ├── Geq
        │   │   ├── #10
        │   │   └── Cast { cast_to: Date32, expr: "2023-01-01" }
        │   ├── Lt
        │   │   ├── #10
        │   │   └── Cast { cast_to: Date32, expr: "2024-01-01" }
        │   ├── Between { expr: Cast { cast_to: Decimal128(30, 15), expr: #6 }, lower: Cast { cast_to: Decimal128(30, 15), expr: 0.05(float) }, upper: Cast { cast_to: Decimal128(30, 15), expr: 0.07(float) } }
        │   └── Lt
        │       ├── Cast { cast_to: Decimal128(22, 2), expr: #4 }
        │       └── Cast { cast_to: Decimal128(22, 2), expr: 24(i64) }
        └── PhysicalScan { table: lineitem }
*/

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
            │           ├── Cast { cast_to: Decimal128(20, 0), expr: 1(i64) }
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
                │   └── Between { expr: #17, lower: Cast { cast_to: Date32, expr: "1995-01-01" }, upper: Cast { cast_to: Date32, expr: "1996-12-31" } }
                └── LogicalJoin { join_type: Cross, cond: true }
                    ├── LogicalJoin { join_type: Cross, cond: true }
                    │   ├── LogicalJoin { join_type: Cross, cond: true }
                    │   │   ├── LogicalJoin { join_type: Cross, cond: true }
                    │   │   │   ├── LogicalJoin { join_type: Cross, cond: true }
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
└── PhysicalProjection { exprs: [ #0, #1, #2, #3 ] }
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
            │           ├── Cast { cast_to: Decimal128(20, 0), expr: 1(i64) }
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
                │   ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #2 ] }
                │   │   ├── PhysicalScan { table: supplier }
                │   │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
                │   │       ├── PhysicalFilter { cond: Between { expr: #10, lower: Cast { cast_to: Date32, expr: "1995-01-01" }, upper: Cast { cast_to: Date32, expr: "1996-12-31" } } }
                │   │       │   └── PhysicalScan { table: lineitem }
                │   │       └── PhysicalHashJoin { join_type: Inner, left_keys: [ #1 ], right_keys: [ #0 ] }
                │   │           ├── PhysicalScan { table: orders }
                │   │           └── PhysicalScan { table: customer }
                │   └── PhysicalScan { table: nation }
                └── PhysicalScan { table: nation }
*/

-- TPC-H Q8 without top-most limit node
select
    o_year,
    sum(case
        when nation = 'IRAQ' then volume
        else 0
    end) / sum(volume) as mkt_share
from
    (
        select
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) as volume,
            n2.n_name as nation
        from
            part,
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2,
            region
        where
            p_partkey = l_partkey
            and s_suppkey = l_suppkey
            and l_orderkey = o_orderkey
            and o_custkey = c_custkey
            and c_nationkey = n1.n_nationkey
            and n1.n_regionkey = r_regionkey
            and r_name = 'AMERICA'
            and s_nationkey = n2.n_nationkey
            and o_orderdate between date '1995-01-01' and date '1996-12-31'
            and p_type = 'ECONOMY ANODIZED STEEL'
    ) as all_nations
group by
    o_year
order by
    o_year;

/*
LogicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── LogicalProjection
    ├── exprs:
    │   ┌── #0
    │   └── Div
    │       ├── #1
    │       └── #2
    └── LogicalAgg
        ├── exprs:
        │   ┌── Agg(Sum)
        │   │   └── Case
        │   │       └── 
        │   │           ┌── Eq
        │   │           │   ├── #2
        │   │           │   └── "IRAQ"
        │   │           ├── #1
        │   │           └── Cast { cast_to: Decimal128(38, 4), expr: 0(i64) }
        │   └── Agg(Sum)
        │       └── [ #1 ]
        ├── groups: [ #0 ]
        └── LogicalProjection
            ├── exprs:
            │   ┌── Scalar(DatePart)
            │   │   └── [ "YEAR", #36 ]
            │   ├── Mul
            │   │   ├── #21
            │   │   └── Sub
            │   │       ├── Cast { cast_to: Decimal128(20, 0), expr: 1(i64) }
            │   │       └── #22
            │   └── #54
            └── LogicalFilter
                ├── cond:And
                │   ├── Eq
                │   │   ├── #0
                │   │   └── #17
                │   ├── Eq
                │   │   ├── #9
                │   │   └── #18
                │   ├── Eq
                │   │   ├── #16
                │   │   └── #32
                │   ├── Eq
                │   │   ├── #33
                │   │   └── #41
                │   ├── Eq
                │   │   ├── #44
                │   │   └── #49
                │   ├── Eq
                │   │   ├── #51
                │   │   └── #57
                │   ├── Eq
                │   │   ├── #58
                │   │   └── "AMERICA"
                │   ├── Eq
                │   │   ├── #12
                │   │   └── #53
                │   ├── Between { expr: #36, lower: Cast { cast_to: Date32, expr: "1995-01-01" }, upper: Cast { cast_to: Date32, expr: "1996-12-31" } }
                │   └── Eq
                │       ├── #4
                │       └── "ECONOMY ANODIZED STEEL"
                └── LogicalJoin { join_type: Cross, cond: true }
                    ├── LogicalJoin { join_type: Cross, cond: true }
                    │   ├── LogicalJoin { join_type: Cross, cond: true }
                    │   │   ├── LogicalJoin { join_type: Cross, cond: true }
                    │   │   │   ├── LogicalJoin { join_type: Cross, cond: true }
                    │   │   │   │   ├── LogicalJoin { join_type: Cross, cond: true }
                    │   │   │   │   │   ├── LogicalJoin { join_type: Cross, cond: true }
                    │   │   │   │   │   │   ├── LogicalScan { table: part }
                    │   │   │   │   │   │   └── LogicalScan { table: supplier }
                    │   │   │   │   │   └── LogicalScan { table: lineitem }
                    │   │   │   │   └── LogicalScan { table: orders }
                    │   │   │   └── LogicalScan { table: customer }
                    │   │   └── LogicalScan { table: nation }
                    │   └── LogicalScan { table: nation }
                    └── LogicalScan { table: region }
PhysicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── PhysicalProjection
    ├── exprs:
    │   ┌── #0
    │   └── Div
    │       ├── #1
    │       └── #2
    └── PhysicalAgg
        ├── aggrs:
        │   ┌── Agg(Sum)
        │   │   └── Case
        │   │       └── 
        │   │           ┌── Eq
        │   │           │   ├── #2
        │   │           │   └── "IRAQ"
        │   │           ├── #1
        │   │           └── Cast { cast_to: Decimal128(38, 4), expr: 0(i64) }
        │   └── Agg(Sum)
        │       └── [ #1 ]
        ├── groups: [ #0 ]
        └── PhysicalProjection
            ├── exprs:
            │   ┌── Scalar(DatePart)
            │   │   └── [ "YEAR", #36 ]
            │   ├── Mul
            │   │   ├── #21
            │   │   └── Sub
            │   │       ├── Cast { cast_to: Decimal128(20, 0), expr: 1(i64) }
            │   │       └── #22
            │   └── #54
            └── PhysicalHashJoin { join_type: Inner, left_keys: [ #51 ], right_keys: [ #0 ] }
                ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #12 ], right_keys: [ #0 ] }
                │   ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #0, #9 ], right_keys: [ #1, #2 ] }
                │   │   ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                │   │   │   ├── PhysicalFilter
                │   │   │   │   ├── cond:Eq
                │   │   │   │   │   ├── #4
                │   │   │   │   │   └── "ECONOMY ANODIZED STEEL"
                │   │   │   │   └── PhysicalScan { table: part }
                │   │   │   └── PhysicalScan { table: supplier }
                │   │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
                │   │       ├── PhysicalScan { table: lineitem }
                │   │       └── PhysicalHashJoin { join_type: Inner, left_keys: [ #1 ], right_keys: [ #0 ] }
                │   │           ├── PhysicalFilter { cond: Between { expr: #4, lower: Cast { cast_to: Date32, expr: "1995-01-01" }, upper: Cast { cast_to: Date32, expr: "1996-12-31" } } }
                │   │           │   └── PhysicalScan { table: orders }
                │   │           └── PhysicalHashJoin { join_type: Inner, left_keys: [ #3 ], right_keys: [ #0 ] }
                │   │               ├── PhysicalScan { table: customer }
                │   │               └── PhysicalScan { table: nation }
                │   └── PhysicalScan { table: nation }
                └── PhysicalFilter
                    ├── cond:Eq
                    │   ├── #1
                    │   └── "AMERICA"
                    └── PhysicalScan { table: region }
*/

-- TPC-H Q9
SELECT
    nation,
    o_year,
    SUM(amount) AS sum_profit
FROM
    (
        SELECT
            n_name AS nation,
            EXTRACT(YEAR FROM o_orderdate) AS o_year,
            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
        FROM
            part,
            supplier,
            lineitem,
            partsupp,
            orders,
            nation
        WHERE
            s_suppkey = l_suppkey
            AND ps_suppkey = l_suppkey
            AND ps_partkey = l_partkey
            AND p_partkey = l_partkey
            AND o_orderkey = l_orderkey
            AND s_nationkey = n_nationkey
            AND p_name LIKE '%green%'
    ) AS profit
GROUP BY
    nation,
    o_year
ORDER BY
    nation,
    o_year DESC;

/*
LogicalSort
├── exprs:
│   ┌── SortOrder { order: Asc }
│   │   └── #0
│   └── SortOrder { order: Desc }
│       └── #1
└── LogicalProjection { exprs: [ #0, #1, #2 ] }
    └── LogicalAgg
        ├── exprs:Agg(Sum)
        │   └── [ #2 ]
        ├── groups: [ #0, #1 ]
        └── LogicalProjection
            ├── exprs:
            │   ┌── #47
            │   ├── Scalar(DatePart)
            │   │   └── [ "YEAR", #41 ]
            │   └── Sub
            │       ├── Mul
            │       │   ├── #21
            │       │   └── Sub
            │       │       ├── Cast { cast_to: Decimal128(20, 0), expr: 1(i64) }
            │       │       └── #22
            │       └── Mul
            │           ├── #35
            │           └── #20
            └── LogicalFilter
                ├── cond:And
                │   ├── Eq
                │   │   ├── #9
                │   │   └── #18
                │   ├── Eq
                │   │   ├── #33
                │   │   └── #18
                │   ├── Eq
                │   │   ├── #32
                │   │   └── #17
                │   ├── Eq
                │   │   ├── #0
                │   │   └── #17
                │   ├── Eq
                │   │   ├── #37
                │   │   └── #16
                │   ├── Eq
                │   │   ├── #12
                │   │   └── #46
                │   └── Like { expr: #1, pattern: "%green%", negated: false, case_insensitive: false }
                └── LogicalJoin { join_type: Cross, cond: true }
                    ├── LogicalJoin { join_type: Cross, cond: true }
                    │   ├── LogicalJoin { join_type: Cross, cond: true }
                    │   │   ├── LogicalJoin { join_type: Cross, cond: true }
                    │   │   │   ├── LogicalJoin { join_type: Cross, cond: true }
                    │   │   │   │   ├── LogicalScan { table: part }
                    │   │   │   │   └── LogicalScan { table: supplier }
                    │   │   │   └── LogicalScan { table: lineitem }
                    │   │   └── LogicalScan { table: partsupp }
                    │   └── LogicalScan { table: orders }
                    └── LogicalScan { table: nation }
PhysicalSort
├── exprs:
│   ┌── SortOrder { order: Asc }
│   │   └── #0
│   └── SortOrder { order: Desc }
│       └── #1
└── PhysicalProjection { exprs: [ #0, #1, #2 ] }
    └── PhysicalAgg
        ├── aggrs:Agg(Sum)
        │   └── [ #2 ]
        ├── groups: [ #0, #1 ]
        └── PhysicalProjection
            ├── exprs:
            │   ┌── #47
            │   ├── Scalar(DatePart)
            │   │   └── [ "YEAR", #41 ]
            │   └── Sub
            │       ├── Mul
            │       │   ├── #21
            │       │   └── Sub
            │       │       ├── Cast { cast_to: Decimal128(20, 0), expr: 1(i64) }
            │       │       └── #22
            │       └── Mul
            │           ├── #35
            │           └── #20
            └── PhysicalHashJoin { join_type: Inner, left_keys: [ #12 ], right_keys: [ #0 ] }
                ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #16 ], right_keys: [ #0 ] }
                │   ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #9, #0 ], right_keys: [ #2, #1 ] }
                │   │   ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                │   │   │   ├── PhysicalFilter { cond: Like { expr: #1, pattern: "%green%", negated: false, case_insensitive: false } }
                │   │   │   │   └── PhysicalScan { table: part }
                │   │   │   └── PhysicalScan { table: supplier }
                │   │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #2, #1 ], right_keys: [ #1, #0 ] }
                │   │       ├── PhysicalScan { table: lineitem }
                │   │       └── PhysicalScan { table: partsupp }
                │   └── PhysicalScan { table: orders }
                └── PhysicalScan { table: nation }
*/

-- TPC-H Q9
SELECT
    nation,
    o_year,
    SUM(amount) AS sum_profit
FROM
    (
        SELECT
            n_name AS nation,
            EXTRACT(YEAR FROM o_orderdate) AS o_year,
            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
        FROM
            part,
            supplier,
            lineitem,
            partsupp,
            orders,
            nation
        WHERE
            s_suppkey = l_suppkey
            AND ps_suppkey = l_suppkey
            AND ps_partkey = l_partkey
            AND p_partkey = l_partkey
            AND o_orderkey = l_orderkey
            AND s_nationkey = n_nationkey
            AND p_name LIKE '%green%'
    ) AS profit
GROUP BY
    nation,
    o_year
ORDER BY
    nation,
    o_year DESC;

/*
LogicalSort
├── exprs:
│   ┌── SortOrder { order: Asc }
│   │   └── #0
│   └── SortOrder { order: Desc }
│       └── #1
└── LogicalProjection { exprs: [ #0, #1, #2 ] }
    └── LogicalAgg
        ├── exprs:Agg(Sum)
        │   └── [ #2 ]
        ├── groups: [ #0, #1 ]
        └── LogicalProjection
            ├── exprs:
            │   ┌── #47
            │   ├── Scalar(DatePart)
            │   │   └── [ "YEAR", #41 ]
            │   └── Sub
            │       ├── Mul
            │       │   ├── #21
            │       │   └── Sub
            │       │       ├── Cast { cast_to: Decimal128(20, 0), expr: 1(i64) }
            │       │       └── #22
            │       └── Mul
            │           ├── #35
            │           └── #20
            └── LogicalFilter
                ├── cond:And
                │   ├── Eq
                │   │   ├── #9
                │   │   └── #18
                │   ├── Eq
                │   │   ├── #33
                │   │   └── #18
                │   ├── Eq
                │   │   ├── #32
                │   │   └── #17
                │   ├── Eq
                │   │   ├── #0
                │   │   └── #17
                │   ├── Eq
                │   │   ├── #37
                │   │   └── #16
                │   ├── Eq
                │   │   ├── #12
                │   │   └── #46
                │   └── Like { expr: #1, pattern: "%green%", negated: false, case_insensitive: false }
                └── LogicalJoin { join_type: Cross, cond: true }
                    ├── LogicalJoin { join_type: Cross, cond: true }
                    │   ├── LogicalJoin { join_type: Cross, cond: true }
                    │   │   ├── LogicalJoin { join_type: Cross, cond: true }
                    │   │   │   ├── LogicalJoin { join_type: Cross, cond: true }
                    │   │   │   │   ├── LogicalScan { table: part }
                    │   │   │   │   └── LogicalScan { table: supplier }
                    │   │   │   └── LogicalScan { table: lineitem }
                    │   │   └── LogicalScan { table: partsupp }
                    │   └── LogicalScan { table: orders }
                    └── LogicalScan { table: nation }
PhysicalSort
├── exprs:
│   ┌── SortOrder { order: Asc }
│   │   └── #0
│   └── SortOrder { order: Desc }
│       └── #1
└── PhysicalProjection { exprs: [ #0, #1, #2 ] }
    └── PhysicalAgg
        ├── aggrs:Agg(Sum)
        │   └── [ #2 ]
        ├── groups: [ #0, #1 ]
        └── PhysicalProjection
            ├── exprs:
            │   ┌── #47
            │   ├── Scalar(DatePart)
            │   │   └── [ "YEAR", #41 ]
            │   └── Sub
            │       ├── Mul
            │       │   ├── #21
            │       │   └── Sub
            │       │       ├── Cast { cast_to: Decimal128(20, 0), expr: 1(i64) }
            │       │       └── #22
            │       └── Mul
            │           ├── #35
            │           └── #20
            └── PhysicalHashJoin { join_type: Inner, left_keys: [ #12 ], right_keys: [ #0 ] }
                ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #16 ], right_keys: [ #0 ] }
                │   ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #9, #0 ], right_keys: [ #2, #1 ] }
                │   │   ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                │   │   │   ├── PhysicalFilter { cond: Like { expr: #1, pattern: "%green%", negated: false, case_insensitive: false } }
                │   │   │   │   └── PhysicalScan { table: part }
                │   │   │   └── PhysicalScan { table: supplier }
                │   │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #2, #1 ], right_keys: [ #1, #0 ] }
                │   │       ├── PhysicalScan { table: lineitem }
                │   │       └── PhysicalScan { table: partsupp }
                │   └── PhysicalScan { table: orders }
                └── PhysicalScan { table: nation }
*/

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
LogicalLimit { skip: 0(u64), fetch: 20(u64) }
└── LogicalSort
    ├── exprs:SortOrder { order: Desc }
    │   └── #2
    └── LogicalProjection { exprs: [ #0, #1, #7, #2, #4, #5, #3, #6 ] }
        └── LogicalAgg
            ├── exprs:Agg(Sum)
            │   └── Mul
            │       ├── #22
            │       └── Sub
            │           ├── Cast { cast_to: Decimal128(20, 0), expr: 1(i64) }
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
                │   │   └── Cast { cast_to: Date32, expr: "1993-07-01" }
                │   ├── Lt
                │   │   ├── #12
                │   │   └── Add
                │   │       ├── Cast { cast_to: Date32, expr: "1993-07-01" }
                │   │       └── INTERVAL_MONTH_DAY_NANO (3, 0, 0)
                │   ├── Eq
                │   │   ├── #25
                │   │   └── "R"
                │   └── Eq
                │       ├── #3
                │       └── #33
                └── LogicalJoin { join_type: Cross, cond: true }
                    ├── LogicalJoin { join_type: Cross, cond: true }
                    │   ├── LogicalJoin { join_type: Cross, cond: true }
                    │   │   ├── LogicalScan { table: customer }
                    │   │   └── LogicalScan { table: orders }
                    │   └── LogicalScan { table: lineitem }
                    └── LogicalScan { table: nation }
PhysicalLimit { skip: 0(u64), fetch: 20(u64) }
└── PhysicalSort
    ├── exprs:SortOrder { order: Desc }
    │   └── #2
    └── PhysicalProjection { exprs: [ #0, #1, #7, #2, #4, #5, #3, #6 ] }
        └── PhysicalAgg
            ├── aggrs:Agg(Sum)
            │   └── Mul
            │       ├── #22
            │       └── Sub
            │           ├── Cast { cast_to: Decimal128(20, 0), expr: 1(i64) }
            │           └── #23
            ├── groups: [ #0, #1, #5, #4, #34, #2, #7 ]
            └── PhysicalHashJoin { join_type: Inner, left_keys: [ #3 ], right_keys: [ #0 ] }
                ├── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #1 ] }
                │   ├── PhysicalScan { table: customer }
                │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
                │       ├── PhysicalFilter
                │       │   ├── cond:And
                │       │   │   ├── Geq
                │       │   │   │   ├── #4
                │       │   │   │   └── Cast { cast_to: Date32, expr: "1993-07-01" }
                │       │   │   └── Lt
                │       │   │       ├── #4
                │       │   │       └── Add
                │       │   │           ├── Cast { cast_to: Date32, expr: "1993-07-01" }
                │       │   │           └── INTERVAL_MONTH_DAY_NANO (3, 0, 0)
                │       │   └── PhysicalScan { table: orders }
                │       └── PhysicalFilter
                │           ├── cond:Eq
                │           │   ├── #8
                │           │   └── "R"
                │           └── PhysicalScan { table: lineitem }
                └── PhysicalScan { table: nation }
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
└── PhysicalProjection { exprs: [ #0, #1 ] }
    └── PhysicalProjection { exprs: [ #0, #1 ] }
        └── PhysicalNestedLoopJoin
            ├── join_type: Inner
            ├── cond:Gt
            │   ├── Cast { cast_to: Decimal128(38, 15), expr: #1 }
            │   └── #0
            ├── PhysicalProjection
            │   ├── exprs:Cast
            │   │   ├── cast_to: Decimal128(38, 15)
            │   │   ├── expr:Mul
            │   │   │   ├── Cast { cast_to: Float64, expr: #0 }
            │   │   │   └── 0.0001(float)

            │   └── PhysicalAgg
            │       ├── aggrs:Agg(Sum)
            │       │   └── Mul
            │       │       ├── #1
            │       │       └── Cast { cast_to: Decimal128(10, 0), expr: #0 }
            │       ├── groups: []
            │       └── PhysicalProjection { exprs: [ #0, #1 ] }
            │           └── PhysicalHashJoin { join_type: Inner, left_keys: [ #2 ], right_keys: [ #0 ] }
            │               ├── PhysicalProjection { exprs: [ #1, #2, #4 ] }
            │               │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
            │               │       ├── PhysicalProjection { exprs: [ #1, #2, #3 ] }
            │               │       │   └── PhysicalScan { table: partsupp }
            │               │       └── PhysicalProjection { exprs: [ #0, #3 ] }
            │               │           └── PhysicalScan { table: supplier }
            │               └── PhysicalProjection { exprs: [ #0 ] }
            │                   └── PhysicalProjection { exprs: [ #0, #1 ] }
            │                       └── PhysicalFilter
            │                           ├── cond:Eq
            │                           │   ├── #1
            │                           │   └── "CHINA"
            │                           └── PhysicalScan { table: nation }
            └── PhysicalAgg
                ├── aggrs:Agg(Sum)
                │   └── Mul
                │       ├── #2
                │       └── Cast { cast_to: Decimal128(10, 0), expr: #1 }
                ├── groups: [ #0 ]
                └── PhysicalProjection { exprs: [ #0, #1, #2 ] }
                    └── PhysicalHashJoin { join_type: Inner, left_keys: [ #3 ], right_keys: [ #0 ] }
                        ├── PhysicalProjection { exprs: [ #0, #2, #3, #5 ] }
                        │   └── PhysicalHashJoin { join_type: Inner, left_keys: [ #1 ], right_keys: [ #0 ] }
                        │       ├── PhysicalProjection { exprs: [ #0, #1, #2, #3 ] }
                        │       │   └── PhysicalScan { table: partsupp }
                        │       └── PhysicalProjection { exprs: [ #0, #3 ] }
                        │           └── PhysicalScan { table: supplier }
                        └── PhysicalProjection { exprs: [ #0 ] }
                            └── PhysicalProjection { exprs: [ #0, #1 ] }
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
└── PhysicalProjection { exprs: [ #0, #1, #2 ] }
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
        └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
            ├── PhysicalScan { table: orders }
            └── PhysicalFilter
                ├── cond:And
                │   ├── InList { expr: #14, list: [ "MAIL", "SHIP" ], negated: false }
                │   ├── Lt
                │   │   ├── #11
                │   │   └── #12
                │   ├── Lt
                │   │   ├── #10
                │   │   └── #11
                │   ├── Geq
                │   │   ├── #12
                │   │   └── Cast { cast_to: Date32, expr: "1994-01-01" }
                │   └── Lt
                │       ├── #12
                │       └── Cast { cast_to: Date32, expr: "1995-01-01" }
                └── PhysicalScan { table: lineitem }
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
└── PhysicalProjection { exprs: [ #0, #1, #2, #3, #4 ] }
    └── PhysicalProjection { exprs: [ #0, #1, #2, #3, #5, #6 ] }
        └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
            ├── PhysicalProjection { exprs: [ #0, #1, #2, #4 ] }
            │   └── PhysicalScan { table: supplier }
            └── PhysicalProjection { exprs: [ #0, #1, #2 ] }
                └── PhysicalProjection { exprs: [ #1, #2, #0 ] }
                    └── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #1 ] }
                        ├── PhysicalAgg
                        │   ├── aggrs:Agg(Max)
                        │   │   └── [ #0 ]
                        │   ├── groups: []
                        │   └── PhysicalProjection { exprs: [ #1 ] }
                        │       └── PhysicalAgg
                        │           ├── aggrs:Agg(Sum)
                        │           │   └── Mul
                        │           │       ├── #1
                        │           │       └── Sub
                        │           │           ├── 1(float)
                        │           │           └── #2
                        │           ├── groups: [ #0 ]
                        │           └── PhysicalProjection { exprs: [ #0, #1, #2 ] }
                        │               └── PhysicalProjection { exprs: [ #2, #5, #6, #10 ] }
                        │                   └── PhysicalFilter
                        │                       ├── cond:And
                        │                       │   ├── Geq
                        │                       │   │   ├── #10
                        │                       │   │   └── 8401(i64)
                        │                       │   └── Lt
                        │                       │       ├── #10
                        │                       │       └── 8491(i64)
                        │                       └── PhysicalScan { table: lineitem }
                        └── PhysicalAgg
                            ├── aggrs:Agg(Sum)
                            │   └── Mul
                            │       ├── #1
                            │       └── Sub
                            │           ├── 1(float)
                            │           └── #2
                            ├── groups: [ #0 ]
                            └── PhysicalProjection { exprs: [ #0, #1, #2 ] }
                                └── PhysicalProjection { exprs: [ #2, #5, #6, #10 ] }
                                    └── PhysicalFilter
                                        ├── cond:And
                                        │   ├── Geq
                                        │   │   ├── #10
                                        │   │   └── 8401(i64)
                                        │   └── Lt
                                        │       ├── #10
                                        │       └── 8491(i64)
                                        └── PhysicalScan { table: lineitem }
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
            │   │   └── #4
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

