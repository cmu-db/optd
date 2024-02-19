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
        │   │           ├── Cast { cast_to: Decimal128(20, 0), expr: 1 }
        │   │           └── #6
        │   ├── Agg(Sum)
        │   │   └── Mul
        │   │       ├── Mul
        │   │       │   ├── #5
        │   │       │   └── Sub
        │   │       │       ├── Cast { cast_to: Decimal128(20, 0), expr: 1 }
        │   │       │       └── #6
        │   │       └── Add
        │   │           ├── Cast { cast_to: Decimal128(20, 0), expr: 1 }
        │   │           └── #7
        │   ├── Agg(Avg)
        │   │   └── [ #4 ]
        │   ├── Agg(Avg)
        │   │   └── [ #5 ]
        │   ├── Agg(Avg)
        │   │   └── [ #6 ]
        │   └── Agg(Count)
        │       └── [ 1 ]
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
        │   │           ├── Cast { cast_to: Decimal128(20, 0), expr: 1 }
        │   │           └── #6
        │   ├── Agg(Sum)
        │   │   └── Mul
        │   │       ├── Mul
        │   │       │   ├── #5
        │   │       │   └── Sub
        │   │       │       ├── Cast { cast_to: Decimal128(20, 0), expr: 1 }
        │   │       │       └── #6
        │   │       └── Add
        │   │           ├── Cast { cast_to: Decimal128(20, 0), expr: 1 }
        │   │           └── #7
        │   ├── Agg(Avg)
        │   │   └── [ #4 ]
        │   ├── Agg(Avg)
        │   │   └── [ #5 ]
        │   ├── Agg(Avg)
        │   │   └── [ #6 ]
        │   └── Agg(Count)
        │       └── [ 1 ]
        ├── groups: [ #8, #9 ]
        └── PhysicalFilter
            ├── cond:Leq
            │   ├── #10
            │   └── Sub
            │       ├── Cast { cast_to: Date32, expr: "1998-12-01" }
            │       └── INTERVAL_MONTH_DAY_NANO (0, 90, 0)
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
        │           ├── Cast { cast_to: Decimal128(20, 0), expr: 1 }
        │           └── #23
        ├── groups: [ #41 ]
        └── LogicalFilter
            ├── cond:And
            │   ├── And
            │   │   ├── And
            │   │   │   ├── And
            │   │   │   │   ├── And
            │   │   │   │   │   ├── And
            │   │   │   │   │   │   ├── And
            │   │   │   │   │   │   │   ├── And
            │   │   │   │   │   │   │   │   ├── Eq
            │   │   │   │   │   │   │   │   │   ├── #0
            │   │   │   │   │   │   │   │   │   └── #9
            │   │   │   │   │   │   │   │   └── Eq
            │   │   │   │   │   │   │   │       ├── #17
            │   │   │   │   │   │   │   │       └── #8
            │   │   │   │   │   │   │   └── Eq
            │   │   │   │   │   │   │       ├── #19
            │   │   │   │   │   │   │       └── #33
            │   │   │   │   │   │   └── Eq
            │   │   │   │   │   │       ├── #3
            │   │   │   │   │   │       └── #36
            │   │   │   │   │   └── Eq
            │   │   │   │   │       ├── #36
            │   │   │   │   │       └── #40
            │   │   │   │   └── Eq
            │   │   │   │       ├── #42
            │   │   │   │       └── #44
            │   │   │   └── Eq
            │   │   │       ├── #45
            │   │   │       └── "Asia"
            │   │   └── Geq
            │   │       ├── #12
            │   │       └── Cast { cast_to: Date32, expr: "2023-01-01" }
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
        │           ├── Cast { cast_to: Decimal128(20, 0), expr: 1 }
        │           └── #23
        ├── groups: [ #41 ]
        └── PhysicalFilter
            ├── cond:And
            │   ├── And
            │   │   ├── And
            │   │   │   ├── And
            │   │   │   │   ├── And
            │   │   │   │   │   ├── And
            │   │   │   │   │   │   ├── And
            │   │   │   │   │   │   │   ├── And
            │   │   │   │   │   │   │   │   ├── Eq
            │   │   │   │   │   │   │   │   │   ├── #0
            │   │   │   │   │   │   │   │   │   └── #9
            │   │   │   │   │   │   │   │   └── Eq
            │   │   │   │   │   │   │   │       ├── #17
            │   │   │   │   │   │   │   │       └── #8
            │   │   │   │   │   │   │   └── Eq
            │   │   │   │   │   │   │       ├── #19
            │   │   │   │   │   │   │       └── #33
            │   │   │   │   │   │   └── Eq
            │   │   │   │   │   │       ├── #3
            │   │   │   │   │   │       └── #36
            │   │   │   │   │   └── Eq
            │   │   │   │   │       ├── #36
            │   │   │   │   │       └── #40
            │   │   │   │   └── Eq
            │   │   │   │       ├── #42
            │   │   │   │       └── #44
            │   │   │   └── Eq
            │   │   │       ├── #45
            │   │   │       └── "Asia"
            │   │   └── Geq
            │   │       ├── #12
            │   │       └── Cast { cast_to: Date32, expr: "2023-01-01" }
            │   └── Lt
            │       ├── #12
            │       └── Cast { cast_to: Date32, expr: "2024-01-01" }
            └── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                │   ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                │   │   ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                │   │   │   ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                │   │   │   │   ├── PhysicalScan { table: customer }
                │   │   │   │   └── PhysicalScan { table: orders }
                │   │   │   └── PhysicalScan { table: lineitem }
                │   │   └── PhysicalScan { table: supplier }
                │   └── PhysicalScan { table: nation }
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
        │   ├── And
        │   │   ├── And
        │   │   │   ├── Geq
        │   │   │   │   ├── #10
        │   │   │   │   └── Cast { cast_to: Date32, expr: "2023-01-01" }
        │   │   │   └── Lt
        │   │   │       ├── #10
        │   │   │       └── Cast { cast_to: Date32, expr: "2024-01-01" }
        │   │   └── Between { expr: Cast { cast_to: Decimal128(30, 15), expr: #6 }, lower: Cast { cast_to: Decimal128(30, 15), expr: 0.05 }, upper: Cast { cast_to: Decimal128(30, 15), expr: 0.07 } }
        │   └── Lt
        │       ├── Cast { cast_to: Decimal128(22, 2), expr: #4 }
        │       └── Cast { cast_to: Decimal128(22, 2), expr: 24 }
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
        │   ├── And
        │   │   ├── And
        │   │   │   ├── Geq
        │   │   │   │   ├── #10
        │   │   │   │   └── Cast { cast_to: Date32, expr: "2023-01-01" }
        │   │   │   └── Lt
        │   │   │       ├── #10
        │   │   │       └── Cast { cast_to: Date32, expr: "2024-01-01" }
        │   │   └── Between { expr: Cast { cast_to: Decimal128(30, 15), expr: #6 }, lower: Cast { cast_to: Decimal128(30, 15), expr: 0.05 }, upper: Cast { cast_to: Decimal128(30, 15), expr: 0.07 } }
        │   └── Lt
        │       ├── Cast { cast_to: Decimal128(22, 2), expr: #4 }
        │       └── Cast { cast_to: Decimal128(22, 2), expr: 24 }
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
            │           ├── Cast { cast_to: Decimal128(20, 0), expr: 1 }
            │           └── #13
            └── LogicalFilter
                ├── cond:And
                │   ├── And
                │   │   ├── And
                │   │   │   ├── And
                │   │   │   │   ├── And
                │   │   │   │   │   ├── And
                │   │   │   │   │   │   ├── Eq
                │   │   │   │   │   │   │   ├── #0
                │   │   │   │   │   │   │   └── #9
                │   │   │   │   │   │   └── Eq
                │   │   │   │   │   │       ├── #23
                │   │   │   │   │   │       └── #7
                │   │   │   │   │   └── Eq
                │   │   │   │   │       ├── #32
                │   │   │   │   │       └── #24
                │   │   │   │   └── Eq
                │   │   │   │       ├── #3
                │   │   │   │       └── #40
                │   │   │   └── Eq
                │   │   │       ├── #35
                │   │   │       └── #44
                │   │   └── Or
                │   │       ├── And
                │   │       │   ├── Eq
                │   │       │   │   ├── #41
                │   │       │   │   └── "FRANCE"
                │   │       │   └── Eq
                │   │       │       ├── #45
                │   │       │       └── "GERMANY"
                │   │       └── And
                │   │           ├── Eq
                │   │           │   ├── #41
                │   │           │   └── "GERMANY"
                │   │           └── Eq
                │   │               ├── #45
                │   │               └── "FRANCE"
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
            │           ├── Cast { cast_to: Decimal128(20, 0), expr: 1 }
            │           └── #13
            └── PhysicalFilter
                ├── cond:And
                │   ├── And
                │   │   ├── And
                │   │   │   ├── And
                │   │   │   │   ├── And
                │   │   │   │   │   ├── And
                │   │   │   │   │   │   ├── Eq
                │   │   │   │   │   │   │   ├── #0
                │   │   │   │   │   │   │   └── #9
                │   │   │   │   │   │   └── Eq
                │   │   │   │   │   │       ├── #23
                │   │   │   │   │   │       └── #7
                │   │   │   │   │   └── Eq
                │   │   │   │   │       ├── #32
                │   │   │   │   │       └── #24
                │   │   │   │   └── Eq
                │   │   │   │       ├── #3
                │   │   │   │       └── #40
                │   │   │   └── Eq
                │   │   │       ├── #35
                │   │   │       └── #44
                │   │   └── Or
                │   │       ├── And
                │   │       │   ├── Eq
                │   │       │   │   ├── #41
                │   │       │   │   └── "FRANCE"
                │   │       │   └── Eq
                │   │       │       ├── #45
                │   │       │       └── "GERMANY"
                │   │       └── And
                │   │           ├── Eq
                │   │           │   ├── #41
                │   │           │   └── "GERMANY"
                │   │           └── Eq
                │   │               ├── #45
                │   │               └── "FRANCE"
                │   └── Between { expr: #17, lower: Cast { cast_to: Date32, expr: "1995-01-01" }, upper: Cast { cast_to: Date32, expr: "1996-12-31" } }
                └── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                    ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                    │   ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                    │   │   ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                    │   │   │   ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                    │   │   │   │   ├── PhysicalScan { table: supplier }
                    │   │   │   │   └── PhysicalScan { table: lineitem }
                    │   │   │   └── PhysicalScan { table: orders }
                    │   │   └── PhysicalScan { table: customer }
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
        │   │           └── Cast { cast_to: Decimal128(38, 4), expr: 0 }
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
            │   │       ├── Cast { cast_to: Decimal128(20, 0), expr: 1 }
            │   │       └── #22
            │   └── #54
            └── LogicalFilter
                ├── cond:And
                │   ├── And
                │   │   ├── And
                │   │   │   ├── And
                │   │   │   │   ├── And
                │   │   │   │   │   ├── And
                │   │   │   │   │   │   ├── And
                │   │   │   │   │   │   │   ├── And
                │   │   │   │   │   │   │   │   ├── And
                │   │   │   │   │   │   │   │   │   ├── Eq
                │   │   │   │   │   │   │   │   │   │   ├── #0
                │   │   │   │   │   │   │   │   │   │   └── #17
                │   │   │   │   │   │   │   │   │   └── Eq
                │   │   │   │   │   │   │   │   │       ├── #9
                │   │   │   │   │   │   │   │   │       └── #18
                │   │   │   │   │   │   │   │   └── Eq
                │   │   │   │   │   │   │   │       ├── #16
                │   │   │   │   │   │   │   │       └── #32
                │   │   │   │   │   │   │   └── Eq
                │   │   │   │   │   │   │       ├── #33
                │   │   │   │   │   │   │       └── #41
                │   │   │   │   │   │   └── Eq
                │   │   │   │   │   │       ├── #44
                │   │   │   │   │   │       └── #49
                │   │   │   │   │   └── Eq
                │   │   │   │   │       ├── #51
                │   │   │   │   │       └── #57
                │   │   │   │   └── Eq
                │   │   │   │       ├── #58
                │   │   │   │       └── "AMERICA"
                │   │   │   └── Eq
                │   │   │       ├── #12
                │   │   │       └── #53
                │   │   └── Between { expr: #36, lower: Cast { cast_to: Date32, expr: "1995-01-01" }, upper: Cast { cast_to: Date32, expr: "1996-12-31" } }
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
        │   │           └── Cast { cast_to: Decimal128(38, 4), expr: 0 }
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
            │   │       ├── Cast { cast_to: Decimal128(20, 0), expr: 1 }
            │   │       └── #22
            │   └── #54
            └── PhysicalFilter
                ├── cond:And
                │   ├── And
                │   │   ├── And
                │   │   │   ├── And
                │   │   │   │   ├── And
                │   │   │   │   │   ├── And
                │   │   │   │   │   │   ├── And
                │   │   │   │   │   │   │   ├── And
                │   │   │   │   │   │   │   │   ├── And
                │   │   │   │   │   │   │   │   │   ├── Eq
                │   │   │   │   │   │   │   │   │   │   ├── #0
                │   │   │   │   │   │   │   │   │   │   └── #17
                │   │   │   │   │   │   │   │   │   └── Eq
                │   │   │   │   │   │   │   │   │       ├── #9
                │   │   │   │   │   │   │   │   │       └── #18
                │   │   │   │   │   │   │   │   └── Eq
                │   │   │   │   │   │   │   │       ├── #16
                │   │   │   │   │   │   │   │       └── #32
                │   │   │   │   │   │   │   └── Eq
                │   │   │   │   │   │   │       ├── #33
                │   │   │   │   │   │   │       └── #41
                │   │   │   │   │   │   └── Eq
                │   │   │   │   │   │       ├── #44
                │   │   │   │   │   │       └── #49
                │   │   │   │   │   └── Eq
                │   │   │   │   │       ├── #51
                │   │   │   │   │       └── #57
                │   │   │   │   └── Eq
                │   │   │   │       ├── #58
                │   │   │   │       └── "AMERICA"
                │   │   │   └── Eq
                │   │   │       ├── #12
                │   │   │       └── #53
                │   │   └── Between { expr: #36, lower: Cast { cast_to: Date32, expr: "1995-01-01" }, upper: Cast { cast_to: Date32, expr: "1996-12-31" } }
                │   └── Eq
                │       ├── #4
                │       └── "ECONOMY ANODIZED STEEL"
                └── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                    ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                    │   ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                    │   │   ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                    │   │   │   ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                    │   │   │   │   ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                    │   │   │   │   │   ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                    │   │   │   │   │   │   ├── PhysicalScan { table: part }
                    │   │   │   │   │   │   └── PhysicalScan { table: supplier }
                    │   │   │   │   │   └── PhysicalScan { table: lineitem }
                    │   │   │   │   └── PhysicalScan { table: orders }
                    │   │   │   └── PhysicalScan { table: customer }
                    │   │   └── PhysicalScan { table: nation }
                    │   └── PhysicalScan { table: nation }
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
            │       │       ├── Cast { cast_to: Decimal128(20, 0), expr: 1 }
            │       │       └── #22
            │       └── Mul
            │           ├── #35
            │           └── #20
            └── LogicalFilter
                ├── cond:And
                │   ├── And
                │   │   ├── And
                │   │   │   ├── And
                │   │   │   │   ├── And
                │   │   │   │   │   ├── And
                │   │   │   │   │   │   ├── Eq
                │   │   │   │   │   │   │   ├── #9
                │   │   │   │   │   │   │   └── #18
                │   │   │   │   │   │   └── Eq
                │   │   │   │   │   │       ├── #33
                │   │   │   │   │   │       └── #18
                │   │   │   │   │   └── Eq
                │   │   │   │   │       ├── #32
                │   │   │   │   │       └── #17
                │   │   │   │   └── Eq
                │   │   │   │       ├── #0
                │   │   │   │       └── #17
                │   │   │   └── Eq
                │   │   │       ├── #37
                │   │   │       └── #16
                │   │   └── Eq
                │   │       ├── #12
                │   │       └── #46
                │   └── Like { expr: #1, pattern: "%green%" }
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
            │       │       ├── Cast { cast_to: Decimal128(20, 0), expr: 1 }
            │       │       └── #22
            │       └── Mul
            │           ├── #35
            │           └── #20
            └── PhysicalFilter
                ├── cond:And
                │   ├── And
                │   │   ├── And
                │   │   │   ├── And
                │   │   │   │   ├── And
                │   │   │   │   │   ├── And
                │   │   │   │   │   │   ├── Eq
                │   │   │   │   │   │   │   ├── #9
                │   │   │   │   │   │   │   └── #18
                │   │   │   │   │   │   └── Eq
                │   │   │   │   │   │       ├── #33
                │   │   │   │   │   │       └── #18
                │   │   │   │   │   └── Eq
                │   │   │   │   │       ├── #32
                │   │   │   │   │       └── #17
                │   │   │   │   └── Eq
                │   │   │   │       ├── #0
                │   │   │   │       └── #17
                │   │   │   └── Eq
                │   │   │       ├── #37
                │   │   │       └── #16
                │   │   └── Eq
                │   │       ├── #12
                │   │       └── #46
                │   └── Like { expr: #1, pattern: "%green%" }
                └── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                    ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                    │   ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                    │   │   ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                    │   │   │   ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                    │   │   │   │   ├── PhysicalScan { table: part }
                    │   │   │   │   └── PhysicalScan { table: supplier }
                    │   │   │   └── PhysicalScan { table: lineitem }
                    │   │   └── PhysicalScan { table: partsupp }
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
LogicalLimit { skip: 0, fetch: 20 }
└── LogicalSort
    ├── exprs:SortOrder { order: Desc }
    │   └── #2
    └── LogicalProjection { exprs: [ #0, #1, #7, #2, #4, #5, #3, #6 ] }
        └── LogicalAgg
            ├── exprs:Agg(Sum)
            │   └── Mul
            │       ├── #22
            │       └── Sub
            │           ├── Cast { cast_to: Decimal128(20, 0), expr: 1 }
            │           └── #23
            ├── groups: [ #0, #1, #5, #4, #34, #2, #7 ]
            └── LogicalFilter
                ├── cond:And
                │   ├── And
                │   │   ├── And
                │   │   │   ├── And
                │   │   │   │   ├── And
                │   │   │   │   │   ├── Eq
                │   │   │   │   │   │   ├── #0
                │   │   │   │   │   │   └── #9
                │   │   │   │   │   └── Eq
                │   │   │   │   │       ├── #17
                │   │   │   │   │       └── #8
                │   │   │   │   └── Geq
                │   │   │   │       ├── #12
                │   │   │   │       └── Cast { cast_to: Date32, expr: "1993-07-01" }
                │   │   │   └── Lt
                │   │   │       ├── #12
                │   │   │       └── Add
                │   │   │           ├── Cast { cast_to: Date32, expr: "1993-07-01" }
                │   │   │           └── INTERVAL_MONTH_DAY_NANO (3, 0, 0)
                │   │   └── Eq
                │   │       ├── #25
                │   │       └── "R"
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
PhysicalLimit { skip: 0, fetch: 20 }
└── PhysicalSort
    ├── exprs:SortOrder { order: Desc }
    │   └── #2
    └── PhysicalProjection { exprs: [ #0, #1, #7, #2, #4, #5, #3, #6 ] }
        └── PhysicalAgg
            ├── aggrs:Agg(Sum)
            │   └── Mul
            │       ├── #22
            │       └── Sub
            │           ├── Cast { cast_to: Decimal128(20, 0), expr: 1 }
            │           └── #23
            ├── groups: [ #0, #1, #5, #4, #34, #2, #7 ]
            └── PhysicalFilter
                ├── cond:And
                │   ├── And
                │   │   ├── And
                │   │   │   ├── And
                │   │   │   │   ├── And
                │   │   │   │   │   ├── Eq
                │   │   │   │   │   │   ├── #0
                │   │   │   │   │   │   └── #9
                │   │   │   │   │   └── Eq
                │   │   │   │   │       ├── #17
                │   │   │   │   │       └── #8
                │   │   │   │   └── Geq
                │   │   │   │       ├── #12
                │   │   │   │       └── Cast { cast_to: Date32, expr: "1993-07-01" }
                │   │   │   └── Lt
                │   │   │       ├── #12
                │   │   │       └── Add
                │   │   │           ├── Cast { cast_to: Date32, expr: "1993-07-01" }
                │   │   │           └── INTERVAL_MONTH_DAY_NANO (3, 0, 0)
                │   │   └── Eq
                │   │       ├── #25
                │   │       └── "R"
                │   └── Eq
                │       ├── #3
                │       └── #33
                └── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                    ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                    │   ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                    │   │   ├── PhysicalScan { table: customer }
                    │   │   └── PhysicalScan { table: orders }
                    │   └── PhysicalScan { table: lineitem }
                    └── PhysicalScan { table: nation }
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
│   │   ├── 100
│   │   └── Cast { cast_to: Float64, expr: #0 }
│   └── Cast { cast_to: Float64, expr: #1 }
└── LogicalAgg
    ├── exprs:
    │   ┌── Agg(Sum)
    │   │   └── Case
    │   │       └── 
    │   │           ┌── Like { expr: #20, pattern: "PROMO%" }
    │   │           ├── Mul
    │   │           │   ├── #5
    │   │           │   └── Sub
    │   │           │       ├── Cast { cast_to: Decimal128(20, 0), expr: 1 }
    │   │           │       └── #6
    │   │           └── Cast { cast_to: Decimal128(38, 4), expr: 0 }
    │   └── Agg(Sum)
    │       └── Mul
    │           ├── #5
    │           └── Sub
    │               ├── Cast { cast_to: Decimal128(20, 0), expr: 1 }
    │               └── #6
    ├── groups: []
    └── LogicalFilter
        ├── cond:And
        │   ├── And
        │   │   ├── Eq
        │   │   │   ├── #1
        │   │   │   └── #16
        │   │   └── Geq
        │   │       ├── #10
        │   │       └── Cast { cast_to: Date32, expr: "1995-09-01" }
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
│   │   ├── 100
│   │   └── Cast { cast_to: Float64, expr: #0 }
│   └── Cast { cast_to: Float64, expr: #1 }
└── PhysicalAgg
    ├── aggrs:
    │   ┌── Agg(Sum)
    │   │   └── Case
    │   │       └── 
    │   │           ┌── Like { expr: #20, pattern: "PROMO%" }
    │   │           ├── Mul
    │   │           │   ├── #5
    │   │           │   └── Sub
    │   │           │       ├── Cast { cast_to: Decimal128(20, 0), expr: 1 }
    │   │           │       └── #6
    │   │           └── Cast { cast_to: Decimal128(38, 4), expr: 0 }
    │   └── Agg(Sum)
    │       └── Mul
    │           ├── #5
    │           └── Sub
    │               ├── Cast { cast_to: Decimal128(20, 0), expr: 1 }
    │               └── #6
    ├── groups: []
    └── PhysicalFilter
        ├── cond:And
        │   ├── And
        │   │   ├── Eq
        │   │   │   ├── #1
        │   │   │   └── #16
        │   │   └── Geq
        │   │       ├── #10
        │   │       └── Cast { cast_to: Date32, expr: "1995-09-01" }
        │   └── Lt
        │       ├── #10
        │       └── Add
        │           ├── Cast { cast_to: Date32, expr: "1995-09-01" }
        │           └── INTERVAL_MONTH_DAY_NANO (1, 0, 0)
        └── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
            ├── PhysicalScan { table: lineitem }
            └── PhysicalScan { table: part }
*/

-- TPC-H Q18
SELECT
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity) as total_quantity
FROM
    customer,
    orders,
    lineitem
WHERE
    c_custkey = o_custkey
    AND o_orderkey = l_orderkey
GROUP BY
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
HAVING
    sum(l_quantity) > 300
ORDER BY
    o_totalprice DESC,
    o_orderdate;

/*
LogicalSort
├── exprs:
│   ┌── SortOrder { order: Desc }
│   │   └── #4
│   └── SortOrder { order: Asc }
│       └── #3
└── LogicalProjection { exprs: [ #0, #1, #2, #3, #4, #5 ] }
    └── LogicalFilter
        ├── cond:Gt
        │   ├── #5
        │   └── Cast { cast_to: Decimal128(25, 2), expr: 300 }
        └── LogicalAgg
            ├── exprs:Agg(Sum)
            │   └── [ #21 ]
            ├── groups: [ #1, #0, #8, #12, #11 ]
            └── LogicalFilter
                ├── cond:And
                │   ├── Eq
                │   │   ├── #0
                │   │   └── #9
                │   └── Eq
                │       ├── #8
                │       └── #17
                └── LogicalJoin { join_type: Cross, cond: true }
                    ├── LogicalJoin { join_type: Cross, cond: true }
                    │   ├── LogicalScan { table: customer }
                    │   └── LogicalScan { table: orders }
                    └── LogicalScan { table: lineitem }
PhysicalSort
├── exprs:
│   ┌── SortOrder { order: Desc }
│   │   └── #4
│   └── SortOrder { order: Asc }
│       └── #3
└── PhysicalProjection { exprs: [ #0, #1, #2, #3, #4, #5 ] }
    └── PhysicalFilter
        ├── cond:Gt
        │   ├── #5
        │   └── Cast { cast_to: Decimal128(25, 2), expr: 300 }
        └── PhysicalAgg
            ├── aggrs:Agg(Sum)
            │   └── [ #21 ]
            ├── groups: [ #1, #0, #8, #12, #11 ]
            └── PhysicalFilter
                ├── cond:And
                │   ├── Eq
                │   │   ├── #0
                │   │   └── #9
                │   └── Eq
                │       ├── #8
                │       └── #17
                └── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                    ├── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
                    │   ├── PhysicalScan { table: customer }
                    │   └── PhysicalScan { table: orders }
                    └── PhysicalScan { table: lineitem }
*/

