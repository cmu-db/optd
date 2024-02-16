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
        │           ├── Cast { cast_to: Decimal128(0), expr: 1 }
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
            │   │       └── Cast { cast_to: Date32(0), expr: "2023-01-01" }
            │   └── Lt
            │       ├── #12
            │       └── Cast { cast_to: Date32(0), expr: "2024-01-01" }
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
        │           ├── Cast { cast_to: Decimal128(0), expr: 1 }
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
            │   │       └── Cast { cast_to: Date32(0), expr: "2023-01-01" }
            │   └── Lt
            │       ├── #12
            │       └── Cast { cast_to: Date32(0), expr: "2024-01-01" }
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
        │   │           └── Cast { cast_to: Decimal128(0), expr: 0 }
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
            │   │       ├── Cast { cast_to: Decimal128(0), expr: 1 }
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
                │   │   └── Between { expr: #36, lower: Cast { cast_to: Date32(0), expr: "1995-01-01" }, upper: Cast { cast_to: Date32(0), expr: "1996-12-31" } }
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
        │   │           └── Cast { cast_to: Decimal128(0), expr: 0 }
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
            │   │       ├── Cast { cast_to: Decimal128(0), expr: 1 }
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
                │   │   └── Between { expr: #36, lower: Cast { cast_to: Date32(0), expr: "1995-01-01" }, upper: Cast { cast_to: Date32(0), expr: "1996-12-31" } }
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

