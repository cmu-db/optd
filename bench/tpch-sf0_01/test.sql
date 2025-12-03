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

CREATE EXTERNAL TABLE customer_tbl STORED AS CSV LOCATION 'bench/tpch-sf0_01/customer.tbl' OPTIONS('DELIMITER' '|', 'HAS_HEADER' 'false');
insert into customer select column_1, column_2, column_3, column_4, column_5, column_6, column_7, column_8 from customer_tbl;
CREATE EXTERNAL TABLE lineitem_tbl STORED AS CSV LOCATION 'bench/tpch-sf0_01/lineitem.tbl' OPTIONS('DELIMITER' '|', 'HAS_HEADER' 'false');
insert into lineitem select column_1, column_2, column_3, column_4, column_5, column_6, column_7, column_8, column_9, column_10, column_11, column_12, column_13, column_14, column_15, column_16 from lineitem_tbl;
CREATE EXTERNAL TABLE nation_tbl STORED AS CSV LOCATION 'bench/tpch-sf0_01/nation.tbl' OPTIONS('DELIMITER' '|', 'HAS_HEADER' 'false');
insert into nation select column_1, column_2, column_3, column_4 from nation_tbl;
CREATE EXTERNAL TABLE orders_tbl STORED AS CSV LOCATION 'bench/tpch-sf0_01/orders.tbl' OPTIONS('DELIMITER' '|', 'HAS_HEADER' 'false');
insert into orders select column_1, column_2, column_3, column_4, column_5, column_6, column_7, column_8, column_9 from orders_tbl;
CREATE EXTERNAL TABLE part_tbl STORED AS CSV LOCATION 'bench/tpch-sf0_01/part.tbl' OPTIONS('DELIMITER' '|', 'HAS_HEADER' 'false');
insert into part select column_1, column_2, column_3, column_4, column_5, column_6, column_7, column_8, column_9 from part_tbl;
CREATE EXTERNAL TABLE partsupp_tbl STORED AS CSV LOCATION 'bench/tpch-sf0_01/partsupp.tbl' OPTIONS('DELIMITER' '|', 'HAS_HEADER' 'false');
insert into partsupp select column_1, column_2, column_3, column_4, column_5 from partsupp_tbl;
CREATE EXTERNAL TABLE region_tbl STORED AS CSV LOCATION 'bench/tpch-sf0_01/region.tbl' OPTIONS('DELIMITER' '|', 'HAS_HEADER' 'false');
insert into region select column_1, column_2, column_3 from region_tbl;
CREATE EXTERNAL TABLE supplier_tbl STORED AS CSV LOCATION 'bench/tpch-sf0_01/supplier.tbl' OPTIONS('DELIMITER' '|', 'HAS_HEADER' 'false');
insert into supplier select column_1, column_2, column_3, column_4, column_5, column_6, column_7 from supplier_tbl;

set optd.optd_predicate_pushdown = true;
-- SELECT * FROM lineitem, orders WHERE l_orderkey = o_orderkey AND l_quantity = 46.00;

-- select
--         s_acctbal,
--         s_name,
--         n_name,
--         p_partkey,
--         p_mfgr,
--         s_address,
--         s_phone,
--         s_comment
-- from
--         part,
--         supplier,
--         partsupp,
--         nation,
--         region
-- where
--         p_partkey = ps_partkey
--         and s_suppkey = ps_suppkey
--         and p_size = 15
--         -- and p_type like '%BRASS'
--         and s_nationkey = n_nationkey
--         and n_regionkey = r_regionkey
--         and r_name = 'EUROPE';


-- Q3
-- select
--         l_orderkey,
--         sum(l_extendedprice * (1 - l_discount)) as revenue,
--         o_orderdate,
--         o_shippriority
-- from
--         customer,
--         orders,
--         lineitem
-- where
--         c_mktsegment = 'BUILDING'
--         and c_custkey = o_custkey
--         and l_orderkey = o_orderkey
--         and o_orderdate < date '1995-03-15'
--         and l_shipdate > date '1995-03-15'
-- group by
--         l_orderkey,
--         o_orderdate,
--         o_shippriority
-- order by
--         revenue desc,
--         o_orderdate;


-- Q5
-- select
--         n_name,
--         sum(l_extendedprice * (1 - l_discount)) as revenue
-- from
--         customer,
--         orders,
--         lineitem,
--         supplier,
--         nation,
--         region
-- where
--         c_custkey = o_custkey
--         and l_orderkey = o_orderkey
--         and l_suppkey = s_suppkey
--         and c_nationkey = s_nationkey
--         and s_nationkey = n_nationkey
--         and n_regionkey = r_regionkey
--         and r_name = 'ASIA'
--         and o_orderdate >= date '1994-01-01'
--         and o_orderdate < date '1994-01-01' + interval '1' year
-- group by
--         n_name
-- order by
--         revenue desc


explain select
        c_custkey,
        c_name,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        c_acctbal,
        n_name,
        c_address,
        c_phone,
        c_comment
from
        customer,
        orders,
        lineitem,
        nation
where
        c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate >= date '1993-10-01'
        and o_orderdate < date '1993-10-01' + interval '3' month
        and l_returnflag = 'R'
        and c_nationkey = n_nationkey
group by
        c_custkey,
        c_name,
        c_acctbal,
        c_phone,
        n_name,
        c_address,
        c_comment
order by
        revenue desc
limit
        20