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
