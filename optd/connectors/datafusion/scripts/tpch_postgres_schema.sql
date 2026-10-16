DROP TABLE IF EXISTS lineitem, orders, customer, part, partsupp, supplier, nation, region CASCADE;

CREATE TABLE region (
    r_regionkey INTEGER,
    r_name TEXT,
    r_comment TEXT
);

CREATE TABLE nation (
    n_nationkey INTEGER,
    n_name TEXT,
    n_regionkey INTEGER,
    n_comment TEXT
);

CREATE TABLE supplier (
    s_suppkey BIGINT,
    s_name TEXT,
    s_address TEXT,
    s_nationkey INTEGER,
    s_phone TEXT,
    s_acctbal NUMERIC(15, 2),
    s_comment TEXT
);

CREATE TABLE customer (
    c_custkey BIGINT,
    c_name TEXT,
    c_address TEXT,
    c_nationkey INTEGER,
    c_phone TEXT,
    c_acctbal NUMERIC(15, 2),
    c_mktsegment TEXT,
    c_comment TEXT
);

CREATE TABLE part (
    p_partkey BIGINT,
    p_name TEXT,
    p_mfgr TEXT,
    p_brand TEXT,
    p_type TEXT,
    p_size INTEGER,
    p_container TEXT,
    p_retailprice NUMERIC(15, 2),
    p_comment TEXT
);

CREATE TABLE partsupp (
    ps_partkey BIGINT,
    ps_suppkey BIGINT,
    ps_availqty BIGINT,
    ps_supplycost NUMERIC(15, 2),
    ps_comment TEXT
);

CREATE TABLE orders (
    o_orderkey BIGINT,
    o_custkey BIGINT,
    o_orderstatus TEXT,
    o_totalprice NUMERIC(15, 2),
    o_orderdate DATE,
    o_orderpriority TEXT,
    o_clerk TEXT,
    o_shippriority INTEGER,
    o_comment TEXT
);

CREATE TABLE lineitem (
    l_orderkey BIGINT,
    l_partkey BIGINT,
    l_suppkey BIGINT,
    l_linenumber BIGINT,
    l_quantity NUMERIC(15, 2),
    l_extendedprice NUMERIC(15, 2),
    l_discount NUMERIC(15, 2),
    l_tax NUMERIC(15, 2),
    l_returnflag TEXT,
    l_linestatus TEXT,
    l_shipdate DATE,
    l_commitdate DATE,
    l_receiptdate DATE,
    l_shipinstruct TEXT,
    l_shipmode TEXT,
    l_comment TEXT
);
