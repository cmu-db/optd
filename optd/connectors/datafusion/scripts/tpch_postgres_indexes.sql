ALTER TABLE region ADD PRIMARY KEY (r_regionkey);
ALTER TABLE nation ADD PRIMARY KEY (n_nationkey);
ALTER TABLE supplier ADD PRIMARY KEY (s_suppkey);
ALTER TABLE customer ADD PRIMARY KEY (c_custkey);
ALTER TABLE part ADD PRIMARY KEY (p_partkey);
ALTER TABLE partsupp ADD PRIMARY KEY (ps_partkey, ps_suppkey);
ALTER TABLE orders ADD PRIMARY KEY (o_orderkey);
ALTER TABLE lineitem ADD PRIMARY KEY (l_orderkey, l_linenumber);

CREATE INDEX nation_regionkey_idx ON nation (n_regionkey);
CREATE INDEX supplier_nationkey_idx ON supplier (s_nationkey);
CREATE INDEX customer_nationkey_idx ON customer (c_nationkey);
CREATE INDEX orders_custkey_idx ON orders (o_custkey);
CREATE INDEX partsupp_suppkey_idx ON partsupp (ps_suppkey);
CREATE INDEX lineitem_part_supp_shipdate_idx
    ON lineitem (l_partkey, l_suppkey, l_shipdate);
CREATE INDEX lineitem_suppkey_idx ON lineitem (l_suppkey);
