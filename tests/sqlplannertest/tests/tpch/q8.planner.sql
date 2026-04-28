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
logical_plan after optd-initial:
OrderBy { ordering_exprs: "__#15.o_year"(#15.0) ASC, (.output_columns): [ "__#15.mkt_share"(#15.1), "__#15.o_year"(#15.0) ], (.cardinality): 0.00 }
└── Project { .table_index: 15, .projections: [ "all_nations.o_year"(#12.0), "__#14.sum(CASE WHEN all_nations.nation = Utf8("IRAQ") THEN all_nations.volume ELSE Int64(0) END)"(#14.0) / "__#14.sum(all_nations.volume)"(#14.1) ], (.output_columns): [ "__#15.mkt_share"(#15.1), "__#15.o_year"(#15.0) ], (.cardinality): 0.00 }
    └── Aggregate { .key_table_index: 13, .aggregate_table_index: 14, .implementation: None, .exprs: [ sum(CASE WHEN "all_nations.nation"(#12.2) = CAST ('IRAQ'::utf8 AS Utf8View) THEN "all_nations.volume"(#12.1) ELSE CAST (0::bigint AS Decimal128(38, 4)) END), sum("all_nations.volume"(#12.1)) ], .keys: "all_nations.o_year"(#12.0), (.output_columns): [ "__#13.o_year"(#13.0), "__#14.sum(CASE WHEN all_nations.nation = Utf8("IRAQ") THEN all_nations.volume ELSE Int64(0) END)"(#14.0), "__#14.sum(all_nations.volume)"(#14.1) ], (.cardinality): 0.00 }
        └── Remap { .table_index: 12, (.output_columns): [ "all_nations.nation"(#12.2), "all_nations.o_year"(#12.0), "all_nations.volume"(#12.1) ], (.cardinality): 0.00 }
            └── Project { .table_index: 11, .projections: [ date_part('YEAR'::utf8, "orders.o_orderdate"(#4.4)), "lineitem.l_extendedprice"(#3.5) * CAST (1::bigint AS Decimal128(20, 0)) - "lineitem.l_discount"(#3.6), "n2.n_name"(#9.1) ], (.output_columns): [ "__#11.nation"(#11.2), "__#11.o_year"(#11.0), "__#11.volume"(#11.1) ], (.cardinality): 0.00 }
                └── Select
                    ├── .predicate: ("part.p_partkey"(#1.0) = "lineitem.l_partkey"(#3.1)) AND ("supplier.s_suppkey"(#2.0) = "lineitem.l_suppkey"(#3.2)) AND ("lineitem.l_orderkey"(#3.0) = "orders.o_orderkey"(#4.0)) AND ("orders.o_custkey"(#4.1) = "customer.c_custkey"(#5.0)) AND ("customer.c_nationkey"(#5.3) = "n1.n_nationkey"(#7.0)) AND ("n1.n_regionkey"(#7.2) = "region.r_regionkey"(#10.0)) AND ("region.r_name"(#10.1) = CAST ('AMERICA'::utf8 AS Utf8View)) AND ("supplier.s_nationkey"(#2.3) = "n2.n_nationkey"(#9.0)) AND (("orders.o_orderdate"(#4.4) >= CAST ('1995-01-01'::utf8 AS Date32)) AND ("orders.o_orderdate"(#4.4) <= CAST ('1996-12-31'::utf8 AS Date32))) AND ("part.p_type"(#1.4) = CAST ('ECONOMY ANODIZED STEEL'::utf8 AS Utf8View))
                    ├── (.output_columns):
                    │   ┌── "customer.c_acctbal"(#5.5)
                    │   ├── "customer.c_address"(#5.2)
                    │   ├── "customer.c_comment"(#5.7)
                    │   ├── "customer.c_custkey"(#5.0)
                    │   ├── "customer.c_mktsegment"(#5.6)
                    │   ├── "customer.c_name"(#5.1)
                    │   ├── "customer.c_nationkey"(#5.3)
                    │   ├── "customer.c_phone"(#5.4)
                    │   ├── "lineitem.l_comment"(#3.15)
                    │   ├── "lineitem.l_commitdate"(#3.11)
                    │   ├── "lineitem.l_discount"(#3.6)
                    │   ├── "lineitem.l_extendedprice"(#3.5)
                    │   ├── "lineitem.l_linenumber"(#3.3)
                    │   ├── "lineitem.l_linestatus"(#3.9)
                    │   ├── "lineitem.l_orderkey"(#3.0)
                    │   ├── "lineitem.l_partkey"(#3.1)
                    │   ├── "lineitem.l_quantity"(#3.4)
                    │   ├── "lineitem.l_receiptdate"(#3.12)
                    │   ├── "lineitem.l_returnflag"(#3.8)
                    │   ├── "lineitem.l_shipdate"(#3.10)
                    │   ├── "lineitem.l_shipinstruct"(#3.13)
                    │   ├── "lineitem.l_shipmode"(#3.14)
                    │   ├── "lineitem.l_suppkey"(#3.2)
                    │   ├── "lineitem.l_tax"(#3.7)
                    │   ├── "n1.n_comment"(#7.3)
                    │   ├── "n1.n_name"(#7.1)
                    │   ├── "n1.n_nationkey"(#7.0)
                    │   ├── "n1.n_regionkey"(#7.2)
                    │   ├── "n2.n_comment"(#9.3)
                    │   ├── "n2.n_name"(#9.1)
                    │   ├── "n2.n_nationkey"(#9.0)
                    │   ├── "n2.n_regionkey"(#9.2)
                    │   ├── "orders.o_clerk"(#4.6)
                    │   ├── "orders.o_comment"(#4.8)
                    │   ├── "orders.o_custkey"(#4.1)
                    │   ├── "orders.o_orderdate"(#4.4)
                    │   ├── "orders.o_orderkey"(#4.0)
                    │   ├── "orders.o_orderpriority"(#4.5)
                    │   ├── "orders.o_orderstatus"(#4.2)
                    │   ├── "orders.o_shippriority"(#4.7)
                    │   ├── "orders.o_totalprice"(#4.3)
                    │   ├── "part.p_brand"(#1.3)
                    │   ├── "part.p_comment"(#1.8)
                    │   ├── "part.p_container"(#1.6)
                    │   ├── "part.p_mfgr"(#1.2)
                    │   ├── "part.p_name"(#1.1)
                    │   ├── "part.p_partkey"(#1.0)
                    │   ├── "part.p_retailprice"(#1.7)
                    │   ├── "part.p_size"(#1.5)
                    │   ├── "part.p_type"(#1.4)
                    │   ├── "region.r_comment"(#10.2)
                    │   ├── "region.r_name"(#10.1)
                    │   ├── "region.r_regionkey"(#10.0)
                    │   ├── "supplier.s_acctbal"(#2.5)
                    │   ├── "supplier.s_address"(#2.2)
                    │   ├── "supplier.s_comment"(#2.6)
                    │   ├── "supplier.s_name"(#2.1)
                    │   ├── "supplier.s_nationkey"(#2.3)
                    │   ├── "supplier.s_phone"(#2.4)
                    │   └── "supplier.s_suppkey"(#2.0)
                    ├── (.cardinality): 0.00
                    └── Join
                        ├── .join_type: Inner
                        ├── .implementation: None
                        ├── .join_cond: 
                        ├── (.output_columns):
                        │   ┌── "customer.c_acctbal"(#5.5)
                        │   ├── "customer.c_address"(#5.2)
                        │   ├── "customer.c_comment"(#5.7)
                        │   ├── "customer.c_custkey"(#5.0)
                        │   ├── "customer.c_mktsegment"(#5.6)
                        │   ├── "customer.c_name"(#5.1)
                        │   ├── "customer.c_nationkey"(#5.3)
                        │   ├── "customer.c_phone"(#5.4)
                        │   ├── "lineitem.l_comment"(#3.15)
                        │   ├── "lineitem.l_commitdate"(#3.11)
                        │   ├── "lineitem.l_discount"(#3.6)
                        │   ├── "lineitem.l_extendedprice"(#3.5)
                        │   ├── "lineitem.l_linenumber"(#3.3)
                        │   ├── "lineitem.l_linestatus"(#3.9)
                        │   ├── "lineitem.l_orderkey"(#3.0)
                        │   ├── "lineitem.l_partkey"(#3.1)
                        │   ├── "lineitem.l_quantity"(#3.4)
                        │   ├── "lineitem.l_receiptdate"(#3.12)
                        │   ├── "lineitem.l_returnflag"(#3.8)
                        │   ├── "lineitem.l_shipdate"(#3.10)
                        │   ├── "lineitem.l_shipinstruct"(#3.13)
                        │   ├── "lineitem.l_shipmode"(#3.14)
                        │   ├── "lineitem.l_suppkey"(#3.2)
                        │   ├── "lineitem.l_tax"(#3.7)
                        │   ├── "n1.n_comment"(#7.3)
                        │   ├── "n1.n_name"(#7.1)
                        │   ├── "n1.n_nationkey"(#7.0)
                        │   ├── "n1.n_regionkey"(#7.2)
                        │   ├── "n2.n_comment"(#9.3)
                        │   ├── "n2.n_name"(#9.1)
                        │   ├── "n2.n_nationkey"(#9.0)
                        │   ├── "n2.n_regionkey"(#9.2)
                        │   ├── "orders.o_clerk"(#4.6)
                        │   ├── "orders.o_comment"(#4.8)
                        │   ├── "orders.o_custkey"(#4.1)
                        │   ├── "orders.o_orderdate"(#4.4)
                        │   ├── "orders.o_orderkey"(#4.0)
                        │   ├── "orders.o_orderpriority"(#4.5)
                        │   ├── "orders.o_orderstatus"(#4.2)
                        │   ├── "orders.o_shippriority"(#4.7)
                        │   ├── "orders.o_totalprice"(#4.3)
                        │   ├── "part.p_brand"(#1.3)
                        │   ├── "part.p_comment"(#1.8)
                        │   ├── "part.p_container"(#1.6)
                        │   ├── "part.p_mfgr"(#1.2)
                        │   ├── "part.p_name"(#1.1)
                        │   ├── "part.p_partkey"(#1.0)
                        │   ├── "part.p_retailprice"(#1.7)
                        │   ├── "part.p_size"(#1.5)
                        │   ├── "part.p_type"(#1.4)
                        │   ├── "region.r_comment"(#10.2)
                        │   ├── "region.r_name"(#10.1)
                        │   ├── "region.r_regionkey"(#10.0)
                        │   ├── "supplier.s_acctbal"(#2.5)
                        │   ├── "supplier.s_address"(#2.2)
                        │   ├── "supplier.s_comment"(#2.6)
                        │   ├── "supplier.s_name"(#2.1)
                        │   ├── "supplier.s_nationkey"(#2.3)
                        │   ├── "supplier.s_phone"(#2.4)
                        │   └── "supplier.s_suppkey"(#2.0)
                        ├── (.cardinality): 0.00
                        ├── Join
                        │   ├── .join_type: Inner
                        │   ├── .implementation: None
                        │   ├── .join_cond: 
                        │   ├── (.output_columns):
                        │   │   ┌── "customer.c_acctbal"(#5.5)
                        │   │   ├── "customer.c_address"(#5.2)
                        │   │   ├── "customer.c_comment"(#5.7)
                        │   │   ├── "customer.c_custkey"(#5.0)
                        │   │   ├── "customer.c_mktsegment"(#5.6)
                        │   │   ├── "customer.c_name"(#5.1)
                        │   │   ├── "customer.c_nationkey"(#5.3)
                        │   │   ├── "customer.c_phone"(#5.4)
                        │   │   ├── "lineitem.l_comment"(#3.15)
                        │   │   ├── "lineitem.l_commitdate"(#3.11)
                        │   │   ├── "lineitem.l_discount"(#3.6)
                        │   │   ├── "lineitem.l_extendedprice"(#3.5)
                        │   │   ├── "lineitem.l_linenumber"(#3.3)
                        │   │   ├── "lineitem.l_linestatus"(#3.9)
                        │   │   ├── "lineitem.l_orderkey"(#3.0)
                        │   │   ├── "lineitem.l_partkey"(#3.1)
                        │   │   ├── "lineitem.l_quantity"(#3.4)
                        │   │   ├── "lineitem.l_receiptdate"(#3.12)
                        │   │   ├── "lineitem.l_returnflag"(#3.8)
                        │   │   ├── "lineitem.l_shipdate"(#3.10)
                        │   │   ├── "lineitem.l_shipinstruct"(#3.13)
                        │   │   ├── "lineitem.l_shipmode"(#3.14)
                        │   │   ├── "lineitem.l_suppkey"(#3.2)
                        │   │   ├── "lineitem.l_tax"(#3.7)
                        │   │   ├── "n1.n_comment"(#7.3)
                        │   │   ├── "n1.n_name"(#7.1)
                        │   │   ├── "n1.n_nationkey"(#7.0)
                        │   │   ├── "n1.n_regionkey"(#7.2)
                        │   │   ├── "n2.n_comment"(#9.3)
                        │   │   ├── "n2.n_name"(#9.1)
                        │   │   ├── "n2.n_nationkey"(#9.0)
                        │   │   ├── "n2.n_regionkey"(#9.2)
                        │   │   ├── "orders.o_clerk"(#4.6)
                        │   │   ├── "orders.o_comment"(#4.8)
                        │   │   ├── "orders.o_custkey"(#4.1)
                        │   │   ├── "orders.o_orderdate"(#4.4)
                        │   │   ├── "orders.o_orderkey"(#4.0)
                        │   │   ├── "orders.o_orderpriority"(#4.5)
                        │   │   ├── "orders.o_orderstatus"(#4.2)
                        │   │   ├── "orders.o_shippriority"(#4.7)
                        │   │   ├── "orders.o_totalprice"(#4.3)
                        │   │   ├── "part.p_brand"(#1.3)
                        │   │   ├── "part.p_comment"(#1.8)
                        │   │   ├── "part.p_container"(#1.6)
                        │   │   ├── "part.p_mfgr"(#1.2)
                        │   │   ├── "part.p_name"(#1.1)
                        │   │   ├── "part.p_partkey"(#1.0)
                        │   │   ├── "part.p_retailprice"(#1.7)
                        │   │   ├── "part.p_size"(#1.5)
                        │   │   ├── "part.p_type"(#1.4)
                        │   │   ├── "supplier.s_acctbal"(#2.5)
                        │   │   ├── "supplier.s_address"(#2.2)
                        │   │   ├── "supplier.s_comment"(#2.6)
                        │   │   ├── "supplier.s_name"(#2.1)
                        │   │   ├── "supplier.s_nationkey"(#2.3)
                        │   │   ├── "supplier.s_phone"(#2.4)
                        │   │   └── "supplier.s_suppkey"(#2.0)
                        │   ├── (.cardinality): 0.00
                        │   ├── Join
                        │   │   ├── .join_type: Inner
                        │   │   ├── .implementation: None
                        │   │   ├── .join_cond: 
                        │   │   ├── (.output_columns):
                        │   │   │   ┌── "customer.c_acctbal"(#5.5)
                        │   │   │   ├── "customer.c_address"(#5.2)
                        │   │   │   ├── "customer.c_comment"(#5.7)
                        │   │   │   ├── "customer.c_custkey"(#5.0)
                        │   │   │   ├── "customer.c_mktsegment"(#5.6)
                        │   │   │   ├── "customer.c_name"(#5.1)
                        │   │   │   ├── "customer.c_nationkey"(#5.3)
                        │   │   │   ├── "customer.c_phone"(#5.4)
                        │   │   │   ├── "lineitem.l_comment"(#3.15)
                        │   │   │   ├── "lineitem.l_commitdate"(#3.11)
                        │   │   │   ├── "lineitem.l_discount"(#3.6)
                        │   │   │   ├── "lineitem.l_extendedprice"(#3.5)
                        │   │   │   ├── "lineitem.l_linenumber"(#3.3)
                        │   │   │   ├── "lineitem.l_linestatus"(#3.9)
                        │   │   │   ├── "lineitem.l_orderkey"(#3.0)
                        │   │   │   ├── "lineitem.l_partkey"(#3.1)
                        │   │   │   ├── "lineitem.l_quantity"(#3.4)
                        │   │   │   ├── "lineitem.l_receiptdate"(#3.12)
                        │   │   │   ├── "lineitem.l_returnflag"(#3.8)
                        │   │   │   ├── "lineitem.l_shipdate"(#3.10)
                        │   │   │   ├── "lineitem.l_shipinstruct"(#3.13)
                        │   │   │   ├── "lineitem.l_shipmode"(#3.14)
                        │   │   │   ├── "lineitem.l_suppkey"(#3.2)
                        │   │   │   ├── "lineitem.l_tax"(#3.7)
                        │   │   │   ├── "n1.n_comment"(#7.3)
                        │   │   │   ├── "n1.n_name"(#7.1)
                        │   │   │   ├── "n1.n_nationkey"(#7.0)
                        │   │   │   ├── "n1.n_regionkey"(#7.2)
                        │   │   │   ├── "orders.o_clerk"(#4.6)
                        │   │   │   ├── "orders.o_comment"(#4.8)
                        │   │   │   ├── "orders.o_custkey"(#4.1)
                        │   │   │   ├── "orders.o_orderdate"(#4.4)
                        │   │   │   ├── "orders.o_orderkey"(#4.0)
                        │   │   │   ├── "orders.o_orderpriority"(#4.5)
                        │   │   │   ├── "orders.o_orderstatus"(#4.2)
                        │   │   │   ├── "orders.o_shippriority"(#4.7)
                        │   │   │   ├── "orders.o_totalprice"(#4.3)
                        │   │   │   ├── "part.p_brand"(#1.3)
                        │   │   │   ├── "part.p_comment"(#1.8)
                        │   │   │   ├── "part.p_container"(#1.6)
                        │   │   │   ├── "part.p_mfgr"(#1.2)
                        │   │   │   ├── "part.p_name"(#1.1)
                        │   │   │   ├── "part.p_partkey"(#1.0)
                        │   │   │   ├── "part.p_retailprice"(#1.7)
                        │   │   │   ├── "part.p_size"(#1.5)
                        │   │   │   ├── "part.p_type"(#1.4)
                        │   │   │   ├── "supplier.s_acctbal"(#2.5)
                        │   │   │   ├── "supplier.s_address"(#2.2)
                        │   │   │   ├── "supplier.s_comment"(#2.6)
                        │   │   │   ├── "supplier.s_name"(#2.1)
                        │   │   │   ├── "supplier.s_nationkey"(#2.3)
                        │   │   │   ├── "supplier.s_phone"(#2.4)
                        │   │   │   └── "supplier.s_suppkey"(#2.0)
                        │   │   ├── (.cardinality): 0.00
                        │   │   ├── Join
                        │   │   │   ├── .join_type: Inner
                        │   │   │   ├── .implementation: None
                        │   │   │   ├── .join_cond: 
                        │   │   │   ├── (.output_columns):
                        │   │   │   │   ┌── "customer.c_acctbal"(#5.5)
                        │   │   │   │   ├── "customer.c_address"(#5.2)
                        │   │   │   │   ├── "customer.c_comment"(#5.7)
                        │   │   │   │   ├── "customer.c_custkey"(#5.0)
                        │   │   │   │   ├── "customer.c_mktsegment"(#5.6)
                        │   │   │   │   ├── "customer.c_name"(#5.1)
                        │   │   │   │   ├── "customer.c_nationkey"(#5.3)
                        │   │   │   │   ├── "customer.c_phone"(#5.4)
                        │   │   │   │   ├── "lineitem.l_comment"(#3.15)
                        │   │   │   │   ├── "lineitem.l_commitdate"(#3.11)
                        │   │   │   │   ├── "lineitem.l_discount"(#3.6)
                        │   │   │   │   ├── "lineitem.l_extendedprice"(#3.5)
                        │   │   │   │   ├── "lineitem.l_linenumber"(#3.3)
                        │   │   │   │   ├── "lineitem.l_linestatus"(#3.9)
                        │   │   │   │   ├── "lineitem.l_orderkey"(#3.0)
                        │   │   │   │   ├── "lineitem.l_partkey"(#3.1)
                        │   │   │   │   ├── "lineitem.l_quantity"(#3.4)
                        │   │   │   │   ├── "lineitem.l_receiptdate"(#3.12)
                        │   │   │   │   ├── "lineitem.l_returnflag"(#3.8)
                        │   │   │   │   ├── "lineitem.l_shipdate"(#3.10)
                        │   │   │   │   ├── "lineitem.l_shipinstruct"(#3.13)
                        │   │   │   │   ├── "lineitem.l_shipmode"(#3.14)
                        │   │   │   │   ├── "lineitem.l_suppkey"(#3.2)
                        │   │   │   │   ├── "lineitem.l_tax"(#3.7)
                        │   │   │   │   ├── "orders.o_clerk"(#4.6)
                        │   │   │   │   ├── "orders.o_comment"(#4.8)
                        │   │   │   │   ├── "orders.o_custkey"(#4.1)
                        │   │   │   │   ├── "orders.o_orderdate"(#4.4)
                        │   │   │   │   ├── "orders.o_orderkey"(#4.0)
                        │   │   │   │   ├── "orders.o_orderpriority"(#4.5)
                        │   │   │   │   ├── "orders.o_orderstatus"(#4.2)
                        │   │   │   │   ├── "orders.o_shippriority"(#4.7)
                        │   │   │   │   ├── "orders.o_totalprice"(#4.3)
                        │   │   │   │   ├── "part.p_brand"(#1.3)
                        │   │   │   │   ├── "part.p_comment"(#1.8)
                        │   │   │   │   ├── "part.p_container"(#1.6)
                        │   │   │   │   ├── "part.p_mfgr"(#1.2)
                        │   │   │   │   ├── "part.p_name"(#1.1)
                        │   │   │   │   ├── "part.p_partkey"(#1.0)
                        │   │   │   │   ├── "part.p_retailprice"(#1.7)
                        │   │   │   │   ├── "part.p_size"(#1.5)
                        │   │   │   │   ├── "part.p_type"(#1.4)
                        │   │   │   │   ├── "supplier.s_acctbal"(#2.5)
                        │   │   │   │   ├── "supplier.s_address"(#2.2)
                        │   │   │   │   ├── "supplier.s_comment"(#2.6)
                        │   │   │   │   ├── "supplier.s_name"(#2.1)
                        │   │   │   │   ├── "supplier.s_nationkey"(#2.3)
                        │   │   │   │   ├── "supplier.s_phone"(#2.4)
                        │   │   │   │   └── "supplier.s_suppkey"(#2.0)
                        │   │   │   ├── (.cardinality): 0.00
                        │   │   │   ├── Join
                        │   │   │   │   ├── .join_type: Inner
                        │   │   │   │   ├── .implementation: None
                        │   │   │   │   ├── .join_cond: 
                        │   │   │   │   ├── (.output_columns):
                        │   │   │   │   │   ┌── "lineitem.l_comment"(#3.15)
                        │   │   │   │   │   ├── "lineitem.l_commitdate"(#3.11)
                        │   │   │   │   │   ├── "lineitem.l_discount"(#3.6)
                        │   │   │   │   │   ├── "lineitem.l_extendedprice"(#3.5)
                        │   │   │   │   │   ├── "lineitem.l_linenumber"(#3.3)
                        │   │   │   │   │   ├── "lineitem.l_linestatus"(#3.9)
                        │   │   │   │   │   ├── "lineitem.l_orderkey"(#3.0)
                        │   │   │   │   │   ├── "lineitem.l_partkey"(#3.1)
                        │   │   │   │   │   ├── "lineitem.l_quantity"(#3.4)
                        │   │   │   │   │   ├── "lineitem.l_receiptdate"(#3.12)
                        │   │   │   │   │   ├── "lineitem.l_returnflag"(#3.8)
                        │   │   │   │   │   ├── "lineitem.l_shipdate"(#3.10)
                        │   │   │   │   │   ├── "lineitem.l_shipinstruct"(#3.13)
                        │   │   │   │   │   ├── "lineitem.l_shipmode"(#3.14)
                        │   │   │   │   │   ├── "lineitem.l_suppkey"(#3.2)
                        │   │   │   │   │   ├── "lineitem.l_tax"(#3.7)
                        │   │   │   │   │   ├── "orders.o_clerk"(#4.6)
                        │   │   │   │   │   ├── "orders.o_comment"(#4.8)
                        │   │   │   │   │   ├── "orders.o_custkey"(#4.1)
                        │   │   │   │   │   ├── "orders.o_orderdate"(#4.4)
                        │   │   │   │   │   ├── "orders.o_orderkey"(#4.0)
                        │   │   │   │   │   ├── "orders.o_orderpriority"(#4.5)
                        │   │   │   │   │   ├── "orders.o_orderstatus"(#4.2)
                        │   │   │   │   │   ├── "orders.o_shippriority"(#4.7)
                        │   │   │   │   │   ├── "orders.o_totalprice"(#4.3)
                        │   │   │   │   │   ├── "part.p_brand"(#1.3)
                        │   │   │   │   │   ├── "part.p_comment"(#1.8)
                        │   │   │   │   │   ├── "part.p_container"(#1.6)
                        │   │   │   │   │   ├── "part.p_mfgr"(#1.2)
                        │   │   │   │   │   ├── "part.p_name"(#1.1)
                        │   │   │   │   │   ├── "part.p_partkey"(#1.0)
                        │   │   │   │   │   ├── "part.p_retailprice"(#1.7)
                        │   │   │   │   │   ├── "part.p_size"(#1.5)
                        │   │   │   │   │   ├── "part.p_type"(#1.4)
                        │   │   │   │   │   ├── "supplier.s_acctbal"(#2.5)
                        │   │   │   │   │   ├── "supplier.s_address"(#2.2)
                        │   │   │   │   │   ├── "supplier.s_comment"(#2.6)
                        │   │   │   │   │   ├── "supplier.s_name"(#2.1)
                        │   │   │   │   │   ├── "supplier.s_nationkey"(#2.3)
                        │   │   │   │   │   ├── "supplier.s_phone"(#2.4)
                        │   │   │   │   │   └── "supplier.s_suppkey"(#2.0)
                        │   │   │   │   ├── (.cardinality): 0.00
                        │   │   │   │   ├── Join
                        │   │   │   │   │   ├── .join_type: Inner
                        │   │   │   │   │   ├── .implementation: None
                        │   │   │   │   │   ├── .join_cond: 
                        │   │   │   │   │   ├── (.output_columns):
                        │   │   │   │   │   │   ┌── "lineitem.l_comment"(#3.15)
                        │   │   │   │   │   │   ├── "lineitem.l_commitdate"(#3.11)
                        │   │   │   │   │   │   ├── "lineitem.l_discount"(#3.6)
                        │   │   │   │   │   │   ├── "lineitem.l_extendedprice"(#3.5)
                        │   │   │   │   │   │   ├── "lineitem.l_linenumber"(#3.3)
                        │   │   │   │   │   │   ├── "lineitem.l_linestatus"(#3.9)
                        │   │   │   │   │   │   ├── "lineitem.l_orderkey"(#3.0)
                        │   │   │   │   │   │   ├── "lineitem.l_partkey"(#3.1)
                        │   │   │   │   │   │   ├── "lineitem.l_quantity"(#3.4)
                        │   │   │   │   │   │   ├── "lineitem.l_receiptdate"(#3.12)
                        │   │   │   │   │   │   ├── "lineitem.l_returnflag"(#3.8)
                        │   │   │   │   │   │   ├── "lineitem.l_shipdate"(#3.10)
                        │   │   │   │   │   │   ├── "lineitem.l_shipinstruct"(#3.13)
                        │   │   │   │   │   │   ├── "lineitem.l_shipmode"(#3.14)
                        │   │   │   │   │   │   ├── "lineitem.l_suppkey"(#3.2)
                        │   │   │   │   │   │   ├── "lineitem.l_tax"(#3.7)
                        │   │   │   │   │   │   ├── "part.p_brand"(#1.3)
                        │   │   │   │   │   │   ├── "part.p_comment"(#1.8)
                        │   │   │   │   │   │   ├── "part.p_container"(#1.6)
                        │   │   │   │   │   │   ├── "part.p_mfgr"(#1.2)
                        │   │   │   │   │   │   ├── "part.p_name"(#1.1)
                        │   │   │   │   │   │   ├── "part.p_partkey"(#1.0)
                        │   │   │   │   │   │   ├── "part.p_retailprice"(#1.7)
                        │   │   │   │   │   │   ├── "part.p_size"(#1.5)
                        │   │   │   │   │   │   ├── "part.p_type"(#1.4)
                        │   │   │   │   │   │   ├── "supplier.s_acctbal"(#2.5)
                        │   │   │   │   │   │   ├── "supplier.s_address"(#2.2)
                        │   │   │   │   │   │   ├── "supplier.s_comment"(#2.6)
                        │   │   │   │   │   │   ├── "supplier.s_name"(#2.1)
                        │   │   │   │   │   │   ├── "supplier.s_nationkey"(#2.3)
                        │   │   │   │   │   │   ├── "supplier.s_phone"(#2.4)
                        │   │   │   │   │   │   └── "supplier.s_suppkey"(#2.0)
                        │   │   │   │   │   ├── (.cardinality): 0.00
                        │   │   │   │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: , (.output_columns): [ "part.p_brand"(#1.3), "part.p_comment"(#1.8), "part.p_container"(#1.6), "part.p_mfgr"(#1.2), "part.p_name"(#1.1), "part.p_partkey"(#1.0), "part.p_retailprice"(#1.7), "part.p_size"(#1.5), "part.p_type"(#1.4), "supplier.s_acctbal"(#2.5), "supplier.s_address"(#2.2), "supplier.s_comment"(#2.6), "supplier.s_name"(#2.1), "supplier.s_nationkey"(#2.3), "supplier.s_phone"(#2.4), "supplier.s_suppkey"(#2.0) ], (.cardinality): 0.00 }
                        │   │   │   │   │   │   ├── Get { .data_source_id: 3, .table_index: 1, .implementation: None, (.output_columns): [ "part.p_brand"(#1.3), "part.p_comment"(#1.8), "part.p_container"(#1.6), "part.p_mfgr"(#1.2), "part.p_name"(#1.1), "part.p_partkey"(#1.0), "part.p_retailprice"(#1.7), "part.p_size"(#1.5), "part.p_type"(#1.4) ], (.cardinality): 0.00 }
                        │   │   │   │   │   │   └── Get { .data_source_id: 4, .table_index: 2, .implementation: None, (.output_columns): [ "supplier.s_acctbal"(#2.5), "supplier.s_address"(#2.2), "supplier.s_comment"(#2.6), "supplier.s_name"(#2.1), "supplier.s_nationkey"(#2.3), "supplier.s_phone"(#2.4), "supplier.s_suppkey"(#2.0) ], (.cardinality): 0.00 }
                        │   │   │   │   │   └── Get { .data_source_id: 8, .table_index: 3, .implementation: None, (.output_columns): [ "lineitem.l_comment"(#3.15), "lineitem.l_commitdate"(#3.11), "lineitem.l_discount"(#3.6), "lineitem.l_extendedprice"(#3.5), "lineitem.l_linenumber"(#3.3), "lineitem.l_linestatus"(#3.9), "lineitem.l_orderkey"(#3.0), "lineitem.l_partkey"(#3.1), "lineitem.l_quantity"(#3.4), "lineitem.l_receiptdate"(#3.12), "lineitem.l_returnflag"(#3.8), "lineitem.l_shipdate"(#3.10), "lineitem.l_shipinstruct"(#3.13), "lineitem.l_shipmode"(#3.14), "lineitem.l_suppkey"(#3.2), "lineitem.l_tax"(#3.7) ], (.cardinality): 0.00 }
                        │   │   │   │   └── Get { .data_source_id: 7, .table_index: 4, .implementation: None, (.output_columns): [ "orders.o_clerk"(#4.6), "orders.o_comment"(#4.8), "orders.o_custkey"(#4.1), "orders.o_orderdate"(#4.4), "orders.o_orderkey"(#4.0), "orders.o_orderpriority"(#4.5), "orders.o_orderstatus"(#4.2), "orders.o_shippriority"(#4.7), "orders.o_totalprice"(#4.3) ], (.cardinality): 0.00 }
                        │   │   │   └── Get { .data_source_id: 6, .table_index: 5, .implementation: None, (.output_columns): [ "customer.c_acctbal"(#5.5), "customer.c_address"(#5.2), "customer.c_comment"(#5.7), "customer.c_custkey"(#5.0), "customer.c_mktsegment"(#5.6), "customer.c_name"(#5.1), "customer.c_nationkey"(#5.3), "customer.c_phone"(#5.4) ], (.cardinality): 0.00 }
                        │   │   └── Remap { .table_index: 7, (.output_columns): [ "n1.n_comment"(#7.3), "n1.n_name"(#7.1), "n1.n_nationkey"(#7.0), "n1.n_regionkey"(#7.2) ], (.cardinality): 0.00 }
                        │   │       └── Get { .data_source_id: 1, .table_index: 6, .implementation: None, (.output_columns): [ "nation.n_comment"(#6.3), "nation.n_name"(#6.1), "nation.n_nationkey"(#6.0), "nation.n_regionkey"(#6.2) ], (.cardinality): 0.00 }
                        │   └── Remap { .table_index: 9, (.output_columns): [ "n2.n_comment"(#9.3), "n2.n_name"(#9.1), "n2.n_nationkey"(#9.0), "n2.n_regionkey"(#9.2) ], (.cardinality): 0.00 }
                        │       └── Get { .data_source_id: 1, .table_index: 8, .implementation: None, (.output_columns): [ "nation.n_comment"(#8.3), "nation.n_name"(#8.1), "nation.n_nationkey"(#8.0), "nation.n_regionkey"(#8.2) ], (.cardinality): 0.00 }
                        └── Get { .data_source_id: 2, .table_index: 10, .implementation: None, (.output_columns): [ "region.r_comment"(#10.2), "region.r_name"(#10.1), "region.r_regionkey"(#10.0) ], (.cardinality): 0.00 }

physical_plan after optd-finalized:
EnforcerSort { tuple_ordering: [(#15.0, Asc)], (.output_columns): [ "__#15.mkt_share"(#15.1), "__#15.o_year"(#15.0) ], (.cardinality): 0.00 }
└── Project
    ├── .table_index: 15
    ├── .projections:
    │   ┌── "all_nations.o_year"(#12.0)
    │   └── "__#14.sum(CASE WHEN all_nations.nation = Utf8("IRAQ") THEN all_nations.volume ELSE Int64(0) END)"(#14.0) / "__#14.sum(all_nations.volume)"(#14.1)
    ├── (.output_columns): [ "__#15.mkt_share"(#15.1), "__#15.o_year"(#15.0) ]
    ├── (.cardinality): 0.00
    └── Aggregate
        ├── .key_table_index: 13
        ├── .aggregate_table_index: 14
        ├── .implementation: None
        ├── .exprs:
        │   ┌── sum(CASE WHEN "all_nations.nation"(#12.2) = 'IRAQ'::utf8_view THEN "all_nations.volume"(#12.1) ELSE 0::decimal128(38, 4) END)
        │   └── sum("all_nations.volume"(#12.1))
        ├── .keys: "all_nations.o_year"(#12.0)
        ├── (.output_columns):
        │   ┌── "__#13.o_year"(#13.0)
        │   ├── "__#14.sum(CASE WHEN all_nations.nation = Utf8("IRAQ") THEN all_nations.volume ELSE Int64(0) END)"(#14.0)
        │   └── "__#14.sum(all_nations.volume)"(#14.1)
        ├── (.cardinality): 0.00
        └── Remap { .table_index: 12, (.output_columns): [ "all_nations.nation"(#12.2), "all_nations.o_year"(#12.0), "all_nations.volume"(#12.1) ], (.cardinality): 0.00 }
            └── Project
                ├── .table_index: 11
                ├── .projections:
                │   ┌── date_part('YEAR'::utf8, "orders.o_orderdate"(#4.4))
                │   ├── "lineitem.l_extendedprice"(#3.5) * 1::decimal128(20, 0) - "lineitem.l_discount"(#3.6)
                │   └── "n2.n_name"(#9.1)
                ├── (.output_columns): [ "__#11.nation"(#11.2), "__#11.o_year"(#11.0), "__#11.volume"(#11.1) ]
                ├── (.cardinality): 0.00
                └── Join
                    ├── .join_type: Inner
                    ├── .implementation: None
                    ├── .join_cond: "n1.n_regionkey"(#7.2) = "region.r_regionkey"(#10.0)
                    ├── (.output_columns):
                    │   ┌── "customer.c_custkey"(#5.0)
                    │   ├── "customer.c_nationkey"(#5.3)
                    │   ├── "lineitem.l_discount"(#3.6)
                    │   ├── "lineitem.l_extendedprice"(#3.5)
                    │   ├── "lineitem.l_orderkey"(#3.0)
                    │   ├── "lineitem.l_partkey"(#3.1)
                    │   ├── "lineitem.l_suppkey"(#3.2)
                    │   ├── "n1.n_comment"(#7.3)
                    │   ├── "n1.n_name"(#7.1)
                    │   ├── "n1.n_nationkey"(#7.0)
                    │   ├── "n1.n_regionkey"(#7.2)
                    │   ├── "n2.n_comment"(#9.3)
                    │   ├── "n2.n_name"(#9.1)
                    │   ├── "n2.n_nationkey"(#9.0)
                    │   ├── "n2.n_regionkey"(#9.2)
                    │   ├── "orders.o_custkey"(#4.1)
                    │   ├── "orders.o_orderdate"(#4.4)
                    │   ├── "orders.o_orderkey"(#4.0)
                    │   ├── "part.p_partkey"(#1.0)
                    │   ├── "part.p_type"(#1.4)
                    │   ├── "region.r_name"(#10.1)
                    │   ├── "region.r_regionkey"(#10.0)
                    │   ├── "supplier.s_nationkey"(#2.3)
                    │   └── "supplier.s_suppkey"(#2.0)
                    ├── (.cardinality): 0.00
                    ├── Join
                    │   ├── .join_type: Inner
                    │   ├── .implementation: None
                    │   ├── .join_cond: "supplier.s_nationkey"(#2.3) = "n2.n_nationkey"(#9.0)
                    │   ├── (.output_columns):
                    │   │   ┌── "customer.c_custkey"(#5.0)
                    │   │   ├── "customer.c_nationkey"(#5.3)
                    │   │   ├── "lineitem.l_discount"(#3.6)
                    │   │   ├── "lineitem.l_extendedprice"(#3.5)
                    │   │   ├── "lineitem.l_orderkey"(#3.0)
                    │   │   ├── "lineitem.l_partkey"(#3.1)
                    │   │   ├── "lineitem.l_suppkey"(#3.2)
                    │   │   ├── "n1.n_comment"(#7.3)
                    │   │   ├── "n1.n_name"(#7.1)
                    │   │   ├── "n1.n_nationkey"(#7.0)
                    │   │   ├── "n1.n_regionkey"(#7.2)
                    │   │   ├── "n2.n_comment"(#9.3)
                    │   │   ├── "n2.n_name"(#9.1)
                    │   │   ├── "n2.n_nationkey"(#9.0)
                    │   │   ├── "n2.n_regionkey"(#9.2)
                    │   │   ├── "orders.o_custkey"(#4.1)
                    │   │   ├── "orders.o_orderdate"(#4.4)
                    │   │   ├── "orders.o_orderkey"(#4.0)
                    │   │   ├── "part.p_partkey"(#1.0)
                    │   │   ├── "part.p_type"(#1.4)
                    │   │   ├── "supplier.s_nationkey"(#2.3)
                    │   │   └── "supplier.s_suppkey"(#2.0)
                    │   ├── (.cardinality): 0.00
                    │   ├── Join
                    │   │   ├── .join_type: Inner
                    │   │   ├── .implementation: None
                    │   │   ├── .join_cond: "customer.c_nationkey"(#5.3) = "n1.n_nationkey"(#7.0)
                    │   │   ├── (.output_columns):
                    │   │   │   ┌── "customer.c_custkey"(#5.0)
                    │   │   │   ├── "customer.c_nationkey"(#5.3)
                    │   │   │   ├── "lineitem.l_discount"(#3.6)
                    │   │   │   ├── "lineitem.l_extendedprice"(#3.5)
                    │   │   │   ├── "lineitem.l_orderkey"(#3.0)
                    │   │   │   ├── "lineitem.l_partkey"(#3.1)
                    │   │   │   ├── "lineitem.l_suppkey"(#3.2)
                    │   │   │   ├── "n1.n_comment"(#7.3)
                    │   │   │   ├── "n1.n_name"(#7.1)
                    │   │   │   ├── "n1.n_nationkey"(#7.0)
                    │   │   │   ├── "n1.n_regionkey"(#7.2)
                    │   │   │   ├── "orders.o_custkey"(#4.1)
                    │   │   │   ├── "orders.o_orderdate"(#4.4)
                    │   │   │   ├── "orders.o_orderkey"(#4.0)
                    │   │   │   ├── "part.p_partkey"(#1.0)
                    │   │   │   ├── "part.p_type"(#1.4)
                    │   │   │   ├── "supplier.s_nationkey"(#2.3)
                    │   │   │   └── "supplier.s_suppkey"(#2.0)
                    │   │   ├── (.cardinality): 0.00
                    │   │   ├── Join
                    │   │   │   ├── .join_type: Inner
                    │   │   │   ├── .implementation: None
                    │   │   │   ├── .join_cond: "orders.o_custkey"(#4.1) = "customer.c_custkey"(#5.0)
                    │   │   │   ├── (.output_columns):
                    │   │   │   │   ┌── "customer.c_custkey"(#5.0)
                    │   │   │   │   ├── "customer.c_nationkey"(#5.3)
                    │   │   │   │   ├── "lineitem.l_discount"(#3.6)
                    │   │   │   │   ├── "lineitem.l_extendedprice"(#3.5)
                    │   │   │   │   ├── "lineitem.l_orderkey"(#3.0)
                    │   │   │   │   ├── "lineitem.l_partkey"(#3.1)
                    │   │   │   │   ├── "lineitem.l_suppkey"(#3.2)
                    │   │   │   │   ├── "orders.o_custkey"(#4.1)
                    │   │   │   │   ├── "orders.o_orderdate"(#4.4)
                    │   │   │   │   ├── "orders.o_orderkey"(#4.0)
                    │   │   │   │   ├── "part.p_partkey"(#1.0)
                    │   │   │   │   ├── "part.p_type"(#1.4)
                    │   │   │   │   ├── "supplier.s_nationkey"(#2.3)
                    │   │   │   │   └── "supplier.s_suppkey"(#2.0)
                    │   │   │   ├── (.cardinality): 0.00
                    │   │   │   ├── Join
                    │   │   │   │   ├── .join_type: Inner
                    │   │   │   │   ├── .implementation: None
                    │   │   │   │   ├── .join_cond: "lineitem.l_orderkey"(#3.0) = "orders.o_orderkey"(#4.0)
                    │   │   │   │   ├── (.output_columns):
                    │   │   │   │   │   ┌── "lineitem.l_discount"(#3.6)
                    │   │   │   │   │   ├── "lineitem.l_extendedprice"(#3.5)
                    │   │   │   │   │   ├── "lineitem.l_orderkey"(#3.0)
                    │   │   │   │   │   ├── "lineitem.l_partkey"(#3.1)
                    │   │   │   │   │   ├── "lineitem.l_suppkey"(#3.2)
                    │   │   │   │   │   ├── "orders.o_custkey"(#4.1)
                    │   │   │   │   │   ├── "orders.o_orderdate"(#4.4)
                    │   │   │   │   │   ├── "orders.o_orderkey"(#4.0)
                    │   │   │   │   │   ├── "part.p_partkey"(#1.0)
                    │   │   │   │   │   ├── "part.p_type"(#1.4)
                    │   │   │   │   │   ├── "supplier.s_nationkey"(#2.3)
                    │   │   │   │   │   └── "supplier.s_suppkey"(#2.0)
                    │   │   │   │   ├── (.cardinality): 0.00
                    │   │   │   │   ├── Join
                    │   │   │   │   │   ├── .join_type: Inner
                    │   │   │   │   │   ├── .implementation: None
                    │   │   │   │   │   ├── .join_cond: ("part.p_partkey"(#1.0) = "lineitem.l_partkey"(#3.1)) AND ("supplier.s_suppkey"(#2.0) = "lineitem.l_suppkey"(#3.2))
                    │   │   │   │   │   ├── (.output_columns):
                    │   │   │   │   │   │   ┌── "lineitem.l_discount"(#3.6)
                    │   │   │   │   │   │   ├── "lineitem.l_extendedprice"(#3.5)
                    │   │   │   │   │   │   ├── "lineitem.l_orderkey"(#3.0)
                    │   │   │   │   │   │   ├── "lineitem.l_partkey"(#3.1)
                    │   │   │   │   │   │   ├── "lineitem.l_suppkey"(#3.2)
                    │   │   │   │   │   │   ├── "part.p_partkey"(#1.0)
                    │   │   │   │   │   │   ├── "part.p_type"(#1.4)
                    │   │   │   │   │   │   ├── "supplier.s_nationkey"(#2.3)
                    │   │   │   │   │   │   └── "supplier.s_suppkey"(#2.0)
                    │   │   │   │   │   ├── (.cardinality): 0.00
                    │   │   │   │   │   ├── Join
                    │   │   │   │   │   │   ├── .join_type: Inner
                    │   │   │   │   │   │   ├── .implementation: None
                    │   │   │   │   │   │   ├── .join_cond: true::boolean
                    │   │   │   │   │   │   ├── (.output_columns):
                    │   │   │   │   │   │   │   ┌── "part.p_partkey"(#1.0)
                    │   │   │   │   │   │   │   ├── "part.p_type"(#1.4)
                    │   │   │   │   │   │   │   ├── "supplier.s_nationkey"(#2.3)
                    │   │   │   │   │   │   │   └── "supplier.s_suppkey"(#2.0)
                    │   │   │   │   │   │   ├── (.cardinality): 0.00
                    │   │   │   │   │   │   ├── Select
                    │   │   │   │   │   │   │   ├── .predicate: "part.p_type"(#1.4) = 'ECONOMY ANODIZED STEEL'::utf8_view
                    │   │   │   │   │   │   │   ├── (.output_columns): [ "part.p_partkey"(#1.0), "part.p_type"(#1.4) ]
                    │   │   │   │   │   │   │   ├── (.cardinality): 0.00
                    │   │   │   │   │   │   │   └── Get
                    │   │   │   │   │   │   │       ├── .data_source_id: 3
                    │   │   │   │   │   │   │       ├── .table_index: 1
                    │   │   │   │   │   │   │       ├── .implementation: None
                    │   │   │   │   │   │   │       ├── (.output_columns): [ "part.p_partkey"(#1.0), "part.p_type"(#1.4) ]
                    │   │   │   │   │   │   │       └── (.cardinality): 0.00
                    │   │   │   │   │   │   └── Get
                    │   │   │   │   │   │       ├── .data_source_id: 4
                    │   │   │   │   │   │       ├── .table_index: 2
                    │   │   │   │   │   │       ├── .implementation: None
                    │   │   │   │   │   │       ├── (.output_columns): [ "supplier.s_nationkey"(#2.3), "supplier.s_suppkey"(#2.0) ]
                    │   │   │   │   │   │       └── (.cardinality): 0.00
                    │   │   │   │   │   └── Get
                    │   │   │   │   │       ├── .data_source_id: 8
                    │   │   │   │   │       ├── .table_index: 3
                    │   │   │   │   │       ├── .implementation: None
                    │   │   │   │   │       ├── (.output_columns):
                    │   │   │   │   │       │   ┌── "lineitem.l_discount"(#3.6)
                    │   │   │   │   │       │   ├── "lineitem.l_extendedprice"(#3.5)
                    │   │   │   │   │       │   ├── "lineitem.l_orderkey"(#3.0)
                    │   │   │   │   │       │   ├── "lineitem.l_partkey"(#3.1)
                    │   │   │   │   │       │   └── "lineitem.l_suppkey"(#3.2)
                    │   │   │   │   │       └── (.cardinality): 0.00
                    │   │   │   │   └── Select
                    │   │   │   │       ├── .predicate: ("orders.o_orderdate"(#4.4) >= 1995-01-01::date32) AND ("orders.o_orderdate"(#4.4) <= 1996-12-31::date32)
                    │   │   │   │       ├── (.output_columns): [ "orders.o_custkey"(#4.1), "orders.o_orderdate"(#4.4), "orders.o_orderkey"(#4.0) ]
                    │   │   │   │       ├── (.cardinality): 0.00
                    │   │   │   │       └── Get
                    │   │   │   │           ├── .data_source_id: 7
                    │   │   │   │           ├── .table_index: 4
                    │   │   │   │           ├── .implementation: None
                    │   │   │   │           ├── (.output_columns): [ "orders.o_custkey"(#4.1), "orders.o_orderdate"(#4.4), "orders.o_orderkey"(#4.0) ]
                    │   │   │   │           └── (.cardinality): 0.00
                    │   │   │   └── Get
                    │   │   │       ├── .data_source_id: 6
                    │   │   │       ├── .table_index: 5
                    │   │   │       ├── .implementation: None
                    │   │   │       ├── (.output_columns): [ "customer.c_custkey"(#5.0), "customer.c_nationkey"(#5.3) ]
                    │   │   │       └── (.cardinality): 0.00
                    │   │   └── Remap
                    │   │       ├── .table_index: 7
                    │   │       ├── (.output_columns): [ "n1.n_comment"(#7.3), "n1.n_name"(#7.1), "n1.n_nationkey"(#7.0), "n1.n_regionkey"(#7.2) ]
                    │   │       ├── (.cardinality): 0.00
                    │   │       └── Get
                    │   │           ├── .data_source_id: 1
                    │   │           ├── .table_index: 6
                    │   │           ├── .implementation: None
                    │   │           ├── (.output_columns): [ "nation.n_comment"(#6.3), "nation.n_name"(#6.1), "nation.n_nationkey"(#6.0), "nation.n_regionkey"(#6.2) ]
                    │   │           └── (.cardinality): 0.00
                    │   └── Remap
                    │       ├── .table_index: 9
                    │       ├── (.output_columns): [ "n2.n_comment"(#9.3), "n2.n_name"(#9.1), "n2.n_nationkey"(#9.0), "n2.n_regionkey"(#9.2) ]
                    │       ├── (.cardinality): 0.00
                    │       └── Get
                    │           ├── .data_source_id: 1
                    │           ├── .table_index: 8
                    │           ├── .implementation: None
                    │           ├── (.output_columns): [ "nation.n_comment"(#8.3), "nation.n_name"(#8.1), "nation.n_nationkey"(#8.0), "nation.n_regionkey"(#8.2) ]
                    │           └── (.cardinality): 0.00
                    └── Select
                        ├── .predicate: "region.r_name"(#10.1) = 'AMERICA'::utf8_view
                        ├── (.output_columns): [ "region.r_name"(#10.1), "region.r_regionkey"(#10.0) ]
                        ├── (.cardinality): 0.00
                        └── Get
                            ├── .data_source_id: 2
                            ├── .table_index: 10
                            ├── .implementation: None
                            ├── (.output_columns): [ "region.r_name"(#10.1), "region.r_regionkey"(#10.0) ]
                            └── (.cardinality): 0.00
*/

