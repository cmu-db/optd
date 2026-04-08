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
OrderBy { ordering_exprs: "__#16.o_year"(#16.0) ASC, (.output_columns): [ "__#16.mkt_share"(#16.1), "__#16.o_year"(#16.0) ], (.cardinality): 0.00 }
└── Project
    ├── .table_index: 16
    ├── .projections: [ "all_nations.o_year"(#13.0), "__#15.sum(CASE WHEN all_nations.nation = Utf8("IRAQ") THEN all_nations.volume ELSE Int64(0) END)"(#15.0) / "__#15.sum(all_nations.volume)"(#15.1) ]
    ├── (.output_columns): [ "__#16.mkt_share"(#16.1), "__#16.o_year"(#16.0) ]
    ├── (.cardinality): 0.00
    └── Aggregate
        ├── .key_table_index: 14
        ├── .aggregate_table_index: 15
        ├── .implementation: None
        ├── .exprs: [ sum(CASE WHEN "all_nations.nation"(#13.2) = 'IRAQ'::utf8_view THEN "all_nations.volume"(#13.1) ELSE 0::decimal128(38, 4) END), sum("all_nations.volume"(#13.1)) ]
        ├── .keys: "all_nations.o_year"(#13.0)
        ├── (.output_columns): [ "__#14.o_year"(#14.0), "__#15.sum(CASE WHEN all_nations.nation = Utf8("IRAQ") THEN all_nations.volume ELSE Int64(0) END)"(#15.0), "__#15.sum(all_nations.volume)"(#15.1) ]
        ├── (.cardinality): 0.00
        └── Remap { .table_index: 13, (.output_columns): [ "all_nations.nation"(#13.2), "all_nations.o_year"(#13.0), "all_nations.volume"(#13.1) ], (.cardinality): 0.00 }
            └── Project
                ├── .table_index: 12
                ├── .projections: [ date_part('YEAR'::utf8, "__#11.o_orderdate"(#11.36)), "__#11.l_extendedprice"(#11.21) * 1::decimal128(20, 0) - "__#11.l_discount"(#11.22), "__#11.n_name"(#11.50) ]
                ├── (.output_columns): [ "__#12.nation"(#12.2), "__#12.o_year"(#12.0), "__#12.volume"(#12.1) ]
                ├── (.cardinality): 0.00
                └── Select
                    ├── .predicate: ("__#11.r_name"(#11.58) = 'AMERICA'::utf8_view) AND (("__#11.o_orderdate"(#11.36) >= 1995-01-01::date32) AND ("__#11.o_orderdate"(#11.36) <= 1996-12-31::date32)) AND ("__#11.p_type"(#11.4) = 'ECONOMY ANODIZED STEEL'::utf8_view)
                    ├── (.output_columns):
                    │   ┌── "__#11.c_acctbal"(#11.46)
                    │   ├── "__#11.c_address"(#11.43)
                    │   ├── "__#11.c_comment"(#11.48)
                    │   ├── "__#11.c_custkey"(#11.41)
                    │   ├── "__#11.c_mktsegment"(#11.47)
                    │   ├── "__#11.c_name"(#11.42)
                    │   ├── "__#11.c_nationkey"(#11.44)
                    │   ├── "__#11.c_phone"(#11.45)
                    │   ├── "__#11.l_comment"(#11.31)
                    │   ├── "__#11.l_commitdate"(#11.27)
                    │   ├── "__#11.l_discount"(#11.22)
                    │   ├── "__#11.l_extendedprice"(#11.21)
                    │   ├── "__#11.l_linenumber"(#11.19)
                    │   ├── "__#11.l_linestatus"(#11.25)
                    │   ├── "__#11.l_orderkey"(#11.16)
                    │   ├── "__#11.l_partkey"(#11.17)
                    │   ├── "__#11.l_quantity"(#11.20)
                    │   ├── "__#11.l_receiptdate"(#11.28)
                    │   ├── "__#11.l_returnflag"(#11.24)
                    │   ├── "__#11.l_shipdate"(#11.26)
                    │   ├── "__#11.l_shipinstruct"(#11.29)
                    │   ├── "__#11.l_shipmode"(#11.30)
                    │   ├── "__#11.l_suppkey"(#11.18)
                    │   ├── "__#11.l_tax"(#11.23)
                    │   ├── "__#11.n_comment"(#11.52)
                    │   ├── "__#11.n_comment"(#11.56)
                    │   ├── "__#11.n_name"(#11.50)
                    │   ├── "__#11.n_name"(#11.54)
                    │   ├── "__#11.n_nationkey"(#11.49)
                    │   ├── "__#11.n_nationkey"(#11.53)
                    │   ├── "__#11.n_regionkey"(#11.51)
                    │   ├── "__#11.n_regionkey"(#11.55)
                    │   ├── "__#11.o_clerk"(#11.38)
                    │   ├── "__#11.o_comment"(#11.40)
                    │   ├── "__#11.o_custkey"(#11.33)
                    │   ├── "__#11.o_orderdate"(#11.36)
                    │   ├── "__#11.o_orderkey"(#11.32)
                    │   ├── "__#11.o_orderpriority"(#11.37)
                    │   ├── "__#11.o_orderstatus"(#11.34)
                    │   ├── "__#11.o_shippriority"(#11.39)
                    │   ├── "__#11.o_totalprice"(#11.35)
                    │   ├── "__#11.p_brand"(#11.3)
                    │   ├── "__#11.p_comment"(#11.8)
                    │   ├── "__#11.p_container"(#11.6)
                    │   ├── "__#11.p_mfgr"(#11.2)
                    │   ├── "__#11.p_name"(#11.1)
                    │   ├── "__#11.p_partkey"(#11.0)
                    │   ├── "__#11.p_retailprice"(#11.7)
                    │   ├── "__#11.p_size"(#11.5)
                    │   ├── "__#11.p_type"(#11.4)
                    │   ├── "__#11.r_comment"(#11.59)
                    │   ├── "__#11.r_name"(#11.58)
                    │   ├── "__#11.r_regionkey"(#11.57)
                    │   ├── "__#11.s_acctbal"(#11.14)
                    │   ├── "__#11.s_address"(#11.11)
                    │   ├── "__#11.s_comment"(#11.15)
                    │   ├── "__#11.s_name"(#11.10)
                    │   ├── "__#11.s_nationkey"(#11.12)
                    │   ├── "__#11.s_phone"(#11.13)
                    │   └── "__#11.s_suppkey"(#11.9)
                    ├── (.cardinality): 0.00
                    └── Project
                        ├── .table_index: 11
                        ├── .projections:
                        │   ┌── "part.p_partkey"(#1.0)
                        │   ├── "part.p_name"(#1.1)
                        │   ├── "part.p_mfgr"(#1.2)
                        │   ├── "part.p_brand"(#1.3)
                        │   ├── "part.p_type"(#1.4)
                        │   ├── "part.p_size"(#1.5)
                        │   ├── "part.p_container"(#1.6)
                        │   ├── "part.p_retailprice"(#1.7)
                        │   ├── "part.p_comment"(#1.8)
                        │   ├── "supplier.s_suppkey"(#3.0)
                        │   ├── "supplier.s_name"(#3.1)
                        │   ├── "supplier.s_address"(#3.2)
                        │   ├── "supplier.s_nationkey"(#3.3)
                        │   ├── "supplier.s_phone"(#3.4)
                        │   ├── "supplier.s_acctbal"(#3.5)
                        │   ├── "supplier.s_comment"(#3.6)
                        │   ├── "lineitem.l_orderkey"(#2.0)
                        │   ├── "lineitem.l_partkey"(#2.1)
                        │   ├── "lineitem.l_suppkey"(#2.2)
                        │   ├── "lineitem.l_linenumber"(#2.3)
                        │   ├── "lineitem.l_quantity"(#2.4)
                        │   ├── "lineitem.l_extendedprice"(#2.5)
                        │   ├── "lineitem.l_discount"(#2.6)
                        │   ├── "lineitem.l_tax"(#2.7)
                        │   ├── "lineitem.l_returnflag"(#2.8)
                        │   ├── "lineitem.l_linestatus"(#2.9)
                        │   ├── "lineitem.l_shipdate"(#2.10)
                        │   ├── "lineitem.l_commitdate"(#2.11)
                        │   ├── "lineitem.l_receiptdate"(#2.12)
                        │   ├── "lineitem.l_shipinstruct"(#2.13)
                        │   ├── "lineitem.l_shipmode"(#2.14)
                        │   ├── "lineitem.l_comment"(#2.15)
                        │   ├── "orders.o_orderkey"(#4.0)
                        │   ├── "orders.o_custkey"(#4.1)
                        │   ├── "orders.o_orderstatus"(#4.2)
                        │   ├── "orders.o_totalprice"(#4.3)
                        │   ├── "orders.o_orderdate"(#4.4)
                        │   ├── "orders.o_orderpriority"(#4.5)
                        │   ├── "orders.o_clerk"(#4.6)
                        │   ├── "orders.o_shippriority"(#4.7)
                        │   ├── "orders.o_comment"(#4.8)
                        │   ├── "customer.c_custkey"(#5.0)
                        │   ├── "customer.c_name"(#5.1)
                        │   ├── "customer.c_address"(#5.2)
                        │   ├── "customer.c_nationkey"(#5.3)
                        │   ├── "customer.c_phone"(#5.4)
                        │   ├── "customer.c_acctbal"(#5.5)
                        │   ├── "customer.c_mktsegment"(#5.6)
                        │   ├── "customer.c_comment"(#5.7)
                        │   ├── "n1.n_nationkey"(#7.0)
                        │   ├── "n1.n_name"(#7.1)
                        │   ├── "n1.n_regionkey"(#7.2)
                        │   ├── "n1.n_comment"(#7.3)
                        │   ├── "n2.n_nationkey"(#9.0)
                        │   ├── "n2.n_name"(#9.1)
                        │   ├── "n2.n_regionkey"(#9.2)
                        │   ├── "n2.n_comment"(#9.3)
                        │   ├── "region.r_regionkey"(#10.0)
                        │   ├── "region.r_name"(#10.1)
                        │   └── "region.r_comment"(#10.2)
                        ├── (.output_columns):
                        │   ┌── "__#11.c_acctbal"(#11.46)
                        │   ├── "__#11.c_address"(#11.43)
                        │   ├── "__#11.c_comment"(#11.48)
                        │   ├── "__#11.c_custkey"(#11.41)
                        │   ├── "__#11.c_mktsegment"(#11.47)
                        │   ├── "__#11.c_name"(#11.42)
                        │   ├── "__#11.c_nationkey"(#11.44)
                        │   ├── "__#11.c_phone"(#11.45)
                        │   ├── "__#11.l_comment"(#11.31)
                        │   ├── "__#11.l_commitdate"(#11.27)
                        │   ├── "__#11.l_discount"(#11.22)
                        │   ├── "__#11.l_extendedprice"(#11.21)
                        │   ├── "__#11.l_linenumber"(#11.19)
                        │   ├── "__#11.l_linestatus"(#11.25)
                        │   ├── "__#11.l_orderkey"(#11.16)
                        │   ├── "__#11.l_partkey"(#11.17)
                        │   ├── "__#11.l_quantity"(#11.20)
                        │   ├── "__#11.l_receiptdate"(#11.28)
                        │   ├── "__#11.l_returnflag"(#11.24)
                        │   ├── "__#11.l_shipdate"(#11.26)
                        │   ├── "__#11.l_shipinstruct"(#11.29)
                        │   ├── "__#11.l_shipmode"(#11.30)
                        │   ├── "__#11.l_suppkey"(#11.18)
                        │   ├── "__#11.l_tax"(#11.23)
                        │   ├── "__#11.n_comment"(#11.52)
                        │   ├── "__#11.n_comment"(#11.56)
                        │   ├── "__#11.n_name"(#11.50)
                        │   ├── "__#11.n_name"(#11.54)
                        │   ├── "__#11.n_nationkey"(#11.49)
                        │   ├── "__#11.n_nationkey"(#11.53)
                        │   ├── "__#11.n_regionkey"(#11.51)
                        │   ├── "__#11.n_regionkey"(#11.55)
                        │   ├── "__#11.o_clerk"(#11.38)
                        │   ├── "__#11.o_comment"(#11.40)
                        │   ├── "__#11.o_custkey"(#11.33)
                        │   ├── "__#11.o_orderdate"(#11.36)
                        │   ├── "__#11.o_orderkey"(#11.32)
                        │   ├── "__#11.o_orderpriority"(#11.37)
                        │   ├── "__#11.o_orderstatus"(#11.34)
                        │   ├── "__#11.o_shippriority"(#11.39)
                        │   ├── "__#11.o_totalprice"(#11.35)
                        │   ├── "__#11.p_brand"(#11.3)
                        │   ├── "__#11.p_comment"(#11.8)
                        │   ├── "__#11.p_container"(#11.6)
                        │   ├── "__#11.p_mfgr"(#11.2)
                        │   ├── "__#11.p_name"(#11.1)
                        │   ├── "__#11.p_partkey"(#11.0)
                        │   ├── "__#11.p_retailprice"(#11.7)
                        │   ├── "__#11.p_size"(#11.5)
                        │   ├── "__#11.p_type"(#11.4)
                        │   ├── "__#11.r_comment"(#11.59)
                        │   ├── "__#11.r_name"(#11.58)
                        │   ├── "__#11.r_regionkey"(#11.57)
                        │   ├── "__#11.s_acctbal"(#11.14)
                        │   ├── "__#11.s_address"(#11.11)
                        │   ├── "__#11.s_comment"(#11.15)
                        │   ├── "__#11.s_name"(#11.10)
                        │   ├── "__#11.s_nationkey"(#11.12)
                        │   ├── "__#11.s_phone"(#11.13)
                        │   └── "__#11.s_suppkey"(#11.9)
                        ├── (.cardinality): 0.00
                        └── Join
                            ├── .join_type: Inner
                            ├── .implementation: None
                            ├── .join_cond: ("n1.n_regionkey"(#7.2) = "region.r_regionkey"(#10.0))
                            ├── (.output_columns):
                            │   ┌── "customer.c_acctbal"(#5.5)
                            │   ├── "customer.c_address"(#5.2)
                            │   ├── "customer.c_comment"(#5.7)
                            │   ├── "customer.c_custkey"(#5.0)
                            │   ├── "customer.c_mktsegment"(#5.6)
                            │   ├── "customer.c_name"(#5.1)
                            │   ├── "customer.c_nationkey"(#5.3)
                            │   ├── "customer.c_phone"(#5.4)
                            │   ├── "lineitem.l_comment"(#2.15)
                            │   ├── "lineitem.l_commitdate"(#2.11)
                            │   ├── "lineitem.l_discount"(#2.6)
                            │   ├── "lineitem.l_extendedprice"(#2.5)
                            │   ├── "lineitem.l_linenumber"(#2.3)
                            │   ├── "lineitem.l_linestatus"(#2.9)
                            │   ├── "lineitem.l_orderkey"(#2.0)
                            │   ├── "lineitem.l_partkey"(#2.1)
                            │   ├── "lineitem.l_quantity"(#2.4)
                            │   ├── "lineitem.l_receiptdate"(#2.12)
                            │   ├── "lineitem.l_returnflag"(#2.8)
                            │   ├── "lineitem.l_shipdate"(#2.10)
                            │   ├── "lineitem.l_shipinstruct"(#2.13)
                            │   ├── "lineitem.l_shipmode"(#2.14)
                            │   ├── "lineitem.l_suppkey"(#2.2)
                            │   ├── "lineitem.l_tax"(#2.7)
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
                            │   ├── "supplier.s_acctbal"(#3.5)
                            │   ├── "supplier.s_address"(#3.2)
                            │   ├── "supplier.s_comment"(#3.6)
                            │   ├── "supplier.s_name"(#3.1)
                            │   ├── "supplier.s_nationkey"(#3.3)
                            │   ├── "supplier.s_phone"(#3.4)
                            │   └── "supplier.s_suppkey"(#3.0)
                            ├── (.cardinality): 0.00
                            ├── Join
                            │   ├── .join_type: Inner
                            │   ├── .implementation: None
                            │   ├── .join_cond: ("supplier.s_nationkey"(#3.3) = "n2.n_nationkey"(#9.0))
                            │   ├── (.output_columns):
                            │   │   ┌── "customer.c_acctbal"(#5.5)
                            │   │   ├── "customer.c_address"(#5.2)
                            │   │   ├── "customer.c_comment"(#5.7)
                            │   │   ├── "customer.c_custkey"(#5.0)
                            │   │   ├── "customer.c_mktsegment"(#5.6)
                            │   │   ├── "customer.c_name"(#5.1)
                            │   │   ├── "customer.c_nationkey"(#5.3)
                            │   │   ├── "customer.c_phone"(#5.4)
                            │   │   ├── "lineitem.l_comment"(#2.15)
                            │   │   ├── "lineitem.l_commitdate"(#2.11)
                            │   │   ├── "lineitem.l_discount"(#2.6)
                            │   │   ├── "lineitem.l_extendedprice"(#2.5)
                            │   │   ├── "lineitem.l_linenumber"(#2.3)
                            │   │   ├── "lineitem.l_linestatus"(#2.9)
                            │   │   ├── "lineitem.l_orderkey"(#2.0)
                            │   │   ├── "lineitem.l_partkey"(#2.1)
                            │   │   ├── "lineitem.l_quantity"(#2.4)
                            │   │   ├── "lineitem.l_receiptdate"(#2.12)
                            │   │   ├── "lineitem.l_returnflag"(#2.8)
                            │   │   ├── "lineitem.l_shipdate"(#2.10)
                            │   │   ├── "lineitem.l_shipinstruct"(#2.13)
                            │   │   ├── "lineitem.l_shipmode"(#2.14)
                            │   │   ├── "lineitem.l_suppkey"(#2.2)
                            │   │   ├── "lineitem.l_tax"(#2.7)
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
                            │   │   ├── "supplier.s_acctbal"(#3.5)
                            │   │   ├── "supplier.s_address"(#3.2)
                            │   │   ├── "supplier.s_comment"(#3.6)
                            │   │   ├── "supplier.s_name"(#3.1)
                            │   │   ├── "supplier.s_nationkey"(#3.3)
                            │   │   ├── "supplier.s_phone"(#3.4)
                            │   │   └── "supplier.s_suppkey"(#3.0)
                            │   ├── (.cardinality): 0.00
                            │   ├── Join
                            │   │   ├── .join_type: Inner
                            │   │   ├── .implementation: None
                            │   │   ├── .join_cond: ("customer.c_nationkey"(#5.3) = "n1.n_nationkey"(#7.0))
                            │   │   ├── (.output_columns):
                            │   │   │   ┌── "customer.c_acctbal"(#5.5)
                            │   │   │   ├── "customer.c_address"(#5.2)
                            │   │   │   ├── "customer.c_comment"(#5.7)
                            │   │   │   ├── "customer.c_custkey"(#5.0)
                            │   │   │   ├── "customer.c_mktsegment"(#5.6)
                            │   │   │   ├── "customer.c_name"(#5.1)
                            │   │   │   ├── "customer.c_nationkey"(#5.3)
                            │   │   │   ├── "customer.c_phone"(#5.4)
                            │   │   │   ├── "lineitem.l_comment"(#2.15)
                            │   │   │   ├── "lineitem.l_commitdate"(#2.11)
                            │   │   │   ├── "lineitem.l_discount"(#2.6)
                            │   │   │   ├── "lineitem.l_extendedprice"(#2.5)
                            │   │   │   ├── "lineitem.l_linenumber"(#2.3)
                            │   │   │   ├── "lineitem.l_linestatus"(#2.9)
                            │   │   │   ├── "lineitem.l_orderkey"(#2.0)
                            │   │   │   ├── "lineitem.l_partkey"(#2.1)
                            │   │   │   ├── "lineitem.l_quantity"(#2.4)
                            │   │   │   ├── "lineitem.l_receiptdate"(#2.12)
                            │   │   │   ├── "lineitem.l_returnflag"(#2.8)
                            │   │   │   ├── "lineitem.l_shipdate"(#2.10)
                            │   │   │   ├── "lineitem.l_shipinstruct"(#2.13)
                            │   │   │   ├── "lineitem.l_shipmode"(#2.14)
                            │   │   │   ├── "lineitem.l_suppkey"(#2.2)
                            │   │   │   ├── "lineitem.l_tax"(#2.7)
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
                            │   │   │   ├── "supplier.s_acctbal"(#3.5)
                            │   │   │   ├── "supplier.s_address"(#3.2)
                            │   │   │   ├── "supplier.s_comment"(#3.6)
                            │   │   │   ├── "supplier.s_name"(#3.1)
                            │   │   │   ├── "supplier.s_nationkey"(#3.3)
                            │   │   │   ├── "supplier.s_phone"(#3.4)
                            │   │   │   └── "supplier.s_suppkey"(#3.0)
                            │   │   ├── (.cardinality): 0.00
                            │   │   ├── Join
                            │   │   │   ├── .join_type: Inner
                            │   │   │   ├── .implementation: None
                            │   │   │   ├── .join_cond: ("orders.o_custkey"(#4.1) = "customer.c_custkey"(#5.0))
                            │   │   │   ├── (.output_columns):
                            │   │   │   │   ┌── "customer.c_acctbal"(#5.5)
                            │   │   │   │   ├── "customer.c_address"(#5.2)
                            │   │   │   │   ├── "customer.c_comment"(#5.7)
                            │   │   │   │   ├── "customer.c_custkey"(#5.0)
                            │   │   │   │   ├── "customer.c_mktsegment"(#5.6)
                            │   │   │   │   ├── "customer.c_name"(#5.1)
                            │   │   │   │   ├── "customer.c_nationkey"(#5.3)
                            │   │   │   │   ├── "customer.c_phone"(#5.4)
                            │   │   │   │   ├── "lineitem.l_comment"(#2.15)
                            │   │   │   │   ├── "lineitem.l_commitdate"(#2.11)
                            │   │   │   │   ├── "lineitem.l_discount"(#2.6)
                            │   │   │   │   ├── "lineitem.l_extendedprice"(#2.5)
                            │   │   │   │   ├── "lineitem.l_linenumber"(#2.3)
                            │   │   │   │   ├── "lineitem.l_linestatus"(#2.9)
                            │   │   │   │   ├── "lineitem.l_orderkey"(#2.0)
                            │   │   │   │   ├── "lineitem.l_partkey"(#2.1)
                            │   │   │   │   ├── "lineitem.l_quantity"(#2.4)
                            │   │   │   │   ├── "lineitem.l_receiptdate"(#2.12)
                            │   │   │   │   ├── "lineitem.l_returnflag"(#2.8)
                            │   │   │   │   ├── "lineitem.l_shipdate"(#2.10)
                            │   │   │   │   ├── "lineitem.l_shipinstruct"(#2.13)
                            │   │   │   │   ├── "lineitem.l_shipmode"(#2.14)
                            │   │   │   │   ├── "lineitem.l_suppkey"(#2.2)
                            │   │   │   │   ├── "lineitem.l_tax"(#2.7)
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
                            │   │   │   │   ├── "supplier.s_acctbal"(#3.5)
                            │   │   │   │   ├── "supplier.s_address"(#3.2)
                            │   │   │   │   ├── "supplier.s_comment"(#3.6)
                            │   │   │   │   ├── "supplier.s_name"(#3.1)
                            │   │   │   │   ├── "supplier.s_nationkey"(#3.3)
                            │   │   │   │   ├── "supplier.s_phone"(#3.4)
                            │   │   │   │   └── "supplier.s_suppkey"(#3.0)
                            │   │   │   ├── (.cardinality): 0.00
                            │   │   │   ├── Join
                            │   │   │   │   ├── .join_type: Inner
                            │   │   │   │   ├── .implementation: None
                            │   │   │   │   ├── .join_cond: ("lineitem.l_orderkey"(#2.0) = "orders.o_orderkey"(#4.0))
                            │   │   │   │   ├── (.output_columns):
                            │   │   │   │   │   ┌── "lineitem.l_comment"(#2.15)
                            │   │   │   │   │   ├── "lineitem.l_commitdate"(#2.11)
                            │   │   │   │   │   ├── "lineitem.l_discount"(#2.6)
                            │   │   │   │   │   ├── "lineitem.l_extendedprice"(#2.5)
                            │   │   │   │   │   ├── "lineitem.l_linenumber"(#2.3)
                            │   │   │   │   │   ├── "lineitem.l_linestatus"(#2.9)
                            │   │   │   │   │   ├── "lineitem.l_orderkey"(#2.0)
                            │   │   │   │   │   ├── "lineitem.l_partkey"(#2.1)
                            │   │   │   │   │   ├── "lineitem.l_quantity"(#2.4)
                            │   │   │   │   │   ├── "lineitem.l_receiptdate"(#2.12)
                            │   │   │   │   │   ├── "lineitem.l_returnflag"(#2.8)
                            │   │   │   │   │   ├── "lineitem.l_shipdate"(#2.10)
                            │   │   │   │   │   ├── "lineitem.l_shipinstruct"(#2.13)
                            │   │   │   │   │   ├── "lineitem.l_shipmode"(#2.14)
                            │   │   │   │   │   ├── "lineitem.l_suppkey"(#2.2)
                            │   │   │   │   │   ├── "lineitem.l_tax"(#2.7)
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
                            │   │   │   │   │   ├── "supplier.s_acctbal"(#3.5)
                            │   │   │   │   │   ├── "supplier.s_address"(#3.2)
                            │   │   │   │   │   ├── "supplier.s_comment"(#3.6)
                            │   │   │   │   │   ├── "supplier.s_name"(#3.1)
                            │   │   │   │   │   ├── "supplier.s_nationkey"(#3.3)
                            │   │   │   │   │   ├── "supplier.s_phone"(#3.4)
                            │   │   │   │   │   └── "supplier.s_suppkey"(#3.0)
                            │   │   │   │   ├── (.cardinality): 0.00
                            │   │   │   │   ├── Join
                            │   │   │   │   │   ├── .join_type: Inner
                            │   │   │   │   │   ├── .implementation: None
                            │   │   │   │   │   ├── .join_cond: ("lineitem.l_suppkey"(#2.2) = "supplier.s_suppkey"(#3.0))
                            │   │   │   │   │   ├── (.output_columns):
                            │   │   │   │   │   │   ┌── "lineitem.l_comment"(#2.15)
                            │   │   │   │   │   │   ├── "lineitem.l_commitdate"(#2.11)
                            │   │   │   │   │   │   ├── "lineitem.l_discount"(#2.6)
                            │   │   │   │   │   │   ├── "lineitem.l_extendedprice"(#2.5)
                            │   │   │   │   │   │   ├── "lineitem.l_linenumber"(#2.3)
                            │   │   │   │   │   │   ├── "lineitem.l_linestatus"(#2.9)
                            │   │   │   │   │   │   ├── "lineitem.l_orderkey"(#2.0)
                            │   │   │   │   │   │   ├── "lineitem.l_partkey"(#2.1)
                            │   │   │   │   │   │   ├── "lineitem.l_quantity"(#2.4)
                            │   │   │   │   │   │   ├── "lineitem.l_receiptdate"(#2.12)
                            │   │   │   │   │   │   ├── "lineitem.l_returnflag"(#2.8)
                            │   │   │   │   │   │   ├── "lineitem.l_shipdate"(#2.10)
                            │   │   │   │   │   │   ├── "lineitem.l_shipinstruct"(#2.13)
                            │   │   │   │   │   │   ├── "lineitem.l_shipmode"(#2.14)
                            │   │   │   │   │   │   ├── "lineitem.l_suppkey"(#2.2)
                            │   │   │   │   │   │   ├── "lineitem.l_tax"(#2.7)
                            │   │   │   │   │   │   ├── "part.p_brand"(#1.3)
                            │   │   │   │   │   │   ├── "part.p_comment"(#1.8)
                            │   │   │   │   │   │   ├── "part.p_container"(#1.6)
                            │   │   │   │   │   │   ├── "part.p_mfgr"(#1.2)
                            │   │   │   │   │   │   ├── "part.p_name"(#1.1)
                            │   │   │   │   │   │   ├── "part.p_partkey"(#1.0)
                            │   │   │   │   │   │   ├── "part.p_retailprice"(#1.7)
                            │   │   │   │   │   │   ├── "part.p_size"(#1.5)
                            │   │   │   │   │   │   ├── "part.p_type"(#1.4)
                            │   │   │   │   │   │   ├── "supplier.s_acctbal"(#3.5)
                            │   │   │   │   │   │   ├── "supplier.s_address"(#3.2)
                            │   │   │   │   │   │   ├── "supplier.s_comment"(#3.6)
                            │   │   │   │   │   │   ├── "supplier.s_name"(#3.1)
                            │   │   │   │   │   │   ├── "supplier.s_nationkey"(#3.3)
                            │   │   │   │   │   │   ├── "supplier.s_phone"(#3.4)
                            │   │   │   │   │   │   └── "supplier.s_suppkey"(#3.0)
                            │   │   │   │   │   ├── (.cardinality): 0.00
                            │   │   │   │   │   ├── Join
                            │   │   │   │   │   │   ├── .join_type: Inner
                            │   │   │   │   │   │   ├── .implementation: None
                            │   │   │   │   │   │   ├── .join_cond: ("part.p_partkey"(#1.0) = "lineitem.l_partkey"(#2.1))
                            │   │   │   │   │   │   ├── (.output_columns):
                            │   │   │   │   │   │   │   ┌── "lineitem.l_comment"(#2.15)
                            │   │   │   │   │   │   │   ├── "lineitem.l_commitdate"(#2.11)
                            │   │   │   │   │   │   │   ├── "lineitem.l_discount"(#2.6)
                            │   │   │   │   │   │   │   ├── "lineitem.l_extendedprice"(#2.5)
                            │   │   │   │   │   │   │   ├── "lineitem.l_linenumber"(#2.3)
                            │   │   │   │   │   │   │   ├── "lineitem.l_linestatus"(#2.9)
                            │   │   │   │   │   │   │   ├── "lineitem.l_orderkey"(#2.0)
                            │   │   │   │   │   │   │   ├── "lineitem.l_partkey"(#2.1)
                            │   │   │   │   │   │   │   ├── "lineitem.l_quantity"(#2.4)
                            │   │   │   │   │   │   │   ├── "lineitem.l_receiptdate"(#2.12)
                            │   │   │   │   │   │   │   ├── "lineitem.l_returnflag"(#2.8)
                            │   │   │   │   │   │   │   ├── "lineitem.l_shipdate"(#2.10)
                            │   │   │   │   │   │   │   ├── "lineitem.l_shipinstruct"(#2.13)
                            │   │   │   │   │   │   │   ├── "lineitem.l_shipmode"(#2.14)
                            │   │   │   │   │   │   │   ├── "lineitem.l_suppkey"(#2.2)
                            │   │   │   │   │   │   │   ├── "lineitem.l_tax"(#2.7)
                            │   │   │   │   │   │   │   ├── "part.p_brand"(#1.3)
                            │   │   │   │   │   │   │   ├── "part.p_comment"(#1.8)
                            │   │   │   │   │   │   │   ├── "part.p_container"(#1.6)
                            │   │   │   │   │   │   │   ├── "part.p_mfgr"(#1.2)
                            │   │   │   │   │   │   │   ├── "part.p_name"(#1.1)
                            │   │   │   │   │   │   │   ├── "part.p_partkey"(#1.0)
                            │   │   │   │   │   │   │   ├── "part.p_retailprice"(#1.7)
                            │   │   │   │   │   │   │   ├── "part.p_size"(#1.5)
                            │   │   │   │   │   │   │   └── "part.p_type"(#1.4)
                            │   │   │   │   │   │   ├── (.cardinality): 0.00
                            │   │   │   │   │   │   ├── Get
                            │   │   │   │   │   │   │   ├── .data_source_id: 3
                            │   │   │   │   │   │   │   ├── .table_index: 1
                            │   │   │   │   │   │   │   ├── .implementation: None
                            │   │   │   │   │   │   │   ├── (.output_columns):
                            │   │   │   │   │   │   │   │   ┌── "part.p_brand"(#1.3)
                            │   │   │   │   │   │   │   │   ├── "part.p_comment"(#1.8)
                            │   │   │   │   │   │   │   │   ├── "part.p_container"(#1.6)
                            │   │   │   │   │   │   │   │   ├── "part.p_mfgr"(#1.2)
                            │   │   │   │   │   │   │   │   ├── "part.p_name"(#1.1)
                            │   │   │   │   │   │   │   │   ├── "part.p_partkey"(#1.0)
                            │   │   │   │   │   │   │   │   ├── "part.p_retailprice"(#1.7)
                            │   │   │   │   │   │   │   │   ├── "part.p_size"(#1.5)
                            │   │   │   │   │   │   │   │   └── "part.p_type"(#1.4)
                            │   │   │   │   │   │   │   └── (.cardinality): 0.00
                            │   │   │   │   │   │   └── Get
                            │   │   │   │   │   │       ├── .data_source_id: 8
                            │   │   │   │   │   │       ├── .table_index: 2
                            │   │   │   │   │   │       ├── .implementation: None
                            │   │   │   │   │   │       ├── (.output_columns):
                            │   │   │   │   │   │       │   ┌── "lineitem.l_comment"(#2.15)
                            │   │   │   │   │   │       │   ├── "lineitem.l_commitdate"(#2.11)
                            │   │   │   │   │   │       │   ├── "lineitem.l_discount"(#2.6)
                            │   │   │   │   │   │       │   ├── "lineitem.l_extendedprice"(#2.5)
                            │   │   │   │   │   │       │   ├── "lineitem.l_linenumber"(#2.3)
                            │   │   │   │   │   │       │   ├── "lineitem.l_linestatus"(#2.9)
                            │   │   │   │   │   │       │   ├── "lineitem.l_orderkey"(#2.0)
                            │   │   │   │   │   │       │   ├── "lineitem.l_partkey"(#2.1)
                            │   │   │   │   │   │       │   ├── "lineitem.l_quantity"(#2.4)
                            │   │   │   │   │   │       │   ├── "lineitem.l_receiptdate"(#2.12)
                            │   │   │   │   │   │       │   ├── "lineitem.l_returnflag"(#2.8)
                            │   │   │   │   │   │       │   ├── "lineitem.l_shipdate"(#2.10)
                            │   │   │   │   │   │       │   ├── "lineitem.l_shipinstruct"(#2.13)
                            │   │   │   │   │   │       │   ├── "lineitem.l_shipmode"(#2.14)
                            │   │   │   │   │   │       │   ├── "lineitem.l_suppkey"(#2.2)
                            │   │   │   │   │   │       │   └── "lineitem.l_tax"(#2.7)
                            │   │   │   │   │   │       └── (.cardinality): 0.00
                            │   │   │   │   │   └── Get
                            │   │   │   │   │       ├── .data_source_id: 4
                            │   │   │   │   │       ├── .table_index: 3
                            │   │   │   │   │       ├── .implementation: None
                            │   │   │   │   │       ├── (.output_columns):
                            │   │   │   │   │       │   ┌── "supplier.s_acctbal"(#3.5)
                            │   │   │   │   │       │   ├── "supplier.s_address"(#3.2)
                            │   │   │   │   │       │   ├── "supplier.s_comment"(#3.6)
                            │   │   │   │   │       │   ├── "supplier.s_name"(#3.1)
                            │   │   │   │   │       │   ├── "supplier.s_nationkey"(#3.3)
                            │   │   │   │   │       │   ├── "supplier.s_phone"(#3.4)
                            │   │   │   │   │       │   └── "supplier.s_suppkey"(#3.0)
                            │   │   │   │   │       └── (.cardinality): 0.00
                            │   │   │   │   └── Get
                            │   │   │   │       ├── .data_source_id: 7
                            │   │   │   │       ├── .table_index: 4
                            │   │   │   │       ├── .implementation: None
                            │   │   │   │       ├── (.output_columns):
                            │   │   │   │       │   ┌── "orders.o_clerk"(#4.6)
                            │   │   │   │       │   ├── "orders.o_comment"(#4.8)
                            │   │   │   │       │   ├── "orders.o_custkey"(#4.1)
                            │   │   │   │       │   ├── "orders.o_orderdate"(#4.4)
                            │   │   │   │       │   ├── "orders.o_orderkey"(#4.0)
                            │   │   │   │       │   ├── "orders.o_orderpriority"(#4.5)
                            │   │   │   │       │   ├── "orders.o_orderstatus"(#4.2)
                            │   │   │   │       │   ├── "orders.o_shippriority"(#4.7)
                            │   │   │   │       │   └── "orders.o_totalprice"(#4.3)
                            │   │   │   │       └── (.cardinality): 0.00
                            │   │   │   └── Get
                            │   │   │       ├── .data_source_id: 6
                            │   │   │       ├── .table_index: 5
                            │   │   │       ├── .implementation: None
                            │   │   │       ├── (.output_columns):
                            │   │   │       │   ┌── "customer.c_acctbal"(#5.5)
                            │   │   │       │   ├── "customer.c_address"(#5.2)
                            │   │   │       │   ├── "customer.c_comment"(#5.7)
                            │   │   │       │   ├── "customer.c_custkey"(#5.0)
                            │   │   │       │   ├── "customer.c_mktsegment"(#5.6)
                            │   │   │       │   ├── "customer.c_name"(#5.1)
                            │   │   │       │   ├── "customer.c_nationkey"(#5.3)
                            │   │   │       │   └── "customer.c_phone"(#5.4)
                            │   │   │       └── (.cardinality): 0.00
                            │   │   └── Remap { .table_index: 7, (.output_columns): [ "n1.n_comment"(#7.3), "n1.n_name"(#7.1), "n1.n_nationkey"(#7.0), "n1.n_regionkey"(#7.2) ], (.cardinality): 0.00 }
                            │   │       └── Get { .data_source_id: 1, .table_index: 6, .implementation: None, (.output_columns): [ "nation.n_comment"(#6.3), "nation.n_name"(#6.1), "nation.n_nationkey"(#6.0), "nation.n_regionkey"(#6.2) ], (.cardinality): 0.00 }
                            │   └── Remap { .table_index: 9, (.output_columns): [ "n2.n_comment"(#9.3), "n2.n_name"(#9.1), "n2.n_nationkey"(#9.0), "n2.n_regionkey"(#9.2) ], (.cardinality): 0.00 }
                            │       └── Get { .data_source_id: 1, .table_index: 8, .implementation: None, (.output_columns): [ "nation.n_comment"(#8.3), "nation.n_name"(#8.1), "nation.n_nationkey"(#8.0), "nation.n_regionkey"(#8.2) ], (.cardinality): 0.00 }
                            └── Get { .data_source_id: 2, .table_index: 10, .implementation: None, (.output_columns): [ "region.r_comment"(#10.2), "region.r_name"(#10.1), "region.r_regionkey"(#10.0) ], (.cardinality): 0.00 }

physical_plan after optd-finalized:
EnforcerSort { tuple_ordering: [(#16.0, Asc)], (.output_columns): [ "__#16.mkt_share"(#16.1), "__#16.o_year"(#16.0) ], (.cardinality): 0.00 }
└── Project
    ├── .table_index: 16
    ├── .projections:
    │   ┌── "all_nations.o_year"(#13.0)
    │   └── "__#15.sum(CASE WHEN all_nations.nation = Utf8("IRAQ") THEN all_nations.volume ELSE Int64(0) END)"(#15.0) / "__#15.sum(all_nations.volume)"(#15.1)
    ├── (.output_columns): [ "__#16.mkt_share"(#16.1), "__#16.o_year"(#16.0) ]
    ├── (.cardinality): 0.00
    └── Aggregate
        ├── .key_table_index: 14
        ├── .aggregate_table_index: 15
        ├── .implementation: None
        ├── .exprs:
        │   ┌── sum(CASE WHEN "all_nations.nation"(#13.2) = 'IRAQ'::utf8_view THEN "all_nations.volume"(#13.1) ELSE 0::decimal128(38, 4) END)
        │   └── sum("all_nations.volume"(#13.1))
        ├── .keys: "all_nations.o_year"(#13.0)
        ├── (.output_columns):
        │   ┌── "__#14.o_year"(#14.0)
        │   ├── "__#15.sum(CASE WHEN all_nations.nation = Utf8("IRAQ") THEN all_nations.volume ELSE Int64(0) END)"(#15.0)
        │   └── "__#15.sum(all_nations.volume)"(#15.1)
        ├── (.cardinality): 0.00
        └── Remap
            ├── .table_index: 13
            ├── (.output_columns): [ "all_nations.nation"(#13.2), "all_nations.o_year"(#13.0), "all_nations.volume"(#13.1) ]
            ├── (.cardinality): 0.00
            └── Project
                ├── .table_index: 12
                ├── .projections:
                │   ┌── date_part('YEAR'::utf8, "orders.o_orderdate"(#4.4))
                │   ├── "lineitem.l_extendedprice"(#2.5) * 1::decimal128(20, 0) - "lineitem.l_discount"(#2.6)
                │   └── "n1.n_name"(#7.1)
                ├── (.output_columns): [ "__#12.nation"(#12.2), "__#12.o_year"(#12.0), "__#12.volume"(#12.1) ]
                ├── (.cardinality): 0.00
                └── Join
                    ├── .join_type: Inner
                    ├── .implementation: None
                    ├── .join_cond: "n1.n_regionkey"(#7.2) = "region.r_regionkey"(#10.0)
                    ├── (.output_columns):
                    │   ┌── "customer.c_custkey"(#5.0)
                    │   ├── "customer.c_nationkey"(#5.3)
                    │   ├── "lineitem.l_discount"(#2.6)
                    │   ├── "lineitem.l_extendedprice"(#2.5)
                    │   ├── "lineitem.l_orderkey"(#2.0)
                    │   ├── "lineitem.l_partkey"(#2.1)
                    │   ├── "lineitem.l_suppkey"(#2.2)
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
                    │   ├── "supplier.s_nationkey"(#3.3)
                    │   └── "supplier.s_suppkey"(#3.0)
                    ├── (.cardinality): 0.00
                    ├── Join
                    │   ├── .join_type: Inner
                    │   ├── .implementation: None
                    │   ├── .join_cond: "supplier.s_nationkey"(#3.3) = "n2.n_nationkey"(#9.0)
                    │   ├── (.output_columns):
                    │   │   ┌── "customer.c_custkey"(#5.0)
                    │   │   ├── "customer.c_nationkey"(#5.3)
                    │   │   ├── "lineitem.l_discount"(#2.6)
                    │   │   ├── "lineitem.l_extendedprice"(#2.5)
                    │   │   ├── "lineitem.l_orderkey"(#2.0)
                    │   │   ├── "lineitem.l_partkey"(#2.1)
                    │   │   ├── "lineitem.l_suppkey"(#2.2)
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
                    │   │   ├── "supplier.s_nationkey"(#3.3)
                    │   │   └── "supplier.s_suppkey"(#3.0)
                    │   ├── (.cardinality): 0.00
                    │   ├── Join
                    │   │   ├── .join_type: Inner
                    │   │   ├── .implementation: None
                    │   │   ├── .join_cond: "customer.c_nationkey"(#5.3) = "n1.n_nationkey"(#7.0)
                    │   │   ├── (.output_columns):
                    │   │   │   ┌── "customer.c_custkey"(#5.0)
                    │   │   │   ├── "customer.c_nationkey"(#5.3)
                    │   │   │   ├── "lineitem.l_discount"(#2.6)
                    │   │   │   ├── "lineitem.l_extendedprice"(#2.5)
                    │   │   │   ├── "lineitem.l_orderkey"(#2.0)
                    │   │   │   ├── "lineitem.l_partkey"(#2.1)
                    │   │   │   ├── "lineitem.l_suppkey"(#2.2)
                    │   │   │   ├── "n1.n_comment"(#7.3)
                    │   │   │   ├── "n1.n_name"(#7.1)
                    │   │   │   ├── "n1.n_nationkey"(#7.0)
                    │   │   │   ├── "n1.n_regionkey"(#7.2)
                    │   │   │   ├── "orders.o_custkey"(#4.1)
                    │   │   │   ├── "orders.o_orderdate"(#4.4)
                    │   │   │   ├── "orders.o_orderkey"(#4.0)
                    │   │   │   ├── "part.p_partkey"(#1.0)
                    │   │   │   ├── "part.p_type"(#1.4)
                    │   │   │   ├── "supplier.s_nationkey"(#3.3)
                    │   │   │   └── "supplier.s_suppkey"(#3.0)
                    │   │   ├── (.cardinality): 0.00
                    │   │   ├── Join
                    │   │   │   ├── .join_type: Inner
                    │   │   │   ├── .implementation: None
                    │   │   │   ├── .join_cond: "orders.o_custkey"(#4.1) = "customer.c_custkey"(#5.0)
                    │   │   │   ├── (.output_columns):
                    │   │   │   │   ┌── "customer.c_custkey"(#5.0)
                    │   │   │   │   ├── "customer.c_nationkey"(#5.3)
                    │   │   │   │   ├── "lineitem.l_discount"(#2.6)
                    │   │   │   │   ├── "lineitem.l_extendedprice"(#2.5)
                    │   │   │   │   ├── "lineitem.l_orderkey"(#2.0)
                    │   │   │   │   ├── "lineitem.l_partkey"(#2.1)
                    │   │   │   │   ├── "lineitem.l_suppkey"(#2.2)
                    │   │   │   │   ├── "orders.o_custkey"(#4.1)
                    │   │   │   │   ├── "orders.o_orderdate"(#4.4)
                    │   │   │   │   ├── "orders.o_orderkey"(#4.0)
                    │   │   │   │   ├── "part.p_partkey"(#1.0)
                    │   │   │   │   ├── "part.p_type"(#1.4)
                    │   │   │   │   ├── "supplier.s_nationkey"(#3.3)
                    │   │   │   │   └── "supplier.s_suppkey"(#3.0)
                    │   │   │   ├── (.cardinality): 0.00
                    │   │   │   ├── Join
                    │   │   │   │   ├── .join_type: Inner
                    │   │   │   │   ├── .implementation: None
                    │   │   │   │   ├── .join_cond: "lineitem.l_orderkey"(#2.0) = "orders.o_orderkey"(#4.0)
                    │   │   │   │   ├── (.output_columns):
                    │   │   │   │   │   ┌── "lineitem.l_discount"(#2.6)
                    │   │   │   │   │   ├── "lineitem.l_extendedprice"(#2.5)
                    │   │   │   │   │   ├── "lineitem.l_orderkey"(#2.0)
                    │   │   │   │   │   ├── "lineitem.l_partkey"(#2.1)
                    │   │   │   │   │   ├── "lineitem.l_suppkey"(#2.2)
                    │   │   │   │   │   ├── "orders.o_custkey"(#4.1)
                    │   │   │   │   │   ├── "orders.o_orderdate"(#4.4)
                    │   │   │   │   │   ├── "orders.o_orderkey"(#4.0)
                    │   │   │   │   │   ├── "part.p_partkey"(#1.0)
                    │   │   │   │   │   ├── "part.p_type"(#1.4)
                    │   │   │   │   │   ├── "supplier.s_nationkey"(#3.3)
                    │   │   │   │   │   └── "supplier.s_suppkey"(#3.0)
                    │   │   │   │   ├── (.cardinality): 0.00
                    │   │   │   │   ├── Join
                    │   │   │   │   │   ├── .join_type: Inner
                    │   │   │   │   │   ├── .implementation: None
                    │   │   │   │   │   ├── .join_cond: "lineitem.l_suppkey"(#2.2) = "supplier.s_suppkey"(#3.0)
                    │   │   │   │   │   ├── (.output_columns):
                    │   │   │   │   │   │   ┌── "lineitem.l_discount"(#2.6)
                    │   │   │   │   │   │   ├── "lineitem.l_extendedprice"(#2.5)
                    │   │   │   │   │   │   ├── "lineitem.l_orderkey"(#2.0)
                    │   │   │   │   │   │   ├── "lineitem.l_partkey"(#2.1)
                    │   │   │   │   │   │   ├── "lineitem.l_suppkey"(#2.2)
                    │   │   │   │   │   │   ├── "part.p_partkey"(#1.0)
                    │   │   │   │   │   │   ├── "part.p_type"(#1.4)
                    │   │   │   │   │   │   ├── "supplier.s_nationkey"(#3.3)
                    │   │   │   │   │   │   └── "supplier.s_suppkey"(#3.0)
                    │   │   │   │   │   ├── (.cardinality): 0.00
                    │   │   │   │   │   ├── Join
                    │   │   │   │   │   │   ├── .join_type: Inner
                    │   │   │   │   │   │   ├── .implementation: None
                    │   │   │   │   │   │   ├── .join_cond: "part.p_partkey"(#1.0) = "lineitem.l_partkey"(#2.1)
                    │   │   │   │   │   │   ├── (.output_columns):
                    │   │   │   │   │   │   │   ┌── "lineitem.l_discount"(#2.6)
                    │   │   │   │   │   │   │   ├── "lineitem.l_extendedprice"(#2.5)
                    │   │   │   │   │   │   │   ├── "lineitem.l_orderkey"(#2.0)
                    │   │   │   │   │   │   │   ├── "lineitem.l_partkey"(#2.1)
                    │   │   │   │   │   │   │   ├── "lineitem.l_suppkey"(#2.2)
                    │   │   │   │   │   │   │   ├── "part.p_partkey"(#1.0)
                    │   │   │   │   │   │   │   └── "part.p_type"(#1.4)
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
                    │   │   │   │   │   │       ├── .data_source_id: 8
                    │   │   │   │   │   │       ├── .table_index: 2
                    │   │   │   │   │   │       ├── .implementation: None
                    │   │   │   │   │   │       ├── (.output_columns):
                    │   │   │   │   │   │       │   ┌── "lineitem.l_discount"(#2.6)
                    │   │   │   │   │   │       │   ├── "lineitem.l_extendedprice"(#2.5)
                    │   │   │   │   │   │       │   ├── "lineitem.l_orderkey"(#2.0)
                    │   │   │   │   │   │       │   ├── "lineitem.l_partkey"(#2.1)
                    │   │   │   │   │   │       │   └── "lineitem.l_suppkey"(#2.2)
                    │   │   │   │   │   │       └── (.cardinality): 0.00
                    │   │   │   │   │   └── Get
                    │   │   │   │   │       ├── .data_source_id: 4
                    │   │   │   │   │       ├── .table_index: 3
                    │   │   │   │   │       ├── .implementation: None
                    │   │   │   │   │       ├── (.output_columns): [ "supplier.s_nationkey"(#3.3), "supplier.s_suppkey"(#3.0) ]
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
                    │   │           ├── (.output_columns):
                    │   │           │   ┌── "nation.n_comment"(#6.3)
                    │   │           │   ├── "nation.n_name"(#6.1)
                    │   │           │   ├── "nation.n_nationkey"(#6.0)
                    │   │           │   └── "nation.n_regionkey"(#6.2)
                    │   │           └── (.cardinality): 0.00
                    │   └── Remap
                    │       ├── .table_index: 9
                    │       ├── (.output_columns): [ "n2.n_comment"(#9.3), "n2.n_name"(#9.1), "n2.n_nationkey"(#9.0), "n2.n_regionkey"(#9.2) ]
                    │       ├── (.cardinality): 0.00
                    │       └── Get
                    │           ├── .data_source_id: 1
                    │           ├── .table_index: 8
                    │           ├── .implementation: None
                    │           ├── (.output_columns):
                    │           │   ┌── "nation.n_comment"(#8.3)
                    │           │   ├── "nation.n_name"(#8.1)
                    │           │   ├── "nation.n_nationkey"(#8.0)
                    │           │   └── "nation.n_regionkey"(#8.2)
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

