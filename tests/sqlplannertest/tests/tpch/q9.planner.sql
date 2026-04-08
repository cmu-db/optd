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
logical_plan after optd-initial:
OrderBy
├── ordering_exprs: [ "__#12.nation"(#12.0) ASC, "__#12.o_year"(#12.1) DESC ]
├── (.output_columns): [ "__#12.nation"(#12.0), "__#12.o_year"(#12.1), "__#12.sum_profit"(#12.2) ]
├── (.cardinality): 0.00
└── Project
    ├── .table_index: 12
    ├── .projections: [ "profit.nation"(#9.0), "profit.o_year"(#9.1), "__#11.sum(profit.amount)"(#11.0) ]
    ├── (.output_columns): [ "__#12.nation"(#12.0), "__#12.o_year"(#12.1), "__#12.sum_profit"(#12.2) ]
    ├── (.cardinality): 0.00
    └── Aggregate
        ├── .key_table_index: 10
        ├── .aggregate_table_index: 11
        ├── .implementation: None
        ├── .exprs: sum("profit.amount"(#9.2))
        ├── .keys: [ "profit.nation"(#9.0), "profit.o_year"(#9.1) ]
        ├── (.output_columns): [ "__#10.nation"(#10.0), "__#10.o_year"(#10.1), "__#11.sum(profit.amount)"(#11.0) ]
        ├── (.cardinality): 0.00
        └── Remap { .table_index: 9, (.output_columns): [ "profit.amount"(#9.2), "profit.nation"(#9.0), "profit.o_year"(#9.1) ], (.cardinality): 0.00 }
            └── Project
                ├── .table_index: 8
                ├── .projections:
                │   ┌── "__#7.n_name"(#7.47)
                │   ├── date_part('YEAR'::utf8, "__#7.o_orderdate"(#7.41))
                │   └── "__#7.l_extendedprice"(#7.21) * 1::decimal128(20, 0) - "__#7.l_discount"(#7.22) - "__#7.ps_supplycost"(#7.35) * "__#7.l_quantity"(#7.20)
                ├── (.output_columns): [ "__#8.amount"(#8.2), "__#8.nation"(#8.0), "__#8.o_year"(#8.1) ]
                ├── (.cardinality): 0.00
                └── Select
                    ├── .predicate: "__#7.p_name"(#7.1) LIKE '%green%'::utf8_view
                    ├── (.output_columns):
                    │   ┌── "__#7.l_comment"(#7.31)
                    │   ├── "__#7.l_commitdate"(#7.27)
                    │   ├── "__#7.l_discount"(#7.22)
                    │   ├── "__#7.l_extendedprice"(#7.21)
                    │   ├── "__#7.l_linenumber"(#7.19)
                    │   ├── "__#7.l_linestatus"(#7.25)
                    │   ├── "__#7.l_orderkey"(#7.16)
                    │   ├── "__#7.l_partkey"(#7.17)
                    │   ├── "__#7.l_quantity"(#7.20)
                    │   ├── "__#7.l_receiptdate"(#7.28)
                    │   ├── "__#7.l_returnflag"(#7.24)
                    │   ├── "__#7.l_shipdate"(#7.26)
                    │   ├── "__#7.l_shipinstruct"(#7.29)
                    │   ├── "__#7.l_shipmode"(#7.30)
                    │   ├── "__#7.l_suppkey"(#7.18)
                    │   ├── "__#7.l_tax"(#7.23)
                    │   ├── "__#7.n_comment"(#7.49)
                    │   ├── "__#7.n_name"(#7.47)
                    │   ├── "__#7.n_nationkey"(#7.46)
                    │   ├── "__#7.n_regionkey"(#7.48)
                    │   ├── "__#7.o_clerk"(#7.43)
                    │   ├── "__#7.o_comment"(#7.45)
                    │   ├── "__#7.o_custkey"(#7.38)
                    │   ├── "__#7.o_orderdate"(#7.41)
                    │   ├── "__#7.o_orderkey"(#7.37)
                    │   ├── "__#7.o_orderpriority"(#7.42)
                    │   ├── "__#7.o_orderstatus"(#7.39)
                    │   ├── "__#7.o_shippriority"(#7.44)
                    │   ├── "__#7.o_totalprice"(#7.40)
                    │   ├── "__#7.p_brand"(#7.3)
                    │   ├── "__#7.p_comment"(#7.8)
                    │   ├── "__#7.p_container"(#7.6)
                    │   ├── "__#7.p_mfgr"(#7.2)
                    │   ├── "__#7.p_name"(#7.1)
                    │   ├── "__#7.p_partkey"(#7.0)
                    │   ├── "__#7.p_retailprice"(#7.7)
                    │   ├── "__#7.p_size"(#7.5)
                    │   ├── "__#7.p_type"(#7.4)
                    │   ├── "__#7.ps_availqty"(#7.34)
                    │   ├── "__#7.ps_comment"(#7.36)
                    │   ├── "__#7.ps_partkey"(#7.32)
                    │   ├── "__#7.ps_suppkey"(#7.33)
                    │   ├── "__#7.ps_supplycost"(#7.35)
                    │   ├── "__#7.s_acctbal"(#7.14)
                    │   ├── "__#7.s_address"(#7.11)
                    │   ├── "__#7.s_comment"(#7.15)
                    │   ├── "__#7.s_name"(#7.10)
                    │   ├── "__#7.s_nationkey"(#7.12)
                    │   ├── "__#7.s_phone"(#7.13)
                    │   └── "__#7.s_suppkey"(#7.9)
                    ├── (.cardinality): 0.00
                    └── Project
                        ├── .table_index: 7
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
                        │   ├── "partsupp.ps_partkey"(#4.0)
                        │   ├── "partsupp.ps_suppkey"(#4.1)
                        │   ├── "partsupp.ps_availqty"(#4.2)
                        │   ├── "partsupp.ps_supplycost"(#4.3)
                        │   ├── "partsupp.ps_comment"(#4.4)
                        │   ├── "orders.o_orderkey"(#5.0)
                        │   ├── "orders.o_custkey"(#5.1)
                        │   ├── "orders.o_orderstatus"(#5.2)
                        │   ├── "orders.o_totalprice"(#5.3)
                        │   ├── "orders.o_orderdate"(#5.4)
                        │   ├── "orders.o_orderpriority"(#5.5)
                        │   ├── "orders.o_clerk"(#5.6)
                        │   ├── "orders.o_shippriority"(#5.7)
                        │   ├── "orders.o_comment"(#5.8)
                        │   ├── "nation.n_nationkey"(#6.0)
                        │   ├── "nation.n_name"(#6.1)
                        │   ├── "nation.n_regionkey"(#6.2)
                        │   └── "nation.n_comment"(#6.3)
                        ├── (.output_columns):
                        │   ┌── "__#7.l_comment"(#7.31)
                        │   ├── "__#7.l_commitdate"(#7.27)
                        │   ├── "__#7.l_discount"(#7.22)
                        │   ├── "__#7.l_extendedprice"(#7.21)
                        │   ├── "__#7.l_linenumber"(#7.19)
                        │   ├── "__#7.l_linestatus"(#7.25)
                        │   ├── "__#7.l_orderkey"(#7.16)
                        │   ├── "__#7.l_partkey"(#7.17)
                        │   ├── "__#7.l_quantity"(#7.20)
                        │   ├── "__#7.l_receiptdate"(#7.28)
                        │   ├── "__#7.l_returnflag"(#7.24)
                        │   ├── "__#7.l_shipdate"(#7.26)
                        │   ├── "__#7.l_shipinstruct"(#7.29)
                        │   ├── "__#7.l_shipmode"(#7.30)
                        │   ├── "__#7.l_suppkey"(#7.18)
                        │   ├── "__#7.l_tax"(#7.23)
                        │   ├── "__#7.n_comment"(#7.49)
                        │   ├── "__#7.n_name"(#7.47)
                        │   ├── "__#7.n_nationkey"(#7.46)
                        │   ├── "__#7.n_regionkey"(#7.48)
                        │   ├── "__#7.o_clerk"(#7.43)
                        │   ├── "__#7.o_comment"(#7.45)
                        │   ├── "__#7.o_custkey"(#7.38)
                        │   ├── "__#7.o_orderdate"(#7.41)
                        │   ├── "__#7.o_orderkey"(#7.37)
                        │   ├── "__#7.o_orderpriority"(#7.42)
                        │   ├── "__#7.o_orderstatus"(#7.39)
                        │   ├── "__#7.o_shippriority"(#7.44)
                        │   ├── "__#7.o_totalprice"(#7.40)
                        │   ├── "__#7.p_brand"(#7.3)
                        │   ├── "__#7.p_comment"(#7.8)
                        │   ├── "__#7.p_container"(#7.6)
                        │   ├── "__#7.p_mfgr"(#7.2)
                        │   ├── "__#7.p_name"(#7.1)
                        │   ├── "__#7.p_partkey"(#7.0)
                        │   ├── "__#7.p_retailprice"(#7.7)
                        │   ├── "__#7.p_size"(#7.5)
                        │   ├── "__#7.p_type"(#7.4)
                        │   ├── "__#7.ps_availqty"(#7.34)
                        │   ├── "__#7.ps_comment"(#7.36)
                        │   ├── "__#7.ps_partkey"(#7.32)
                        │   ├── "__#7.ps_suppkey"(#7.33)
                        │   ├── "__#7.ps_supplycost"(#7.35)
                        │   ├── "__#7.s_acctbal"(#7.14)
                        │   ├── "__#7.s_address"(#7.11)
                        │   ├── "__#7.s_comment"(#7.15)
                        │   ├── "__#7.s_name"(#7.10)
                        │   ├── "__#7.s_nationkey"(#7.12)
                        │   ├── "__#7.s_phone"(#7.13)
                        │   └── "__#7.s_suppkey"(#7.9)
                        ├── (.cardinality): 0.00
                        └── Join
                            ├── .join_type: Inner
                            ├── .implementation: None
                            ├── .join_cond: ("supplier.s_nationkey"(#3.3) = "nation.n_nationkey"(#6.0))
                            ├── (.output_columns):
                            │   ┌── "lineitem.l_comment"(#2.15)
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
                            │   ├── "nation.n_comment"(#6.3)
                            │   ├── "nation.n_name"(#6.1)
                            │   ├── "nation.n_nationkey"(#6.0)
                            │   ├── "nation.n_regionkey"(#6.2)
                            │   ├── "orders.o_clerk"(#5.6)
                            │   ├── "orders.o_comment"(#5.8)
                            │   ├── "orders.o_custkey"(#5.1)
                            │   ├── "orders.o_orderdate"(#5.4)
                            │   ├── "orders.o_orderkey"(#5.0)
                            │   ├── "orders.o_orderpriority"(#5.5)
                            │   ├── "orders.o_orderstatus"(#5.2)
                            │   ├── "orders.o_shippriority"(#5.7)
                            │   ├── "orders.o_totalprice"(#5.3)
                            │   ├── "part.p_brand"(#1.3)
                            │   ├── "part.p_comment"(#1.8)
                            │   ├── "part.p_container"(#1.6)
                            │   ├── "part.p_mfgr"(#1.2)
                            │   ├── "part.p_name"(#1.1)
                            │   ├── "part.p_partkey"(#1.0)
                            │   ├── "part.p_retailprice"(#1.7)
                            │   ├── "part.p_size"(#1.5)
                            │   ├── "part.p_type"(#1.4)
                            │   ├── "partsupp.ps_availqty"(#4.2)
                            │   ├── "partsupp.ps_comment"(#4.4)
                            │   ├── "partsupp.ps_partkey"(#4.0)
                            │   ├── "partsupp.ps_suppkey"(#4.1)
                            │   ├── "partsupp.ps_supplycost"(#4.3)
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
                            │   ├── .join_cond: ("lineitem.l_orderkey"(#2.0) = "orders.o_orderkey"(#5.0))
                            │   ├── (.output_columns):
                            │   │   ┌── "lineitem.l_comment"(#2.15)
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
                            │   │   ├── "orders.o_clerk"(#5.6)
                            │   │   ├── "orders.o_comment"(#5.8)
                            │   │   ├── "orders.o_custkey"(#5.1)
                            │   │   ├── "orders.o_orderdate"(#5.4)
                            │   │   ├── "orders.o_orderkey"(#5.0)
                            │   │   ├── "orders.o_orderpriority"(#5.5)
                            │   │   ├── "orders.o_orderstatus"(#5.2)
                            │   │   ├── "orders.o_shippriority"(#5.7)
                            │   │   ├── "orders.o_totalprice"(#5.3)
                            │   │   ├── "part.p_brand"(#1.3)
                            │   │   ├── "part.p_comment"(#1.8)
                            │   │   ├── "part.p_container"(#1.6)
                            │   │   ├── "part.p_mfgr"(#1.2)
                            │   │   ├── "part.p_name"(#1.1)
                            │   │   ├── "part.p_partkey"(#1.0)
                            │   │   ├── "part.p_retailprice"(#1.7)
                            │   │   ├── "part.p_size"(#1.5)
                            │   │   ├── "part.p_type"(#1.4)
                            │   │   ├── "partsupp.ps_availqty"(#4.2)
                            │   │   ├── "partsupp.ps_comment"(#4.4)
                            │   │   ├── "partsupp.ps_partkey"(#4.0)
                            │   │   ├── "partsupp.ps_suppkey"(#4.1)
                            │   │   ├── "partsupp.ps_supplycost"(#4.3)
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
                            │   │   ├── .join_cond: ("lineitem.l_suppkey"(#2.2) = "partsupp.ps_suppkey"(#4.1)) AND ("lineitem.l_partkey"(#2.1) = "partsupp.ps_partkey"(#4.0))
                            │   │   ├── (.output_columns):
                            │   │   │   ┌── "lineitem.l_comment"(#2.15)
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
                            │   │   │   ├── "part.p_brand"(#1.3)
                            │   │   │   ├── "part.p_comment"(#1.8)
                            │   │   │   ├── "part.p_container"(#1.6)
                            │   │   │   ├── "part.p_mfgr"(#1.2)
                            │   │   │   ├── "part.p_name"(#1.1)
                            │   │   │   ├── "part.p_partkey"(#1.0)
                            │   │   │   ├── "part.p_retailprice"(#1.7)
                            │   │   │   ├── "part.p_size"(#1.5)
                            │   │   │   ├── "part.p_type"(#1.4)
                            │   │   │   ├── "partsupp.ps_availqty"(#4.2)
                            │   │   │   ├── "partsupp.ps_comment"(#4.4)
                            │   │   │   ├── "partsupp.ps_partkey"(#4.0)
                            │   │   │   ├── "partsupp.ps_suppkey"(#4.1)
                            │   │   │   ├── "partsupp.ps_supplycost"(#4.3)
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
                            │   │   │   ├── .join_cond: ("lineitem.l_suppkey"(#2.2) = "supplier.s_suppkey"(#3.0))
                            │   │   │   ├── (.output_columns):
                            │   │   │   │   ┌── "lineitem.l_comment"(#2.15)
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
                            │   │   │   │   ├── .join_cond: ("part.p_partkey"(#1.0) = "lineitem.l_partkey"(#2.1))
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
                            │   │   │   │   │   ├── "part.p_brand"(#1.3)
                            │   │   │   │   │   ├── "part.p_comment"(#1.8)
                            │   │   │   │   │   ├── "part.p_container"(#1.6)
                            │   │   │   │   │   ├── "part.p_mfgr"(#1.2)
                            │   │   │   │   │   ├── "part.p_name"(#1.1)
                            │   │   │   │   │   ├── "part.p_partkey"(#1.0)
                            │   │   │   │   │   ├── "part.p_retailprice"(#1.7)
                            │   │   │   │   │   ├── "part.p_size"(#1.5)
                            │   │   │   │   │   └── "part.p_type"(#1.4)
                            │   │   │   │   ├── (.cardinality): 0.00
                            │   │   │   │   ├── Get
                            │   │   │   │   │   ├── .data_source_id: 3
                            │   │   │   │   │   ├── .table_index: 1
                            │   │   │   │   │   ├── .implementation: None
                            │   │   │   │   │   ├── (.output_columns):
                            │   │   │   │   │   │   ┌── "part.p_brand"(#1.3)
                            │   │   │   │   │   │   ├── "part.p_comment"(#1.8)
                            │   │   │   │   │   │   ├── "part.p_container"(#1.6)
                            │   │   │   │   │   │   ├── "part.p_mfgr"(#1.2)
                            │   │   │   │   │   │   ├── "part.p_name"(#1.1)
                            │   │   │   │   │   │   ├── "part.p_partkey"(#1.0)
                            │   │   │   │   │   │   ├── "part.p_retailprice"(#1.7)
                            │   │   │   │   │   │   ├── "part.p_size"(#1.5)
                            │   │   │   │   │   │   └── "part.p_type"(#1.4)
                            │   │   │   │   │   └── (.cardinality): 0.00
                            │   │   │   │   └── Get
                            │   │   │   │       ├── .data_source_id: 8
                            │   │   │   │       ├── .table_index: 2
                            │   │   │   │       ├── .implementation: None
                            │   │   │   │       ├── (.output_columns):
                            │   │   │   │       │   ┌── "lineitem.l_comment"(#2.15)
                            │   │   │   │       │   ├── "lineitem.l_commitdate"(#2.11)
                            │   │   │   │       │   ├── "lineitem.l_discount"(#2.6)
                            │   │   │   │       │   ├── "lineitem.l_extendedprice"(#2.5)
                            │   │   │   │       │   ├── "lineitem.l_linenumber"(#2.3)
                            │   │   │   │       │   ├── "lineitem.l_linestatus"(#2.9)
                            │   │   │   │       │   ├── "lineitem.l_orderkey"(#2.0)
                            │   │   │   │       │   ├── "lineitem.l_partkey"(#2.1)
                            │   │   │   │       │   ├── "lineitem.l_quantity"(#2.4)
                            │   │   │   │       │   ├── "lineitem.l_receiptdate"(#2.12)
                            │   │   │   │       │   ├── "lineitem.l_returnflag"(#2.8)
                            │   │   │   │       │   ├── "lineitem.l_shipdate"(#2.10)
                            │   │   │   │       │   ├── "lineitem.l_shipinstruct"(#2.13)
                            │   │   │   │       │   ├── "lineitem.l_shipmode"(#2.14)
                            │   │   │   │       │   ├── "lineitem.l_suppkey"(#2.2)
                            │   │   │   │       │   └── "lineitem.l_tax"(#2.7)
                            │   │   │   │       └── (.cardinality): 0.00
                            │   │   │   └── Get
                            │   │   │       ├── .data_source_id: 4
                            │   │   │       ├── .table_index: 3
                            │   │   │       ├── .implementation: None
                            │   │   │       ├── (.output_columns):
                            │   │   │       │   ┌── "supplier.s_acctbal"(#3.5)
                            │   │   │       │   ├── "supplier.s_address"(#3.2)
                            │   │   │       │   ├── "supplier.s_comment"(#3.6)
                            │   │   │       │   ├── "supplier.s_name"(#3.1)
                            │   │   │       │   ├── "supplier.s_nationkey"(#3.3)
                            │   │   │       │   ├── "supplier.s_phone"(#3.4)
                            │   │   │       │   └── "supplier.s_suppkey"(#3.0)
                            │   │   │       └── (.cardinality): 0.00
                            │   │   └── Get
                            │   │       ├── .data_source_id: 5
                            │   │       ├── .table_index: 4
                            │   │       ├── .implementation: None
                            │   │       ├── (.output_columns):
                            │   │       │   ┌── "partsupp.ps_availqty"(#4.2)
                            │   │       │   ├── "partsupp.ps_comment"(#4.4)
                            │   │       │   ├── "partsupp.ps_partkey"(#4.0)
                            │   │       │   ├── "partsupp.ps_suppkey"(#4.1)
                            │   │       │   └── "partsupp.ps_supplycost"(#4.3)
                            │   │       └── (.cardinality): 0.00
                            │   └── Get
                            │       ├── .data_source_id: 7
                            │       ├── .table_index: 5
                            │       ├── .implementation: None
                            │       ├── (.output_columns):
                            │       │   ┌── "orders.o_clerk"(#5.6)
                            │       │   ├── "orders.o_comment"(#5.8)
                            │       │   ├── "orders.o_custkey"(#5.1)
                            │       │   ├── "orders.o_orderdate"(#5.4)
                            │       │   ├── "orders.o_orderkey"(#5.0)
                            │       │   ├── "orders.o_orderpriority"(#5.5)
                            │       │   ├── "orders.o_orderstatus"(#5.2)
                            │       │   ├── "orders.o_shippriority"(#5.7)
                            │       │   └── "orders.o_totalprice"(#5.3)
                            │       └── (.cardinality): 0.00
                            └── Get
                                ├── .data_source_id: 1
                                ├── .table_index: 6
                                ├── .implementation: None
                                ├── (.output_columns): [ "nation.n_comment"(#6.3), "nation.n_name"(#6.1), "nation.n_nationkey"(#6.0), "nation.n_regionkey"(#6.2) ]
                                └── (.cardinality): 0.00

physical_plan after optd-finalized:
EnforcerSort
├── tuple_ordering: [(#12.0, Asc), (#12.1, Desc)]
├── (.output_columns): [ "__#12.nation"(#12.0), "__#12.o_year"(#12.1), "__#12.sum_profit"(#12.2) ]
├── (.cardinality): 0.00
└── Project
    ├── .table_index: 12
    ├── .projections: [ "profit.nation"(#9.0), "profit.o_year"(#9.1), "__#11.sum(profit.amount)"(#11.0) ]
    ├── (.output_columns): [ "__#12.nation"(#12.0), "__#12.o_year"(#12.1), "__#12.sum_profit"(#12.2) ]
    ├── (.cardinality): 0.00
    └── Aggregate
        ├── .key_table_index: 10
        ├── .aggregate_table_index: 11
        ├── .implementation: None
        ├── .exprs: sum("profit.amount"(#9.2))
        ├── .keys: [ "profit.nation"(#9.0), "profit.o_year"(#9.1) ]
        ├── (.output_columns): [ "__#10.nation"(#10.0), "__#10.o_year"(#10.1), "__#11.sum(profit.amount)"(#11.0) ]
        ├── (.cardinality): 0.00
        └── Remap { .table_index: 9, (.output_columns): [ "profit.amount"(#9.2), "profit.nation"(#9.0), "profit.o_year"(#9.1) ], (.cardinality): 0.00 }
            └── Project
                ├── .table_index: 8
                ├── .projections:
                │   ┌── "nation.n_name"(#6.1)
                │   ├── date_part('YEAR'::utf8, "orders.o_orderdate"(#5.4))
                │   └── "lineitem.l_extendedprice"(#2.5) * 1::decimal128(20, 0) - "lineitem.l_discount"(#2.6) - "partsupp.ps_supplycost"(#4.3) * "lineitem.l_quantity"(#2.4)
                ├── (.output_columns): [ "__#8.amount"(#8.2), "__#8.nation"(#8.0), "__#8.o_year"(#8.1) ]
                ├── (.cardinality): 0.00
                └── Join
                    ├── .join_type: Inner
                    ├── .implementation: None
                    ├── .join_cond: "supplier.s_nationkey"(#3.3) = "nation.n_nationkey"(#6.0)
                    ├── (.output_columns):
                    │   ┌── "lineitem.l_discount"(#2.6)
                    │   ├── "lineitem.l_extendedprice"(#2.5)
                    │   ├── "lineitem.l_orderkey"(#2.0)
                    │   ├── "lineitem.l_partkey"(#2.1)
                    │   ├── "lineitem.l_quantity"(#2.4)
                    │   ├── "lineitem.l_suppkey"(#2.2)
                    │   ├── "nation.n_name"(#6.1)
                    │   ├── "nation.n_nationkey"(#6.0)
                    │   ├── "orders.o_orderdate"(#5.4)
                    │   ├── "orders.o_orderkey"(#5.0)
                    │   ├── "part.p_name"(#1.1)
                    │   ├── "part.p_partkey"(#1.0)
                    │   ├── "partsupp.ps_partkey"(#4.0)
                    │   ├── "partsupp.ps_suppkey"(#4.1)
                    │   ├── "partsupp.ps_supplycost"(#4.3)
                    │   ├── "supplier.s_nationkey"(#3.3)
                    │   └── "supplier.s_suppkey"(#3.0)
                    ├── (.cardinality): 0.00
                    ├── Join
                    │   ├── .join_type: Inner
                    │   ├── .implementation: None
                    │   ├── .join_cond: "lineitem.l_orderkey"(#2.0) = "orders.o_orderkey"(#5.0)
                    │   ├── (.output_columns):
                    │   │   ┌── "lineitem.l_discount"(#2.6)
                    │   │   ├── "lineitem.l_extendedprice"(#2.5)
                    │   │   ├── "lineitem.l_orderkey"(#2.0)
                    │   │   ├── "lineitem.l_partkey"(#2.1)
                    │   │   ├── "lineitem.l_quantity"(#2.4)
                    │   │   ├── "lineitem.l_suppkey"(#2.2)
                    │   │   ├── "orders.o_orderdate"(#5.4)
                    │   │   ├── "orders.o_orderkey"(#5.0)
                    │   │   ├── "part.p_name"(#1.1)
                    │   │   ├── "part.p_partkey"(#1.0)
                    │   │   ├── "partsupp.ps_partkey"(#4.0)
                    │   │   ├── "partsupp.ps_suppkey"(#4.1)
                    │   │   ├── "partsupp.ps_supplycost"(#4.3)
                    │   │   ├── "supplier.s_nationkey"(#3.3)
                    │   │   └── "supplier.s_suppkey"(#3.0)
                    │   ├── (.cardinality): 0.00
                    │   ├── Join
                    │   │   ├── .join_type: Inner
                    │   │   ├── .implementation: None
                    │   │   ├── .join_cond: ("lineitem.l_suppkey"(#2.2) = "partsupp.ps_suppkey"(#4.1)) AND ("lineitem.l_partkey"(#2.1) = "partsupp.ps_partkey"(#4.0))
                    │   │   ├── (.output_columns):
                    │   │   │   ┌── "lineitem.l_discount"(#2.6)
                    │   │   │   ├── "lineitem.l_extendedprice"(#2.5)
                    │   │   │   ├── "lineitem.l_orderkey"(#2.0)
                    │   │   │   ├── "lineitem.l_partkey"(#2.1)
                    │   │   │   ├── "lineitem.l_quantity"(#2.4)
                    │   │   │   ├── "lineitem.l_suppkey"(#2.2)
                    │   │   │   ├── "part.p_name"(#1.1)
                    │   │   │   ├── "part.p_partkey"(#1.0)
                    │   │   │   ├── "partsupp.ps_partkey"(#4.0)
                    │   │   │   ├── "partsupp.ps_suppkey"(#4.1)
                    │   │   │   ├── "partsupp.ps_supplycost"(#4.3)
                    │   │   │   ├── "supplier.s_nationkey"(#3.3)
                    │   │   │   └── "supplier.s_suppkey"(#3.0)
                    │   │   ├── (.cardinality): 0.00
                    │   │   ├── Join
                    │   │   │   ├── .join_type: Inner
                    │   │   │   ├── .implementation: None
                    │   │   │   ├── .join_cond: "lineitem.l_suppkey"(#2.2) = "supplier.s_suppkey"(#3.0)
                    │   │   │   ├── (.output_columns):
                    │   │   │   │   ┌── "lineitem.l_discount"(#2.6)
                    │   │   │   │   ├── "lineitem.l_extendedprice"(#2.5)
                    │   │   │   │   ├── "lineitem.l_orderkey"(#2.0)
                    │   │   │   │   ├── "lineitem.l_partkey"(#2.1)
                    │   │   │   │   ├── "lineitem.l_quantity"(#2.4)
                    │   │   │   │   ├── "lineitem.l_suppkey"(#2.2)
                    │   │   │   │   ├── "part.p_name"(#1.1)
                    │   │   │   │   ├── "part.p_partkey"(#1.0)
                    │   │   │   │   ├── "supplier.s_nationkey"(#3.3)
                    │   │   │   │   └── "supplier.s_suppkey"(#3.0)
                    │   │   │   ├── (.cardinality): 0.00
                    │   │   │   ├── Join
                    │   │   │   │   ├── .join_type: Inner
                    │   │   │   │   ├── .implementation: None
                    │   │   │   │   ├── .join_cond: "part.p_partkey"(#1.0) = "lineitem.l_partkey"(#2.1)
                    │   │   │   │   ├── (.output_columns):
                    │   │   │   │   │   ┌── "lineitem.l_discount"(#2.6)
                    │   │   │   │   │   ├── "lineitem.l_extendedprice"(#2.5)
                    │   │   │   │   │   ├── "lineitem.l_orderkey"(#2.0)
                    │   │   │   │   │   ├── "lineitem.l_partkey"(#2.1)
                    │   │   │   │   │   ├── "lineitem.l_quantity"(#2.4)
                    │   │   │   │   │   ├── "lineitem.l_suppkey"(#2.2)
                    │   │   │   │   │   ├── "part.p_name"(#1.1)
                    │   │   │   │   │   └── "part.p_partkey"(#1.0)
                    │   │   │   │   ├── (.cardinality): 0.00
                    │   │   │   │   ├── Select
                    │   │   │   │   │   ├── .predicate: "part.p_name"(#1.1) LIKE '%green%'::utf8_view
                    │   │   │   │   │   ├── (.output_columns): [ "part.p_name"(#1.1), "part.p_partkey"(#1.0) ]
                    │   │   │   │   │   ├── (.cardinality): 0.00
                    │   │   │   │   │   └── Get
                    │   │   │   │   │       ├── .data_source_id: 3
                    │   │   │   │   │       ├── .table_index: 1
                    │   │   │   │   │       ├── .implementation: None
                    │   │   │   │   │       ├── (.output_columns): [ "part.p_name"(#1.1), "part.p_partkey"(#1.0) ]
                    │   │   │   │   │       └── (.cardinality): 0.00
                    │   │   │   │   └── Get
                    │   │   │   │       ├── .data_source_id: 8
                    │   │   │   │       ├── .table_index: 2
                    │   │   │   │       ├── .implementation: None
                    │   │   │   │       ├── (.output_columns):
                    │   │   │   │       │   ┌── "lineitem.l_discount"(#2.6)
                    │   │   │   │       │   ├── "lineitem.l_extendedprice"(#2.5)
                    │   │   │   │       │   ├── "lineitem.l_orderkey"(#2.0)
                    │   │   │   │       │   ├── "lineitem.l_partkey"(#2.1)
                    │   │   │   │       │   ├── "lineitem.l_quantity"(#2.4)
                    │   │   │   │       │   └── "lineitem.l_suppkey"(#2.2)
                    │   │   │   │       └── (.cardinality): 0.00
                    │   │   │   └── Get
                    │   │   │       ├── .data_source_id: 4
                    │   │   │       ├── .table_index: 3
                    │   │   │       ├── .implementation: None
                    │   │   │       ├── (.output_columns): [ "supplier.s_nationkey"(#3.3), "supplier.s_suppkey"(#3.0) ]
                    │   │   │       └── (.cardinality): 0.00
                    │   │   └── Get
                    │   │       ├── .data_source_id: 5
                    │   │       ├── .table_index: 4
                    │   │       ├── .implementation: None
                    │   │       ├── (.output_columns): [ "partsupp.ps_partkey"(#4.0), "partsupp.ps_suppkey"(#4.1), "partsupp.ps_supplycost"(#4.3) ]
                    │   │       └── (.cardinality): 0.00
                    │   └── Get
                    │       ├── .data_source_id: 7
                    │       ├── .table_index: 5
                    │       ├── .implementation: None
                    │       ├── (.output_columns): [ "orders.o_orderdate"(#5.4), "orders.o_orderkey"(#5.0) ]
                    │       └── (.cardinality): 0.00
                    └── Get
                        ├── .data_source_id: 1
                        ├── .table_index: 6
                        ├── .implementation: None
                        ├── (.output_columns): [ "nation.n_name"(#6.1), "nation.n_nationkey"(#6.0) ]
                        └── (.cardinality): 0.00
*/

