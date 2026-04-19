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
OrderBy { ordering_exprs: [ "__#11.nation"(#11.0) ASC, "__#11.o_year"(#11.1) DESC ], (.output_columns): [ "__#11.nation"(#11.0), "__#11.o_year"(#11.1), "__#11.sum_profit"(#11.2) ], (.cardinality): 0.00 }
└── Project { .table_index: 11, .projections: [ "profit.nation"(#8.0), "profit.o_year"(#8.1), "__#10.sum(profit.amount)"(#10.0) ], (.output_columns): [ "__#11.nation"(#11.0), "__#11.o_year"(#11.1), "__#11.sum_profit"(#11.2) ], (.cardinality): 0.00 }
    └── Aggregate { .key_table_index: 9, .aggregate_table_index: 10, .implementation: None, .exprs: sum("profit.amount"(#8.2)), .keys: [ "profit.nation"(#8.0), "profit.o_year"(#8.1) ], (.output_columns): [ "__#10.sum(profit.amount)"(#10.0), "__#9.nation"(#9.0), "__#9.o_year"(#9.1) ], (.cardinality): 0.00 }
        └── Remap { .table_index: 8, (.output_columns): [ "profit.amount"(#8.2), "profit.nation"(#8.0), "profit.o_year"(#8.1) ], (.cardinality): 0.00 }
            └── Project { .table_index: 7, .projections: [ "nation.n_name"(#6.1), date_part('YEAR'::utf8, "orders.o_orderdate"(#5.4)), "lineitem.l_extendedprice"(#3.5) * CAST (1::bigint AS Decimal128(20, 0)) - "lineitem.l_discount"(#3.6) - "partsupp.ps_supplycost"(#4.3) * "lineitem.l_quantity"(#3.4) ], (.output_columns): [ "__#7.amount"(#7.2), "__#7.nation"(#7.0), "__#7.o_year"(#7.1) ], (.cardinality): 0.00 }
                └── Select
                    ├── .predicate: ("supplier.s_suppkey"(#2.0) = "lineitem.l_suppkey"(#3.2)) AND ("partsupp.ps_suppkey"(#4.1) = "lineitem.l_suppkey"(#3.2)) AND ("partsupp.ps_partkey"(#4.0) = "lineitem.l_partkey"(#3.1)) AND ("part.p_partkey"(#1.0) = "lineitem.l_partkey"(#3.1)) AND ("orders.o_orderkey"(#5.0) = "lineitem.l_orderkey"(#3.0)) AND ("supplier.s_nationkey"(#2.3) = "nation.n_nationkey"(#6.0)) AND ("part.p_name"(#1.1) LIKE CAST ('%green%'::utf8 AS Utf8View))
                    ├── (.output_columns):
                    │   ┌── "lineitem.l_comment"(#3.15)
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
                        │   ┌── "lineitem.l_comment"(#3.15)
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
                        │   │   ┌── "lineitem.l_comment"(#3.15)
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
                        │   │   │   ┌── "lineitem.l_comment"(#3.15)
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
                        │   │   │   │   ┌── "lineitem.l_comment"(#3.15)
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
                        │   │   │   │   ├── (.output_columns): [ "part.p_brand"(#1.3), "part.p_comment"(#1.8), "part.p_container"(#1.6), "part.p_mfgr"(#1.2), "part.p_name"(#1.1), "part.p_partkey"(#1.0), "part.p_retailprice"(#1.7), "part.p_size"(#1.5), "part.p_type"(#1.4), "supplier.s_acctbal"(#2.5), "supplier.s_address"(#2.2), "supplier.s_comment"(#2.6), "supplier.s_name"(#2.1), "supplier.s_nationkey"(#2.3), "supplier.s_phone"(#2.4), "supplier.s_suppkey"(#2.0) ]
                        │   │   │   │   ├── (.cardinality): 0.00
                        │   │   │   │   ├── Get { .data_source_id: 3, .table_index: 1, .implementation: None, (.output_columns): [ "part.p_brand"(#1.3), "part.p_comment"(#1.8), "part.p_container"(#1.6), "part.p_mfgr"(#1.2), "part.p_name"(#1.1), "part.p_partkey"(#1.0), "part.p_retailprice"(#1.7), "part.p_size"(#1.5), "part.p_type"(#1.4) ], (.cardinality): 0.00 }
                        │   │   │   │   └── Get { .data_source_id: 4, .table_index: 2, .implementation: None, (.output_columns): [ "supplier.s_acctbal"(#2.5), "supplier.s_address"(#2.2), "supplier.s_comment"(#2.6), "supplier.s_name"(#2.1), "supplier.s_nationkey"(#2.3), "supplier.s_phone"(#2.4), "supplier.s_suppkey"(#2.0) ], (.cardinality): 0.00 }
                        │   │   │   └── Get
                        │   │   │       ├── .data_source_id: 8
                        │   │   │       ├── .table_index: 3
                        │   │   │       ├── .implementation: None
                        │   │   │       ├── (.output_columns):
                        │   │   │       │   ┌── "lineitem.l_comment"(#3.15)
                        │   │   │       │   ├── "lineitem.l_commitdate"(#3.11)
                        │   │   │       │   ├── "lineitem.l_discount"(#3.6)
                        │   │   │       │   ├── "lineitem.l_extendedprice"(#3.5)
                        │   │   │       │   ├── "lineitem.l_linenumber"(#3.3)
                        │   │   │       │   ├── "lineitem.l_linestatus"(#3.9)
                        │   │   │       │   ├── "lineitem.l_orderkey"(#3.0)
                        │   │   │       │   ├── "lineitem.l_partkey"(#3.1)
                        │   │   │       │   ├── "lineitem.l_quantity"(#3.4)
                        │   │   │       │   ├── "lineitem.l_receiptdate"(#3.12)
                        │   │   │       │   ├── "lineitem.l_returnflag"(#3.8)
                        │   │   │       │   ├── "lineitem.l_shipdate"(#3.10)
                        │   │   │       │   ├── "lineitem.l_shipinstruct"(#3.13)
                        │   │   │       │   ├── "lineitem.l_shipmode"(#3.14)
                        │   │   │       │   ├── "lineitem.l_suppkey"(#3.2)
                        │   │   │       │   └── "lineitem.l_tax"(#3.7)
                        │   │   │       └── (.cardinality): 0.00
                        │   │   └── Get { .data_source_id: 5, .table_index: 4, .implementation: None, (.output_columns): [ "partsupp.ps_availqty"(#4.2), "partsupp.ps_comment"(#4.4), "partsupp.ps_partkey"(#4.0), "partsupp.ps_suppkey"(#4.1), "partsupp.ps_supplycost"(#4.3) ], (.cardinality): 0.00 }
                        │   └── Get { .data_source_id: 7, .table_index: 5, .implementation: None, (.output_columns): [ "orders.o_clerk"(#5.6), "orders.o_comment"(#5.8), "orders.o_custkey"(#5.1), "orders.o_orderdate"(#5.4), "orders.o_orderkey"(#5.0), "orders.o_orderpriority"(#5.5), "orders.o_orderstatus"(#5.2), "orders.o_shippriority"(#5.7), "orders.o_totalprice"(#5.3) ], (.cardinality): 0.00 }
                        └── Get { .data_source_id: 1, .table_index: 6, .implementation: None, (.output_columns): [ "nation.n_comment"(#6.3), "nation.n_name"(#6.1), "nation.n_nationkey"(#6.0), "nation.n_regionkey"(#6.2) ], (.cardinality): 0.00 }

physical_plan after optd-finalized:
EnforcerSort
├── tuple_ordering: [(#11.0, Asc), (#11.1, Desc)]
├── (.output_columns): [ "__#11.nation"(#11.0), "__#11.o_year"(#11.1), "__#11.sum_profit"(#11.2) ]
├── (.cardinality): 0.00
└── Project
    ├── .table_index: 11
    ├── .projections: [ "profit.nation"(#8.0), "profit.o_year"(#8.1), "__#10.sum(profit.amount)"(#10.0) ]
    ├── (.output_columns): [ "__#11.nation"(#11.0), "__#11.o_year"(#11.1), "__#11.sum_profit"(#11.2) ]
    ├── (.cardinality): 0.00
    └── Aggregate
        ├── .key_table_index: 9
        ├── .aggregate_table_index: 10
        ├── .implementation: None
        ├── .exprs: sum("profit.amount"(#8.2))
        ├── .keys: [ "profit.nation"(#8.0), "profit.o_year"(#8.1) ]
        ├── (.output_columns): [ "__#10.sum(profit.amount)"(#10.0), "__#9.nation"(#9.0), "__#9.o_year"(#9.1) ]
        ├── (.cardinality): 0.00
        └── Remap { .table_index: 8, (.output_columns): [ "profit.amount"(#8.2), "profit.nation"(#8.0), "profit.o_year"(#8.1) ], (.cardinality): 0.00 }
            └── Project
                ├── .table_index: 7
                ├── .projections:
                │   ┌── "nation.n_name"(#6.1)
                │   ├── date_part('YEAR'::utf8, "orders.o_orderdate"(#5.4))
                │   └── "lineitem.l_extendedprice"(#3.5) * 1::decimal128(20, 0) - "lineitem.l_discount"(#3.6) - "partsupp.ps_supplycost"(#4.3) * "lineitem.l_quantity"(#3.4)
                ├── (.output_columns): [ "__#7.amount"(#7.2), "__#7.nation"(#7.0), "__#7.o_year"(#7.1) ]
                ├── (.cardinality): 0.00
                └── Join
                    ├── .join_type: Inner
                    ├── .implementation: None
                    ├── .join_cond: "supplier.s_nationkey"(#2.3) = "nation.n_nationkey"(#6.0)
                    ├── (.output_columns):
                    │   ┌── "lineitem.l_discount"(#3.6)
                    │   ├── "lineitem.l_extendedprice"(#3.5)
                    │   ├── "lineitem.l_orderkey"(#3.0)
                    │   ├── "lineitem.l_partkey"(#3.1)
                    │   ├── "lineitem.l_quantity"(#3.4)
                    │   ├── "lineitem.l_suppkey"(#3.2)
                    │   ├── "nation.n_name"(#6.1)
                    │   ├── "nation.n_nationkey"(#6.0)
                    │   ├── "orders.o_orderdate"(#5.4)
                    │   ├── "orders.o_orderkey"(#5.0)
                    │   ├── "part.p_name"(#1.1)
                    │   ├── "part.p_partkey"(#1.0)
                    │   ├── "partsupp.ps_partkey"(#4.0)
                    │   ├── "partsupp.ps_suppkey"(#4.1)
                    │   ├── "partsupp.ps_supplycost"(#4.3)
                    │   ├── "supplier.s_nationkey"(#2.3)
                    │   └── "supplier.s_suppkey"(#2.0)
                    ├── (.cardinality): 0.00
                    ├── Join
                    │   ├── .join_type: Inner
                    │   ├── .implementation: None
                    │   ├── .join_cond: "orders.o_orderkey"(#5.0) = "lineitem.l_orderkey"(#3.0)
                    │   ├── (.output_columns):
                    │   │   ┌── "lineitem.l_discount"(#3.6)
                    │   │   ├── "lineitem.l_extendedprice"(#3.5)
                    │   │   ├── "lineitem.l_orderkey"(#3.0)
                    │   │   ├── "lineitem.l_partkey"(#3.1)
                    │   │   ├── "lineitem.l_quantity"(#3.4)
                    │   │   ├── "lineitem.l_suppkey"(#3.2)
                    │   │   ├── "orders.o_orderdate"(#5.4)
                    │   │   ├── "orders.o_orderkey"(#5.0)
                    │   │   ├── "part.p_name"(#1.1)
                    │   │   ├── "part.p_partkey"(#1.0)
                    │   │   ├── "partsupp.ps_partkey"(#4.0)
                    │   │   ├── "partsupp.ps_suppkey"(#4.1)
                    │   │   ├── "partsupp.ps_supplycost"(#4.3)
                    │   │   ├── "supplier.s_nationkey"(#2.3)
                    │   │   └── "supplier.s_suppkey"(#2.0)
                    │   ├── (.cardinality): 0.00
                    │   ├── Join
                    │   │   ├── .join_type: Inner
                    │   │   ├── .implementation: None
                    │   │   ├── .join_cond: ("partsupp.ps_suppkey"(#4.1) = "lineitem.l_suppkey"(#3.2)) AND ("partsupp.ps_partkey"(#4.0) = "lineitem.l_partkey"(#3.1))
                    │   │   ├── (.output_columns):
                    │   │   │   ┌── "lineitem.l_discount"(#3.6)
                    │   │   │   ├── "lineitem.l_extendedprice"(#3.5)
                    │   │   │   ├── "lineitem.l_orderkey"(#3.0)
                    │   │   │   ├── "lineitem.l_partkey"(#3.1)
                    │   │   │   ├── "lineitem.l_quantity"(#3.4)
                    │   │   │   ├── "lineitem.l_suppkey"(#3.2)
                    │   │   │   ├── "part.p_name"(#1.1)
                    │   │   │   ├── "part.p_partkey"(#1.0)
                    │   │   │   ├── "partsupp.ps_partkey"(#4.0)
                    │   │   │   ├── "partsupp.ps_suppkey"(#4.1)
                    │   │   │   ├── "partsupp.ps_supplycost"(#4.3)
                    │   │   │   ├── "supplier.s_nationkey"(#2.3)
                    │   │   │   └── "supplier.s_suppkey"(#2.0)
                    │   │   ├── (.cardinality): 0.00
                    │   │   ├── Join
                    │   │   │   ├── .join_type: Inner
                    │   │   │   ├── .implementation: None
                    │   │   │   ├── .join_cond: ("supplier.s_suppkey"(#2.0) = "lineitem.l_suppkey"(#3.2)) AND ("part.p_partkey"(#1.0) = "lineitem.l_partkey"(#3.1))
                    │   │   │   ├── (.output_columns):
                    │   │   │   │   ┌── "lineitem.l_discount"(#3.6)
                    │   │   │   │   ├── "lineitem.l_extendedprice"(#3.5)
                    │   │   │   │   ├── "lineitem.l_orderkey"(#3.0)
                    │   │   │   │   ├── "lineitem.l_partkey"(#3.1)
                    │   │   │   │   ├── "lineitem.l_quantity"(#3.4)
                    │   │   │   │   ├── "lineitem.l_suppkey"(#3.2)
                    │   │   │   │   ├── "part.p_name"(#1.1)
                    │   │   │   │   ├── "part.p_partkey"(#1.0)
                    │   │   │   │   ├── "supplier.s_nationkey"(#2.3)
                    │   │   │   │   └── "supplier.s_suppkey"(#2.0)
                    │   │   │   ├── (.cardinality): 0.00
                    │   │   │   ├── Join
                    │   │   │   │   ├── .join_type: Inner
                    │   │   │   │   ├── .implementation: None
                    │   │   │   │   ├── .join_cond: true::boolean
                    │   │   │   │   ├── (.output_columns): [ "part.p_name"(#1.1), "part.p_partkey"(#1.0), "supplier.s_nationkey"(#2.3), "supplier.s_suppkey"(#2.0) ]
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
                    │   │   │   │       ├── .data_source_id: 4
                    │   │   │   │       ├── .table_index: 2
                    │   │   │   │       ├── .implementation: None
                    │   │   │   │       ├── (.output_columns): [ "supplier.s_nationkey"(#2.3), "supplier.s_suppkey"(#2.0) ]
                    │   │   │   │       └── (.cardinality): 0.00
                    │   │   │   └── Get
                    │   │   │       ├── .data_source_id: 8
                    │   │   │       ├── .table_index: 3
                    │   │   │       ├── .implementation: None
                    │   │   │       ├── (.output_columns):
                    │   │   │       │   ┌── "lineitem.l_discount"(#3.6)
                    │   │   │       │   ├── "lineitem.l_extendedprice"(#3.5)
                    │   │   │       │   ├── "lineitem.l_orderkey"(#3.0)
                    │   │   │       │   ├── "lineitem.l_partkey"(#3.1)
                    │   │   │       │   ├── "lineitem.l_quantity"(#3.4)
                    │   │   │       │   └── "lineitem.l_suppkey"(#3.2)
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

