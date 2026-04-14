-- TPC-H Q21
select
    s_name,
    count(*) as numwait
from
    supplier,
    lineitem l1,
    orders,
    nation
where
    s_suppkey = l1.l_suppkey
    and o_orderkey = l1.l_orderkey
    and o_orderstatus = 'F'
    and l1.l_receiptdate > l1.l_commitdate
    and exists (
        select
            *
        from
            lineitem l2
        where
            l2.l_orderkey = l1.l_orderkey
            and l2.l_suppkey <> l1.l_suppkey
    )
    and not exists (
        select
            *
        from
            lineitem l3
        where
            l3.l_orderkey = l1.l_orderkey
            and l3.l_suppkey <> l1.l_suppkey
            and l3.l_receiptdate > l3.l_commitdate
    )
    and s_nationkey = n_nationkey
    and n_name = 'SAUDI ARABIA'
group by
    s_name
order by
    numwait desc,
    s_name
limit 100;

/*
logical_plan after optd-initial:
Limit { .skip: 0::bigint, .fetch: 100::bigint, (.output_columns): [ "__#16.numwait"(#16.1), "__#16.s_name"(#16.0) ], (.cardinality): 0.00 }
└── OrderBy { ordering_exprs: [ "__#16.numwait"(#16.1) DESC, "__#16.s_name"(#16.0) ASC ], (.output_columns): [ "__#16.numwait"(#16.1), "__#16.s_name"(#16.0) ], (.cardinality): 0.00 }
    └── Project { .table_index: 16, .projections: [ "supplier.s_name"(#1.1), "__#15.count(Int64(1))"(#15.0) ], (.output_columns): [ "__#16.numwait"(#16.1), "__#16.s_name"(#16.0) ], (.cardinality): 0.00 }
        └── Aggregate { .key_table_index: 14, .aggregate_table_index: 15, .implementation: None, .exprs: count(1::bigint), .keys: "supplier.s_name"(#1.1), (.output_columns): [ "__#14.s_name"(#14.0), "__#15.count(Int64(1))"(#15.0) ], (.cardinality): 0.00 }
            └── Select
                ├── .predicate: ("supplier.s_suppkey"(#1.0) = "l1.l_suppkey"(#3.2)) AND ("orders.o_orderkey"(#4.0) = "l1.l_orderkey"(#3.0)) AND ("orders.o_orderstatus"(#4.2) = 'F'::utf8_view) AND ("l1.l_receiptdate"(#3.12) > "l1.l_commitdate"(#3.11)) AND ("supplier.s_nationkey"(#1.3) = "nation.n_nationkey"(#5.0)) AND ("nation.n_name"(#5.1) = 'SAUDI ARABIA'::utf8_view)
                ├── (.output_columns):
                │   ┌── "l1.l_comment"(#3.15)
                │   ├── "l1.l_commitdate"(#3.11)
                │   ├── "l1.l_discount"(#3.6)
                │   ├── "l1.l_extendedprice"(#3.5)
                │   ├── "l1.l_linenumber"(#3.3)
                │   ├── "l1.l_linestatus"(#3.9)
                │   ├── "l1.l_orderkey"(#3.0)
                │   ├── "l1.l_partkey"(#3.1)
                │   ├── "l1.l_quantity"(#3.4)
                │   ├── "l1.l_receiptdate"(#3.12)
                │   ├── "l1.l_returnflag"(#3.8)
                │   ├── "l1.l_shipdate"(#3.10)
                │   ├── "l1.l_shipinstruct"(#3.13)
                │   ├── "l1.l_shipmode"(#3.14)
                │   ├── "l1.l_suppkey"(#3.2)
                │   ├── "l1.l_tax"(#3.7)
                │   ├── "nation.n_comment"(#5.3)
                │   ├── "nation.n_name"(#5.1)
                │   ├── "nation.n_nationkey"(#5.0)
                │   ├── "nation.n_regionkey"(#5.2)
                │   ├── "orders.o_clerk"(#4.6)
                │   ├── "orders.o_comment"(#4.8)
                │   ├── "orders.o_custkey"(#4.1)
                │   ├── "orders.o_orderdate"(#4.4)
                │   ├── "orders.o_orderkey"(#4.0)
                │   ├── "orders.o_orderpriority"(#4.5)
                │   ├── "orders.o_orderstatus"(#4.2)
                │   ├── "orders.o_shippriority"(#4.7)
                │   ├── "orders.o_totalprice"(#4.3)
                │   ├── "supplier.s_acctbal"(#1.5)
                │   ├── "supplier.s_address"(#1.2)
                │   ├── "supplier.s_comment"(#1.6)
                │   ├── "supplier.s_name"(#1.1)
                │   ├── "supplier.s_nationkey"(#1.3)
                │   ├── "supplier.s_phone"(#1.4)
                │   └── "supplier.s_suppkey"(#1.0)
                ├── (.cardinality): 0.00
                └── Join
                    ├── .join_type: LeftAnti
                    ├── .implementation: None
                    ├── .join_cond: ("l1.l_orderkey"(#3.0) = "__correlated_sq_2.l_orderkey"(#13.0)) AND ("__correlated_sq_2.l_suppkey"(#13.2) != "l1.l_suppkey"(#3.2))
                    ├── (.output_columns):
                    │   ┌── "l1.l_comment"(#3.15)
                    │   ├── "l1.l_commitdate"(#3.11)
                    │   ├── "l1.l_discount"(#3.6)
                    │   ├── "l1.l_extendedprice"(#3.5)
                    │   ├── "l1.l_linenumber"(#3.3)
                    │   ├── "l1.l_linestatus"(#3.9)
                    │   ├── "l1.l_orderkey"(#3.0)
                    │   ├── "l1.l_partkey"(#3.1)
                    │   ├── "l1.l_quantity"(#3.4)
                    │   ├── "l1.l_receiptdate"(#3.12)
                    │   ├── "l1.l_returnflag"(#3.8)
                    │   ├── "l1.l_shipdate"(#3.10)
                    │   ├── "l1.l_shipinstruct"(#3.13)
                    │   ├── "l1.l_shipmode"(#3.14)
                    │   ├── "l1.l_suppkey"(#3.2)
                    │   ├── "l1.l_tax"(#3.7)
                    │   ├── "nation.n_comment"(#5.3)
                    │   ├── "nation.n_name"(#5.1)
                    │   ├── "nation.n_nationkey"(#5.0)
                    │   ├── "nation.n_regionkey"(#5.2)
                    │   ├── "orders.o_clerk"(#4.6)
                    │   ├── "orders.o_comment"(#4.8)
                    │   ├── "orders.o_custkey"(#4.1)
                    │   ├── "orders.o_orderdate"(#4.4)
                    │   ├── "orders.o_orderkey"(#4.0)
                    │   ├── "orders.o_orderpriority"(#4.5)
                    │   ├── "orders.o_orderstatus"(#4.2)
                    │   ├── "orders.o_shippriority"(#4.7)
                    │   ├── "orders.o_totalprice"(#4.3)
                    │   ├── "supplier.s_acctbal"(#1.5)
                    │   ├── "supplier.s_address"(#1.2)
                    │   ├── "supplier.s_comment"(#1.6)
                    │   ├── "supplier.s_name"(#1.1)
                    │   ├── "supplier.s_nationkey"(#1.3)
                    │   ├── "supplier.s_phone"(#1.4)
                    │   └── "supplier.s_suppkey"(#1.0)
                    ├── (.cardinality): 0.00
                    ├── Join
                    │   ├── .join_type: LeftSemi
                    │   ├── .implementation: None
                    │   ├── .join_cond: ("l1.l_orderkey"(#3.0) = "__correlated_sq_1.l_orderkey"(#9.0)) AND ("__correlated_sq_1.l_suppkey"(#9.2) != "l1.l_suppkey"(#3.2))
                    │   ├── (.output_columns):
                    │   │   ┌── "l1.l_comment"(#3.15)
                    │   │   ├── "l1.l_commitdate"(#3.11)
                    │   │   ├── "l1.l_discount"(#3.6)
                    │   │   ├── "l1.l_extendedprice"(#3.5)
                    │   │   ├── "l1.l_linenumber"(#3.3)
                    │   │   ├── "l1.l_linestatus"(#3.9)
                    │   │   ├── "l1.l_orderkey"(#3.0)
                    │   │   ├── "l1.l_partkey"(#3.1)
                    │   │   ├── "l1.l_quantity"(#3.4)
                    │   │   ├── "l1.l_receiptdate"(#3.12)
                    │   │   ├── "l1.l_returnflag"(#3.8)
                    │   │   ├── "l1.l_shipdate"(#3.10)
                    │   │   ├── "l1.l_shipinstruct"(#3.13)
                    │   │   ├── "l1.l_shipmode"(#3.14)
                    │   │   ├── "l1.l_suppkey"(#3.2)
                    │   │   ├── "l1.l_tax"(#3.7)
                    │   │   ├── "nation.n_comment"(#5.3)
                    │   │   ├── "nation.n_name"(#5.1)
                    │   │   ├── "nation.n_nationkey"(#5.0)
                    │   │   ├── "nation.n_regionkey"(#5.2)
                    │   │   ├── "orders.o_clerk"(#4.6)
                    │   │   ├── "orders.o_comment"(#4.8)
                    │   │   ├── "orders.o_custkey"(#4.1)
                    │   │   ├── "orders.o_orderdate"(#4.4)
                    │   │   ├── "orders.o_orderkey"(#4.0)
                    │   │   ├── "orders.o_orderpriority"(#4.5)
                    │   │   ├── "orders.o_orderstatus"(#4.2)
                    │   │   ├── "orders.o_shippriority"(#4.7)
                    │   │   ├── "orders.o_totalprice"(#4.3)
                    │   │   ├── "supplier.s_acctbal"(#1.5)
                    │   │   ├── "supplier.s_address"(#1.2)
                    │   │   ├── "supplier.s_comment"(#1.6)
                    │   │   ├── "supplier.s_name"(#1.1)
                    │   │   ├── "supplier.s_nationkey"(#1.3)
                    │   │   ├── "supplier.s_phone"(#1.4)
                    │   │   └── "supplier.s_suppkey"(#1.0)
                    │   ├── (.cardinality): 0.00
                    │   ├── Join
                    │   │   ├── .join_type: Inner
                    │   │   ├── .implementation: None
                    │   │   ├── .join_cond: 
                    │   │   ├── (.output_columns):
                    │   │   │   ┌── "l1.l_comment"(#3.15)
                    │   │   │   ├── "l1.l_commitdate"(#3.11)
                    │   │   │   ├── "l1.l_discount"(#3.6)
                    │   │   │   ├── "l1.l_extendedprice"(#3.5)
                    │   │   │   ├── "l1.l_linenumber"(#3.3)
                    │   │   │   ├── "l1.l_linestatus"(#3.9)
                    │   │   │   ├── "l1.l_orderkey"(#3.0)
                    │   │   │   ├── "l1.l_partkey"(#3.1)
                    │   │   │   ├── "l1.l_quantity"(#3.4)
                    │   │   │   ├── "l1.l_receiptdate"(#3.12)
                    │   │   │   ├── "l1.l_returnflag"(#3.8)
                    │   │   │   ├── "l1.l_shipdate"(#3.10)
                    │   │   │   ├── "l1.l_shipinstruct"(#3.13)
                    │   │   │   ├── "l1.l_shipmode"(#3.14)
                    │   │   │   ├── "l1.l_suppkey"(#3.2)
                    │   │   │   ├── "l1.l_tax"(#3.7)
                    │   │   │   ├── "nation.n_comment"(#5.3)
                    │   │   │   ├── "nation.n_name"(#5.1)
                    │   │   │   ├── "nation.n_nationkey"(#5.0)
                    │   │   │   ├── "nation.n_regionkey"(#5.2)
                    │   │   │   ├── "orders.o_clerk"(#4.6)
                    │   │   │   ├── "orders.o_comment"(#4.8)
                    │   │   │   ├── "orders.o_custkey"(#4.1)
                    │   │   │   ├── "orders.o_orderdate"(#4.4)
                    │   │   │   ├── "orders.o_orderkey"(#4.0)
                    │   │   │   ├── "orders.o_orderpriority"(#4.5)
                    │   │   │   ├── "orders.o_orderstatus"(#4.2)
                    │   │   │   ├── "orders.o_shippriority"(#4.7)
                    │   │   │   ├── "orders.o_totalprice"(#4.3)
                    │   │   │   ├── "supplier.s_acctbal"(#1.5)
                    │   │   │   ├── "supplier.s_address"(#1.2)
                    │   │   │   ├── "supplier.s_comment"(#1.6)
                    │   │   │   ├── "supplier.s_name"(#1.1)
                    │   │   │   ├── "supplier.s_nationkey"(#1.3)
                    │   │   │   ├── "supplier.s_phone"(#1.4)
                    │   │   │   └── "supplier.s_suppkey"(#1.0)
                    │   │   ├── (.cardinality): 0.00
                    │   │   ├── Join
                    │   │   │   ├── .join_type: Inner
                    │   │   │   ├── .implementation: None
                    │   │   │   ├── .join_cond: 
                    │   │   │   ├── (.output_columns):
                    │   │   │   │   ┌── "l1.l_comment"(#3.15)
                    │   │   │   │   ├── "l1.l_commitdate"(#3.11)
                    │   │   │   │   ├── "l1.l_discount"(#3.6)
                    │   │   │   │   ├── "l1.l_extendedprice"(#3.5)
                    │   │   │   │   ├── "l1.l_linenumber"(#3.3)
                    │   │   │   │   ├── "l1.l_linestatus"(#3.9)
                    │   │   │   │   ├── "l1.l_orderkey"(#3.0)
                    │   │   │   │   ├── "l1.l_partkey"(#3.1)
                    │   │   │   │   ├── "l1.l_quantity"(#3.4)
                    │   │   │   │   ├── "l1.l_receiptdate"(#3.12)
                    │   │   │   │   ├── "l1.l_returnflag"(#3.8)
                    │   │   │   │   ├── "l1.l_shipdate"(#3.10)
                    │   │   │   │   ├── "l1.l_shipinstruct"(#3.13)
                    │   │   │   │   ├── "l1.l_shipmode"(#3.14)
                    │   │   │   │   ├── "l1.l_suppkey"(#3.2)
                    │   │   │   │   ├── "l1.l_tax"(#3.7)
                    │   │   │   │   ├── "orders.o_clerk"(#4.6)
                    │   │   │   │   ├── "orders.o_comment"(#4.8)
                    │   │   │   │   ├── "orders.o_custkey"(#4.1)
                    │   │   │   │   ├── "orders.o_orderdate"(#4.4)
                    │   │   │   │   ├── "orders.o_orderkey"(#4.0)
                    │   │   │   │   ├── "orders.o_orderpriority"(#4.5)
                    │   │   │   │   ├── "orders.o_orderstatus"(#4.2)
                    │   │   │   │   ├── "orders.o_shippriority"(#4.7)
                    │   │   │   │   ├── "orders.o_totalprice"(#4.3)
                    │   │   │   │   ├── "supplier.s_acctbal"(#1.5)
                    │   │   │   │   ├── "supplier.s_address"(#1.2)
                    │   │   │   │   ├── "supplier.s_comment"(#1.6)
                    │   │   │   │   ├── "supplier.s_name"(#1.1)
                    │   │   │   │   ├── "supplier.s_nationkey"(#1.3)
                    │   │   │   │   ├── "supplier.s_phone"(#1.4)
                    │   │   │   │   └── "supplier.s_suppkey"(#1.0)
                    │   │   │   ├── (.cardinality): 0.00
                    │   │   │   ├── Join
                    │   │   │   │   ├── .join_type: Inner
                    │   │   │   │   ├── .implementation: None
                    │   │   │   │   ├── .join_cond: 
                    │   │   │   │   ├── (.output_columns):
                    │   │   │   │   │   ┌── "l1.l_comment"(#3.15)
                    │   │   │   │   │   ├── "l1.l_commitdate"(#3.11)
                    │   │   │   │   │   ├── "l1.l_discount"(#3.6)
                    │   │   │   │   │   ├── "l1.l_extendedprice"(#3.5)
                    │   │   │   │   │   ├── "l1.l_linenumber"(#3.3)
                    │   │   │   │   │   ├── "l1.l_linestatus"(#3.9)
                    │   │   │   │   │   ├── "l1.l_orderkey"(#3.0)
                    │   │   │   │   │   ├── "l1.l_partkey"(#3.1)
                    │   │   │   │   │   ├── "l1.l_quantity"(#3.4)
                    │   │   │   │   │   ├── "l1.l_receiptdate"(#3.12)
                    │   │   │   │   │   ├── "l1.l_returnflag"(#3.8)
                    │   │   │   │   │   ├── "l1.l_shipdate"(#3.10)
                    │   │   │   │   │   ├── "l1.l_shipinstruct"(#3.13)
                    │   │   │   │   │   ├── "l1.l_shipmode"(#3.14)
                    │   │   │   │   │   ├── "l1.l_suppkey"(#3.2)
                    │   │   │   │   │   ├── "l1.l_tax"(#3.7)
                    │   │   │   │   │   ├── "supplier.s_acctbal"(#1.5)
                    │   │   │   │   │   ├── "supplier.s_address"(#1.2)
                    │   │   │   │   │   ├── "supplier.s_comment"(#1.6)
                    │   │   │   │   │   ├── "supplier.s_name"(#1.1)
                    │   │   │   │   │   ├── "supplier.s_nationkey"(#1.3)
                    │   │   │   │   │   ├── "supplier.s_phone"(#1.4)
                    │   │   │   │   │   └── "supplier.s_suppkey"(#1.0)
                    │   │   │   │   ├── (.cardinality): 0.00
                    │   │   │   │   ├── Get { .data_source_id: 4, .table_index: 1, .implementation: None, (.output_columns): [ "supplier.s_acctbal"(#1.5), "supplier.s_address"(#1.2), "supplier.s_comment"(#1.6), "supplier.s_name"(#1.1), "supplier.s_nationkey"(#1.3), "supplier.s_phone"(#1.4), "supplier.s_suppkey"(#1.0) ], (.cardinality): 0.00 }
                    │   │   │   │   └── Remap
                    │   │   │   │       ├── .table_index: 3
                    │   │   │   │       ├── (.output_columns):
                    │   │   │   │       │   ┌── "l1.l_comment"(#3.15)
                    │   │   │   │       │   ├── "l1.l_commitdate"(#3.11)
                    │   │   │   │       │   ├── "l1.l_discount"(#3.6)
                    │   │   │   │       │   ├── "l1.l_extendedprice"(#3.5)
                    │   │   │   │       │   ├── "l1.l_linenumber"(#3.3)
                    │   │   │   │       │   ├── "l1.l_linestatus"(#3.9)
                    │   │   │   │       │   ├── "l1.l_orderkey"(#3.0)
                    │   │   │   │       │   ├── "l1.l_partkey"(#3.1)
                    │   │   │   │       │   ├── "l1.l_quantity"(#3.4)
                    │   │   │   │       │   ├── "l1.l_receiptdate"(#3.12)
                    │   │   │   │       │   ├── "l1.l_returnflag"(#3.8)
                    │   │   │   │       │   ├── "l1.l_shipdate"(#3.10)
                    │   │   │   │       │   ├── "l1.l_shipinstruct"(#3.13)
                    │   │   │   │       │   ├── "l1.l_shipmode"(#3.14)
                    │   │   │   │       │   ├── "l1.l_suppkey"(#3.2)
                    │   │   │   │       │   └── "l1.l_tax"(#3.7)
                    │   │   │   │       ├── (.cardinality): 0.00
                    │   │   │   │       └── Get
                    │   │   │   │           ├── .data_source_id: 8
                    │   │   │   │           ├── .table_index: 2
                    │   │   │   │           ├── .implementation: None
                    │   │   │   │           ├── (.output_columns):
                    │   │   │   │           │   ┌── "lineitem.l_comment"(#2.15)
                    │   │   │   │           │   ├── "lineitem.l_commitdate"(#2.11)
                    │   │   │   │           │   ├── "lineitem.l_discount"(#2.6)
                    │   │   │   │           │   ├── "lineitem.l_extendedprice"(#2.5)
                    │   │   │   │           │   ├── "lineitem.l_linenumber"(#2.3)
                    │   │   │   │           │   ├── "lineitem.l_linestatus"(#2.9)
                    │   │   │   │           │   ├── "lineitem.l_orderkey"(#2.0)
                    │   │   │   │           │   ├── "lineitem.l_partkey"(#2.1)
                    │   │   │   │           │   ├── "lineitem.l_quantity"(#2.4)
                    │   │   │   │           │   ├── "lineitem.l_receiptdate"(#2.12)
                    │   │   │   │           │   ├── "lineitem.l_returnflag"(#2.8)
                    │   │   │   │           │   ├── "lineitem.l_shipdate"(#2.10)
                    │   │   │   │           │   ├── "lineitem.l_shipinstruct"(#2.13)
                    │   │   │   │           │   ├── "lineitem.l_shipmode"(#2.14)
                    │   │   │   │           │   ├── "lineitem.l_suppkey"(#2.2)
                    │   │   │   │           │   └── "lineitem.l_tax"(#2.7)
                    │   │   │   │           └── (.cardinality): 0.00
                    │   │   │   └── Get
                    │   │   │       ├── .data_source_id: 7
                    │   │   │       ├── .table_index: 4
                    │   │   │       ├── .implementation: None
                    │   │   │       ├── (.output_columns): [ "orders.o_clerk"(#4.6), "orders.o_comment"(#4.8), "orders.o_custkey"(#4.1), "orders.o_orderdate"(#4.4), "orders.o_orderkey"(#4.0), "orders.o_orderpriority"(#4.5), "orders.o_orderstatus"(#4.2), "orders.o_shippriority"(#4.7), "orders.o_totalprice"(#4.3) ]
                    │   │   │       └── (.cardinality): 0.00
                    │   │   └── Get { .data_source_id: 1, .table_index: 5, .implementation: None, (.output_columns): [ "nation.n_comment"(#5.3), "nation.n_name"(#5.1), "nation.n_nationkey"(#5.0), "nation.n_regionkey"(#5.2) ], (.cardinality): 0.00 }
                    │   └── Remap
                    │       ├── .table_index: 9
                    │       ├── (.output_columns):
                    │       │   ┌── "__correlated_sq_1.l_comment"(#9.15)
                    │       │   ├── "__correlated_sq_1.l_commitdate"(#9.11)
                    │       │   ├── "__correlated_sq_1.l_discount"(#9.6)
                    │       │   ├── "__correlated_sq_1.l_extendedprice"(#9.5)
                    │       │   ├── "__correlated_sq_1.l_linenumber"(#9.3)
                    │       │   ├── "__correlated_sq_1.l_linestatus"(#9.9)
                    │       │   ├── "__correlated_sq_1.l_orderkey"(#9.0)
                    │       │   ├── "__correlated_sq_1.l_partkey"(#9.1)
                    │       │   ├── "__correlated_sq_1.l_quantity"(#9.4)
                    │       │   ├── "__correlated_sq_1.l_receiptdate"(#9.12)
                    │       │   ├── "__correlated_sq_1.l_returnflag"(#9.8)
                    │       │   ├── "__correlated_sq_1.l_shipdate"(#9.10)
                    │       │   ├── "__correlated_sq_1.l_shipinstruct"(#9.13)
                    │       │   ├── "__correlated_sq_1.l_shipmode"(#9.14)
                    │       │   ├── "__correlated_sq_1.l_suppkey"(#9.2)
                    │       │   └── "__correlated_sq_1.l_tax"(#9.7)
                    │       ├── (.cardinality): 0.00
                    │       └── Project
                    │           ├── .table_index: 8
                    │           ├── .projections:
                    │           │   ┌── "l2.l_orderkey"(#7.0)
                    │           │   ├── "l2.l_partkey"(#7.1)
                    │           │   ├── "l2.l_suppkey"(#7.2)
                    │           │   ├── "l2.l_linenumber"(#7.3)
                    │           │   ├── "l2.l_quantity"(#7.4)
                    │           │   ├── "l2.l_extendedprice"(#7.5)
                    │           │   ├── "l2.l_discount"(#7.6)
                    │           │   ├── "l2.l_tax"(#7.7)
                    │           │   ├── "l2.l_returnflag"(#7.8)
                    │           │   ├── "l2.l_linestatus"(#7.9)
                    │           │   ├── "l2.l_shipdate"(#7.10)
                    │           │   ├── "l2.l_commitdate"(#7.11)
                    │           │   ├── "l2.l_receiptdate"(#7.12)
                    │           │   ├── "l2.l_shipinstruct"(#7.13)
                    │           │   ├── "l2.l_shipmode"(#7.14)
                    │           │   └── "l2.l_comment"(#7.15)
                    │           ├── (.output_columns):
                    │           │   ┌── "__#8.l_comment"(#8.15)
                    │           │   ├── "__#8.l_commitdate"(#8.11)
                    │           │   ├── "__#8.l_discount"(#8.6)
                    │           │   ├── "__#8.l_extendedprice"(#8.5)
                    │           │   ├── "__#8.l_linenumber"(#8.3)
                    │           │   ├── "__#8.l_linestatus"(#8.9)
                    │           │   ├── "__#8.l_orderkey"(#8.0)
                    │           │   ├── "__#8.l_partkey"(#8.1)
                    │           │   ├── "__#8.l_quantity"(#8.4)
                    │           │   ├── "__#8.l_receiptdate"(#8.12)
                    │           │   ├── "__#8.l_returnflag"(#8.8)
                    │           │   ├── "__#8.l_shipdate"(#8.10)
                    │           │   ├── "__#8.l_shipinstruct"(#8.13)
                    │           │   ├── "__#8.l_shipmode"(#8.14)
                    │           │   ├── "__#8.l_suppkey"(#8.2)
                    │           │   └── "__#8.l_tax"(#8.7)
                    │           ├── (.cardinality): 0.00
                    │           └── Remap
                    │               ├── .table_index: 7
                    │               ├── (.output_columns):
                    │               │   ┌── "l2.l_comment"(#7.15)
                    │               │   ├── "l2.l_commitdate"(#7.11)
                    │               │   ├── "l2.l_discount"(#7.6)
                    │               │   ├── "l2.l_extendedprice"(#7.5)
                    │               │   ├── "l2.l_linenumber"(#7.3)
                    │               │   ├── "l2.l_linestatus"(#7.9)
                    │               │   ├── "l2.l_orderkey"(#7.0)
                    │               │   ├── "l2.l_partkey"(#7.1)
                    │               │   ├── "l2.l_quantity"(#7.4)
                    │               │   ├── "l2.l_receiptdate"(#7.12)
                    │               │   ├── "l2.l_returnflag"(#7.8)
                    │               │   ├── "l2.l_shipdate"(#7.10)
                    │               │   ├── "l2.l_shipinstruct"(#7.13)
                    │               │   ├── "l2.l_shipmode"(#7.14)
                    │               │   ├── "l2.l_suppkey"(#7.2)
                    │               │   └── "l2.l_tax"(#7.7)
                    │               ├── (.cardinality): 0.00
                    │               └── Get
                    │                   ├── .data_source_id: 8
                    │                   ├── .table_index: 6
                    │                   ├── .implementation: None
                    │                   ├── (.output_columns):
                    │                   │   ┌── "lineitem.l_comment"(#6.15)
                    │                   │   ├── "lineitem.l_commitdate"(#6.11)
                    │                   │   ├── "lineitem.l_discount"(#6.6)
                    │                   │   ├── "lineitem.l_extendedprice"(#6.5)
                    │                   │   ├── "lineitem.l_linenumber"(#6.3)
                    │                   │   ├── "lineitem.l_linestatus"(#6.9)
                    │                   │   ├── "lineitem.l_orderkey"(#6.0)
                    │                   │   ├── "lineitem.l_partkey"(#6.1)
                    │                   │   ├── "lineitem.l_quantity"(#6.4)
                    │                   │   ├── "lineitem.l_receiptdate"(#6.12)
                    │                   │   ├── "lineitem.l_returnflag"(#6.8)
                    │                   │   ├── "lineitem.l_shipdate"(#6.10)
                    │                   │   ├── "lineitem.l_shipinstruct"(#6.13)
                    │                   │   ├── "lineitem.l_shipmode"(#6.14)
                    │                   │   ├── "lineitem.l_suppkey"(#6.2)
                    │                   │   └── "lineitem.l_tax"(#6.7)
                    │                   └── (.cardinality): 0.00
                    └── Remap
                        ├── .table_index: 13
                        ├── (.output_columns):
                        │   ┌── "__correlated_sq_2.l_comment"(#13.15)
                        │   ├── "__correlated_sq_2.l_commitdate"(#13.11)
                        │   ├── "__correlated_sq_2.l_discount"(#13.6)
                        │   ├── "__correlated_sq_2.l_extendedprice"(#13.5)
                        │   ├── "__correlated_sq_2.l_linenumber"(#13.3)
                        │   ├── "__correlated_sq_2.l_linestatus"(#13.9)
                        │   ├── "__correlated_sq_2.l_orderkey"(#13.0)
                        │   ├── "__correlated_sq_2.l_partkey"(#13.1)
                        │   ├── "__correlated_sq_2.l_quantity"(#13.4)
                        │   ├── "__correlated_sq_2.l_receiptdate"(#13.12)
                        │   ├── "__correlated_sq_2.l_returnflag"(#13.8)
                        │   ├── "__correlated_sq_2.l_shipdate"(#13.10)
                        │   ├── "__correlated_sq_2.l_shipinstruct"(#13.13)
                        │   ├── "__correlated_sq_2.l_shipmode"(#13.14)
                        │   ├── "__correlated_sq_2.l_suppkey"(#13.2)
                        │   └── "__correlated_sq_2.l_tax"(#13.7)
                        ├── (.cardinality): 0.00
                        └── Project
                            ├── .table_index: 12
                            ├── .projections:
                            │   ┌── "l3.l_orderkey"(#11.0)
                            │   ├── "l3.l_partkey"(#11.1)
                            │   ├── "l3.l_suppkey"(#11.2)
                            │   ├── "l3.l_linenumber"(#11.3)
                            │   ├── "l3.l_quantity"(#11.4)
                            │   ├── "l3.l_extendedprice"(#11.5)
                            │   ├── "l3.l_discount"(#11.6)
                            │   ├── "l3.l_tax"(#11.7)
                            │   ├── "l3.l_returnflag"(#11.8)
                            │   ├── "l3.l_linestatus"(#11.9)
                            │   ├── "l3.l_shipdate"(#11.10)
                            │   ├── "l3.l_commitdate"(#11.11)
                            │   ├── "l3.l_receiptdate"(#11.12)
                            │   ├── "l3.l_shipinstruct"(#11.13)
                            │   ├── "l3.l_shipmode"(#11.14)
                            │   └── "l3.l_comment"(#11.15)
                            ├── (.output_columns):
                            │   ┌── "__#12.l_comment"(#12.15)
                            │   ├── "__#12.l_commitdate"(#12.11)
                            │   ├── "__#12.l_discount"(#12.6)
                            │   ├── "__#12.l_extendedprice"(#12.5)
                            │   ├── "__#12.l_linenumber"(#12.3)
                            │   ├── "__#12.l_linestatus"(#12.9)
                            │   ├── "__#12.l_orderkey"(#12.0)
                            │   ├── "__#12.l_partkey"(#12.1)
                            │   ├── "__#12.l_quantity"(#12.4)
                            │   ├── "__#12.l_receiptdate"(#12.12)
                            │   ├── "__#12.l_returnflag"(#12.8)
                            │   ├── "__#12.l_shipdate"(#12.10)
                            │   ├── "__#12.l_shipinstruct"(#12.13)
                            │   ├── "__#12.l_shipmode"(#12.14)
                            │   ├── "__#12.l_suppkey"(#12.2)
                            │   └── "__#12.l_tax"(#12.7)
                            ├── (.cardinality): 0.00
                            └── Select
                                ├── .predicate: "l3.l_receiptdate"(#11.12) > "l3.l_commitdate"(#11.11)
                                ├── (.output_columns):
                                │   ┌── "l3.l_comment"(#11.15)
                                │   ├── "l3.l_commitdate"(#11.11)
                                │   ├── "l3.l_discount"(#11.6)
                                │   ├── "l3.l_extendedprice"(#11.5)
                                │   ├── "l3.l_linenumber"(#11.3)
                                │   ├── "l3.l_linestatus"(#11.9)
                                │   ├── "l3.l_orderkey"(#11.0)
                                │   ├── "l3.l_partkey"(#11.1)
                                │   ├── "l3.l_quantity"(#11.4)
                                │   ├── "l3.l_receiptdate"(#11.12)
                                │   ├── "l3.l_returnflag"(#11.8)
                                │   ├── "l3.l_shipdate"(#11.10)
                                │   ├── "l3.l_shipinstruct"(#11.13)
                                │   ├── "l3.l_shipmode"(#11.14)
                                │   ├── "l3.l_suppkey"(#11.2)
                                │   └── "l3.l_tax"(#11.7)
                                ├── (.cardinality): 0.00
                                └── Remap
                                    ├── .table_index: 11
                                    ├── (.output_columns):
                                    │   ┌── "l3.l_comment"(#11.15)
                                    │   ├── "l3.l_commitdate"(#11.11)
                                    │   ├── "l3.l_discount"(#11.6)
                                    │   ├── "l3.l_extendedprice"(#11.5)
                                    │   ├── "l3.l_linenumber"(#11.3)
                                    │   ├── "l3.l_linestatus"(#11.9)
                                    │   ├── "l3.l_orderkey"(#11.0)
                                    │   ├── "l3.l_partkey"(#11.1)
                                    │   ├── "l3.l_quantity"(#11.4)
                                    │   ├── "l3.l_receiptdate"(#11.12)
                                    │   ├── "l3.l_returnflag"(#11.8)
                                    │   ├── "l3.l_shipdate"(#11.10)
                                    │   ├── "l3.l_shipinstruct"(#11.13)
                                    │   ├── "l3.l_shipmode"(#11.14)
                                    │   ├── "l3.l_suppkey"(#11.2)
                                    │   └── "l3.l_tax"(#11.7)
                                    ├── (.cardinality): 0.00
                                    └── Get
                                        ├── .data_source_id: 8
                                        ├── .table_index: 10
                                        ├── .implementation: None
                                        ├── (.output_columns):
                                        │   ┌── "lineitem.l_comment"(#10.15)
                                        │   ├── "lineitem.l_commitdate"(#10.11)
                                        │   ├── "lineitem.l_discount"(#10.6)
                                        │   ├── "lineitem.l_extendedprice"(#10.5)
                                        │   ├── "lineitem.l_linenumber"(#10.3)
                                        │   ├── "lineitem.l_linestatus"(#10.9)
                                        │   ├── "lineitem.l_orderkey"(#10.0)
                                        │   ├── "lineitem.l_partkey"(#10.1)
                                        │   ├── "lineitem.l_quantity"(#10.4)
                                        │   ├── "lineitem.l_receiptdate"(#10.12)
                                        │   ├── "lineitem.l_returnflag"(#10.8)
                                        │   ├── "lineitem.l_shipdate"(#10.10)
                                        │   ├── "lineitem.l_shipinstruct"(#10.13)
                                        │   ├── "lineitem.l_shipmode"(#10.14)
                                        │   ├── "lineitem.l_suppkey"(#10.2)
                                        │   └── "lineitem.l_tax"(#10.7)
                                        └── (.cardinality): 0.00

physical_plan after optd-finalized:
Limit { .skip: 0::bigint, .fetch: 100::bigint, (.output_columns): [ "__#16.numwait"(#16.1), "__#16.s_name"(#16.0) ], (.cardinality): 0.00 }
└── EnforcerSort { tuple_ordering: [(#16.1, Desc), (#16.0, Asc)], (.output_columns): [ "__#16.numwait"(#16.1), "__#16.s_name"(#16.0) ], (.cardinality): 0.00 }
    └── Project { .table_index: 16, .projections: [ "supplier.s_name"(#1.1), "__#15.count(Int64(1))"(#15.0) ], (.output_columns): [ "__#16.numwait"(#16.1), "__#16.s_name"(#16.0) ], (.cardinality): 0.00 }
        └── Aggregate { .key_table_index: 14, .aggregate_table_index: 15, .implementation: None, .exprs: count(1::bigint), .keys: "supplier.s_name"(#1.1), (.output_columns): [ "__#14.s_name"(#14.0), "__#15.count(Int64(1))"(#15.0) ], (.cardinality): 0.00 }
            └── Select
                ├── .predicate: ("supplier.s_suppkey"(#1.0) = "l1.l_suppkey"(#3.2)) AND ("orders.o_orderkey"(#4.0) = "l1.l_orderkey"(#3.0)) AND ("orders.o_orderstatus"(#4.2) = 'F'::utf8_view) AND ("l1.l_receiptdate"(#3.12) > "l1.l_commitdate"(#3.11)) AND ("supplier.s_nationkey"(#1.3) = "nation.n_nationkey"(#5.0)) AND ("nation.n_name"(#5.1) = 'SAUDI ARABIA'::utf8_view)
                ├── (.output_columns):
                │   ┌── "l1.l_comment"(#3.15)
                │   ├── "l1.l_commitdate"(#3.11)
                │   ├── "l1.l_discount"(#3.6)
                │   ├── "l1.l_extendedprice"(#3.5)
                │   ├── "l1.l_linenumber"(#3.3)
                │   ├── "l1.l_linestatus"(#3.9)
                │   ├── "l1.l_orderkey"(#3.0)
                │   ├── "l1.l_partkey"(#3.1)
                │   ├── "l1.l_quantity"(#3.4)
                │   ├── "l1.l_receiptdate"(#3.12)
                │   ├── "l1.l_returnflag"(#3.8)
                │   ├── "l1.l_shipdate"(#3.10)
                │   ├── "l1.l_shipinstruct"(#3.13)
                │   ├── "l1.l_shipmode"(#3.14)
                │   ├── "l1.l_suppkey"(#3.2)
                │   ├── "l1.l_tax"(#3.7)
                │   ├── "nation.n_name"(#5.1)
                │   ├── "nation.n_nationkey"(#5.0)
                │   ├── "orders.o_orderkey"(#4.0)
                │   ├── "orders.o_orderstatus"(#4.2)
                │   ├── "supplier.s_name"(#1.1)
                │   ├── "supplier.s_nationkey"(#1.3)
                │   └── "supplier.s_suppkey"(#1.0)
                ├── (.cardinality): 0.00
                └── Join
                    ├── .join_type: LeftAnti
                    ├── .implementation: None
                    ├── .join_cond: ("l1.l_orderkey"(#3.0) = "__correlated_sq_2.l_orderkey"(#13.0)) AND ("__correlated_sq_2.l_suppkey"(#13.2) != "l1.l_suppkey"(#3.2))
                    ├── (.output_columns):
                    │   ┌── "l1.l_comment"(#3.15)
                    │   ├── "l1.l_commitdate"(#3.11)
                    │   ├── "l1.l_discount"(#3.6)
                    │   ├── "l1.l_extendedprice"(#3.5)
                    │   ├── "l1.l_linenumber"(#3.3)
                    │   ├── "l1.l_linestatus"(#3.9)
                    │   ├── "l1.l_orderkey"(#3.0)
                    │   ├── "l1.l_partkey"(#3.1)
                    │   ├── "l1.l_quantity"(#3.4)
                    │   ├── "l1.l_receiptdate"(#3.12)
                    │   ├── "l1.l_returnflag"(#3.8)
                    │   ├── "l1.l_shipdate"(#3.10)
                    │   ├── "l1.l_shipinstruct"(#3.13)
                    │   ├── "l1.l_shipmode"(#3.14)
                    │   ├── "l1.l_suppkey"(#3.2)
                    │   ├── "l1.l_tax"(#3.7)
                    │   ├── "nation.n_name"(#5.1)
                    │   ├── "nation.n_nationkey"(#5.0)
                    │   ├── "orders.o_orderkey"(#4.0)
                    │   ├── "orders.o_orderstatus"(#4.2)
                    │   ├── "supplier.s_name"(#1.1)
                    │   ├── "supplier.s_nationkey"(#1.3)
                    │   └── "supplier.s_suppkey"(#1.0)
                    ├── (.cardinality): 0.00
                    ├── Join
                    │   ├── .join_type: LeftSemi
                    │   ├── .implementation: None
                    │   ├── .join_cond: ("l1.l_orderkey"(#3.0) = "__correlated_sq_1.l_orderkey"(#9.0)) AND ("__correlated_sq_1.l_suppkey"(#9.2) != "l1.l_suppkey"(#3.2))
                    │   ├── (.output_columns):
                    │   │   ┌── "l1.l_comment"(#3.15)
                    │   │   ├── "l1.l_commitdate"(#3.11)
                    │   │   ├── "l1.l_discount"(#3.6)
                    │   │   ├── "l1.l_extendedprice"(#3.5)
                    │   │   ├── "l1.l_linenumber"(#3.3)
                    │   │   ├── "l1.l_linestatus"(#3.9)
                    │   │   ├── "l1.l_orderkey"(#3.0)
                    │   │   ├── "l1.l_partkey"(#3.1)
                    │   │   ├── "l1.l_quantity"(#3.4)
                    │   │   ├── "l1.l_receiptdate"(#3.12)
                    │   │   ├── "l1.l_returnflag"(#3.8)
                    │   │   ├── "l1.l_shipdate"(#3.10)
                    │   │   ├── "l1.l_shipinstruct"(#3.13)
                    │   │   ├── "l1.l_shipmode"(#3.14)
                    │   │   ├── "l1.l_suppkey"(#3.2)
                    │   │   ├── "l1.l_tax"(#3.7)
                    │   │   ├── "nation.n_name"(#5.1)
                    │   │   ├── "nation.n_nationkey"(#5.0)
                    │   │   ├── "orders.o_orderkey"(#4.0)
                    │   │   ├── "orders.o_orderstatus"(#4.2)
                    │   │   ├── "supplier.s_name"(#1.1)
                    │   │   ├── "supplier.s_nationkey"(#1.3)
                    │   │   └── "supplier.s_suppkey"(#1.0)
                    │   ├── (.cardinality): 0.00
                    │   ├── Join
                    │   │   ├── .join_type: Inner
                    │   │   ├── .implementation: None
                    │   │   ├── .join_cond: true::boolean
                    │   │   ├── (.output_columns):
                    │   │   │   ┌── "l1.l_comment"(#3.15)
                    │   │   │   ├── "l1.l_commitdate"(#3.11)
                    │   │   │   ├── "l1.l_discount"(#3.6)
                    │   │   │   ├── "l1.l_extendedprice"(#3.5)
                    │   │   │   ├── "l1.l_linenumber"(#3.3)
                    │   │   │   ├── "l1.l_linestatus"(#3.9)
                    │   │   │   ├── "l1.l_orderkey"(#3.0)
                    │   │   │   ├── "l1.l_partkey"(#3.1)
                    │   │   │   ├── "l1.l_quantity"(#3.4)
                    │   │   │   ├── "l1.l_receiptdate"(#3.12)
                    │   │   │   ├── "l1.l_returnflag"(#3.8)
                    │   │   │   ├── "l1.l_shipdate"(#3.10)
                    │   │   │   ├── "l1.l_shipinstruct"(#3.13)
                    │   │   │   ├── "l1.l_shipmode"(#3.14)
                    │   │   │   ├── "l1.l_suppkey"(#3.2)
                    │   │   │   ├── "l1.l_tax"(#3.7)
                    │   │   │   ├── "nation.n_name"(#5.1)
                    │   │   │   ├── "nation.n_nationkey"(#5.0)
                    │   │   │   ├── "orders.o_orderkey"(#4.0)
                    │   │   │   ├── "orders.o_orderstatus"(#4.2)
                    │   │   │   ├── "supplier.s_name"(#1.1)
                    │   │   │   ├── "supplier.s_nationkey"(#1.3)
                    │   │   │   └── "supplier.s_suppkey"(#1.0)
                    │   │   ├── (.cardinality): 0.00
                    │   │   ├── Join
                    │   │   │   ├── .join_type: Inner
                    │   │   │   ├── .implementation: None
                    │   │   │   ├── .join_cond: true::boolean
                    │   │   │   ├── (.output_columns):
                    │   │   │   │   ┌── "l1.l_comment"(#3.15)
                    │   │   │   │   ├── "l1.l_commitdate"(#3.11)
                    │   │   │   │   ├── "l1.l_discount"(#3.6)
                    │   │   │   │   ├── "l1.l_extendedprice"(#3.5)
                    │   │   │   │   ├── "l1.l_linenumber"(#3.3)
                    │   │   │   │   ├── "l1.l_linestatus"(#3.9)
                    │   │   │   │   ├── "l1.l_orderkey"(#3.0)
                    │   │   │   │   ├── "l1.l_partkey"(#3.1)
                    │   │   │   │   ├── "l1.l_quantity"(#3.4)
                    │   │   │   │   ├── "l1.l_receiptdate"(#3.12)
                    │   │   │   │   ├── "l1.l_returnflag"(#3.8)
                    │   │   │   │   ├── "l1.l_shipdate"(#3.10)
                    │   │   │   │   ├── "l1.l_shipinstruct"(#3.13)
                    │   │   │   │   ├── "l1.l_shipmode"(#3.14)
                    │   │   │   │   ├── "l1.l_suppkey"(#3.2)
                    │   │   │   │   ├── "l1.l_tax"(#3.7)
                    │   │   │   │   ├── "orders.o_orderkey"(#4.0)
                    │   │   │   │   ├── "orders.o_orderstatus"(#4.2)
                    │   │   │   │   ├── "supplier.s_name"(#1.1)
                    │   │   │   │   ├── "supplier.s_nationkey"(#1.3)
                    │   │   │   │   └── "supplier.s_suppkey"(#1.0)
                    │   │   │   ├── (.cardinality): 0.00
                    │   │   │   ├── Join
                    │   │   │   │   ├── .join_type: Inner
                    │   │   │   │   ├── .implementation: None
                    │   │   │   │   ├── .join_cond: true::boolean
                    │   │   │   │   ├── (.output_columns):
                    │   │   │   │   │   ┌── "l1.l_comment"(#3.15)
                    │   │   │   │   │   ├── "l1.l_commitdate"(#3.11)
                    │   │   │   │   │   ├── "l1.l_discount"(#3.6)
                    │   │   │   │   │   ├── "l1.l_extendedprice"(#3.5)
                    │   │   │   │   │   ├── "l1.l_linenumber"(#3.3)
                    │   │   │   │   │   ├── "l1.l_linestatus"(#3.9)
                    │   │   │   │   │   ├── "l1.l_orderkey"(#3.0)
                    │   │   │   │   │   ├── "l1.l_partkey"(#3.1)
                    │   │   │   │   │   ├── "l1.l_quantity"(#3.4)
                    │   │   │   │   │   ├── "l1.l_receiptdate"(#3.12)
                    │   │   │   │   │   ├── "l1.l_returnflag"(#3.8)
                    │   │   │   │   │   ├── "l1.l_shipdate"(#3.10)
                    │   │   │   │   │   ├── "l1.l_shipinstruct"(#3.13)
                    │   │   │   │   │   ├── "l1.l_shipmode"(#3.14)
                    │   │   │   │   │   ├── "l1.l_suppkey"(#3.2)
                    │   │   │   │   │   ├── "l1.l_tax"(#3.7)
                    │   │   │   │   │   ├── "supplier.s_name"(#1.1)
                    │   │   │   │   │   ├── "supplier.s_nationkey"(#1.3)
                    │   │   │   │   │   └── "supplier.s_suppkey"(#1.0)
                    │   │   │   │   ├── (.cardinality): 0.00
                    │   │   │   │   ├── Get { .data_source_id: 4, .table_index: 1, .implementation: None, (.output_columns): [ "supplier.s_name"(#1.1), "supplier.s_nationkey"(#1.3), "supplier.s_suppkey"(#1.0) ], (.cardinality): 0.00 }
                    │   │   │   │   └── Remap
                    │   │   │   │       ├── .table_index: 3
                    │   │   │   │       ├── (.output_columns):
                    │   │   │   │       │   ┌── "l1.l_comment"(#3.15)
                    │   │   │   │       │   ├── "l1.l_commitdate"(#3.11)
                    │   │   │   │       │   ├── "l1.l_discount"(#3.6)
                    │   │   │   │       │   ├── "l1.l_extendedprice"(#3.5)
                    │   │   │   │       │   ├── "l1.l_linenumber"(#3.3)
                    │   │   │   │       │   ├── "l1.l_linestatus"(#3.9)
                    │   │   │   │       │   ├── "l1.l_orderkey"(#3.0)
                    │   │   │   │       │   ├── "l1.l_partkey"(#3.1)
                    │   │   │   │       │   ├── "l1.l_quantity"(#3.4)
                    │   │   │   │       │   ├── "l1.l_receiptdate"(#3.12)
                    │   │   │   │       │   ├── "l1.l_returnflag"(#3.8)
                    │   │   │   │       │   ├── "l1.l_shipdate"(#3.10)
                    │   │   │   │       │   ├── "l1.l_shipinstruct"(#3.13)
                    │   │   │   │       │   ├── "l1.l_shipmode"(#3.14)
                    │   │   │   │       │   ├── "l1.l_suppkey"(#3.2)
                    │   │   │   │       │   └── "l1.l_tax"(#3.7)
                    │   │   │   │       ├── (.cardinality): 0.00
                    │   │   │   │       └── Get
                    │   │   │   │           ├── .data_source_id: 8
                    │   │   │   │           ├── .table_index: 2
                    │   │   │   │           ├── .implementation: None
                    │   │   │   │           ├── (.output_columns):
                    │   │   │   │           │   ┌── "lineitem.l_comment"(#2.15)
                    │   │   │   │           │   ├── "lineitem.l_commitdate"(#2.11)
                    │   │   │   │           │   ├── "lineitem.l_discount"(#2.6)
                    │   │   │   │           │   ├── "lineitem.l_extendedprice"(#2.5)
                    │   │   │   │           │   ├── "lineitem.l_linenumber"(#2.3)
                    │   │   │   │           │   ├── "lineitem.l_linestatus"(#2.9)
                    │   │   │   │           │   ├── "lineitem.l_orderkey"(#2.0)
                    │   │   │   │           │   ├── "lineitem.l_partkey"(#2.1)
                    │   │   │   │           │   ├── "lineitem.l_quantity"(#2.4)
                    │   │   │   │           │   ├── "lineitem.l_receiptdate"(#2.12)
                    │   │   │   │           │   ├── "lineitem.l_returnflag"(#2.8)
                    │   │   │   │           │   ├── "lineitem.l_shipdate"(#2.10)
                    │   │   │   │           │   ├── "lineitem.l_shipinstruct"(#2.13)
                    │   │   │   │           │   ├── "lineitem.l_shipmode"(#2.14)
                    │   │   │   │           │   ├── "lineitem.l_suppkey"(#2.2)
                    │   │   │   │           │   └── "lineitem.l_tax"(#2.7)
                    │   │   │   │           └── (.cardinality): 0.00
                    │   │   │   └── Get { .data_source_id: 7, .table_index: 4, .implementation: None, (.output_columns): [ "orders.o_orderkey"(#4.0), "orders.o_orderstatus"(#4.2) ], (.cardinality): 0.00 }
                    │   │   └── Get { .data_source_id: 1, .table_index: 5, .implementation: None, (.output_columns): [ "nation.n_name"(#5.1), "nation.n_nationkey"(#5.0) ], (.cardinality): 0.00 }
                    │   └── Remap
                    │       ├── .table_index: 9
                    │       ├── (.output_columns):
                    │       │   ┌── "__correlated_sq_1.l_comment"(#9.15)
                    │       │   ├── "__correlated_sq_1.l_commitdate"(#9.11)
                    │       │   ├── "__correlated_sq_1.l_discount"(#9.6)
                    │       │   ├── "__correlated_sq_1.l_extendedprice"(#9.5)
                    │       │   ├── "__correlated_sq_1.l_linenumber"(#9.3)
                    │       │   ├── "__correlated_sq_1.l_linestatus"(#9.9)
                    │       │   ├── "__correlated_sq_1.l_orderkey"(#9.0)
                    │       │   ├── "__correlated_sq_1.l_partkey"(#9.1)
                    │       │   ├── "__correlated_sq_1.l_quantity"(#9.4)
                    │       │   ├── "__correlated_sq_1.l_receiptdate"(#9.12)
                    │       │   ├── "__correlated_sq_1.l_returnflag"(#9.8)
                    │       │   ├── "__correlated_sq_1.l_shipdate"(#9.10)
                    │       │   ├── "__correlated_sq_1.l_shipinstruct"(#9.13)
                    │       │   ├── "__correlated_sq_1.l_shipmode"(#9.14)
                    │       │   ├── "__correlated_sq_1.l_suppkey"(#9.2)
                    │       │   └── "__correlated_sq_1.l_tax"(#9.7)
                    │       ├── (.cardinality): 0.00
                    │       └── Project
                    │           ├── .table_index: 8
                    │           ├── .projections:
                    │           │   ┌── "l2.l_orderkey"(#7.0)
                    │           │   ├── "l2.l_partkey"(#7.1)
                    │           │   ├── "l2.l_suppkey"(#7.2)
                    │           │   ├── "l2.l_linenumber"(#7.3)
                    │           │   ├── "l2.l_quantity"(#7.4)
                    │           │   ├── "l2.l_extendedprice"(#7.5)
                    │           │   ├── "l2.l_discount"(#7.6)
                    │           │   ├── "l2.l_tax"(#7.7)
                    │           │   ├── "l2.l_returnflag"(#7.8)
                    │           │   ├── "l2.l_linestatus"(#7.9)
                    │           │   ├── "l2.l_shipdate"(#7.10)
                    │           │   ├── "l2.l_commitdate"(#7.11)
                    │           │   ├── "l2.l_receiptdate"(#7.12)
                    │           │   ├── "l2.l_shipinstruct"(#7.13)
                    │           │   ├── "l2.l_shipmode"(#7.14)
                    │           │   └── "l2.l_comment"(#7.15)
                    │           ├── (.output_columns):
                    │           │   ┌── "__#8.l_comment"(#8.15)
                    │           │   ├── "__#8.l_commitdate"(#8.11)
                    │           │   ├── "__#8.l_discount"(#8.6)
                    │           │   ├── "__#8.l_extendedprice"(#8.5)
                    │           │   ├── "__#8.l_linenumber"(#8.3)
                    │           │   ├── "__#8.l_linestatus"(#8.9)
                    │           │   ├── "__#8.l_orderkey"(#8.0)
                    │           │   ├── "__#8.l_partkey"(#8.1)
                    │           │   ├── "__#8.l_quantity"(#8.4)
                    │           │   ├── "__#8.l_receiptdate"(#8.12)
                    │           │   ├── "__#8.l_returnflag"(#8.8)
                    │           │   ├── "__#8.l_shipdate"(#8.10)
                    │           │   ├── "__#8.l_shipinstruct"(#8.13)
                    │           │   ├── "__#8.l_shipmode"(#8.14)
                    │           │   ├── "__#8.l_suppkey"(#8.2)
                    │           │   └── "__#8.l_tax"(#8.7)
                    │           ├── (.cardinality): 0.00
                    │           └── Remap
                    │               ├── .table_index: 7
                    │               ├── (.output_columns):
                    │               │   ┌── "l2.l_comment"(#7.15)
                    │               │   ├── "l2.l_commitdate"(#7.11)
                    │               │   ├── "l2.l_discount"(#7.6)
                    │               │   ├── "l2.l_extendedprice"(#7.5)
                    │               │   ├── "l2.l_linenumber"(#7.3)
                    │               │   ├── "l2.l_linestatus"(#7.9)
                    │               │   ├── "l2.l_orderkey"(#7.0)
                    │               │   ├── "l2.l_partkey"(#7.1)
                    │               │   ├── "l2.l_quantity"(#7.4)
                    │               │   ├── "l2.l_receiptdate"(#7.12)
                    │               │   ├── "l2.l_returnflag"(#7.8)
                    │               │   ├── "l2.l_shipdate"(#7.10)
                    │               │   ├── "l2.l_shipinstruct"(#7.13)
                    │               │   ├── "l2.l_shipmode"(#7.14)
                    │               │   ├── "l2.l_suppkey"(#7.2)
                    │               │   └── "l2.l_tax"(#7.7)
                    │               ├── (.cardinality): 0.00
                    │               └── Get
                    │                   ├── .data_source_id: 8
                    │                   ├── .table_index: 6
                    │                   ├── .implementation: None
                    │                   ├── (.output_columns):
                    │                   │   ┌── "lineitem.l_comment"(#6.15)
                    │                   │   ├── "lineitem.l_commitdate"(#6.11)
                    │                   │   ├── "lineitem.l_discount"(#6.6)
                    │                   │   ├── "lineitem.l_extendedprice"(#6.5)
                    │                   │   ├── "lineitem.l_linenumber"(#6.3)
                    │                   │   ├── "lineitem.l_linestatus"(#6.9)
                    │                   │   ├── "lineitem.l_orderkey"(#6.0)
                    │                   │   ├── "lineitem.l_partkey"(#6.1)
                    │                   │   ├── "lineitem.l_quantity"(#6.4)
                    │                   │   ├── "lineitem.l_receiptdate"(#6.12)
                    │                   │   ├── "lineitem.l_returnflag"(#6.8)
                    │                   │   ├── "lineitem.l_shipdate"(#6.10)
                    │                   │   ├── "lineitem.l_shipinstruct"(#6.13)
                    │                   │   ├── "lineitem.l_shipmode"(#6.14)
                    │                   │   ├── "lineitem.l_suppkey"(#6.2)
                    │                   │   └── "lineitem.l_tax"(#6.7)
                    │                   └── (.cardinality): 0.00
                    └── Remap
                        ├── .table_index: 13
                        ├── (.output_columns):
                        │   ┌── "__correlated_sq_2.l_comment"(#13.15)
                        │   ├── "__correlated_sq_2.l_commitdate"(#13.11)
                        │   ├── "__correlated_sq_2.l_discount"(#13.6)
                        │   ├── "__correlated_sq_2.l_extendedprice"(#13.5)
                        │   ├── "__correlated_sq_2.l_linenumber"(#13.3)
                        │   ├── "__correlated_sq_2.l_linestatus"(#13.9)
                        │   ├── "__correlated_sq_2.l_orderkey"(#13.0)
                        │   ├── "__correlated_sq_2.l_partkey"(#13.1)
                        │   ├── "__correlated_sq_2.l_quantity"(#13.4)
                        │   ├── "__correlated_sq_2.l_receiptdate"(#13.12)
                        │   ├── "__correlated_sq_2.l_returnflag"(#13.8)
                        │   ├── "__correlated_sq_2.l_shipdate"(#13.10)
                        │   ├── "__correlated_sq_2.l_shipinstruct"(#13.13)
                        │   ├── "__correlated_sq_2.l_shipmode"(#13.14)
                        │   ├── "__correlated_sq_2.l_suppkey"(#13.2)
                        │   └── "__correlated_sq_2.l_tax"(#13.7)
                        ├── (.cardinality): 0.00
                        └── Project
                            ├── .table_index: 12
                            ├── .projections:
                            │   ┌── "l3.l_orderkey"(#11.0)
                            │   ├── "l3.l_partkey"(#11.1)
                            │   ├── "l3.l_suppkey"(#11.2)
                            │   ├── "l3.l_linenumber"(#11.3)
                            │   ├── "l3.l_quantity"(#11.4)
                            │   ├── "l3.l_extendedprice"(#11.5)
                            │   ├── "l3.l_discount"(#11.6)
                            │   ├── "l3.l_tax"(#11.7)
                            │   ├── "l3.l_returnflag"(#11.8)
                            │   ├── "l3.l_linestatus"(#11.9)
                            │   ├── "l3.l_shipdate"(#11.10)
                            │   ├── "l3.l_commitdate"(#11.11)
                            │   ├── "l3.l_receiptdate"(#11.12)
                            │   ├── "l3.l_shipinstruct"(#11.13)
                            │   ├── "l3.l_shipmode"(#11.14)
                            │   └── "l3.l_comment"(#11.15)
                            ├── (.output_columns):
                            │   ┌── "__#12.l_comment"(#12.15)
                            │   ├── "__#12.l_commitdate"(#12.11)
                            │   ├── "__#12.l_discount"(#12.6)
                            │   ├── "__#12.l_extendedprice"(#12.5)
                            │   ├── "__#12.l_linenumber"(#12.3)
                            │   ├── "__#12.l_linestatus"(#12.9)
                            │   ├── "__#12.l_orderkey"(#12.0)
                            │   ├── "__#12.l_partkey"(#12.1)
                            │   ├── "__#12.l_quantity"(#12.4)
                            │   ├── "__#12.l_receiptdate"(#12.12)
                            │   ├── "__#12.l_returnflag"(#12.8)
                            │   ├── "__#12.l_shipdate"(#12.10)
                            │   ├── "__#12.l_shipinstruct"(#12.13)
                            │   ├── "__#12.l_shipmode"(#12.14)
                            │   ├── "__#12.l_suppkey"(#12.2)
                            │   └── "__#12.l_tax"(#12.7)
                            ├── (.cardinality): 0.00
                            └── Select
                                ├── .predicate: "l3.l_receiptdate"(#11.12) > "l3.l_commitdate"(#11.11)
                                ├── (.output_columns):
                                │   ┌── "l3.l_comment"(#11.15)
                                │   ├── "l3.l_commitdate"(#11.11)
                                │   ├── "l3.l_discount"(#11.6)
                                │   ├── "l3.l_extendedprice"(#11.5)
                                │   ├── "l3.l_linenumber"(#11.3)
                                │   ├── "l3.l_linestatus"(#11.9)
                                │   ├── "l3.l_orderkey"(#11.0)
                                │   ├── "l3.l_partkey"(#11.1)
                                │   ├── "l3.l_quantity"(#11.4)
                                │   ├── "l3.l_receiptdate"(#11.12)
                                │   ├── "l3.l_returnflag"(#11.8)
                                │   ├── "l3.l_shipdate"(#11.10)
                                │   ├── "l3.l_shipinstruct"(#11.13)
                                │   ├── "l3.l_shipmode"(#11.14)
                                │   ├── "l3.l_suppkey"(#11.2)
                                │   └── "l3.l_tax"(#11.7)
                                ├── (.cardinality): 0.00
                                └── Remap
                                    ├── .table_index: 11
                                    ├── (.output_columns):
                                    │   ┌── "l3.l_comment"(#11.15)
                                    │   ├── "l3.l_commitdate"(#11.11)
                                    │   ├── "l3.l_discount"(#11.6)
                                    │   ├── "l3.l_extendedprice"(#11.5)
                                    │   ├── "l3.l_linenumber"(#11.3)
                                    │   ├── "l3.l_linestatus"(#11.9)
                                    │   ├── "l3.l_orderkey"(#11.0)
                                    │   ├── "l3.l_partkey"(#11.1)
                                    │   ├── "l3.l_quantity"(#11.4)
                                    │   ├── "l3.l_receiptdate"(#11.12)
                                    │   ├── "l3.l_returnflag"(#11.8)
                                    │   ├── "l3.l_shipdate"(#11.10)
                                    │   ├── "l3.l_shipinstruct"(#11.13)
                                    │   ├── "l3.l_shipmode"(#11.14)
                                    │   ├── "l3.l_suppkey"(#11.2)
                                    │   └── "l3.l_tax"(#11.7)
                                    ├── (.cardinality): 0.00
                                    └── Get
                                        ├── .data_source_id: 8
                                        ├── .table_index: 10
                                        ├── .implementation: None
                                        ├── (.output_columns):
                                        │   ┌── "lineitem.l_comment"(#10.15)
                                        │   ├── "lineitem.l_commitdate"(#10.11)
                                        │   ├── "lineitem.l_discount"(#10.6)
                                        │   ├── "lineitem.l_extendedprice"(#10.5)
                                        │   ├── "lineitem.l_linenumber"(#10.3)
                                        │   ├── "lineitem.l_linestatus"(#10.9)
                                        │   ├── "lineitem.l_orderkey"(#10.0)
                                        │   ├── "lineitem.l_partkey"(#10.1)
                                        │   ├── "lineitem.l_quantity"(#10.4)
                                        │   ├── "lineitem.l_receiptdate"(#10.12)
                                        │   ├── "lineitem.l_returnflag"(#10.8)
                                        │   ├── "lineitem.l_shipdate"(#10.10)
                                        │   ├── "lineitem.l_shipinstruct"(#10.13)
                                        │   ├── "lineitem.l_shipmode"(#10.14)
                                        │   ├── "lineitem.l_suppkey"(#10.2)
                                        │   └── "lineitem.l_tax"(#10.7)
                                        └── (.cardinality): 0.00
*/

