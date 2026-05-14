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
Limit { .skip: 0::bigint, .fetch: 100::bigint, (.output_columns): [ "__#14.numwait"(#14.1), "__#14.s_name"(#14.0) ], (.cardinality): 0.00 }
└── OrderBy { ordering_exprs: [ "__#14.numwait"(#14.1) DESC, "__#14.s_name"(#14.0) ASC ], (.output_columns): [ "__#14.numwait"(#14.1), "__#14.s_name"(#14.0) ], (.cardinality): 0.00 }
    └── Project { .table_index: 14, .projections: [ "supplier.s_name"(#1.1), "__#13.count(Int64(1))"(#13.0) ], (.output_columns): [ "__#14.numwait"(#14.1), "__#14.s_name"(#14.0) ], (.cardinality): 0.00 }
        └── Aggregate { .key_table_index: 12, .aggregate_table_index: 13, .implementation: None, .exprs: count(1::bigint), .keys: "supplier.s_name"(#1.1), (.output_columns): [ "__#12.s_name"(#12.0), "__#13.count(Int64(1))"(#13.0) ], (.cardinality): 0.00 }
            └── Select
                ├── .predicate: ("supplier.s_nationkey"(#1.3) = "nation.n_nationkey"(#5.0)) AND ("nation.n_name"(#5.1) = CAST ('SAUDI ARABIA'::utf8 AS Utf8View))
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
                └── DependentJoin
                    ├── .join_type: LeftAnti
                    ├── .join_cond: true::boolean
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
                    ├── DependentJoin
                    │   ├── .join_type: LeftSemi
                    │   ├── .join_cond: true::boolean
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
                    │   ├── Select
                    │   │   ├── .predicate: ("supplier.s_suppkey"(#1.0) = "l1.l_suppkey"(#3.2)) AND ("orders.o_orderkey"(#4.0) = "l1.l_orderkey"(#3.0)) AND ("orders.o_orderstatus"(#4.2) = CAST ('F'::utf8 AS Utf8View)) AND ("l1.l_receiptdate"(#3.12) > "l1.l_commitdate"(#3.11))
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
                    │   │   └── Join
                    │   │       ├── .join_type: Inner
                    │   │       ├── .implementation: None
                    │   │       ├── .join_cond: 
                    │   │       ├── (.output_columns):
                    │   │       │   ┌── "l1.l_comment"(#3.15)
                    │   │       │   ├── "l1.l_commitdate"(#3.11)
                    │   │       │   ├── "l1.l_discount"(#3.6)
                    │   │       │   ├── "l1.l_extendedprice"(#3.5)
                    │   │       │   ├── "l1.l_linenumber"(#3.3)
                    │   │       │   ├── "l1.l_linestatus"(#3.9)
                    │   │       │   ├── "l1.l_orderkey"(#3.0)
                    │   │       │   ├── "l1.l_partkey"(#3.1)
                    │   │       │   ├── "l1.l_quantity"(#3.4)
                    │   │       │   ├── "l1.l_receiptdate"(#3.12)
                    │   │       │   ├── "l1.l_returnflag"(#3.8)
                    │   │       │   ├── "l1.l_shipdate"(#3.10)
                    │   │       │   ├── "l1.l_shipinstruct"(#3.13)
                    │   │       │   ├── "l1.l_shipmode"(#3.14)
                    │   │       │   ├── "l1.l_suppkey"(#3.2)
                    │   │       │   ├── "l1.l_tax"(#3.7)
                    │   │       │   ├── "nation.n_comment"(#5.3)
                    │   │       │   ├── "nation.n_name"(#5.1)
                    │   │       │   ├── "nation.n_nationkey"(#5.0)
                    │   │       │   ├── "nation.n_regionkey"(#5.2)
                    │   │       │   ├── "orders.o_clerk"(#4.6)
                    │   │       │   ├── "orders.o_comment"(#4.8)
                    │   │       │   ├── "orders.o_custkey"(#4.1)
                    │   │       │   ├── "orders.o_orderdate"(#4.4)
                    │   │       │   ├── "orders.o_orderkey"(#4.0)
                    │   │       │   ├── "orders.o_orderpriority"(#4.5)
                    │   │       │   ├── "orders.o_orderstatus"(#4.2)
                    │   │       │   ├── "orders.o_shippriority"(#4.7)
                    │   │       │   ├── "orders.o_totalprice"(#4.3)
                    │   │       │   ├── "supplier.s_acctbal"(#1.5)
                    │   │       │   ├── "supplier.s_address"(#1.2)
                    │   │       │   ├── "supplier.s_comment"(#1.6)
                    │   │       │   ├── "supplier.s_name"(#1.1)
                    │   │       │   ├── "supplier.s_nationkey"(#1.3)
                    │   │       │   ├── "supplier.s_phone"(#1.4)
                    │   │       │   └── "supplier.s_suppkey"(#1.0)
                    │   │       ├── (.cardinality): 0.00
                    │   │       ├── Join
                    │   │       │   ├── .join_type: Inner
                    │   │       │   ├── .implementation: None
                    │   │       │   ├── .join_cond: 
                    │   │       │   ├── (.output_columns):
                    │   │       │   │   ┌── "l1.l_comment"(#3.15)
                    │   │       │   │   ├── "l1.l_commitdate"(#3.11)
                    │   │       │   │   ├── "l1.l_discount"(#3.6)
                    │   │       │   │   ├── "l1.l_extendedprice"(#3.5)
                    │   │       │   │   ├── "l1.l_linenumber"(#3.3)
                    │   │       │   │   ├── "l1.l_linestatus"(#3.9)
                    │   │       │   │   ├── "l1.l_orderkey"(#3.0)
                    │   │       │   │   ├── "l1.l_partkey"(#3.1)
                    │   │       │   │   ├── "l1.l_quantity"(#3.4)
                    │   │       │   │   ├── "l1.l_receiptdate"(#3.12)
                    │   │       │   │   ├── "l1.l_returnflag"(#3.8)
                    │   │       │   │   ├── "l1.l_shipdate"(#3.10)
                    │   │       │   │   ├── "l1.l_shipinstruct"(#3.13)
                    │   │       │   │   ├── "l1.l_shipmode"(#3.14)
                    │   │       │   │   ├── "l1.l_suppkey"(#3.2)
                    │   │       │   │   ├── "l1.l_tax"(#3.7)
                    │   │       │   │   ├── "orders.o_clerk"(#4.6)
                    │   │       │   │   ├── "orders.o_comment"(#4.8)
                    │   │       │   │   ├── "orders.o_custkey"(#4.1)
                    │   │       │   │   ├── "orders.o_orderdate"(#4.4)
                    │   │       │   │   ├── "orders.o_orderkey"(#4.0)
                    │   │       │   │   ├── "orders.o_orderpriority"(#4.5)
                    │   │       │   │   ├── "orders.o_orderstatus"(#4.2)
                    │   │       │   │   ├── "orders.o_shippriority"(#4.7)
                    │   │       │   │   ├── "orders.o_totalprice"(#4.3)
                    │   │       │   │   ├── "supplier.s_acctbal"(#1.5)
                    │   │       │   │   ├── "supplier.s_address"(#1.2)
                    │   │       │   │   ├── "supplier.s_comment"(#1.6)
                    │   │       │   │   ├── "supplier.s_name"(#1.1)
                    │   │       │   │   ├── "supplier.s_nationkey"(#1.3)
                    │   │       │   │   ├── "supplier.s_phone"(#1.4)
                    │   │       │   │   └── "supplier.s_suppkey"(#1.0)
                    │   │       │   ├── (.cardinality): 0.00
                    │   │       │   ├── Join
                    │   │       │   │   ├── .join_type: Inner
                    │   │       │   │   ├── .implementation: None
                    │   │       │   │   ├── .join_cond: 
                    │   │       │   │   ├── (.output_columns):
                    │   │       │   │   │   ┌── "l1.l_comment"(#3.15)
                    │   │       │   │   │   ├── "l1.l_commitdate"(#3.11)
                    │   │       │   │   │   ├── "l1.l_discount"(#3.6)
                    │   │       │   │   │   ├── "l1.l_extendedprice"(#3.5)
                    │   │       │   │   │   ├── "l1.l_linenumber"(#3.3)
                    │   │       │   │   │   ├── "l1.l_linestatus"(#3.9)
                    │   │       │   │   │   ├── "l1.l_orderkey"(#3.0)
                    │   │       │   │   │   ├── "l1.l_partkey"(#3.1)
                    │   │       │   │   │   ├── "l1.l_quantity"(#3.4)
                    │   │       │   │   │   ├── "l1.l_receiptdate"(#3.12)
                    │   │       │   │   │   ├── "l1.l_returnflag"(#3.8)
                    │   │       │   │   │   ├── "l1.l_shipdate"(#3.10)
                    │   │       │   │   │   ├── "l1.l_shipinstruct"(#3.13)
                    │   │       │   │   │   ├── "l1.l_shipmode"(#3.14)
                    │   │       │   │   │   ├── "l1.l_suppkey"(#3.2)
                    │   │       │   │   │   ├── "l1.l_tax"(#3.7)
                    │   │       │   │   │   ├── "supplier.s_acctbal"(#1.5)
                    │   │       │   │   │   ├── "supplier.s_address"(#1.2)
                    │   │       │   │   │   ├── "supplier.s_comment"(#1.6)
                    │   │       │   │   │   ├── "supplier.s_name"(#1.1)
                    │   │       │   │   │   ├── "supplier.s_nationkey"(#1.3)
                    │   │       │   │   │   ├── "supplier.s_phone"(#1.4)
                    │   │       │   │   │   └── "supplier.s_suppkey"(#1.0)
                    │   │       │   │   ├── (.cardinality): 0.00
                    │   │       │   │   ├── Get
                    │   │       │   │   │   ├── .data_source_id: 4
                    │   │       │   │   │   ├── .table_index: 1
                    │   │       │   │   │   ├── .implementation: None
                    │   │       │   │   │   ├── (.output_columns): [ "supplier.s_acctbal"(#1.5), "supplier.s_address"(#1.2), "supplier.s_comment"(#1.6), "supplier.s_name"(#1.1), "supplier.s_nationkey"(#1.3), "supplier.s_phone"(#1.4), "supplier.s_suppkey"(#1.0) ]
                    │   │       │   │   │   └── (.cardinality): 0.00
                    │   │       │   │   └── Remap
                    │   │       │   │       ├── .table_index: 3
                    │   │       │   │       ├── (.output_columns):
                    │   │       │   │       │   ┌── "l1.l_comment"(#3.15)
                    │   │       │   │       │   ├── "l1.l_commitdate"(#3.11)
                    │   │       │   │       │   ├── "l1.l_discount"(#3.6)
                    │   │       │   │       │   ├── "l1.l_extendedprice"(#3.5)
                    │   │       │   │       │   ├── "l1.l_linenumber"(#3.3)
                    │   │       │   │       │   ├── "l1.l_linestatus"(#3.9)
                    │   │       │   │       │   ├── "l1.l_orderkey"(#3.0)
                    │   │       │   │       │   ├── "l1.l_partkey"(#3.1)
                    │   │       │   │       │   ├── "l1.l_quantity"(#3.4)
                    │   │       │   │       │   ├── "l1.l_receiptdate"(#3.12)
                    │   │       │   │       │   ├── "l1.l_returnflag"(#3.8)
                    │   │       │   │       │   ├── "l1.l_shipdate"(#3.10)
                    │   │       │   │       │   ├── "l1.l_shipinstruct"(#3.13)
                    │   │       │   │       │   ├── "l1.l_shipmode"(#3.14)
                    │   │       │   │       │   ├── "l1.l_suppkey"(#3.2)
                    │   │       │   │       │   └── "l1.l_tax"(#3.7)
                    │   │       │   │       ├── (.cardinality): 0.00
                    │   │       │   │       └── Get
                    │   │       │   │           ├── .data_source_id: 8
                    │   │       │   │           ├── .table_index: 2
                    │   │       │   │           ├── .implementation: None
                    │   │       │   │           ├── (.output_columns):
                    │   │       │   │           │   ┌── "lineitem.l_comment"(#2.15)
                    │   │       │   │           │   ├── "lineitem.l_commitdate"(#2.11)
                    │   │       │   │           │   ├── "lineitem.l_discount"(#2.6)
                    │   │       │   │           │   ├── "lineitem.l_extendedprice"(#2.5)
                    │   │       │   │           │   ├── "lineitem.l_linenumber"(#2.3)
                    │   │       │   │           │   ├── "lineitem.l_linestatus"(#2.9)
                    │   │       │   │           │   ├── "lineitem.l_orderkey"(#2.0)
                    │   │       │   │           │   ├── "lineitem.l_partkey"(#2.1)
                    │   │       │   │           │   ├── "lineitem.l_quantity"(#2.4)
                    │   │       │   │           │   ├── "lineitem.l_receiptdate"(#2.12)
                    │   │       │   │           │   ├── "lineitem.l_returnflag"(#2.8)
                    │   │       │   │           │   ├── "lineitem.l_shipdate"(#2.10)
                    │   │       │   │           │   ├── "lineitem.l_shipinstruct"(#2.13)
                    │   │       │   │           │   ├── "lineitem.l_shipmode"(#2.14)
                    │   │       │   │           │   ├── "lineitem.l_suppkey"(#2.2)
                    │   │       │   │           │   └── "lineitem.l_tax"(#2.7)
                    │   │       │   │           └── (.cardinality): 0.00
                    │   │       │   └── Get
                    │   │       │       ├── .data_source_id: 7
                    │   │       │       ├── .table_index: 4
                    │   │       │       ├── .implementation: None
                    │   │       │       ├── (.output_columns):
                    │   │       │       │   ┌── "orders.o_clerk"(#4.6)
                    │   │       │       │   ├── "orders.o_comment"(#4.8)
                    │   │       │       │   ├── "orders.o_custkey"(#4.1)
                    │   │       │       │   ├── "orders.o_orderdate"(#4.4)
                    │   │       │       │   ├── "orders.o_orderkey"(#4.0)
                    │   │       │       │   ├── "orders.o_orderpriority"(#4.5)
                    │   │       │       │   ├── "orders.o_orderstatus"(#4.2)
                    │   │       │       │   ├── "orders.o_shippriority"(#4.7)
                    │   │       │       │   └── "orders.o_totalprice"(#4.3)
                    │   │       │       └── (.cardinality): 0.00
                    │   │       └── Get { .data_source_id: 1, .table_index: 5, .implementation: None, (.output_columns): [ "nation.n_comment"(#5.3), "nation.n_name"(#5.1), "nation.n_nationkey"(#5.0), "nation.n_regionkey"(#5.2) ], (.cardinality): 0.00 }
                    │   └── Project
                    │       ├── .table_index: 8
                    │       ├── .projections:
                    │       │   ┌── "l2.l_orderkey"(#7.0)
                    │       │   ├── "l2.l_partkey"(#7.1)
                    │       │   ├── "l2.l_suppkey"(#7.2)
                    │       │   ├── "l2.l_linenumber"(#7.3)
                    │       │   ├── "l2.l_quantity"(#7.4)
                    │       │   ├── "l2.l_extendedprice"(#7.5)
                    │       │   ├── "l2.l_discount"(#7.6)
                    │       │   ├── "l2.l_tax"(#7.7)
                    │       │   ├── "l2.l_returnflag"(#7.8)
                    │       │   ├── "l2.l_linestatus"(#7.9)
                    │       │   ├── "l2.l_shipdate"(#7.10)
                    │       │   ├── "l2.l_commitdate"(#7.11)
                    │       │   ├── "l2.l_receiptdate"(#7.12)
                    │       │   ├── "l2.l_shipinstruct"(#7.13)
                    │       │   ├── "l2.l_shipmode"(#7.14)
                    │       │   └── "l2.l_comment"(#7.15)
                    │       ├── (.output_columns):
                    │       │   ┌── "__#8.l_comment"(#8.15)
                    │       │   ├── "__#8.l_commitdate"(#8.11)
                    │       │   ├── "__#8.l_discount"(#8.6)
                    │       │   ├── "__#8.l_extendedprice"(#8.5)
                    │       │   ├── "__#8.l_linenumber"(#8.3)
                    │       │   ├── "__#8.l_linestatus"(#8.9)
                    │       │   ├── "__#8.l_orderkey"(#8.0)
                    │       │   ├── "__#8.l_partkey"(#8.1)
                    │       │   ├── "__#8.l_quantity"(#8.4)
                    │       │   ├── "__#8.l_receiptdate"(#8.12)
                    │       │   ├── "__#8.l_returnflag"(#8.8)
                    │       │   ├── "__#8.l_shipdate"(#8.10)
                    │       │   ├── "__#8.l_shipinstruct"(#8.13)
                    │       │   ├── "__#8.l_shipmode"(#8.14)
                    │       │   ├── "__#8.l_suppkey"(#8.2)
                    │       │   └── "__#8.l_tax"(#8.7)
                    │       ├── (.cardinality): 0.00
                    │       └── Select
                    │           ├── .predicate: ("l2.l_orderkey"(#7.0) = "l1.l_orderkey"(#3.0)) AND ("l2.l_suppkey"(#7.2) != "l1.l_suppkey"(#3.2))
                    │           ├── (.output_columns):
                    │           │   ┌── "l2.l_comment"(#7.15)
                    │           │   ├── "l2.l_commitdate"(#7.11)
                    │           │   ├── "l2.l_discount"(#7.6)
                    │           │   ├── "l2.l_extendedprice"(#7.5)
                    │           │   ├── "l2.l_linenumber"(#7.3)
                    │           │   ├── "l2.l_linestatus"(#7.9)
                    │           │   ├── "l2.l_orderkey"(#7.0)
                    │           │   ├── "l2.l_partkey"(#7.1)
                    │           │   ├── "l2.l_quantity"(#7.4)
                    │           │   ├── "l2.l_receiptdate"(#7.12)
                    │           │   ├── "l2.l_returnflag"(#7.8)
                    │           │   ├── "l2.l_shipdate"(#7.10)
                    │           │   ├── "l2.l_shipinstruct"(#7.13)
                    │           │   ├── "l2.l_shipmode"(#7.14)
                    │           │   ├── "l2.l_suppkey"(#7.2)
                    │           │   └── "l2.l_tax"(#7.7)
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
                    └── Project
                        ├── .table_index: 11
                        ├── .projections:
                        │   ┌── "l3.l_orderkey"(#10.0)
                        │   ├── "l3.l_partkey"(#10.1)
                        │   ├── "l3.l_suppkey"(#10.2)
                        │   ├── "l3.l_linenumber"(#10.3)
                        │   ├── "l3.l_quantity"(#10.4)
                        │   ├── "l3.l_extendedprice"(#10.5)
                        │   ├── "l3.l_discount"(#10.6)
                        │   ├── "l3.l_tax"(#10.7)
                        │   ├── "l3.l_returnflag"(#10.8)
                        │   ├── "l3.l_linestatus"(#10.9)
                        │   ├── "l3.l_shipdate"(#10.10)
                        │   ├── "l3.l_commitdate"(#10.11)
                        │   ├── "l3.l_receiptdate"(#10.12)
                        │   ├── "l3.l_shipinstruct"(#10.13)
                        │   ├── "l3.l_shipmode"(#10.14)
                        │   └── "l3.l_comment"(#10.15)
                        ├── (.output_columns):
                        │   ┌── "__#11.l_comment"(#11.15)
                        │   ├── "__#11.l_commitdate"(#11.11)
                        │   ├── "__#11.l_discount"(#11.6)
                        │   ├── "__#11.l_extendedprice"(#11.5)
                        │   ├── "__#11.l_linenumber"(#11.3)
                        │   ├── "__#11.l_linestatus"(#11.9)
                        │   ├── "__#11.l_orderkey"(#11.0)
                        │   ├── "__#11.l_partkey"(#11.1)
                        │   ├── "__#11.l_quantity"(#11.4)
                        │   ├── "__#11.l_receiptdate"(#11.12)
                        │   ├── "__#11.l_returnflag"(#11.8)
                        │   ├── "__#11.l_shipdate"(#11.10)
                        │   ├── "__#11.l_shipinstruct"(#11.13)
                        │   ├── "__#11.l_shipmode"(#11.14)
                        │   ├── "__#11.l_suppkey"(#11.2)
                        │   └── "__#11.l_tax"(#11.7)
                        ├── (.cardinality): 0.00
                        └── Select
                            ├── .predicate: ("l3.l_orderkey"(#10.0) = "l1.l_orderkey"(#3.0)) AND ("l3.l_suppkey"(#10.2) != "l1.l_suppkey"(#3.2)) AND ("l3.l_receiptdate"(#10.12) > "l3.l_commitdate"(#10.11))
                            ├── (.output_columns):
                            │   ┌── "l3.l_comment"(#10.15)
                            │   ├── "l3.l_commitdate"(#10.11)
                            │   ├── "l3.l_discount"(#10.6)
                            │   ├── "l3.l_extendedprice"(#10.5)
                            │   ├── "l3.l_linenumber"(#10.3)
                            │   ├── "l3.l_linestatus"(#10.9)
                            │   ├── "l3.l_orderkey"(#10.0)
                            │   ├── "l3.l_partkey"(#10.1)
                            │   ├── "l3.l_quantity"(#10.4)
                            │   ├── "l3.l_receiptdate"(#10.12)
                            │   ├── "l3.l_returnflag"(#10.8)
                            │   ├── "l3.l_shipdate"(#10.10)
                            │   ├── "l3.l_shipinstruct"(#10.13)
                            │   ├── "l3.l_shipmode"(#10.14)
                            │   ├── "l3.l_suppkey"(#10.2)
                            │   └── "l3.l_tax"(#10.7)
                            ├── (.cardinality): 0.00
                            └── Remap
                                ├── .table_index: 10
                                ├── (.output_columns):
                                │   ┌── "l3.l_comment"(#10.15)
                                │   ├── "l3.l_commitdate"(#10.11)
                                │   ├── "l3.l_discount"(#10.6)
                                │   ├── "l3.l_extendedprice"(#10.5)
                                │   ├── "l3.l_linenumber"(#10.3)
                                │   ├── "l3.l_linestatus"(#10.9)
                                │   ├── "l3.l_orderkey"(#10.0)
                                │   ├── "l3.l_partkey"(#10.1)
                                │   ├── "l3.l_quantity"(#10.4)
                                │   ├── "l3.l_receiptdate"(#10.12)
                                │   ├── "l3.l_returnflag"(#10.8)
                                │   ├── "l3.l_shipdate"(#10.10)
                                │   ├── "l3.l_shipinstruct"(#10.13)
                                │   ├── "l3.l_shipmode"(#10.14)
                                │   ├── "l3.l_suppkey"(#10.2)
                                │   └── "l3.l_tax"(#10.7)
                                ├── (.cardinality): 0.00
                                └── Get
                                    ├── .data_source_id: 8
                                    ├── .table_index: 9
                                    ├── .implementation: None
                                    ├── (.output_columns):
                                    │   ┌── "lineitem.l_comment"(#9.15)
                                    │   ├── "lineitem.l_commitdate"(#9.11)
                                    │   ├── "lineitem.l_discount"(#9.6)
                                    │   ├── "lineitem.l_extendedprice"(#9.5)
                                    │   ├── "lineitem.l_linenumber"(#9.3)
                                    │   ├── "lineitem.l_linestatus"(#9.9)
                                    │   ├── "lineitem.l_orderkey"(#9.0)
                                    │   ├── "lineitem.l_partkey"(#9.1)
                                    │   ├── "lineitem.l_quantity"(#9.4)
                                    │   ├── "lineitem.l_receiptdate"(#9.12)
                                    │   ├── "lineitem.l_returnflag"(#9.8)
                                    │   ├── "lineitem.l_shipdate"(#9.10)
                                    │   ├── "lineitem.l_shipinstruct"(#9.13)
                                    │   ├── "lineitem.l_shipmode"(#9.14)
                                    │   ├── "lineitem.l_suppkey"(#9.2)
                                    │   └── "lineitem.l_tax"(#9.7)
                                    └── (.cardinality): 0.00

physical_plan after optd-finalized:
Limit { .skip: 0::bigint, .fetch: 100::bigint, (.output_columns): [ "__#14.numwait"(#14.1), "__#14.s_name"(#14.0) ], (.cardinality): 0.00 }
└── EnforcerSort { tuple_ordering: [(#14.1, Desc), (#14.0, Asc)], (.output_columns): [ "__#14.numwait"(#14.1), "__#14.s_name"(#14.0) ], (.cardinality): 0.00 }
    └── Project { .table_index: 14, .projections: [ "supplier.s_name"(#1.1), "__#13.count(Int64(1))"(#13.0) ], (.output_columns): [ "__#14.numwait"(#14.1), "__#14.s_name"(#14.0) ], (.cardinality): 0.00 }
        └── Aggregate
            ├── .key_table_index: 12
            ├── .aggregate_table_index: 13
            ├── .implementation: None
            ├── .exprs: count(1::bigint)
            ├── .keys: "supplier.s_name"(#1.1)
            ├── (.output_columns): [ "__#12.s_name"(#12.0), "__#13.count(Int64(1))"(#13.0) ]
            ├── (.cardinality): 0.00
            └── Join
                ├── .join_type: LeftAnti
                ├── .implementation: None
                ├── .join_cond: ("l1.l_orderkey"(#3.0) IS NOT DISTINCT FROM "__#17.l_orderkey__optd_1"(#17.16)) AND ("l1.l_suppkey"(#3.2) IS NOT DISTINCT FROM "__#17.l_suppkey__optd_1"(#17.17))
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
                │   ├── .join_cond: ("l1.l_orderkey"(#3.0) IS NOT DISTINCT FROM "__#20.l_orderkey__optd_1"(#20.16)) AND ("l1.l_suppkey"(#3.2) IS NOT DISTINCT FROM "__#20.l_suppkey__optd_1"(#20.17))
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
                │   │   ├── .join_cond: "supplier.s_nationkey"(#1.3) = "nation.n_nationkey"(#5.0)
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
                │   │   │   ├── .join_cond: "orders.o_orderkey"(#4.0) = "l1.l_orderkey"(#3.0)
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
                │   │   │   │   ├── .join_cond: "supplier.s_suppkey"(#1.0) = "l1.l_suppkey"(#3.2)
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
                │   │   │   │   ├── Get
                │   │   │   │   │   ├── .data_source_id: 4
                │   │   │   │   │   ├── .table_index: 1
                │   │   │   │   │   ├── .implementation: None
                │   │   │   │   │   ├── (.output_columns): [ "supplier.s_name"(#1.1), "supplier.s_nationkey"(#1.3), "supplier.s_suppkey"(#1.0) ]
                │   │   │   │   │   └── (.cardinality): 0.00
                │   │   │   │   └── Select
                │   │   │   │       ├── .predicate: "l1.l_receiptdate"(#3.12) > "l1.l_commitdate"(#3.11)
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
                │   │   │   │       └── Remap
                │   │   │   │           ├── .table_index: 3
                │   │   │   │           ├── (.output_columns):
                │   │   │   │           │   ┌── "l1.l_comment"(#3.15)
                │   │   │   │           │   ├── "l1.l_commitdate"(#3.11)
                │   │   │   │           │   ├── "l1.l_discount"(#3.6)
                │   │   │   │           │   ├── "l1.l_extendedprice"(#3.5)
                │   │   │   │           │   ├── "l1.l_linenumber"(#3.3)
                │   │   │   │           │   ├── "l1.l_linestatus"(#3.9)
                │   │   │   │           │   ├── "l1.l_orderkey"(#3.0)
                │   │   │   │           │   ├── "l1.l_partkey"(#3.1)
                │   │   │   │           │   ├── "l1.l_quantity"(#3.4)
                │   │   │   │           │   ├── "l1.l_receiptdate"(#3.12)
                │   │   │   │           │   ├── "l1.l_returnflag"(#3.8)
                │   │   │   │           │   ├── "l1.l_shipdate"(#3.10)
                │   │   │   │           │   ├── "l1.l_shipinstruct"(#3.13)
                │   │   │   │           │   ├── "l1.l_shipmode"(#3.14)
                │   │   │   │           │   ├── "l1.l_suppkey"(#3.2)
                │   │   │   │           │   └── "l1.l_tax"(#3.7)
                │   │   │   │           ├── (.cardinality): 0.00
                │   │   │   │           └── Get
                │   │   │   │               ├── .data_source_id: 8
                │   │   │   │               ├── .table_index: 2
                │   │   │   │               ├── .implementation: None
                │   │   │   │               ├── (.output_columns):
                │   │   │   │               │   ┌── "lineitem.l_comment"(#2.15)
                │   │   │   │               │   ├── "lineitem.l_commitdate"(#2.11)
                │   │   │   │               │   ├── "lineitem.l_discount"(#2.6)
                │   │   │   │               │   ├── "lineitem.l_extendedprice"(#2.5)
                │   │   │   │               │   ├── "lineitem.l_linenumber"(#2.3)
                │   │   │   │               │   ├── "lineitem.l_linestatus"(#2.9)
                │   │   │   │               │   ├── "lineitem.l_orderkey"(#2.0)
                │   │   │   │               │   ├── "lineitem.l_partkey"(#2.1)
                │   │   │   │               │   ├── "lineitem.l_quantity"(#2.4)
                │   │   │   │               │   ├── "lineitem.l_receiptdate"(#2.12)
                │   │   │   │               │   ├── "lineitem.l_returnflag"(#2.8)
                │   │   │   │               │   ├── "lineitem.l_shipdate"(#2.10)
                │   │   │   │               │   ├── "lineitem.l_shipinstruct"(#2.13)
                │   │   │   │               │   ├── "lineitem.l_shipmode"(#2.14)
                │   │   │   │               │   ├── "lineitem.l_suppkey"(#2.2)
                │   │   │   │               │   └── "lineitem.l_tax"(#2.7)
                │   │   │   │               └── (.cardinality): 0.00
                │   │   │   └── Select { .predicate: "orders.o_orderstatus"(#4.2) = 'F'::utf8_view, (.output_columns): [ "orders.o_orderkey"(#4.0), "orders.o_orderstatus"(#4.2) ], (.cardinality): 0.00 }
                │   │   │       └── Get { .data_source_id: 7, .table_index: 4, .implementation: None, (.output_columns): [ "orders.o_orderkey"(#4.0), "orders.o_orderstatus"(#4.2) ], (.cardinality): 0.00 }
                │   │   └── Select { .predicate: "nation.n_name"(#5.1) = 'SAUDI ARABIA'::utf8_view, (.output_columns): [ "nation.n_name"(#5.1), "nation.n_nationkey"(#5.0) ], (.cardinality): 0.00 }
                │   │       └── Get { .data_source_id: 1, .table_index: 5, .implementation: None, (.output_columns): [ "nation.n_name"(#5.1), "nation.n_nationkey"(#5.0) ], (.cardinality): 0.00 }
                │   └── Project
                │       ├── .table_index: 20
                │       ├── .projections:
                │       │   ┌── "l2.l_orderkey"(#7.0)
                │       │   ├── "l2.l_partkey"(#7.1)
                │       │   ├── "l2.l_suppkey"(#7.2)
                │       │   ├── "l2.l_linenumber"(#7.3)
                │       │   ├── "l2.l_quantity"(#7.4)
                │       │   ├── "l2.l_extendedprice"(#7.5)
                │       │   ├── "l2.l_discount"(#7.6)
                │       │   ├── "l2.l_tax"(#7.7)
                │       │   ├── "l2.l_returnflag"(#7.8)
                │       │   ├── "l2.l_linestatus"(#7.9)
                │       │   ├── "l2.l_shipdate"(#7.10)
                │       │   ├── "l2.l_commitdate"(#7.11)
                │       │   ├── "l2.l_receiptdate"(#7.12)
                │       │   ├── "l2.l_shipinstruct"(#7.13)
                │       │   ├── "l2.l_shipmode"(#7.14)
                │       │   ├── "l2.l_comment"(#7.15)
                │       │   ├── "__#18.l_orderkey"(#18.0)
                │       │   └── "__#18.l_suppkey"(#18.1)
                │       ├── (.output_columns):
                │       │   ┌── "__#20.l_comment"(#20.15)
                │       │   ├── "__#20.l_commitdate"(#20.11)
                │       │   ├── "__#20.l_discount"(#20.6)
                │       │   ├── "__#20.l_extendedprice"(#20.5)
                │       │   ├── "__#20.l_linenumber"(#20.3)
                │       │   ├── "__#20.l_linestatus"(#20.9)
                │       │   ├── "__#20.l_orderkey"(#20.0)
                │       │   ├── "__#20.l_orderkey__optd_1"(#20.16)
                │       │   ├── "__#20.l_partkey"(#20.1)
                │       │   ├── "__#20.l_quantity"(#20.4)
                │       │   ├── "__#20.l_receiptdate"(#20.12)
                │       │   ├── "__#20.l_returnflag"(#20.8)
                │       │   ├── "__#20.l_shipdate"(#20.10)
                │       │   ├── "__#20.l_shipinstruct"(#20.13)
                │       │   ├── "__#20.l_shipmode"(#20.14)
                │       │   ├── "__#20.l_suppkey"(#20.2)
                │       │   ├── "__#20.l_suppkey__optd_1"(#20.17)
                │       │   └── "__#20.l_tax"(#20.7)
                │       ├── (.cardinality): 0.00
                │       └── Join
                │           ├── .join_type: Inner
                │           ├── .implementation: None
                │           ├── .join_cond: ("l2.l_orderkey"(#7.0) = "__#18.l_orderkey"(#18.0)) AND ("l2.l_suppkey"(#7.2) != "__#18.l_suppkey"(#18.1))
                │           ├── (.output_columns):
                │           │   ┌── "__#18.l_orderkey"(#18.0)
                │           │   ├── "__#18.l_suppkey"(#18.1)
                │           │   ├── "l2.l_comment"(#7.15)
                │           │   ├── "l2.l_commitdate"(#7.11)
                │           │   ├── "l2.l_discount"(#7.6)
                │           │   ├── "l2.l_extendedprice"(#7.5)
                │           │   ├── "l2.l_linenumber"(#7.3)
                │           │   ├── "l2.l_linestatus"(#7.9)
                │           │   ├── "l2.l_orderkey"(#7.0)
                │           │   ├── "l2.l_partkey"(#7.1)
                │           │   ├── "l2.l_quantity"(#7.4)
                │           │   ├── "l2.l_receiptdate"(#7.12)
                │           │   ├── "l2.l_returnflag"(#7.8)
                │           │   ├── "l2.l_shipdate"(#7.10)
                │           │   ├── "l2.l_shipinstruct"(#7.13)
                │           │   ├── "l2.l_shipmode"(#7.14)
                │           │   ├── "l2.l_suppkey"(#7.2)
                │           │   └── "l2.l_tax"(#7.7)
                │           ├── (.cardinality): 0.00
                │           ├── Aggregate
                │           │   ├── .key_table_index: 18
                │           │   ├── .aggregate_table_index: 19
                │           │   ├── .implementation: None
                │           │   ├── .exprs: []
                │           │   ├── .keys: [ "l1.l_orderkey"(#3.0), "l1.l_suppkey"(#3.2) ]
                │           │   ├── (.output_columns): [ "__#18.l_orderkey"(#18.0), "__#18.l_suppkey"(#18.1) ]
                │           │   ├── (.cardinality): 0.00
                │           │   └── Join
                │           │       ├── .join_type: Inner
                │           │       ├── .implementation: None
                │           │       ├── .join_cond: true::boolean
                │           │       ├── (.output_columns):
                │           │       │   ┌── "l1.l_comment"(#3.15)
                │           │       │   ├── "l1.l_commitdate"(#3.11)
                │           │       │   ├── "l1.l_discount"(#3.6)
                │           │       │   ├── "l1.l_extendedprice"(#3.5)
                │           │       │   ├── "l1.l_linenumber"(#3.3)
                │           │       │   ├── "l1.l_linestatus"(#3.9)
                │           │       │   ├── "l1.l_orderkey"(#3.0)
                │           │       │   ├── "l1.l_partkey"(#3.1)
                │           │       │   ├── "l1.l_quantity"(#3.4)
                │           │       │   ├── "l1.l_receiptdate"(#3.12)
                │           │       │   ├── "l1.l_returnflag"(#3.8)
                │           │       │   ├── "l1.l_shipdate"(#3.10)
                │           │       │   ├── "l1.l_shipinstruct"(#3.13)
                │           │       │   ├── "l1.l_shipmode"(#3.14)
                │           │       │   ├── "l1.l_suppkey"(#3.2)
                │           │       │   ├── "l1.l_tax"(#3.7)
                │           │       │   ├── "nation.n_nationkey"(#5.0)
                │           │       │   ├── "orders.o_orderkey"(#4.0)
                │           │       │   ├── "orders.o_orderstatus"(#4.2)
                │           │       │   └── "supplier.s_suppkey"(#1.0)
                │           │       ├── (.cardinality): 0.00
                │           │       ├── Join
                │           │       │   ├── .join_type: Inner
                │           │       │   ├── .implementation: None
                │           │       │   ├── .join_cond: "orders.o_orderkey"(#4.0) = "l1.l_orderkey"(#3.0)
                │           │       │   ├── (.output_columns):
                │           │       │   │   ┌── "l1.l_comment"(#3.15)
                │           │       │   │   ├── "l1.l_commitdate"(#3.11)
                │           │       │   │   ├── "l1.l_discount"(#3.6)
                │           │       │   │   ├── "l1.l_extendedprice"(#3.5)
                │           │       │   │   ├── "l1.l_linenumber"(#3.3)
                │           │       │   │   ├── "l1.l_linestatus"(#3.9)
                │           │       │   │   ├── "l1.l_orderkey"(#3.0)
                │           │       │   │   ├── "l1.l_partkey"(#3.1)
                │           │       │   │   ├── "l1.l_quantity"(#3.4)
                │           │       │   │   ├── "l1.l_receiptdate"(#3.12)
                │           │       │   │   ├── "l1.l_returnflag"(#3.8)
                │           │       │   │   ├── "l1.l_shipdate"(#3.10)
                │           │       │   │   ├── "l1.l_shipinstruct"(#3.13)
                │           │       │   │   ├── "l1.l_shipmode"(#3.14)
                │           │       │   │   ├── "l1.l_suppkey"(#3.2)
                │           │       │   │   ├── "l1.l_tax"(#3.7)
                │           │       │   │   ├── "orders.o_orderkey"(#4.0)
                │           │       │   │   ├── "orders.o_orderstatus"(#4.2)
                │           │       │   │   └── "supplier.s_suppkey"(#1.0)
                │           │       │   ├── (.cardinality): 0.00
                │           │       │   ├── Join
                │           │       │   │   ├── .join_type: Inner
                │           │       │   │   ├── .implementation: None
                │           │       │   │   ├── .join_cond: "supplier.s_suppkey"(#1.0) = "l1.l_suppkey"(#3.2)
                │           │       │   │   ├── (.output_columns):
                │           │       │   │   │   ┌── "l1.l_comment"(#3.15)
                │           │       │   │   │   ├── "l1.l_commitdate"(#3.11)
                │           │       │   │   │   ├── "l1.l_discount"(#3.6)
                │           │       │   │   │   ├── "l1.l_extendedprice"(#3.5)
                │           │       │   │   │   ├── "l1.l_linenumber"(#3.3)
                │           │       │   │   │   ├── "l1.l_linestatus"(#3.9)
                │           │       │   │   │   ├── "l1.l_orderkey"(#3.0)
                │           │       │   │   │   ├── "l1.l_partkey"(#3.1)
                │           │       │   │   │   ├── "l1.l_quantity"(#3.4)
                │           │       │   │   │   ├── "l1.l_receiptdate"(#3.12)
                │           │       │   │   │   ├── "l1.l_returnflag"(#3.8)
                │           │       │   │   │   ├── "l1.l_shipdate"(#3.10)
                │           │       │   │   │   ├── "l1.l_shipinstruct"(#3.13)
                │           │       │   │   │   ├── "l1.l_shipmode"(#3.14)
                │           │       │   │   │   ├── "l1.l_suppkey"(#3.2)
                │           │       │   │   │   ├── "l1.l_tax"(#3.7)
                │           │       │   │   │   └── "supplier.s_suppkey"(#1.0)
                │           │       │   │   ├── (.cardinality): 0.00
                │           │       │   │   ├── Get { .data_source_id: 4, .table_index: 1, .implementation: None, (.output_columns): "supplier.s_suppkey"(#1.0), (.cardinality): 0.00 }
                │           │       │   │   └── Select
                │           │       │   │       ├── .predicate: "l1.l_receiptdate"(#3.12) > "l1.l_commitdate"(#3.11)
                │           │       │   │       ├── (.output_columns):
                │           │       │   │       │   ┌── "l1.l_comment"(#3.15)
                │           │       │   │       │   ├── "l1.l_commitdate"(#3.11)
                │           │       │   │       │   ├── "l1.l_discount"(#3.6)
                │           │       │   │       │   ├── "l1.l_extendedprice"(#3.5)
                │           │       │   │       │   ├── "l1.l_linenumber"(#3.3)
                │           │       │   │       │   ├── "l1.l_linestatus"(#3.9)
                │           │       │   │       │   ├── "l1.l_orderkey"(#3.0)
                │           │       │   │       │   ├── "l1.l_partkey"(#3.1)
                │           │       │   │       │   ├── "l1.l_quantity"(#3.4)
                │           │       │   │       │   ├── "l1.l_receiptdate"(#3.12)
                │           │       │   │       │   ├── "l1.l_returnflag"(#3.8)
                │           │       │   │       │   ├── "l1.l_shipdate"(#3.10)
                │           │       │   │       │   ├── "l1.l_shipinstruct"(#3.13)
                │           │       │   │       │   ├── "l1.l_shipmode"(#3.14)
                │           │       │   │       │   ├── "l1.l_suppkey"(#3.2)
                │           │       │   │       │   └── "l1.l_tax"(#3.7)
                │           │       │   │       ├── (.cardinality): 0.00
                │           │       │   │       └── Remap
                │           │       │   │           ├── .table_index: 3
                │           │       │   │           ├── (.output_columns):
                │           │       │   │           │   ┌── "l1.l_comment"(#3.15)
                │           │       │   │           │   ├── "l1.l_commitdate"(#3.11)
                │           │       │   │           │   ├── "l1.l_discount"(#3.6)
                │           │       │   │           │   ├── "l1.l_extendedprice"(#3.5)
                │           │       │   │           │   ├── "l1.l_linenumber"(#3.3)
                │           │       │   │           │   ├── "l1.l_linestatus"(#3.9)
                │           │       │   │           │   ├── "l1.l_orderkey"(#3.0)
                │           │       │   │           │   ├── "l1.l_partkey"(#3.1)
                │           │       │   │           │   ├── "l1.l_quantity"(#3.4)
                │           │       │   │           │   ├── "l1.l_receiptdate"(#3.12)
                │           │       │   │           │   ├── "l1.l_returnflag"(#3.8)
                │           │       │   │           │   ├── "l1.l_shipdate"(#3.10)
                │           │       │   │           │   ├── "l1.l_shipinstruct"(#3.13)
                │           │       │   │           │   ├── "l1.l_shipmode"(#3.14)
                │           │       │   │           │   ├── "l1.l_suppkey"(#3.2)
                │           │       │   │           │   └── "l1.l_tax"(#3.7)
                │           │       │   │           ├── (.cardinality): 0.00
                │           │       │   │           └── Get
                │           │       │   │               ├── .data_source_id: 8
                │           │       │   │               ├── .table_index: 2
                │           │       │   │               ├── .implementation: None
                │           │       │   │               ├── (.output_columns):
                │           │       │   │               │   ┌── "lineitem.l_comment"(#2.15)
                │           │       │   │               │   ├── "lineitem.l_commitdate"(#2.11)
                │           │       │   │               │   ├── "lineitem.l_discount"(#2.6)
                │           │       │   │               │   ├── "lineitem.l_extendedprice"(#2.5)
                │           │       │   │               │   ├── "lineitem.l_linenumber"(#2.3)
                │           │       │   │               │   ├── "lineitem.l_linestatus"(#2.9)
                │           │       │   │               │   ├── "lineitem.l_orderkey"(#2.0)
                │           │       │   │               │   ├── "lineitem.l_partkey"(#2.1)
                │           │       │   │               │   ├── "lineitem.l_quantity"(#2.4)
                │           │       │   │               │   ├── "lineitem.l_receiptdate"(#2.12)
                │           │       │   │               │   ├── "lineitem.l_returnflag"(#2.8)
                │           │       │   │               │   ├── "lineitem.l_shipdate"(#2.10)
                │           │       │   │               │   ├── "lineitem.l_shipinstruct"(#2.13)
                │           │       │   │               │   ├── "lineitem.l_shipmode"(#2.14)
                │           │       │   │               │   ├── "lineitem.l_suppkey"(#2.2)
                │           │       │   │               │   └── "lineitem.l_tax"(#2.7)
                │           │       │   │               └── (.cardinality): 0.00
                │           │       │   └── Select
                │           │       │       ├── .predicate: "orders.o_orderstatus"(#4.2) = 'F'::utf8_view
                │           │       │       ├── (.output_columns): [ "orders.o_orderkey"(#4.0), "orders.o_orderstatus"(#4.2) ]
                │           │       │       ├── (.cardinality): 0.00
                │           │       │       └── Get
                │           │       │           ├── .data_source_id: 7
                │           │       │           ├── .table_index: 4
                │           │       │           ├── .implementation: None
                │           │       │           ├── (.output_columns): [ "orders.o_orderkey"(#4.0), "orders.o_orderstatus"(#4.2) ]
                │           │       │           └── (.cardinality): 0.00
                │           │       └── Get { .data_source_id: 1, .table_index: 5, .implementation: None, (.output_columns): "nation.n_nationkey"(#5.0), (.cardinality): 0.00 }
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
                └── Project
                    ├── .table_index: 17
                    ├── .projections:
                    │   ┌── "l3.l_orderkey"(#10.0)
                    │   ├── "l3.l_partkey"(#10.1)
                    │   ├── "l3.l_suppkey"(#10.2)
                    │   ├── "l3.l_linenumber"(#10.3)
                    │   ├── "l3.l_quantity"(#10.4)
                    │   ├── "l3.l_extendedprice"(#10.5)
                    │   ├── "l3.l_discount"(#10.6)
                    │   ├── "l3.l_tax"(#10.7)
                    │   ├── "l3.l_returnflag"(#10.8)
                    │   ├── "l3.l_linestatus"(#10.9)
                    │   ├── "l3.l_shipdate"(#10.10)
                    │   ├── "l3.l_commitdate"(#10.11)
                    │   ├── "l3.l_receiptdate"(#10.12)
                    │   ├── "l3.l_shipinstruct"(#10.13)
                    │   ├── "l3.l_shipmode"(#10.14)
                    │   ├── "l3.l_comment"(#10.15)
                    │   ├── "__#15.l_orderkey"(#15.0)
                    │   └── "__#15.l_suppkey"(#15.1)
                    ├── (.output_columns):
                    │   ┌── "__#17.l_comment"(#17.15)
                    │   ├── "__#17.l_commitdate"(#17.11)
                    │   ├── "__#17.l_discount"(#17.6)
                    │   ├── "__#17.l_extendedprice"(#17.5)
                    │   ├── "__#17.l_linenumber"(#17.3)
                    │   ├── "__#17.l_linestatus"(#17.9)
                    │   ├── "__#17.l_orderkey"(#17.0)
                    │   ├── "__#17.l_orderkey__optd_1"(#17.16)
                    │   ├── "__#17.l_partkey"(#17.1)
                    │   ├── "__#17.l_quantity"(#17.4)
                    │   ├── "__#17.l_receiptdate"(#17.12)
                    │   ├── "__#17.l_returnflag"(#17.8)
                    │   ├── "__#17.l_shipdate"(#17.10)
                    │   ├── "__#17.l_shipinstruct"(#17.13)
                    │   ├── "__#17.l_shipmode"(#17.14)
                    │   ├── "__#17.l_suppkey"(#17.2)
                    │   ├── "__#17.l_suppkey__optd_1"(#17.17)
                    │   └── "__#17.l_tax"(#17.7)
                    ├── (.cardinality): 0.00
                    └── Join
                        ├── .join_type: Inner
                        ├── .implementation: None
                        ├── .join_cond: ("l3.l_orderkey"(#10.0) = "__#15.l_orderkey"(#15.0)) AND ("l3.l_suppkey"(#10.2) != "__#15.l_suppkey"(#15.1))
                        ├── (.output_columns):
                        │   ┌── "__#15.l_orderkey"(#15.0)
                        │   ├── "__#15.l_suppkey"(#15.1)
                        │   ├── "l3.l_comment"(#10.15)
                        │   ├── "l3.l_commitdate"(#10.11)
                        │   ├── "l3.l_discount"(#10.6)
                        │   ├── "l3.l_extendedprice"(#10.5)
                        │   ├── "l3.l_linenumber"(#10.3)
                        │   ├── "l3.l_linestatus"(#10.9)
                        │   ├── "l3.l_orderkey"(#10.0)
                        │   ├── "l3.l_partkey"(#10.1)
                        │   ├── "l3.l_quantity"(#10.4)
                        │   ├── "l3.l_receiptdate"(#10.12)
                        │   ├── "l3.l_returnflag"(#10.8)
                        │   ├── "l3.l_shipdate"(#10.10)
                        │   ├── "l3.l_shipinstruct"(#10.13)
                        │   ├── "l3.l_shipmode"(#10.14)
                        │   ├── "l3.l_suppkey"(#10.2)
                        │   └── "l3.l_tax"(#10.7)
                        ├── (.cardinality): 0.00
                        ├── Aggregate
                        │   ├── .key_table_index: 15
                        │   ├── .aggregate_table_index: 16
                        │   ├── .implementation: None
                        │   ├── .exprs: []
                        │   ├── .keys: [ "l1.l_orderkey"(#3.0), "l1.l_suppkey"(#3.2) ]
                        │   ├── (.output_columns): [ "__#15.l_orderkey"(#15.0), "__#15.l_suppkey"(#15.1) ]
                        │   ├── (.cardinality): 0.00
                        │   └── Join
                        │       ├── .join_type: LeftSemi
                        │       ├── .implementation: None
                        │       ├── .join_cond: ("l1.l_orderkey"(#3.0) IS NOT DISTINCT FROM "__#23.l_orderkey__optd_1"(#23.16)) AND ("l1.l_suppkey"(#3.2) IS NOT DISTINCT FROM "__#23.l_suppkey__optd_1"(#23.17))
                        │       ├── (.output_columns):
                        │       │   ┌── "l1.l_comment"(#3.15)
                        │       │   ├── "l1.l_commitdate"(#3.11)
                        │       │   ├── "l1.l_discount"(#3.6)
                        │       │   ├── "l1.l_extendedprice"(#3.5)
                        │       │   ├── "l1.l_linenumber"(#3.3)
                        │       │   ├── "l1.l_linestatus"(#3.9)
                        │       │   ├── "l1.l_orderkey"(#3.0)
                        │       │   ├── "l1.l_partkey"(#3.1)
                        │       │   ├── "l1.l_quantity"(#3.4)
                        │       │   ├── "l1.l_receiptdate"(#3.12)
                        │       │   ├── "l1.l_returnflag"(#3.8)
                        │       │   ├── "l1.l_shipdate"(#3.10)
                        │       │   ├── "l1.l_shipinstruct"(#3.13)
                        │       │   ├── "l1.l_shipmode"(#3.14)
                        │       │   ├── "l1.l_suppkey"(#3.2)
                        │       │   ├── "l1.l_tax"(#3.7)
                        │       │   ├── "nation.n_nationkey"(#5.0)
                        │       │   ├── "orders.o_orderkey"(#4.0)
                        │       │   ├── "orders.o_orderstatus"(#4.2)
                        │       │   └── "supplier.s_suppkey"(#1.0)
                        │       ├── (.cardinality): 0.00
                        │       ├── Join
                        │       │   ├── .join_type: Inner
                        │       │   ├── .implementation: None
                        │       │   ├── .join_cond: true::boolean
                        │       │   ├── (.output_columns):
                        │       │   │   ┌── "l1.l_comment"(#3.15)
                        │       │   │   ├── "l1.l_commitdate"(#3.11)
                        │       │   │   ├── "l1.l_discount"(#3.6)
                        │       │   │   ├── "l1.l_extendedprice"(#3.5)
                        │       │   │   ├── "l1.l_linenumber"(#3.3)
                        │       │   │   ├── "l1.l_linestatus"(#3.9)
                        │       │   │   ├── "l1.l_orderkey"(#3.0)
                        │       │   │   ├── "l1.l_partkey"(#3.1)
                        │       │   │   ├── "l1.l_quantity"(#3.4)
                        │       │   │   ├── "l1.l_receiptdate"(#3.12)
                        │       │   │   ├── "l1.l_returnflag"(#3.8)
                        │       │   │   ├── "l1.l_shipdate"(#3.10)
                        │       │   │   ├── "l1.l_shipinstruct"(#3.13)
                        │       │   │   ├── "l1.l_shipmode"(#3.14)
                        │       │   │   ├── "l1.l_suppkey"(#3.2)
                        │       │   │   ├── "l1.l_tax"(#3.7)
                        │       │   │   ├── "nation.n_nationkey"(#5.0)
                        │       │   │   ├── "orders.o_orderkey"(#4.0)
                        │       │   │   ├── "orders.o_orderstatus"(#4.2)
                        │       │   │   └── "supplier.s_suppkey"(#1.0)
                        │       │   ├── (.cardinality): 0.00
                        │       │   ├── Join
                        │       │   │   ├── .join_type: Inner
                        │       │   │   ├── .implementation: None
                        │       │   │   ├── .join_cond: "orders.o_orderkey"(#4.0) = "l1.l_orderkey"(#3.0)
                        │       │   │   ├── (.output_columns):
                        │       │   │   │   ┌── "l1.l_comment"(#3.15)
                        │       │   │   │   ├── "l1.l_commitdate"(#3.11)
                        │       │   │   │   ├── "l1.l_discount"(#3.6)
                        │       │   │   │   ├── "l1.l_extendedprice"(#3.5)
                        │       │   │   │   ├── "l1.l_linenumber"(#3.3)
                        │       │   │   │   ├── "l1.l_linestatus"(#3.9)
                        │       │   │   │   ├── "l1.l_orderkey"(#3.0)
                        │       │   │   │   ├── "l1.l_partkey"(#3.1)
                        │       │   │   │   ├── "l1.l_quantity"(#3.4)
                        │       │   │   │   ├── "l1.l_receiptdate"(#3.12)
                        │       │   │   │   ├── "l1.l_returnflag"(#3.8)
                        │       │   │   │   ├── "l1.l_shipdate"(#3.10)
                        │       │   │   │   ├── "l1.l_shipinstruct"(#3.13)
                        │       │   │   │   ├── "l1.l_shipmode"(#3.14)
                        │       │   │   │   ├── "l1.l_suppkey"(#3.2)
                        │       │   │   │   ├── "l1.l_tax"(#3.7)
                        │       │   │   │   ├── "orders.o_orderkey"(#4.0)
                        │       │   │   │   ├── "orders.o_orderstatus"(#4.2)
                        │       │   │   │   └── "supplier.s_suppkey"(#1.0)
                        │       │   │   ├── (.cardinality): 0.00
                        │       │   │   ├── Join
                        │       │   │   │   ├── .join_type: Inner
                        │       │   │   │   ├── .implementation: None
                        │       │   │   │   ├── .join_cond: "supplier.s_suppkey"(#1.0) = "l1.l_suppkey"(#3.2)
                        │       │   │   │   ├── (.output_columns):
                        │       │   │   │   │   ┌── "l1.l_comment"(#3.15)
                        │       │   │   │   │   ├── "l1.l_commitdate"(#3.11)
                        │       │   │   │   │   ├── "l1.l_discount"(#3.6)
                        │       │   │   │   │   ├── "l1.l_extendedprice"(#3.5)
                        │       │   │   │   │   ├── "l1.l_linenumber"(#3.3)
                        │       │   │   │   │   ├── "l1.l_linestatus"(#3.9)
                        │       │   │   │   │   ├── "l1.l_orderkey"(#3.0)
                        │       │   │   │   │   ├── "l1.l_partkey"(#3.1)
                        │       │   │   │   │   ├── "l1.l_quantity"(#3.4)
                        │       │   │   │   │   ├── "l1.l_receiptdate"(#3.12)
                        │       │   │   │   │   ├── "l1.l_returnflag"(#3.8)
                        │       │   │   │   │   ├── "l1.l_shipdate"(#3.10)
                        │       │   │   │   │   ├── "l1.l_shipinstruct"(#3.13)
                        │       │   │   │   │   ├── "l1.l_shipmode"(#3.14)
                        │       │   │   │   │   ├── "l1.l_suppkey"(#3.2)
                        │       │   │   │   │   ├── "l1.l_tax"(#3.7)
                        │       │   │   │   │   └── "supplier.s_suppkey"(#1.0)
                        │       │   │   │   ├── (.cardinality): 0.00
                        │       │   │   │   ├── Get { .data_source_id: 4, .table_index: 1, .implementation: None, (.output_columns): "supplier.s_suppkey"(#1.0), (.cardinality): 0.00 }
                        │       │   │   │   └── Select
                        │       │   │   │       ├── .predicate: "l1.l_receiptdate"(#3.12) > "l1.l_commitdate"(#3.11)
                        │       │   │   │       ├── (.output_columns):
                        │       │   │   │       │   ┌── "l1.l_comment"(#3.15)
                        │       │   │   │       │   ├── "l1.l_commitdate"(#3.11)
                        │       │   │   │       │   ├── "l1.l_discount"(#3.6)
                        │       │   │   │       │   ├── "l1.l_extendedprice"(#3.5)
                        │       │   │   │       │   ├── "l1.l_linenumber"(#3.3)
                        │       │   │   │       │   ├── "l1.l_linestatus"(#3.9)
                        │       │   │   │       │   ├── "l1.l_orderkey"(#3.0)
                        │       │   │   │       │   ├── "l1.l_partkey"(#3.1)
                        │       │   │   │       │   ├── "l1.l_quantity"(#3.4)
                        │       │   │   │       │   ├── "l1.l_receiptdate"(#3.12)
                        │       │   │   │       │   ├── "l1.l_returnflag"(#3.8)
                        │       │   │   │       │   ├── "l1.l_shipdate"(#3.10)
                        │       │   │   │       │   ├── "l1.l_shipinstruct"(#3.13)
                        │       │   │   │       │   ├── "l1.l_shipmode"(#3.14)
                        │       │   │   │       │   ├── "l1.l_suppkey"(#3.2)
                        │       │   │   │       │   └── "l1.l_tax"(#3.7)
                        │       │   │   │       ├── (.cardinality): 0.00
                        │       │   │   │       └── Remap
                        │       │   │   │           ├── .table_index: 3
                        │       │   │   │           ├── (.output_columns):
                        │       │   │   │           │   ┌── "l1.l_comment"(#3.15)
                        │       │   │   │           │   ├── "l1.l_commitdate"(#3.11)
                        │       │   │   │           │   ├── "l1.l_discount"(#3.6)
                        │       │   │   │           │   ├── "l1.l_extendedprice"(#3.5)
                        │       │   │   │           │   ├── "l1.l_linenumber"(#3.3)
                        │       │   │   │           │   ├── "l1.l_linestatus"(#3.9)
                        │       │   │   │           │   ├── "l1.l_orderkey"(#3.0)
                        │       │   │   │           │   ├── "l1.l_partkey"(#3.1)
                        │       │   │   │           │   ├── "l1.l_quantity"(#3.4)
                        │       │   │   │           │   ├── "l1.l_receiptdate"(#3.12)
                        │       │   │   │           │   ├── "l1.l_returnflag"(#3.8)
                        │       │   │   │           │   ├── "l1.l_shipdate"(#3.10)
                        │       │   │   │           │   ├── "l1.l_shipinstruct"(#3.13)
                        │       │   │   │           │   ├── "l1.l_shipmode"(#3.14)
                        │       │   │   │           │   ├── "l1.l_suppkey"(#3.2)
                        │       │   │   │           │   └── "l1.l_tax"(#3.7)
                        │       │   │   │           ├── (.cardinality): 0.00
                        │       │   │   │           └── Get
                        │       │   │   │               ├── .data_source_id: 8
                        │       │   │   │               ├── .table_index: 2
                        │       │   │   │               ├── .implementation: None
                        │       │   │   │               ├── (.output_columns):
                        │       │   │   │               │   ┌── "lineitem.l_comment"(#2.15)
                        │       │   │   │               │   ├── "lineitem.l_commitdate"(#2.11)
                        │       │   │   │               │   ├── "lineitem.l_discount"(#2.6)
                        │       │   │   │               │   ├── "lineitem.l_extendedprice"(#2.5)
                        │       │   │   │               │   ├── "lineitem.l_linenumber"(#2.3)
                        │       │   │   │               │   ├── "lineitem.l_linestatus"(#2.9)
                        │       │   │   │               │   ├── "lineitem.l_orderkey"(#2.0)
                        │       │   │   │               │   ├── "lineitem.l_partkey"(#2.1)
                        │       │   │   │               │   ├── "lineitem.l_quantity"(#2.4)
                        │       │   │   │               │   ├── "lineitem.l_receiptdate"(#2.12)
                        │       │   │   │               │   ├── "lineitem.l_returnflag"(#2.8)
                        │       │   │   │               │   ├── "lineitem.l_shipdate"(#2.10)
                        │       │   │   │               │   ├── "lineitem.l_shipinstruct"(#2.13)
                        │       │   │   │               │   ├── "lineitem.l_shipmode"(#2.14)
                        │       │   │   │               │   ├── "lineitem.l_suppkey"(#2.2)
                        │       │   │   │               │   └── "lineitem.l_tax"(#2.7)
                        │       │   │   │               └── (.cardinality): 0.00
                        │       │   │   └── Select
                        │       │   │       ├── .predicate: "orders.o_orderstatus"(#4.2) = 'F'::utf8_view
                        │       │   │       ├── (.output_columns): [ "orders.o_orderkey"(#4.0), "orders.o_orderstatus"(#4.2) ]
                        │       │   │       ├── (.cardinality): 0.00
                        │       │   │       └── Get
                        │       │   │           ├── .data_source_id: 7
                        │       │   │           ├── .table_index: 4
                        │       │   │           ├── .implementation: None
                        │       │   │           ├── (.output_columns): [ "orders.o_orderkey"(#4.0), "orders.o_orderstatus"(#4.2) ]
                        │       │   │           └── (.cardinality): 0.00
                        │       │   └── Get { .data_source_id: 1, .table_index: 5, .implementation: None, (.output_columns): "nation.n_nationkey"(#5.0), (.cardinality): 0.00 }
                        │       └── Project
                        │           ├── .table_index: 23
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
                        │           │   ├── "l2.l_comment"(#7.15)
                        │           │   ├── "__#21.l_orderkey"(#21.0)
                        │           │   └── "__#21.l_suppkey"(#21.1)
                        │           ├── (.output_columns):
                        │           │   ┌── "__#23.l_comment"(#23.15)
                        │           │   ├── "__#23.l_commitdate"(#23.11)
                        │           │   ├── "__#23.l_discount"(#23.6)
                        │           │   ├── "__#23.l_extendedprice"(#23.5)
                        │           │   ├── "__#23.l_linenumber"(#23.3)
                        │           │   ├── "__#23.l_linestatus"(#23.9)
                        │           │   ├── "__#23.l_orderkey"(#23.0)
                        │           │   ├── "__#23.l_orderkey__optd_1"(#23.16)
                        │           │   ├── "__#23.l_partkey"(#23.1)
                        │           │   ├── "__#23.l_quantity"(#23.4)
                        │           │   ├── "__#23.l_receiptdate"(#23.12)
                        │           │   ├── "__#23.l_returnflag"(#23.8)
                        │           │   ├── "__#23.l_shipdate"(#23.10)
                        │           │   ├── "__#23.l_shipinstruct"(#23.13)
                        │           │   ├── "__#23.l_shipmode"(#23.14)
                        │           │   ├── "__#23.l_suppkey"(#23.2)
                        │           │   ├── "__#23.l_suppkey__optd_1"(#23.17)
                        │           │   └── "__#23.l_tax"(#23.7)
                        │           ├── (.cardinality): 0.00
                        │           └── Join
                        │               ├── .join_type: Inner
                        │               ├── .implementation: None
                        │               ├── .join_cond: ("l2.l_orderkey"(#7.0) = "__#21.l_orderkey"(#21.0)) AND ("l2.l_suppkey"(#7.2) != "__#21.l_suppkey"(#21.1))
                        │               ├── (.output_columns):
                        │               │   ┌── "__#21.l_orderkey"(#21.0)
                        │               │   ├── "__#21.l_suppkey"(#21.1)
                        │               │   ├── "l2.l_comment"(#7.15)
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
                        │               ├── Aggregate
                        │               │   ├── .key_table_index: 21
                        │               │   ├── .aggregate_table_index: 22
                        │               │   ├── .implementation: None
                        │               │   ├── .exprs: []
                        │               │   ├── .keys: [ "l1.l_orderkey"(#3.0), "l1.l_suppkey"(#3.2) ]
                        │               │   ├── (.output_columns): [ "__#21.l_orderkey"(#21.0), "__#21.l_suppkey"(#21.1) ]
                        │               │   ├── (.cardinality): 0.00
                        │               │   └── Join
                        │               │       ├── .join_type: Inner
                        │               │       ├── .implementation: None
                        │               │       ├── .join_cond: true::boolean
                        │               │       ├── (.output_columns):
                        │               │       │   ┌── "l1.l_comment"(#3.15)
                        │               │       │   ├── "l1.l_commitdate"(#3.11)
                        │               │       │   ├── "l1.l_discount"(#3.6)
                        │               │       │   ├── "l1.l_extendedprice"(#3.5)
                        │               │       │   ├── "l1.l_linenumber"(#3.3)
                        │               │       │   ├── "l1.l_linestatus"(#3.9)
                        │               │       │   ├── "l1.l_orderkey"(#3.0)
                        │               │       │   ├── "l1.l_partkey"(#3.1)
                        │               │       │   ├── "l1.l_quantity"(#3.4)
                        │               │       │   ├── "l1.l_receiptdate"(#3.12)
                        │               │       │   ├── "l1.l_returnflag"(#3.8)
                        │               │       │   ├── "l1.l_shipdate"(#3.10)
                        │               │       │   ├── "l1.l_shipinstruct"(#3.13)
                        │               │       │   ├── "l1.l_shipmode"(#3.14)
                        │               │       │   ├── "l1.l_suppkey"(#3.2)
                        │               │       │   ├── "l1.l_tax"(#3.7)
                        │               │       │   ├── "nation.n_nationkey"(#5.0)
                        │               │       │   ├── "orders.o_orderkey"(#4.0)
                        │               │       │   ├── "orders.o_orderstatus"(#4.2)
                        │               │       │   └── "supplier.s_suppkey"(#1.0)
                        │               │       ├── (.cardinality): 0.00
                        │               │       ├── Join
                        │               │       │   ├── .join_type: Inner
                        │               │       │   ├── .implementation: None
                        │               │       │   ├── .join_cond: "orders.o_orderkey"(#4.0) = "l1.l_orderkey"(#3.0)
                        │               │       │   ├── (.output_columns):
                        │               │       │   │   ┌── "l1.l_comment"(#3.15)
                        │               │       │   │   ├── "l1.l_commitdate"(#3.11)
                        │               │       │   │   ├── "l1.l_discount"(#3.6)
                        │               │       │   │   ├── "l1.l_extendedprice"(#3.5)
                        │               │       │   │   ├── "l1.l_linenumber"(#3.3)
                        │               │       │   │   ├── "l1.l_linestatus"(#3.9)
                        │               │       │   │   ├── "l1.l_orderkey"(#3.0)
                        │               │       │   │   ├── "l1.l_partkey"(#3.1)
                        │               │       │   │   ├── "l1.l_quantity"(#3.4)
                        │               │       │   │   ├── "l1.l_receiptdate"(#3.12)
                        │               │       │   │   ├── "l1.l_returnflag"(#3.8)
                        │               │       │   │   ├── "l1.l_shipdate"(#3.10)
                        │               │       │   │   ├── "l1.l_shipinstruct"(#3.13)
                        │               │       │   │   ├── "l1.l_shipmode"(#3.14)
                        │               │       │   │   ├── "l1.l_suppkey"(#3.2)
                        │               │       │   │   ├── "l1.l_tax"(#3.7)
                        │               │       │   │   ├── "orders.o_orderkey"(#4.0)
                        │               │       │   │   ├── "orders.o_orderstatus"(#4.2)
                        │               │       │   │   └── "supplier.s_suppkey"(#1.0)
                        │               │       │   ├── (.cardinality): 0.00
                        │               │       │   ├── Join
                        │               │       │   │   ├── .join_type: Inner
                        │               │       │   │   ├── .implementation: None
                        │               │       │   │   ├── .join_cond: "supplier.s_suppkey"(#1.0) = "l1.l_suppkey"(#3.2)
                        │               │       │   │   ├── (.output_columns):
                        │               │       │   │   │   ┌── "l1.l_comment"(#3.15)
                        │               │       │   │   │   ├── "l1.l_commitdate"(#3.11)
                        │               │       │   │   │   ├── "l1.l_discount"(#3.6)
                        │               │       │   │   │   ├── "l1.l_extendedprice"(#3.5)
                        │               │       │   │   │   ├── "l1.l_linenumber"(#3.3)
                        │               │       │   │   │   ├── "l1.l_linestatus"(#3.9)
                        │               │       │   │   │   ├── "l1.l_orderkey"(#3.0)
                        │               │       │   │   │   ├── "l1.l_partkey"(#3.1)
                        │               │       │   │   │   ├── "l1.l_quantity"(#3.4)
                        │               │       │   │   │   ├── "l1.l_receiptdate"(#3.12)
                        │               │       │   │   │   ├── "l1.l_returnflag"(#3.8)
                        │               │       │   │   │   ├── "l1.l_shipdate"(#3.10)
                        │               │       │   │   │   ├── "l1.l_shipinstruct"(#3.13)
                        │               │       │   │   │   ├── "l1.l_shipmode"(#3.14)
                        │               │       │   │   │   ├── "l1.l_suppkey"(#3.2)
                        │               │       │   │   │   ├── "l1.l_tax"(#3.7)
                        │               │       │   │   │   └── "supplier.s_suppkey"(#1.0)
                        │               │       │   │   ├── (.cardinality): 0.00
                        │               │       │   │   ├── Get { .data_source_id: 4, .table_index: 1, .implementation: None, (.output_columns): "supplier.s_suppkey"(#1.0), (.cardinality): 0.00 }
                        │               │       │   │   └── Select
                        │               │       │   │       ├── .predicate: "l1.l_receiptdate"(#3.12) > "l1.l_commitdate"(#3.11)
                        │               │       │   │       ├── (.output_columns):
                        │               │       │   │       │   ┌── "l1.l_comment"(#3.15)
                        │               │       │   │       │   ├── "l1.l_commitdate"(#3.11)
                        │               │       │   │       │   ├── "l1.l_discount"(#3.6)
                        │               │       │   │       │   ├── "l1.l_extendedprice"(#3.5)
                        │               │       │   │       │   ├── "l1.l_linenumber"(#3.3)
                        │               │       │   │       │   ├── "l1.l_linestatus"(#3.9)
                        │               │       │   │       │   ├── "l1.l_orderkey"(#3.0)
                        │               │       │   │       │   ├── "l1.l_partkey"(#3.1)
                        │               │       │   │       │   ├── "l1.l_quantity"(#3.4)
                        │               │       │   │       │   ├── "l1.l_receiptdate"(#3.12)
                        │               │       │   │       │   ├── "l1.l_returnflag"(#3.8)
                        │               │       │   │       │   ├── "l1.l_shipdate"(#3.10)
                        │               │       │   │       │   ├── "l1.l_shipinstruct"(#3.13)
                        │               │       │   │       │   ├── "l1.l_shipmode"(#3.14)
                        │               │       │   │       │   ├── "l1.l_suppkey"(#3.2)
                        │               │       │   │       │   └── "l1.l_tax"(#3.7)
                        │               │       │   │       ├── (.cardinality): 0.00
                        │               │       │   │       └── Remap
                        │               │       │   │           ├── .table_index: 3
                        │               │       │   │           ├── (.output_columns):
                        │               │       │   │           │   ┌── "l1.l_comment"(#3.15)
                        │               │       │   │           │   ├── "l1.l_commitdate"(#3.11)
                        │               │       │   │           │   ├── "l1.l_discount"(#3.6)
                        │               │       │   │           │   ├── "l1.l_extendedprice"(#3.5)
                        │               │       │   │           │   ├── "l1.l_linenumber"(#3.3)
                        │               │       │   │           │   ├── "l1.l_linestatus"(#3.9)
                        │               │       │   │           │   ├── "l1.l_orderkey"(#3.0)
                        │               │       │   │           │   ├── "l1.l_partkey"(#3.1)
                        │               │       │   │           │   ├── "l1.l_quantity"(#3.4)
                        │               │       │   │           │   ├── "l1.l_receiptdate"(#3.12)
                        │               │       │   │           │   ├── "l1.l_returnflag"(#3.8)
                        │               │       │   │           │   ├── "l1.l_shipdate"(#3.10)
                        │               │       │   │           │   ├── "l1.l_shipinstruct"(#3.13)
                        │               │       │   │           │   ├── "l1.l_shipmode"(#3.14)
                        │               │       │   │           │   ├── "l1.l_suppkey"(#3.2)
                        │               │       │   │           │   └── "l1.l_tax"(#3.7)
                        │               │       │   │           ├── (.cardinality): 0.00
                        │               │       │   │           └── Get
                        │               │       │   │               ├── .data_source_id: 8
                        │               │       │   │               ├── .table_index: 2
                        │               │       │   │               ├── .implementation: None
                        │               │       │   │               ├── (.output_columns):
                        │               │       │   │               │   ┌── "lineitem.l_comment"(#2.15)
                        │               │       │   │               │   ├── "lineitem.l_commitdate"(#2.11)
                        │               │       │   │               │   ├── "lineitem.l_discount"(#2.6)
                        │               │       │   │               │   ├── "lineitem.l_extendedprice"(#2.5)
                        │               │       │   │               │   ├── "lineitem.l_linenumber"(#2.3)
                        │               │       │   │               │   ├── "lineitem.l_linestatus"(#2.9)
                        │               │       │   │               │   ├── "lineitem.l_orderkey"(#2.0)
                        │               │       │   │               │   ├── "lineitem.l_partkey"(#2.1)
                        │               │       │   │               │   ├── "lineitem.l_quantity"(#2.4)
                        │               │       │   │               │   ├── "lineitem.l_receiptdate"(#2.12)
                        │               │       │   │               │   ├── "lineitem.l_returnflag"(#2.8)
                        │               │       │   │               │   ├── "lineitem.l_shipdate"(#2.10)
                        │               │       │   │               │   ├── "lineitem.l_shipinstruct"(#2.13)
                        │               │       │   │               │   ├── "lineitem.l_shipmode"(#2.14)
                        │               │       │   │               │   ├── "lineitem.l_suppkey"(#2.2)
                        │               │       │   │               │   └── "lineitem.l_tax"(#2.7)
                        │               │       │   │               └── (.cardinality): 0.00
                        │               │       │   └── Select
                        │               │       │       ├── .predicate: "orders.o_orderstatus"(#4.2) = 'F'::utf8_view
                        │               │       │       ├── (.output_columns): [ "orders.o_orderkey"(#4.0), "orders.o_orderstatus"(#4.2) ]
                        │               │       │       ├── (.cardinality): 0.00
                        │               │       │       └── Get
                        │               │       │           ├── .data_source_id: 7
                        │               │       │           ├── .table_index: 4
                        │               │       │           ├── .implementation: None
                        │               │       │           ├── (.output_columns): [ "orders.o_orderkey"(#4.0), "orders.o_orderstatus"(#4.2) ]
                        │               │       │           └── (.cardinality): 0.00
                        │               │       └── Get { .data_source_id: 1, .table_index: 5, .implementation: None, (.output_columns): "nation.n_nationkey"(#5.0), (.cardinality): 0.00 }
                        │               └── Remap
                        │                   ├── .table_index: 7
                        │                   ├── (.output_columns):
                        │                   │   ┌── "l2.l_comment"(#7.15)
                        │                   │   ├── "l2.l_commitdate"(#7.11)
                        │                   │   ├── "l2.l_discount"(#7.6)
                        │                   │   ├── "l2.l_extendedprice"(#7.5)
                        │                   │   ├── "l2.l_linenumber"(#7.3)
                        │                   │   ├── "l2.l_linestatus"(#7.9)
                        │                   │   ├── "l2.l_orderkey"(#7.0)
                        │                   │   ├── "l2.l_partkey"(#7.1)
                        │                   │   ├── "l2.l_quantity"(#7.4)
                        │                   │   ├── "l2.l_receiptdate"(#7.12)
                        │                   │   ├── "l2.l_returnflag"(#7.8)
                        │                   │   ├── "l2.l_shipdate"(#7.10)
                        │                   │   ├── "l2.l_shipinstruct"(#7.13)
                        │                   │   ├── "l2.l_shipmode"(#7.14)
                        │                   │   ├── "l2.l_suppkey"(#7.2)
                        │                   │   └── "l2.l_tax"(#7.7)
                        │                   ├── (.cardinality): 0.00
                        │                   └── Get
                        │                       ├── .data_source_id: 8
                        │                       ├── .table_index: 6
                        │                       ├── .implementation: None
                        │                       ├── (.output_columns):
                        │                       │   ┌── "lineitem.l_comment"(#6.15)
                        │                       │   ├── "lineitem.l_commitdate"(#6.11)
                        │                       │   ├── "lineitem.l_discount"(#6.6)
                        │                       │   ├── "lineitem.l_extendedprice"(#6.5)
                        │                       │   ├── "lineitem.l_linenumber"(#6.3)
                        │                       │   ├── "lineitem.l_linestatus"(#6.9)
                        │                       │   ├── "lineitem.l_orderkey"(#6.0)
                        │                       │   ├── "lineitem.l_partkey"(#6.1)
                        │                       │   ├── "lineitem.l_quantity"(#6.4)
                        │                       │   ├── "lineitem.l_receiptdate"(#6.12)
                        │                       │   ├── "lineitem.l_returnflag"(#6.8)
                        │                       │   ├── "lineitem.l_shipdate"(#6.10)
                        │                       │   ├── "lineitem.l_shipinstruct"(#6.13)
                        │                       │   ├── "lineitem.l_shipmode"(#6.14)
                        │                       │   ├── "lineitem.l_suppkey"(#6.2)
                        │                       │   └── "lineitem.l_tax"(#6.7)
                        │                       └── (.cardinality): 0.00
                        └── Select
                            ├── .predicate: "l3.l_receiptdate"(#10.12) > "l3.l_commitdate"(#10.11)
                            ├── (.output_columns):
                            │   ┌── "l3.l_comment"(#10.15)
                            │   ├── "l3.l_commitdate"(#10.11)
                            │   ├── "l3.l_discount"(#10.6)
                            │   ├── "l3.l_extendedprice"(#10.5)
                            │   ├── "l3.l_linenumber"(#10.3)
                            │   ├── "l3.l_linestatus"(#10.9)
                            │   ├── "l3.l_orderkey"(#10.0)
                            │   ├── "l3.l_partkey"(#10.1)
                            │   ├── "l3.l_quantity"(#10.4)
                            │   ├── "l3.l_receiptdate"(#10.12)
                            │   ├── "l3.l_returnflag"(#10.8)
                            │   ├── "l3.l_shipdate"(#10.10)
                            │   ├── "l3.l_shipinstruct"(#10.13)
                            │   ├── "l3.l_shipmode"(#10.14)
                            │   ├── "l3.l_suppkey"(#10.2)
                            │   └── "l3.l_tax"(#10.7)
                            ├── (.cardinality): 0.00
                            └── Remap
                                ├── .table_index: 10
                                ├── (.output_columns):
                                │   ┌── "l3.l_comment"(#10.15)
                                │   ├── "l3.l_commitdate"(#10.11)
                                │   ├── "l3.l_discount"(#10.6)
                                │   ├── "l3.l_extendedprice"(#10.5)
                                │   ├── "l3.l_linenumber"(#10.3)
                                │   ├── "l3.l_linestatus"(#10.9)
                                │   ├── "l3.l_orderkey"(#10.0)
                                │   ├── "l3.l_partkey"(#10.1)
                                │   ├── "l3.l_quantity"(#10.4)
                                │   ├── "l3.l_receiptdate"(#10.12)
                                │   ├── "l3.l_returnflag"(#10.8)
                                │   ├── "l3.l_shipdate"(#10.10)
                                │   ├── "l3.l_shipinstruct"(#10.13)
                                │   ├── "l3.l_shipmode"(#10.14)
                                │   ├── "l3.l_suppkey"(#10.2)
                                │   └── "l3.l_tax"(#10.7)
                                ├── (.cardinality): 0.00
                                └── Get
                                    ├── .data_source_id: 8
                                    ├── .table_index: 9
                                    ├── .implementation: None
                                    ├── (.output_columns):
                                    │   ┌── "lineitem.l_comment"(#9.15)
                                    │   ├── "lineitem.l_commitdate"(#9.11)
                                    │   ├── "lineitem.l_discount"(#9.6)
                                    │   ├── "lineitem.l_extendedprice"(#9.5)
                                    │   ├── "lineitem.l_linenumber"(#9.3)
                                    │   ├── "lineitem.l_linestatus"(#9.9)
                                    │   ├── "lineitem.l_orderkey"(#9.0)
                                    │   ├── "lineitem.l_partkey"(#9.1)
                                    │   ├── "lineitem.l_quantity"(#9.4)
                                    │   ├── "lineitem.l_receiptdate"(#9.12)
                                    │   ├── "lineitem.l_returnflag"(#9.8)
                                    │   ├── "lineitem.l_shipdate"(#9.10)
                                    │   ├── "lineitem.l_shipinstruct"(#9.13)
                                    │   ├── "lineitem.l_shipmode"(#9.14)
                                    │   ├── "lineitem.l_suppkey"(#9.2)
                                    │   └── "lineitem.l_tax"(#9.7)
                                    └── (.cardinality): 0.00
*/

