-- TPC-H Q18
select
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
from
    customer,
    orders,
    lineitem
where
    o_orderkey in (
        select
            l_orderkey
        from
            lineitem
        group by
            l_orderkey having
                sum(l_quantity) > 250 -- original: 300
    )
    and c_custkey = o_custkey
    and o_orderkey = l_orderkey
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
order by
    o_totalprice desc,
    o_orderdate
limit 100;

/*
logical_plan after optd-initial:
Limit
├── .skip: 0::bigint
├── .fetch: 100::bigint
├── (.output_columns):
│   ┌── "__#10.c_custkey"(#10.1)
│   ├── "__#10.c_name"(#10.0)
│   ├── "__#10.o_orderdate"(#10.3)
│   ├── "__#10.o_orderkey"(#10.2)
│   ├── "__#10.o_totalprice"(#10.4)
│   └── "__#10.sum(lineitem.l_quantity)"(#10.5)
├── (.cardinality): 0.00
└── OrderBy
    ├── ordering_exprs: [ "__#10.o_totalprice"(#10.4) DESC, "__#10.o_orderdate"(#10.3) ASC ]
    ├── (.output_columns):
    │   ┌── "__#10.c_custkey"(#10.1)
    │   ├── "__#10.c_name"(#10.0)
    │   ├── "__#10.o_orderdate"(#10.3)
    │   ├── "__#10.o_orderkey"(#10.2)
    │   ├── "__#10.o_totalprice"(#10.4)
    │   └── "__#10.sum(lineitem.l_quantity)"(#10.5)
    ├── (.cardinality): 0.00
    └── Project
        ├── .table_index: 10
        ├── .projections:
        │   ┌── "customer.c_name"(#1.1)
        │   ├── "customer.c_custkey"(#1.0)
        │   ├── "orders.o_orderkey"(#2.0)
        │   ├── "orders.o_orderdate"(#2.4)
        │   ├── "orders.o_totalprice"(#2.3)
        │   └── "__#9.sum(lineitem.l_quantity)"(#9.0)
        ├── (.output_columns):
        │   ┌── "__#10.c_custkey"(#10.1)
        │   ├── "__#10.c_name"(#10.0)
        │   ├── "__#10.o_orderdate"(#10.3)
        │   ├── "__#10.o_orderkey"(#10.2)
        │   ├── "__#10.o_totalprice"(#10.4)
        │   └── "__#10.sum(lineitem.l_quantity)"(#10.5)
        ├── (.cardinality): 0.00
        └── Aggregate
            ├── .key_table_index: 8
            ├── .aggregate_table_index: 9
            ├── .implementation: None
            ├── .exprs: sum("lineitem.l_quantity"(#3.4))
            ├── .keys:
            │   ┌── "customer.c_name"(#1.1)
            │   ├── "customer.c_custkey"(#1.0)
            │   ├── "orders.o_orderkey"(#2.0)
            │   ├── "orders.o_orderdate"(#2.4)
            │   └── "orders.o_totalprice"(#2.3)
            ├── (.output_columns):
            │   ┌── "__#8.c_custkey"(#8.1)
            │   ├── "__#8.c_name"(#8.0)
            │   ├── "__#8.o_orderdate"(#8.3)
            │   ├── "__#8.o_orderkey"(#8.2)
            │   ├── "__#8.o_totalprice"(#8.4)
            │   └── "__#9.sum(lineitem.l_quantity)"(#9.0)
            ├── (.cardinality): 0.00
            └── Select
                ├── .predicate: ("customer.c_custkey"(#1.0) = "orders.o_custkey"(#2.1)) AND ("orders.o_orderkey"(#2.0) = "lineitem.l_orderkey"(#3.0))
                ├── (.output_columns):
                │   ┌── "customer.c_acctbal"(#1.5)
                │   ├── "customer.c_address"(#1.2)
                │   ├── "customer.c_comment"(#1.7)
                │   ├── "customer.c_custkey"(#1.0)
                │   ├── "customer.c_mktsegment"(#1.6)
                │   ├── "customer.c_name"(#1.1)
                │   ├── "customer.c_nationkey"(#1.3)
                │   ├── "customer.c_phone"(#1.4)
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
                │   ├── "orders.o_clerk"(#2.6)
                │   ├── "orders.o_comment"(#2.8)
                │   ├── "orders.o_custkey"(#2.1)
                │   ├── "orders.o_orderdate"(#2.4)
                │   ├── "orders.o_orderkey"(#2.0)
                │   ├── "orders.o_orderpriority"(#2.5)
                │   ├── "orders.o_orderstatus"(#2.2)
                │   ├── "orders.o_shippriority"(#2.7)
                │   └── "orders.o_totalprice"(#2.3)
                ├── (.cardinality): 0.00
                └── DependentJoin
                    ├── .join_type: LeftSemi
                    ├── .join_cond: "orders.o_orderkey"(#2.0) = "__#7.l_orderkey"(#7.0)
                    ├── (.output_columns):
                    │   ┌── "customer.c_acctbal"(#1.5)
                    │   ├── "customer.c_address"(#1.2)
                    │   ├── "customer.c_comment"(#1.7)
                    │   ├── "customer.c_custkey"(#1.0)
                    │   ├── "customer.c_mktsegment"(#1.6)
                    │   ├── "customer.c_name"(#1.1)
                    │   ├── "customer.c_nationkey"(#1.3)
                    │   ├── "customer.c_phone"(#1.4)
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
                    │   ├── "orders.o_clerk"(#2.6)
                    │   ├── "orders.o_comment"(#2.8)
                    │   ├── "orders.o_custkey"(#2.1)
                    │   ├── "orders.o_orderdate"(#2.4)
                    │   ├── "orders.o_orderkey"(#2.0)
                    │   ├── "orders.o_orderpriority"(#2.5)
                    │   ├── "orders.o_orderstatus"(#2.2)
                    │   ├── "orders.o_shippriority"(#2.7)
                    │   └── "orders.o_totalprice"(#2.3)
                    ├── (.cardinality): 0.00
                    ├── Join
                    │   ├── .join_type: Inner
                    │   ├── .implementation: None
                    │   ├── .join_cond: 
                    │   ├── (.output_columns):
                    │   │   ┌── "customer.c_acctbal"(#1.5)
                    │   │   ├── "customer.c_address"(#1.2)
                    │   │   ├── "customer.c_comment"(#1.7)
                    │   │   ├── "customer.c_custkey"(#1.0)
                    │   │   ├── "customer.c_mktsegment"(#1.6)
                    │   │   ├── "customer.c_name"(#1.1)
                    │   │   ├── "customer.c_nationkey"(#1.3)
                    │   │   ├── "customer.c_phone"(#1.4)
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
                    │   │   ├── "orders.o_clerk"(#2.6)
                    │   │   ├── "orders.o_comment"(#2.8)
                    │   │   ├── "orders.o_custkey"(#2.1)
                    │   │   ├── "orders.o_orderdate"(#2.4)
                    │   │   ├── "orders.o_orderkey"(#2.0)
                    │   │   ├── "orders.o_orderpriority"(#2.5)
                    │   │   ├── "orders.o_orderstatus"(#2.2)
                    │   │   ├── "orders.o_shippriority"(#2.7)
                    │   │   └── "orders.o_totalprice"(#2.3)
                    │   ├── (.cardinality): 0.00
                    │   ├── Join
                    │   │   ├── .join_type: Inner
                    │   │   ├── .implementation: None
                    │   │   ├── .join_cond: 
                    │   │   ├── (.output_columns):
                    │   │   │   ┌── "customer.c_acctbal"(#1.5)
                    │   │   │   ├── "customer.c_address"(#1.2)
                    │   │   │   ├── "customer.c_comment"(#1.7)
                    │   │   │   ├── "customer.c_custkey"(#1.0)
                    │   │   │   ├── "customer.c_mktsegment"(#1.6)
                    │   │   │   ├── "customer.c_name"(#1.1)
                    │   │   │   ├── "customer.c_nationkey"(#1.3)
                    │   │   │   ├── "customer.c_phone"(#1.4)
                    │   │   │   ├── "orders.o_clerk"(#2.6)
                    │   │   │   ├── "orders.o_comment"(#2.8)
                    │   │   │   ├── "orders.o_custkey"(#2.1)
                    │   │   │   ├── "orders.o_orderdate"(#2.4)
                    │   │   │   ├── "orders.o_orderkey"(#2.0)
                    │   │   │   ├── "orders.o_orderpriority"(#2.5)
                    │   │   │   ├── "orders.o_orderstatus"(#2.2)
                    │   │   │   ├── "orders.o_shippriority"(#2.7)
                    │   │   │   └── "orders.o_totalprice"(#2.3)
                    │   │   ├── (.cardinality): 0.00
                    │   │   ├── Get
                    │   │   │   ├── .data_source_id: 6
                    │   │   │   ├── .table_index: 1
                    │   │   │   ├── .implementation: None
                    │   │   │   ├── (.output_columns):
                    │   │   │   │   ┌── "customer.c_acctbal"(#1.5)
                    │   │   │   │   ├── "customer.c_address"(#1.2)
                    │   │   │   │   ├── "customer.c_comment"(#1.7)
                    │   │   │   │   ├── "customer.c_custkey"(#1.0)
                    │   │   │   │   ├── "customer.c_mktsegment"(#1.6)
                    │   │   │   │   ├── "customer.c_name"(#1.1)
                    │   │   │   │   ├── "customer.c_nationkey"(#1.3)
                    │   │   │   │   └── "customer.c_phone"(#1.4)
                    │   │   │   └── (.cardinality): 0.00
                    │   │   └── Get
                    │   │       ├── .data_source_id: 7
                    │   │       ├── .table_index: 2
                    │   │       ├── .implementation: None
                    │   │       ├── (.output_columns):
                    │   │       │   ┌── "orders.o_clerk"(#2.6)
                    │   │       │   ├── "orders.o_comment"(#2.8)
                    │   │       │   ├── "orders.o_custkey"(#2.1)
                    │   │       │   ├── "orders.o_orderdate"(#2.4)
                    │   │       │   ├── "orders.o_orderkey"(#2.0)
                    │   │       │   ├── "orders.o_orderpriority"(#2.5)
                    │   │       │   ├── "orders.o_orderstatus"(#2.2)
                    │   │       │   ├── "orders.o_shippriority"(#2.7)
                    │   │       │   └── "orders.o_totalprice"(#2.3)
                    │   │       └── (.cardinality): 0.00
                    │   └── Get
                    │       ├── .data_source_id: 8
                    │       ├── .table_index: 3
                    │       ├── .implementation: None
                    │       ├── (.output_columns):
                    │       │   ┌── "lineitem.l_comment"(#3.15)
                    │       │   ├── "lineitem.l_commitdate"(#3.11)
                    │       │   ├── "lineitem.l_discount"(#3.6)
                    │       │   ├── "lineitem.l_extendedprice"(#3.5)
                    │       │   ├── "lineitem.l_linenumber"(#3.3)
                    │       │   ├── "lineitem.l_linestatus"(#3.9)
                    │       │   ├── "lineitem.l_orderkey"(#3.0)
                    │       │   ├── "lineitem.l_partkey"(#3.1)
                    │       │   ├── "lineitem.l_quantity"(#3.4)
                    │       │   ├── "lineitem.l_receiptdate"(#3.12)
                    │       │   ├── "lineitem.l_returnflag"(#3.8)
                    │       │   ├── "lineitem.l_shipdate"(#3.10)
                    │       │   ├── "lineitem.l_shipinstruct"(#3.13)
                    │       │   ├── "lineitem.l_shipmode"(#3.14)
                    │       │   ├── "lineitem.l_suppkey"(#3.2)
                    │       │   └── "lineitem.l_tax"(#3.7)
                    │       └── (.cardinality): 0.00
                    └── Project
                        ├── .table_index: 7
                        ├── .projections: "lineitem.l_orderkey"(#4.0)
                        ├── (.output_columns): "__#7.l_orderkey"(#7.0)
                        ├── (.cardinality): 0.00
                        └── Select
                            ├── .predicate: "__#6.sum(lineitem.l_quantity)"(#6.0) > CAST (250::bigint AS Decimal128(25, 2))
                            ├── (.output_columns): [ "__#5.l_orderkey"(#5.0), "__#6.sum(lineitem.l_quantity)"(#6.0) ]
                            ├── (.cardinality): 0.00
                            └── Aggregate
                                ├── .key_table_index: 5
                                ├── .aggregate_table_index: 6
                                ├── .implementation: None
                                ├── .exprs: sum("lineitem.l_quantity"(#4.4))
                                ├── .keys: "lineitem.l_orderkey"(#4.0)
                                ├── (.output_columns): [ "__#5.l_orderkey"(#5.0), "__#6.sum(lineitem.l_quantity)"(#6.0) ]
                                ├── (.cardinality): 0.00
                                └── Get
                                    ├── .data_source_id: 8
                                    ├── .table_index: 4
                                    ├── .implementation: None
                                    ├── (.output_columns):
                                    │   ┌── "lineitem.l_comment"(#4.15)
                                    │   ├── "lineitem.l_commitdate"(#4.11)
                                    │   ├── "lineitem.l_discount"(#4.6)
                                    │   ├── "lineitem.l_extendedprice"(#4.5)
                                    │   ├── "lineitem.l_linenumber"(#4.3)
                                    │   ├── "lineitem.l_linestatus"(#4.9)
                                    │   ├── "lineitem.l_orderkey"(#4.0)
                                    │   ├── "lineitem.l_partkey"(#4.1)
                                    │   ├── "lineitem.l_quantity"(#4.4)
                                    │   ├── "lineitem.l_receiptdate"(#4.12)
                                    │   ├── "lineitem.l_returnflag"(#4.8)
                                    │   ├── "lineitem.l_shipdate"(#4.10)
                                    │   ├── "lineitem.l_shipinstruct"(#4.13)
                                    │   ├── "lineitem.l_shipmode"(#4.14)
                                    │   ├── "lineitem.l_suppkey"(#4.2)
                                    │   └── "lineitem.l_tax"(#4.7)
                                    └── (.cardinality): 0.00

physical_plan after optd-finalized:
Limit
├── .skip: 0::bigint
├── .fetch: 100::bigint
├── (.output_columns):
│   ┌── "__#10.c_custkey"(#10.1)
│   ├── "__#10.c_name"(#10.0)
│   ├── "__#10.o_orderdate"(#10.3)
│   ├── "__#10.o_orderkey"(#10.2)
│   ├── "__#10.o_totalprice"(#10.4)
│   └── "__#10.sum(lineitem.l_quantity)"(#10.5)
├── (.cardinality): 0.00
└── EnforcerSort
    ├── tuple_ordering: [(#10.4, Desc), (#10.3, Asc)]
    ├── (.output_columns):
    │   ┌── "__#10.c_custkey"(#10.1)
    │   ├── "__#10.c_name"(#10.0)
    │   ├── "__#10.o_orderdate"(#10.3)
    │   ├── "__#10.o_orderkey"(#10.2)
    │   ├── "__#10.o_totalprice"(#10.4)
    │   └── "__#10.sum(lineitem.l_quantity)"(#10.5)
    ├── (.cardinality): 0.00
    └── Project
        ├── .table_index: 10
        ├── .projections:
        │   ┌── "customer.c_name"(#1.1)
        │   ├── "customer.c_custkey"(#1.0)
        │   ├── "orders.o_orderkey"(#2.0)
        │   ├── "orders.o_orderdate"(#2.4)
        │   ├── "orders.o_totalprice"(#2.3)
        │   └── "__#9.sum(lineitem.l_quantity)"(#9.0)
        ├── (.output_columns):
        │   ┌── "__#10.c_custkey"(#10.1)
        │   ├── "__#10.c_name"(#10.0)
        │   ├── "__#10.o_orderdate"(#10.3)
        │   ├── "__#10.o_orderkey"(#10.2)
        │   ├── "__#10.o_totalprice"(#10.4)
        │   └── "__#10.sum(lineitem.l_quantity)"(#10.5)
        ├── (.cardinality): 0.00
        └── Aggregate
            ├── .key_table_index: 8
            ├── .aggregate_table_index: 9
            ├── .implementation: None
            ├── .exprs: sum("lineitem.l_quantity"(#3.4))
            ├── .keys:
            │   ┌── "customer.c_name"(#1.1)
            │   ├── "customer.c_custkey"(#1.0)
            │   ├── "orders.o_orderkey"(#2.0)
            │   ├── "orders.o_orderdate"(#2.4)
            │   └── "orders.o_totalprice"(#2.3)
            ├── (.output_columns):
            │   ┌── "__#8.c_custkey"(#8.1)
            │   ├── "__#8.c_name"(#8.0)
            │   ├── "__#8.o_orderdate"(#8.3)
            │   ├── "__#8.o_orderkey"(#8.2)
            │   ├── "__#8.o_totalprice"(#8.4)
            │   └── "__#9.sum(lineitem.l_quantity)"(#9.0)
            ├── (.cardinality): 0.00
            └── Join
                ├── .join_type: LeftSemi
                ├── .implementation: None
                ├── .join_cond: "orders.o_orderkey"(#2.0) = "__#13.l_orderkey"(#13.0)
                ├── (.output_columns):
                │   ┌── "customer.c_custkey"(#1.0)
                │   ├── "customer.c_name"(#1.1)
                │   ├── "lineitem.l_orderkey"(#3.0)
                │   ├── "lineitem.l_quantity"(#3.4)
                │   ├── "orders.o_custkey"(#2.1)
                │   ├── "orders.o_orderdate"(#2.4)
                │   ├── "orders.o_orderkey"(#2.0)
                │   └── "orders.o_totalprice"(#2.3)
                ├── (.cardinality): 0.00
                ├── Join
                │   ├── .join_type: Inner
                │   ├── .implementation: None
                │   ├── .join_cond: "orders.o_orderkey"(#2.0) = "lineitem.l_orderkey"(#3.0)
                │   ├── (.output_columns):
                │   │   ┌── "customer.c_custkey"(#1.0)
                │   │   ├── "customer.c_name"(#1.1)
                │   │   ├── "lineitem.l_orderkey"(#3.0)
                │   │   ├── "lineitem.l_quantity"(#3.4)
                │   │   ├── "orders.o_custkey"(#2.1)
                │   │   ├── "orders.o_orderdate"(#2.4)
                │   │   ├── "orders.o_orderkey"(#2.0)
                │   │   └── "orders.o_totalprice"(#2.3)
                │   ├── (.cardinality): 0.00
                │   ├── Join
                │   │   ├── .join_type: Inner
                │   │   ├── .implementation: None
                │   │   ├── .join_cond: "customer.c_custkey"(#1.0) = "orders.o_custkey"(#2.1)
                │   │   ├── (.output_columns):
                │   │   │   ┌── "customer.c_custkey"(#1.0)
                │   │   │   ├── "customer.c_name"(#1.1)
                │   │   │   ├── "orders.o_custkey"(#2.1)
                │   │   │   ├── "orders.o_orderdate"(#2.4)
                │   │   │   ├── "orders.o_orderkey"(#2.0)
                │   │   │   └── "orders.o_totalprice"(#2.3)
                │   │   ├── (.cardinality): 0.00
                │   │   ├── Get
                │   │   │   ├── .data_source_id: 6
                │   │   │   ├── .table_index: 1
                │   │   │   ├── .implementation: None
                │   │   │   ├── (.output_columns):
                │   │   │   │   ┌── "customer.c_custkey"(#1.0)
                │   │   │   │   └── "customer.c_name"(#1.1)
                │   │   │   └── (.cardinality): 0.00
                │   │   └── Get
                │   │       ├── .data_source_id: 7
                │   │       ├── .table_index: 2
                │   │       ├── .implementation: None
                │   │       ├── (.output_columns):
                │   │       │   ┌── "orders.o_custkey"(#2.1)
                │   │       │   ├── "orders.o_orderdate"(#2.4)
                │   │       │   ├── "orders.o_orderkey"(#2.0)
                │   │       │   └── "orders.o_totalprice"(#2.3)
                │   │       └── (.cardinality): 0.00
                │   └── Get
                │       ├── .data_source_id: 8
                │       ├── .table_index: 3
                │       ├── .implementation: None
                │       ├── (.output_columns):
                │       │   ┌── "lineitem.l_orderkey"(#3.0)
                │       │   └── "lineitem.l_quantity"(#3.4)
                │       └── (.cardinality): 0.00
                └── Project
                    ├── .table_index: 13
                    ├── .projections: "lineitem.l_orderkey"(#4.0)
                    ├── (.output_columns): "__#13.l_orderkey"(#13.0)
                    ├── (.cardinality): 0.00
                    └── Select
                        ├── .predicate: "__#6.sum(lineitem.l_quantity)"(#6.0) > 25000::decimal128(25, 2)
                        ├── (.output_columns):
                        │   ┌── "__#5.l_orderkey"(#5.0)
                        │   └── "__#6.sum(lineitem.l_quantity)"(#6.0)
                        ├── (.cardinality): 0.00
                        └── Aggregate
                            ├── .key_table_index: 5
                            ├── .aggregate_table_index: 6
                            ├── .implementation: None
                            ├── .exprs: sum("lineitem.l_quantity"(#4.4))
                            ├── .keys: "lineitem.l_orderkey"(#4.0)
                            ├── (.output_columns):
                            │   ┌── "__#5.l_orderkey"(#5.0)
                            │   └── "__#6.sum(lineitem.l_quantity)"(#6.0)
                            ├── (.cardinality): 0.00
                            └── Get
                                ├── .data_source_id: 8
                                ├── .table_index: 4
                                ├── .implementation: None
                                ├── (.output_columns):
                                │   ┌── "lineitem.l_orderkey"(#4.0)
                                │   └── "lineitem.l_quantity"(#4.4)
                                └── (.cardinality): 0.00
*/

