-- TPC-H Q4
select
    o_orderpriority,
    count(*) as order_count
from
    orders
where
    o_orderdate >= date '1993-07-01'
    and o_orderdate < date '1993-07-01' + interval '3' month
    and exists (
        select
            *
        from
            lineitem
        where
            l_orderkey = o_orderkey
            and l_commitdate < l_receiptdate
    )
group by
    o_orderpriority
order by
    o_orderpriority;

/*
logical_plan after optd-initial:
OrderBy
├── ordering_exprs: "__#7.o_orderpriority"(#7.0) ASC
├── (.output_columns): [ "__#7.o_orderpriority"(#7.0), "__#7.order_count"(#7.1) ]
├── (.cardinality): 0.00
└── Project
    ├── .table_index: 7
    ├── .projections: [ "orders.o_orderpriority"(#1.5), "__#6.count(Int64(1))"(#6.0) ]
    ├── (.output_columns): [ "__#7.o_orderpriority"(#7.0), "__#7.order_count"(#7.1) ]
    ├── (.cardinality): 0.00
    └── Aggregate
        ├── .key_table_index: 5
        ├── .aggregate_table_index: 6
        ├── .implementation: None
        ├── .exprs: count(1::bigint)
        ├── .keys: "orders.o_orderpriority"(#1.5)
        ├── (.output_columns): [ "__#5.o_orderpriority"(#5.0), "__#6.count(Int64(1))"(#6.0) ]
        ├── (.cardinality): 0.00
        └── Select
            ├── .predicate: ("orders.o_orderdate"(#1.4) >= 1993-07-01::date32) AND ("orders.o_orderdate"(#1.4) < 1993-10-01::date32)
            ├── (.output_columns):
            │   ┌── "orders.o_clerk"(#1.6)
            │   ├── "orders.o_comment"(#1.8)
            │   ├── "orders.o_custkey"(#1.1)
            │   ├── "orders.o_orderdate"(#1.4)
            │   ├── "orders.o_orderkey"(#1.0)
            │   ├── "orders.o_orderpriority"(#1.5)
            │   ├── "orders.o_orderstatus"(#1.2)
            │   ├── "orders.o_shippriority"(#1.7)
            │   └── "orders.o_totalprice"(#1.3)
            ├── (.cardinality): 0.00
            └── Join
                ├── .join_type: LeftSemi
                ├── .implementation: None
                ├── .join_cond: ("orders.o_orderkey"(#1.0) = "__correlated_sq_1.l_orderkey"(#4.0))
                ├── (.output_columns):
                │   ┌── "orders.o_clerk"(#1.6)
                │   ├── "orders.o_comment"(#1.8)
                │   ├── "orders.o_custkey"(#1.1)
                │   ├── "orders.o_orderdate"(#1.4)
                │   ├── "orders.o_orderkey"(#1.0)
                │   ├── "orders.o_orderpriority"(#1.5)
                │   ├── "orders.o_orderstatus"(#1.2)
                │   ├── "orders.o_shippriority"(#1.7)
                │   └── "orders.o_totalprice"(#1.3)
                ├── (.cardinality): 0.00
                ├── Get
                │   ├── .data_source_id: 7
                │   ├── .table_index: 1
                │   ├── .implementation: None
                │   ├── (.output_columns):
                │   │   ┌── "orders.o_clerk"(#1.6)
                │   │   ├── "orders.o_comment"(#1.8)
                │   │   ├── "orders.o_custkey"(#1.1)
                │   │   ├── "orders.o_orderdate"(#1.4)
                │   │   ├── "orders.o_orderkey"(#1.0)
                │   │   ├── "orders.o_orderpriority"(#1.5)
                │   │   ├── "orders.o_orderstatus"(#1.2)
                │   │   ├── "orders.o_shippriority"(#1.7)
                │   │   └── "orders.o_totalprice"(#1.3)
                │   └── (.cardinality): 0.00
                └── Remap
                    ├── .table_index: 4
                    ├── (.output_columns):
                    │   ┌── "__correlated_sq_1.l_comment"(#4.15)
                    │   ├── "__correlated_sq_1.l_commitdate"(#4.11)
                    │   ├── "__correlated_sq_1.l_discount"(#4.6)
                    │   ├── "__correlated_sq_1.l_extendedprice"(#4.5)
                    │   ├── "__correlated_sq_1.l_linenumber"(#4.3)
                    │   ├── "__correlated_sq_1.l_linestatus"(#4.9)
                    │   ├── "__correlated_sq_1.l_orderkey"(#4.0)
                    │   ├── "__correlated_sq_1.l_partkey"(#4.1)
                    │   ├── "__correlated_sq_1.l_quantity"(#4.4)
                    │   ├── "__correlated_sq_1.l_receiptdate"(#4.12)
                    │   ├── "__correlated_sq_1.l_returnflag"(#4.8)
                    │   ├── "__correlated_sq_1.l_shipdate"(#4.10)
                    │   ├── "__correlated_sq_1.l_shipinstruct"(#4.13)
                    │   ├── "__correlated_sq_1.l_shipmode"(#4.14)
                    │   ├── "__correlated_sq_1.l_suppkey"(#4.2)
                    │   └── "__correlated_sq_1.l_tax"(#4.7)
                    ├── (.cardinality): 0.00
                    └── Project
                        ├── .table_index: 3
                        ├── .projections:
                        │   ┌── "lineitem.l_orderkey"(#2.0)
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
                        │   └── "lineitem.l_comment"(#2.15)
                        ├── (.output_columns):
                        │   ┌── "__#3.l_comment"(#3.15)
                        │   ├── "__#3.l_commitdate"(#3.11)
                        │   ├── "__#3.l_discount"(#3.6)
                        │   ├── "__#3.l_extendedprice"(#3.5)
                        │   ├── "__#3.l_linenumber"(#3.3)
                        │   ├── "__#3.l_linestatus"(#3.9)
                        │   ├── "__#3.l_orderkey"(#3.0)
                        │   ├── "__#3.l_partkey"(#3.1)
                        │   ├── "__#3.l_quantity"(#3.4)
                        │   ├── "__#3.l_receiptdate"(#3.12)
                        │   ├── "__#3.l_returnflag"(#3.8)
                        │   ├── "__#3.l_shipdate"(#3.10)
                        │   ├── "__#3.l_shipinstruct"(#3.13)
                        │   ├── "__#3.l_shipmode"(#3.14)
                        │   ├── "__#3.l_suppkey"(#3.2)
                        │   └── "__#3.l_tax"(#3.7)
                        ├── (.cardinality): 0.00
                        └── Select
                            ├── .predicate: "lineitem.l_receiptdate"(#2.12) > "lineitem.l_commitdate"(#2.11)
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
                            │   └── "lineitem.l_tax"(#2.7)
                            ├── (.cardinality): 0.00
                            └── Get
                                ├── .data_source_id: 8
                                ├── .table_index: 2
                                ├── .implementation: None
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
                                │   └── "lineitem.l_tax"(#2.7)
                                └── (.cardinality): 0.00

physical_plan after optd-finalized:
EnforcerSort
├── tuple_ordering: [(#7.0, Asc)]
├── (.output_columns): [ "__#7.o_orderpriority"(#7.0), "__#7.order_count"(#7.1) ]
├── (.cardinality): 0.00
└── Project
    ├── .table_index: 7
    ├── .projections: [ "orders.o_orderpriority"(#1.5), "__#6.count(Int64(1))"(#6.0) ]
    ├── (.output_columns): [ "__#7.o_orderpriority"(#7.0), "__#7.order_count"(#7.1) ]
    ├── (.cardinality): 0.00
    └── Aggregate
        ├── .key_table_index: 5
        ├── .aggregate_table_index: 6
        ├── .implementation: None
        ├── .exprs: count(1::bigint)
        ├── .keys: "orders.o_orderpriority"(#1.5)
        ├── (.output_columns): [ "__#5.o_orderpriority"(#5.0), "__#6.count(Int64(1))"(#6.0) ]
        ├── (.cardinality): 0.00
        └── Select
            ├── .predicate: ("orders.o_orderdate"(#1.4) >= 1993-07-01::date32) AND ("orders.o_orderdate"(#1.4) < 1993-10-01::date32)
            ├── (.output_columns): [ "orders.o_orderdate"(#1.4), "orders.o_orderkey"(#1.0), "orders.o_orderpriority"(#1.5) ]
            ├── (.cardinality): 0.00
            └── Join
                ├── .join_type: LeftSemi
                ├── .implementation: None
                ├── .join_cond: "orders.o_orderkey"(#1.0) = "__correlated_sq_1.l_orderkey"(#4.0)
                ├── (.output_columns): [ "orders.o_orderdate"(#1.4), "orders.o_orderkey"(#1.0), "orders.o_orderpriority"(#1.5) ]
                ├── (.cardinality): 0.00
                ├── Get
                │   ├── .data_source_id: 7
                │   ├── .table_index: 1
                │   ├── .implementation: None
                │   ├── (.output_columns): [ "orders.o_orderdate"(#1.4), "orders.o_orderkey"(#1.0), "orders.o_orderpriority"(#1.5) ]
                │   └── (.cardinality): 0.00
                └── Remap
                    ├── .table_index: 4
                    ├── (.output_columns):
                    │   ┌── "__correlated_sq_1.l_comment"(#4.15)
                    │   ├── "__correlated_sq_1.l_commitdate"(#4.11)
                    │   ├── "__correlated_sq_1.l_discount"(#4.6)
                    │   ├── "__correlated_sq_1.l_extendedprice"(#4.5)
                    │   ├── "__correlated_sq_1.l_linenumber"(#4.3)
                    │   ├── "__correlated_sq_1.l_linestatus"(#4.9)
                    │   ├── "__correlated_sq_1.l_orderkey"(#4.0)
                    │   ├── "__correlated_sq_1.l_partkey"(#4.1)
                    │   ├── "__correlated_sq_1.l_quantity"(#4.4)
                    │   ├── "__correlated_sq_1.l_receiptdate"(#4.12)
                    │   ├── "__correlated_sq_1.l_returnflag"(#4.8)
                    │   ├── "__correlated_sq_1.l_shipdate"(#4.10)
                    │   ├── "__correlated_sq_1.l_shipinstruct"(#4.13)
                    │   ├── "__correlated_sq_1.l_shipmode"(#4.14)
                    │   ├── "__correlated_sq_1.l_suppkey"(#4.2)
                    │   └── "__correlated_sq_1.l_tax"(#4.7)
                    ├── (.cardinality): 0.00
                    └── Project
                        ├── .table_index: 3
                        ├── .projections:
                        │   ┌── "lineitem.l_orderkey"(#2.0)
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
                        │   └── "lineitem.l_comment"(#2.15)
                        ├── (.output_columns):
                        │   ┌── "__#3.l_comment"(#3.15)
                        │   ├── "__#3.l_commitdate"(#3.11)
                        │   ├── "__#3.l_discount"(#3.6)
                        │   ├── "__#3.l_extendedprice"(#3.5)
                        │   ├── "__#3.l_linenumber"(#3.3)
                        │   ├── "__#3.l_linestatus"(#3.9)
                        │   ├── "__#3.l_orderkey"(#3.0)
                        │   ├── "__#3.l_partkey"(#3.1)
                        │   ├── "__#3.l_quantity"(#3.4)
                        │   ├── "__#3.l_receiptdate"(#3.12)
                        │   ├── "__#3.l_returnflag"(#3.8)
                        │   ├── "__#3.l_shipdate"(#3.10)
                        │   ├── "__#3.l_shipinstruct"(#3.13)
                        │   ├── "__#3.l_shipmode"(#3.14)
                        │   ├── "__#3.l_suppkey"(#3.2)
                        │   └── "__#3.l_tax"(#3.7)
                        ├── (.cardinality): 0.00
                        └── Select
                            ├── .predicate: "lineitem.l_receiptdate"(#2.12) > "lineitem.l_commitdate"(#2.11)
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
                            │   └── "lineitem.l_tax"(#2.7)
                            ├── (.cardinality): 0.00
                            └── Get
                                ├── .data_source_id: 8
                                ├── .table_index: 2
                                ├── .implementation: None
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
                                │   └── "lineitem.l_tax"(#2.7)
                                └── (.cardinality): 0.00
*/

