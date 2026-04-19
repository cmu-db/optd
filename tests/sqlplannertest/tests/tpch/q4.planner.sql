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
OrderBy { ordering_exprs: "__#6.o_orderpriority"(#6.0) ASC, (.output_columns): [ "__#6.o_orderpriority"(#6.0), "__#6.order_count"(#6.1) ], (.cardinality): 0.00 }
└── Project { .table_index: 6, .projections: [ "orders.o_orderpriority"(#1.5), "__#5.count(Int64(1))"(#5.0) ], (.output_columns): [ "__#6.o_orderpriority"(#6.0), "__#6.order_count"(#6.1) ], (.cardinality): 0.00 }
    └── Aggregate
        ├── .key_table_index: 4
        ├── .aggregate_table_index: 5
        ├── .implementation: None
        ├── .exprs: count(1::bigint)
        ├── .keys: "orders.o_orderpriority"(#1.5)
        ├── (.output_columns): [ "__#4.o_orderpriority"(#4.0), "__#5.count(Int64(1))"(#5.0) ]
        ├── (.cardinality): 0.00
        └── DependentJoin
            ├── .join_type: LeftSemi
            ├── .join_cond: true::boolean
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
            ├── Select
            │   ├── .predicate: ("orders.o_orderdate"(#1.4) >= CAST ('1993-07-01'::utf8 AS Date32)) AND ("orders.o_orderdate"(#1.4) < CAST ('1993-07-01'::utf8 AS Date32) + IntervalMonthDayNano { months: 3, days: 0, nanoseconds: 0 }::interval_month_day_nano)
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
            │   ├── (.cardinality): 0.00
            │   └── Get
            │       ├── .data_source_id: 7
            │       ├── .table_index: 1
            │       ├── .implementation: None
            │       ├── (.output_columns):
            │       │   ┌── "orders.o_clerk"(#1.6)
            │       │   ├── "orders.o_comment"(#1.8)
            │       │   ├── "orders.o_custkey"(#1.1)
            │       │   ├── "orders.o_orderdate"(#1.4)
            │       │   ├── "orders.o_orderkey"(#1.0)
            │       │   ├── "orders.o_orderpriority"(#1.5)
            │       │   ├── "orders.o_orderstatus"(#1.2)
            │       │   ├── "orders.o_shippriority"(#1.7)
            │       │   └── "orders.o_totalprice"(#1.3)
            │       └── (.cardinality): 0.00
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
                    ├── .predicate: ("lineitem.l_orderkey"(#2.0) = "orders.o_orderkey"(#1.0)) AND ("lineitem.l_commitdate"(#2.11) < "lineitem.l_receiptdate"(#2.12))
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
EnforcerSort { tuple_ordering: [(#6.0, Asc)], (.output_columns): [ "__#6.o_orderpriority"(#6.0), "__#6.order_count"(#6.1) ], (.cardinality): 0.00 }
└── Project
    ├── .table_index: 6
    ├── .projections: [ "orders.o_orderpriority"(#1.5), "__#5.count(Int64(1))"(#5.0) ]
    ├── (.output_columns): [ "__#6.o_orderpriority"(#6.0), "__#6.order_count"(#6.1) ]
    ├── (.cardinality): 0.00
    └── Aggregate
        ├── .key_table_index: 4
        ├── .aggregate_table_index: 5
        ├── .implementation: None
        ├── .exprs: count(1::bigint)
        ├── .keys: "orders.o_orderpriority"(#1.5)
        ├── (.output_columns): [ "__#4.o_orderpriority"(#4.0), "__#5.count(Int64(1))"(#5.0) ]
        ├── (.cardinality): 0.00
        └── Join
            ├── .join_type: LeftSemi
            ├── .implementation: None
            ├── .join_cond: "orders.o_orderkey"(#1.0) IS NOT DISTINCT FROM "__#9.l_orderkey"(#9.0)
            ├── (.output_columns): [ "orders.o_orderdate"(#1.4), "orders.o_orderkey"(#1.0), "orders.o_orderpriority"(#1.5) ]
            ├── (.cardinality): 0.00
            ├── Select
            │   ├── .predicate: ("orders.o_orderdate"(#1.4) >= 1993-07-01::date32) AND ("orders.o_orderdate"(#1.4) < 1993-10-01::date32)
            │   ├── (.output_columns): [ "orders.o_orderdate"(#1.4), "orders.o_orderkey"(#1.0), "orders.o_orderpriority"(#1.5) ]
            │   ├── (.cardinality): 0.00
            │   └── Get
            │       ├── .data_source_id: 7
            │       ├── .table_index: 1
            │       ├── .implementation: None
            │       ├── (.output_columns): [ "orders.o_orderdate"(#1.4), "orders.o_orderkey"(#1.0), "orders.o_orderpriority"(#1.5) ]
            │       └── (.cardinality): 0.00
            └── Project
                ├── .table_index: 9
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
                │   ┌── "__#9.l_comment"(#9.15)
                │   ├── "__#9.l_commitdate"(#9.11)
                │   ├── "__#9.l_discount"(#9.6)
                │   ├── "__#9.l_extendedprice"(#9.5)
                │   ├── "__#9.l_linenumber"(#9.3)
                │   ├── "__#9.l_linestatus"(#9.9)
                │   ├── "__#9.l_orderkey"(#9.0)
                │   ├── "__#9.l_partkey"(#9.1)
                │   ├── "__#9.l_quantity"(#9.4)
                │   ├── "__#9.l_receiptdate"(#9.12)
                │   ├── "__#9.l_returnflag"(#9.8)
                │   ├── "__#9.l_shipdate"(#9.10)
                │   ├── "__#9.l_shipinstruct"(#9.13)
                │   ├── "__#9.l_shipmode"(#9.14)
                │   ├── "__#9.l_suppkey"(#9.2)
                │   └── "__#9.l_tax"(#9.7)
                ├── (.cardinality): 0.00
                └── Select
                    ├── .predicate: ("lineitem.l_orderkey"(#2.0) = "lineitem.l_orderkey"(#2.0)) AND ("lineitem.l_commitdate"(#2.11) < "lineitem.l_receiptdate"(#2.12))
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

