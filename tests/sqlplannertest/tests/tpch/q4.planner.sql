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
OrderBy { ordering_exprs: [ o_orderpriority(#7.0) ASC ], (.output_columns): o_orderpriority(#7.0), order_count(#7.1), (.cardinality): 0.00 }
└── Project { .table_index: 7, .projections: [ o_orderpriority(#1.5), count(Int64(1))(#6.0) ], (.output_columns): o_orderpriority(#7.0), order_count(#7.1), (.cardinality): 0.00 }
    └── Aggregate { .key_table_index: 5, .aggregate_table_index: 6, .implementation: None, .exprs: [ count(1::bigint) ], .keys: [ o_orderpriority(#1.5) ], (.output_columns): count(Int64(1))(#6.0), orders.o_orderpriority(#5.0), (.cardinality): 0.00 }
        └── Join { .join_type: LeftSemi, .implementation: None, .join_cond: (o_orderkey(#1.0) = l_orderkey(#4.0)), (.output_columns): o_clerk(#1.6), o_comment(#1.8), o_custkey(#1.1), o_orderdate(#1.4), o_orderkey(#1.0), o_orderpriority(#1.5), o_orderstatus(#1.2), o_shippriority(#1.7), o_totalprice(#1.3), (.cardinality): 0.00 }
            ├── Select { .predicate: (o_orderdate(#1.4) >= 1993-07-01::date32) AND (o_orderdate(#1.4) < 1993-10-01::date32), (.output_columns): o_clerk(#1.6), o_comment(#1.8), o_custkey(#1.1), o_orderdate(#1.4), o_orderkey(#1.0), o_orderpriority(#1.5), o_orderstatus(#1.2), o_shippriority(#1.7), o_totalprice(#1.3), (.cardinality): 0.00 }
            │   └── Get { .data_source_id: 7, .table_index: 1, .implementation: None, (.output_columns): o_clerk(#1.6), o_comment(#1.8), o_custkey(#1.1), o_orderdate(#1.4), o_orderkey(#1.0), o_orderpriority(#1.5), o_orderstatus(#1.2), o_shippriority(#1.7), o_totalprice(#1.3), (.cardinality): 0.00 }
            └── Remap
                ├── .table_index: 4
                ├── (.output_columns): l_comment(#4.15), l_commitdate(#4.11), l_discount(#4.6), l_extendedprice(#4.5), l_linenumber(#4.3), l_linestatus(#4.9), l_orderkey(#4.0), l_partkey(#4.1), l_quantity(#4.4), l_receiptdate(#4.12), l_returnflag(#4.8), l_shipdate(#4.10), l_shipinstruct(#4.13), l_shipmode(#4.14), l_suppkey(#4.2), l_tax(#4.7)
                ├── (.cardinality): 0.00
                └── Project
                    ├── .table_index: 3
                    ├── .projections: [ l_orderkey(#2.0), l_partkey(#2.1), l_suppkey(#2.2), l_linenumber(#2.3), l_quantity(#2.4), l_extendedprice(#2.5), l_discount(#2.6), l_tax(#2.7), l_returnflag(#2.8), l_linestatus(#2.9), l_shipdate(#2.10), l_commitdate(#2.11), l_receiptdate(#2.12), l_shipinstruct(#2.13), l_shipmode(#2.14), l_comment(#2.15) ]
                    ├── (.output_columns): l_comment(#3.15), l_commitdate(#3.11), l_discount(#3.6), l_extendedprice(#3.5), l_linenumber(#3.3), l_linestatus(#3.9), l_orderkey(#3.0), l_partkey(#3.1), l_quantity(#3.4), l_receiptdate(#3.12), l_returnflag(#3.8), l_shipdate(#3.10), l_shipinstruct(#3.13), l_shipmode(#3.14), l_suppkey(#3.2), l_tax(#3.7)
                    ├── (.cardinality): 0.00
                    └── Select
                        ├── .predicate: l_receiptdate(#2.12) > l_commitdate(#2.11)
                        ├── (.output_columns): l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7)
                        ├── (.cardinality): 0.00
                        └── Get
                            ├── .data_source_id: 8
                            ├── .table_index: 2
                            ├── .implementation: None
                            ├── (.output_columns): l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7)
                            └── (.cardinality): 0.00

physical_plan after optd-finalized:
EnforcerSort { tuple_ordering: [(#7.0, Asc)], (.output_columns): o_orderpriority(#7.0), order_count(#7.1), (.cardinality): 0.00 }
└── Project { .table_index: 7, .projections: [ o_orderpriority(#1.5), count(Int64(1))(#6.0) ], (.output_columns): o_orderpriority(#7.0), order_count(#7.1), (.cardinality): 0.00 }
    └── Aggregate { .key_table_index: 5, .aggregate_table_index: 6, .implementation: None, .exprs: [ count(1::bigint) ], .keys: [ o_orderpriority(#1.5) ], (.output_columns): count(Int64(1))(#6.0), orders.o_orderpriority(#5.0), (.cardinality): 0.00 }
        └── Join { .join_type: LeftSemi, .implementation: None, .join_cond: o_orderkey(#1.0) = l_orderkey(#4.0), (.output_columns): o_orderdate(#1.4), o_orderkey(#1.0), o_orderpriority(#1.5), (.cardinality): 0.00 }
            ├── Select { .predicate: (o_orderdate(#1.4) >= 1993-07-01::date32) AND (o_orderdate(#1.4) < 1993-10-01::date32), (.output_columns): o_orderdate(#1.4), o_orderkey(#1.0), o_orderpriority(#1.5), (.cardinality): 0.00 }
            │   └── Get { .data_source_id: 7, .table_index: 1, .implementation: None, (.output_columns): o_orderdate(#1.4), o_orderkey(#1.0), o_orderpriority(#1.5), (.cardinality): 0.00 }
            └── Remap
                ├── .table_index: 4
                ├── (.output_columns): l_comment(#4.15), l_commitdate(#4.11), l_discount(#4.6), l_extendedprice(#4.5), l_linenumber(#4.3), l_linestatus(#4.9), l_orderkey(#4.0), l_partkey(#4.1), l_quantity(#4.4), l_receiptdate(#4.12), l_returnflag(#4.8), l_shipdate(#4.10), l_shipinstruct(#4.13), l_shipmode(#4.14), l_suppkey(#4.2), l_tax(#4.7)
                ├── (.cardinality): 0.00
                └── Project
                    ├── .table_index: 3
                    ├── .projections: [ l_orderkey(#2.0), l_partkey(#2.1), l_suppkey(#2.2), l_linenumber(#2.3), l_quantity(#2.4), l_extendedprice(#2.5), l_discount(#2.6), l_tax(#2.7), l_returnflag(#2.8), l_linestatus(#2.9), l_shipdate(#2.10), l_commitdate(#2.11), l_receiptdate(#2.12), l_shipinstruct(#2.13), l_shipmode(#2.14), l_comment(#2.15) ]
                    ├── (.output_columns): l_comment(#3.15), l_commitdate(#3.11), l_discount(#3.6), l_extendedprice(#3.5), l_linenumber(#3.3), l_linestatus(#3.9), l_orderkey(#3.0), l_partkey(#3.1), l_quantity(#3.4), l_receiptdate(#3.12), l_returnflag(#3.8), l_shipdate(#3.10), l_shipinstruct(#3.13), l_shipmode(#3.14), l_suppkey(#3.2), l_tax(#3.7)
                    ├── (.cardinality): 0.00
                    └── Select
                        ├── .predicate: l_receiptdate(#2.12) > l_commitdate(#2.11)
                        ├── (.output_columns): l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7)
                        ├── (.cardinality): 0.00
                        └── Get
                            ├── .data_source_id: 8
                            ├── .table_index: 2
                            ├── .implementation: None
                            ├── (.output_columns): l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7)
                            └── (.cardinality): 0.00
*/

