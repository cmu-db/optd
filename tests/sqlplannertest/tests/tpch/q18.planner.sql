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
Limit { .skip: 0::bigint, .fetch: 100::bigint, (.output_columns): c_custkey(#9.1), c_name(#9.0), o_orderdate(#9.3), o_orderkey(#9.2), o_totalprice(#9.4), sum(lineitem.l_quantity)(#9.5), (.cardinality): 0.00 }
└── OrderBy { ordering_exprs: [ o_totalprice(#9.4) DESC, o_orderdate(#9.3) ASC ], (.output_columns): c_custkey(#9.1), c_name(#9.0), o_orderdate(#9.3), o_orderkey(#9.2), o_totalprice(#9.4), sum(lineitem.l_quantity)(#9.5), (.cardinality): 0.00 }
    └── Project { .table_index: 9, .projections: [ c_name(#1.1), c_custkey(#1.0), o_orderkey(#2.0), o_orderdate(#2.4), o_totalprice(#2.3), sum(lineitem.l_quantity)(#8.0) ], (.output_columns): c_custkey(#9.1), c_name(#9.0), o_orderdate(#9.3), o_orderkey(#9.2), o_totalprice(#9.4), sum(lineitem.l_quantity)(#9.5), (.cardinality): 0.00 }
        └── Aggregate { .aggregate_table_index: 8, .implementation: None, .exprs: [ sum(l_quantity(#3.4)) ], .keys: [ c_name(#1.1), c_custkey(#1.0), o_orderkey(#2.0), o_orderdate(#2.4), o_totalprice(#2.3) ], (.output_columns): c_custkey(#1.0), c_name(#1.1), o_orderdate(#2.4), o_orderkey(#2.0), o_totalprice(#2.3), sum(lineitem.l_quantity)(#8.0), (.cardinality): 0.00 }
            └── Join
                ├── .join_type: LeftSemi
                ├── .implementation: None
                ├── .join_cond: (o_orderkey(#2.0) = l_orderkey(#7.0))
                ├── (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4), l_comment(#3.15), l_commitdate(#3.11), l_discount(#3.6), l_extendedprice(#3.5), l_linenumber(#3.3), l_linestatus(#3.9), l_orderkey(#3.0), l_partkey(#3.1), l_quantity(#3.4), l_receiptdate(#3.12), l_returnflag(#3.8), l_shipdate(#3.10), l_shipinstruct(#3.13), l_shipmode(#3.14), l_suppkey(#3.2), l_tax(#3.7), o_clerk(#2.6), o_comment(#2.8), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_orderpriority(#2.5), o_orderstatus(#2.2), o_shippriority(#2.7), o_totalprice(#2.3)
                ├── (.cardinality): 0.00
                ├── Join
                │   ├── .join_type: Inner
                │   ├── .implementation: None
                │   ├── .join_cond: (o_orderkey(#2.0) = l_orderkey(#3.0))
                │   ├── (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4), l_comment(#3.15), l_commitdate(#3.11), l_discount(#3.6), l_extendedprice(#3.5), l_linenumber(#3.3), l_linestatus(#3.9), l_orderkey(#3.0), l_partkey(#3.1), l_quantity(#3.4), l_receiptdate(#3.12), l_returnflag(#3.8), l_shipdate(#3.10), l_shipinstruct(#3.13), l_shipmode(#3.14), l_suppkey(#3.2), l_tax(#3.7), o_clerk(#2.6), o_comment(#2.8), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_orderpriority(#2.5), o_orderstatus(#2.2), o_shippriority(#2.7), o_totalprice(#2.3)
                │   ├── (.cardinality): 0.00
                │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: (c_custkey(#1.0) = o_custkey(#2.1)), (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4), o_clerk(#2.6), o_comment(#2.8), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_orderpriority(#2.5), o_orderstatus(#2.2), o_shippriority(#2.7), o_totalprice(#2.3), (.cardinality): 0.00 }
                │   │   ├── Get { .data_source_id: 6, .table_index: 1, .implementation: None, (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4), (.cardinality): 0.00 }
                │   │   └── Get { .data_source_id: 7, .table_index: 2, .implementation: None, (.output_columns): o_clerk(#2.6), o_comment(#2.8), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_orderpriority(#2.5), o_orderstatus(#2.2), o_shippriority(#2.7), o_totalprice(#2.3), (.cardinality): 0.00 }
                │   └── Get { .data_source_id: 8, .table_index: 3, .implementation: None, (.output_columns): l_comment(#3.15), l_commitdate(#3.11), l_discount(#3.6), l_extendedprice(#3.5), l_linenumber(#3.3), l_linestatus(#3.9), l_orderkey(#3.0), l_partkey(#3.1), l_quantity(#3.4), l_receiptdate(#3.12), l_returnflag(#3.8), l_shipdate(#3.10), l_shipinstruct(#3.13), l_shipmode(#3.14), l_suppkey(#3.2), l_tax(#3.7), (.cardinality): 0.00 }
                └── Remap { .table_index: 7, (.output_columns): l_orderkey(#7.0), (.cardinality): 0.00 }
                    └── Project { .table_index: 6, .projections: [ l_orderkey(#4.0) ], (.output_columns): l_orderkey(#6.0), (.cardinality): 0.00 }
                        └── Select { .predicate: sum(lineitem.l_quantity)(#5.0) > 25000::decimal128(25, 2), (.output_columns): l_orderkey(#4.0), sum(lineitem.l_quantity)(#5.0), (.cardinality): 0.00 }
                            └── Aggregate { .aggregate_table_index: 5, .implementation: None, .exprs: [ sum(l_quantity(#4.4)) ], .keys: [ l_orderkey(#4.0) ], (.output_columns): l_orderkey(#4.0), sum(lineitem.l_quantity)(#5.0), (.cardinality): 0.00 }
                                └── Get { .data_source_id: 8, .table_index: 4, .implementation: None, (.output_columns): l_comment(#4.15), l_commitdate(#4.11), l_discount(#4.6), l_extendedprice(#4.5), l_linenumber(#4.3), l_linestatus(#4.9), l_orderkey(#4.0), l_partkey(#4.1), l_quantity(#4.4), l_receiptdate(#4.12), l_returnflag(#4.8), l_shipdate(#4.10), l_shipinstruct(#4.13), l_shipmode(#4.14), l_suppkey(#4.2), l_tax(#4.7), (.cardinality): 0.00 }

physical_plan after optd-finalized:
Limit
├── .skip: 0::bigint
├── .fetch: 100::bigint
├── (.output_columns): c_custkey(#9.1), c_name(#9.0), o_orderdate(#9.3), o_orderkey(#9.2), o_totalprice(#9.4), sum(lineitem.l_quantity)(#9.5)
├── (.cardinality): 0.00
└── EnforcerSort
    ├── tuple_ordering: [(#9.4, Desc), (#9.3, Asc)]
    ├── (.output_columns): c_custkey(#9.1), c_name(#9.0), o_orderdate(#9.3), o_orderkey(#9.2), o_totalprice(#9.4), sum(lineitem.l_quantity)(#9.5)
    ├── (.cardinality): 0.00
    └── Project
        ├── .table_index: 9
        ├── .projections: [ c_name(#1.1), c_custkey(#1.0), o_orderkey(#2.0), o_orderdate(#2.4), o_totalprice(#2.3), sum(lineitem.l_quantity)(#8.0) ]
        ├── (.output_columns): c_custkey(#9.1), c_name(#9.0), o_orderdate(#9.3), o_orderkey(#9.2), o_totalprice(#9.4), sum(lineitem.l_quantity)(#9.5)
        ├── (.cardinality): 0.00
        └── Aggregate
            ├── .aggregate_table_index: 8
            ├── .implementation: None
            ├── .exprs: [ sum(l_quantity(#3.4)) ]
            ├── .keys: [ c_name(#1.1), c_custkey(#1.0), o_orderkey(#2.0), o_orderdate(#2.4), o_totalprice(#2.3) ]
            ├── (.output_columns): c_custkey(#1.0), c_name(#1.1), o_orderdate(#2.4), o_orderkey(#2.0), o_totalprice(#2.3), sum(lineitem.l_quantity)(#8.0)
            ├── (.cardinality): 0.00
            └── Join
                ├── .join_type: LeftSemi
                ├── .implementation: None
                ├── .join_cond: o_orderkey(#2.0) = l_orderkey(#7.0)
                ├── (.output_columns): c_custkey(#1.0), c_name(#1.1), l_orderkey(#3.0), l_quantity(#3.4), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_totalprice(#2.3)
                ├── (.cardinality): 0.00
                ├── Join
                │   ├── .join_type: Inner
                │   ├── .implementation: None
                │   ├── .join_cond: o_orderkey(#2.0) = l_orderkey(#3.0)
                │   ├── (.output_columns): c_custkey(#1.0), c_name(#1.1), l_orderkey(#3.0), l_quantity(#3.4), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_totalprice(#2.3)
                │   ├── (.cardinality): 0.00
                │   ├── Join
                │   │   ├── .join_type: Inner
                │   │   ├── .implementation: None
                │   │   ├── .join_cond: c_custkey(#1.0) = o_custkey(#2.1)
                │   │   ├── (.output_columns): c_custkey(#1.0), c_name(#1.1), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_totalprice(#2.3)
                │   │   ├── (.cardinality): 0.00
                │   │   ├── Get { .data_source_id: 6, .table_index: 1, .implementation: None, (.output_columns): c_custkey(#1.0), c_name(#1.1), (.cardinality): 0.00 }
                │   │   └── Get
                │   │       ├── .data_source_id: 7
                │   │       ├── .table_index: 2
                │   │       ├── .implementation: None
                │   │       ├── (.output_columns): o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_totalprice(#2.3)
                │   │       └── (.cardinality): 0.00
                │   └── Get { .data_source_id: 8, .table_index: 3, .implementation: None, (.output_columns): l_orderkey(#3.0), l_quantity(#3.4), (.cardinality): 0.00 }
                └── Remap { .table_index: 7, (.output_columns): l_orderkey(#7.0), (.cardinality): 0.00 }
                    └── Project { .table_index: 6, .projections: [ l_orderkey(#4.0) ], (.output_columns): l_orderkey(#6.0), (.cardinality): 0.00 }
                        └── Select
                            ├── .predicate: sum(lineitem.l_quantity)(#5.0) > 25000::decimal128(25, 2)
                            ├── (.output_columns): l_orderkey(#4.0), sum(lineitem.l_quantity)(#5.0)
                            ├── (.cardinality): 0.00
                            └── Aggregate
                                ├── .aggregate_table_index: 5
                                ├── .implementation: None
                                ├── .exprs: [ sum(l_quantity(#4.4)) ]
                                ├── .keys: [ l_orderkey(#4.0) ]
                                ├── (.output_columns): l_orderkey(#4.0), sum(lineitem.l_quantity)(#5.0)
                                ├── (.cardinality): 0.00
                                └── Get { .data_source_id: 8, .table_index: 4, .implementation: None, (.output_columns): l_orderkey(#4.0), l_quantity(#4.4), (.cardinality): 0.00 }
*/

