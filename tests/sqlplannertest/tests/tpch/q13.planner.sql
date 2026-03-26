-- TPC-H Q13
select
    c_count,
    count(*) as custdist
from
    (
        select
            c_custkey,
            count(o_orderkey)
        from
            customer left outer join orders on
                c_custkey = o_custkey
                and o_comment not like '%special%requests%'
        group by
            c_custkey
    ) as c_orders (c_custkey, c_count)
group by
    c_count
order by
    custdist desc,
    c_count desc;

/*
logical_plan after optd-initial:
OrderBy { ordering_exprs: [ custdist(#8.1) DESC, c_count(#8.0) DESC ], (.output_columns): c_count(#8.0), custdist(#8.1), (.cardinality): 0.00 }
└── Project { .table_index: 8, .projections: [ c_count(#6.1), count(Int64(1))(#7.0) ], (.output_columns): c_count(#8.0), custdist(#8.1), (.cardinality): 0.00 }
    └── Aggregate { .aggregate_table_index: 7, .implementation: None, .exprs: [ count(1::bigint) ], .keys: [ c_count(#6.1) ], (.output_columns): c_count(#6.1), count(Int64(1))(#7.0), (.cardinality): 0.00 }
        └── Remap { .table_index: 6, (.output_columns): c_count(#6.1), c_custkey(#6.0), (.cardinality): 0.00 }
            └── Project { .table_index: 5, .projections: [ c_custkey(#4.0), count(orders.o_orderkey)(#4.1) ], (.output_columns): c_count(#5.1), c_custkey(#5.0), (.cardinality): 0.00 }
                └── Project { .table_index: 4, .projections: [ c_custkey(#1.0), count(orders.o_orderkey)(#3.0) ], (.output_columns): c_custkey(#4.0), count(orders.o_orderkey)(#4.1), (.cardinality): 0.00 }
                    └── Aggregate { .aggregate_table_index: 3, .implementation: None, .exprs: [ count(o_orderkey(#2.0)) ], .keys: [ c_custkey(#1.0) ], (.output_columns): c_custkey(#1.0), count(orders.o_orderkey)(#3.0), (.cardinality): 0.00 }
                        └── Join
                            ├── .join_type: LeftOuter
                            ├── .implementation: None
                            ├── .join_cond: (c_custkey(#1.0) = o_custkey(#2.1))
                            ├── (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4), o_clerk(#2.6), o_comment(#2.8), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_orderpriority(#2.5), o_orderstatus(#2.2), o_shippriority(#2.7), o_totalprice(#2.3)
                            ├── (.cardinality): 0.00
                            ├── Get { .data_source_id: 6, .table_index: 1, .implementation: None, (.output_columns): c_acctbal(#1.5), c_address(#1.2), c_comment(#1.7), c_custkey(#1.0), c_mktsegment(#1.6), c_name(#1.1), c_nationkey(#1.3), c_phone(#1.4), (.cardinality): 0.00 }
                            └── Select { .predicate: o_comment(#2.8) NOT LIKE %special%requests%::utf8_view, (.output_columns): o_clerk(#2.6), o_comment(#2.8), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_orderpriority(#2.5), o_orderstatus(#2.2), o_shippriority(#2.7), o_totalprice(#2.3), (.cardinality): 0.00 }
                                └── Get { .data_source_id: 7, .table_index: 2, .implementation: None, (.output_columns): o_clerk(#2.6), o_comment(#2.8), o_custkey(#2.1), o_orderdate(#2.4), o_orderkey(#2.0), o_orderpriority(#2.5), o_orderstatus(#2.2), o_shippriority(#2.7), o_totalprice(#2.3), (.cardinality): 0.00 }

physical_plan after optd-finalized:
EnforcerSort
├── tuple_ordering: [(#8.1, Desc), (#8.0, Desc)]
├── (.output_columns): c_count(#8.0), custdist(#8.1)
├── (.cardinality): 0.00
└── Project
    ├── .table_index: 8
    ├── .projections: [ c_count(#6.1), count(Int64(1))(#7.0) ]
    ├── (.output_columns): c_count(#8.0), custdist(#8.1)
    ├── (.cardinality): 0.00
    └── Aggregate
        ├── .aggregate_table_index: 7
        ├── .implementation: None
        ├── .exprs: [ count(1::bigint) ]
        ├── .keys: [ c_count(#6.1) ]
        ├── (.output_columns): c_count(#6.1), count(Int64(1))(#7.0)
        ├── (.cardinality): 0.00
        └── Remap { .table_index: 6, (.output_columns): c_count(#6.1), c_custkey(#6.0), (.cardinality): 0.00 }
            └── Project
                ├── .table_index: 5
                ├── .projections: [ c_custkey(#1.0), count(orders.o_orderkey)(#3.0) ]
                ├── (.output_columns): c_count(#5.1), c_custkey(#5.0)
                ├── (.cardinality): 0.00
                └── Aggregate
                    ├── .aggregate_table_index: 3
                    ├── .implementation: None
                    ├── .exprs: [ count(o_orderkey(#2.0)) ]
                    ├── .keys: [ c_custkey(#1.0) ]
                    ├── (.output_columns): c_custkey(#1.0), count(orders.o_orderkey)(#3.0)
                    ├── (.cardinality): 0.00
                    └── Join
                        ├── .join_type: LeftOuter
                        ├── .implementation: None
                        ├── .join_cond: c_custkey(#1.0) = o_custkey(#2.1)
                        ├── (.output_columns): c_custkey(#1.0), o_comment(#2.8), o_custkey(#2.1), o_orderkey(#2.0)
                        ├── (.cardinality): 0.00
                        ├── Get
                        │   ├── .data_source_id: 6
                        │   ├── .table_index: 1
                        │   ├── .implementation: None
                        │   ├── (.output_columns): c_custkey(#1.0)
                        │   └── (.cardinality): 0.00
                        └── Select
                            ├── .predicate: o_comment(#2.8) NOT LIKE %special%requests%::utf8_view
                            ├── (.output_columns): o_comment(#2.8), o_custkey(#2.1), o_orderkey(#2.0)
                            ├── (.cardinality): 0.00
                            └── Get
                                ├── .data_source_id: 7
                                ├── .table_index: 2
                                ├── .implementation: None
                                ├── (.output_columns): o_comment(#2.8), o_custkey(#2.1), o_orderkey(#2.0)
                                └── (.cardinality): 0.00
*/

