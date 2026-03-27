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
OrderBy { ordering_exprs: [ `__internal_#10`.`custdist`(#10.1) DESC, `__internal_#10`.`c_count`(#10.0) DESC ], (.output_columns): `__internal_#10`.`c_count`(#10.0), `__internal_#10`.`custdist`(#10.1), (.cardinality): 0.00 }
└── Project { .table_index: 10, .projections: [ `c_orders`.`c_count`(#7.1), `__internal_#9`.`count(Int64(1))`(#9.0) ], (.output_columns): `__internal_#10`.`c_count`(#10.0), `__internal_#10`.`custdist`(#10.1), (.cardinality): 0.00 }
    └── Aggregate { .key_table_index: 8, .aggregate_table_index: 9, .implementation: None, .exprs: [ count(1::bigint) ], .keys: [ `c_orders`.`c_count`(#7.1) ], (.output_columns): `__internal_#8`.`c_count`(#8.0), `__internal_#9`.`count(Int64(1))`(#9.0), (.cardinality): 0.00 }
        └── Remap { .table_index: 7, (.output_columns): `c_orders`.`c_count`(#7.1), `c_orders`.`c_custkey`(#7.0), (.cardinality): 0.00 }
            └── Project { .table_index: 6, .projections: [ `__internal_#5`.`c_custkey`(#5.0), `__internal_#5`.`count(orders.o_orderkey)`(#5.1) ], (.output_columns): `__internal_#6`.`c_count`(#6.1), `__internal_#6`.`c_custkey`(#6.0), (.cardinality): 0.00 }
                └── Project { .table_index: 5, .projections: [ `customer`.`c_custkey`(#1.0), `__internal_#4`.`count(orders.o_orderkey)`(#4.0) ], (.output_columns): `__internal_#5`.`c_custkey`(#5.0), `__internal_#5`.`count(orders.o_orderkey)`(#5.1), (.cardinality): 0.00 }
                    └── Aggregate { .key_table_index: 3, .aggregate_table_index: 4, .implementation: None, .exprs: count(`orders`.`o_orderkey`(#2.0)), .keys: [ `customer`.`c_custkey`(#1.0) ], (.output_columns): `__internal_#3`.`c_custkey`(#3.0), `__internal_#4`.`count(orders.o_orderkey)`(#4.0), (.cardinality): 0.00 }
                        └── Join
                            ├── .join_type: LeftOuter
                            ├── .implementation: None
                            ├── .join_cond: (`customer`.`c_custkey`(#1.0) = `orders`.`o_custkey`(#2.1))
                            ├── (.output_columns): `customer`.`c_acctbal`(#1.5), `customer`.`c_address`(#1.2), `customer`.`c_comment`(#1.7), `customer`.`c_custkey`(#1.0), `customer`.`c_mktsegment`(#1.6), `customer`.`c_name`(#1.1), `customer`.`c_nationkey`(#1.3), `customer`.`c_phone`(#1.4), `orders`.`o_clerk`(#2.6), `orders`.`o_comment`(#2.8), `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0), `orders`.`o_orderpriority`(#2.5), `orders`.`o_orderstatus`(#2.2), `orders`.`o_shippriority`(#2.7), `orders`.`o_totalprice`(#2.3)
                            ├── (.cardinality): 0.00
                            ├── Get { .data_source_id: 6, .table_index: 1, .implementation: None, (.output_columns): `customer`.`c_acctbal`(#1.5), `customer`.`c_address`(#1.2), `customer`.`c_comment`(#1.7), `customer`.`c_custkey`(#1.0), `customer`.`c_mktsegment`(#1.6), `customer`.`c_name`(#1.1), `customer`.`c_nationkey`(#1.3), `customer`.`c_phone`(#1.4), (.cardinality): 0.00 }
                            └── Select { .predicate: `orders`.`o_comment`(#2.8) NOT LIKE %special%requests%::utf8_view, (.output_columns): `orders`.`o_clerk`(#2.6), `orders`.`o_comment`(#2.8), `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0), `orders`.`o_orderpriority`(#2.5), `orders`.`o_orderstatus`(#2.2), `orders`.`o_shippriority`(#2.7), `orders`.`o_totalprice`(#2.3), (.cardinality): 0.00 }
                                └── Get { .data_source_id: 7, .table_index: 2, .implementation: None, (.output_columns): `orders`.`o_clerk`(#2.6), `orders`.`o_comment`(#2.8), `orders`.`o_custkey`(#2.1), `orders`.`o_orderdate`(#2.4), `orders`.`o_orderkey`(#2.0), `orders`.`o_orderpriority`(#2.5), `orders`.`o_orderstatus`(#2.2), `orders`.`o_shippriority`(#2.7), `orders`.`o_totalprice`(#2.3), (.cardinality): 0.00 }

physical_plan after optd-finalized:
EnforcerSort
├── tuple_ordering: [(#10.1, Desc), (#10.0, Desc)]
├── (.output_columns): `__internal_#10`.`c_count`(#10.0), `__internal_#10`.`custdist`(#10.1)
├── (.cardinality): 0.00
└── Project
    ├── .table_index: 10
    ├── .projections: [ `c_orders`.`c_count`(#7.1), `__internal_#9`.`count(Int64(1))`(#9.0) ]
    ├── (.output_columns): `__internal_#10`.`c_count`(#10.0), `__internal_#10`.`custdist`(#10.1)
    ├── (.cardinality): 0.00
    └── Aggregate
        ├── .key_table_index: 8
        ├── .aggregate_table_index: 9
        ├── .implementation: None
        ├── .exprs: [ count(1::bigint) ]
        ├── .keys: [ `c_orders`.`c_count`(#7.1) ]
        ├── (.output_columns): `__internal_#8`.`c_count`(#8.0), `__internal_#9`.`count(Int64(1))`(#9.0)
        ├── (.cardinality): 0.00
        └── Remap { .table_index: 7, (.output_columns): `c_orders`.`c_count`(#7.1), `c_orders`.`c_custkey`(#7.0), (.cardinality): 0.00 }
            └── Project
                ├── .table_index: 6
                ├── .projections: [ `customer`.`c_custkey`(#1.0), `__internal_#4`.`count(orders.o_orderkey)`(#4.0) ]
                ├── (.output_columns): `__internal_#6`.`c_count`(#6.1), `__internal_#6`.`c_custkey`(#6.0)
                ├── (.cardinality): 0.00
                └── Aggregate
                    ├── .key_table_index: 3
                    ├── .aggregate_table_index: 4
                    ├── .implementation: None
                    ├── .exprs: [ count(`orders`.`o_orderkey`(#2.0)) ]
                    ├── .keys: [ `customer`.`c_custkey`(#1.0) ]
                    ├── (.output_columns): `__internal_#3`.`c_custkey`(#3.0), `__internal_#4`.`count(orders.o_orderkey)`(#4.0)
                    ├── (.cardinality): 0.00
                    └── Join
                        ├── .join_type: LeftOuter
                        ├── .implementation: None
                        ├── .join_cond: `customer`.`c_custkey`(#1.0) = `orders`.`o_custkey`(#2.1)
                        ├── (.output_columns): `customer`.`c_custkey`(#1.0), `orders`.`o_comment`(#2.8), `orders`.`o_custkey`(#2.1), `orders`.`o_orderkey`(#2.0)
                        ├── (.cardinality): 0.00
                        ├── Get
                        │   ├── .data_source_id: 6
                        │   ├── .table_index: 1
                        │   ├── .implementation: None
                        │   ├── (.output_columns): `customer`.`c_custkey`(#1.0)
                        │   └── (.cardinality): 0.00
                        └── Select
                            ├── .predicate: `orders`.`o_comment`(#2.8) NOT LIKE %special%requests%::utf8_view
                            ├── (.output_columns): `orders`.`o_comment`(#2.8), `orders`.`o_custkey`(#2.1), `orders`.`o_orderkey`(#2.0)
                            ├── (.cardinality): 0.00
                            └── Get
                                ├── .data_source_id: 7
                                ├── .table_index: 2
                                ├── .implementation: None
                                ├── (.output_columns): `orders`.`o_comment`(#2.8), `orders`.`o_custkey`(#2.1), `orders`.`o_orderkey`(#2.0)
                                └── (.cardinality): 0.00
*/

