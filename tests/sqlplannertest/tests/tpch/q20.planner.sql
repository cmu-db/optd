-- TPC-H Q20
select
    s_name,
    s_address
from
    supplier,
    nation
where
    s_suppkey in (
        select
            ps_suppkey
        from
            partsupp
        where
            ps_partkey in (
                select
                    p_partkey
                from
                    part
                where
                    p_name like 'indian%'
            )
            and ps_availqty > (
                select
                    0.5 * sum(l_quantity)
                from
                    lineitem
                where
                    l_partkey = ps_partkey
                    and l_suppkey = ps_suppkey
                    and l_shipdate >= date '1996-01-01'
                    and l_shipdate < date '1996-01-01' + interval '1' year
            )
    )
    and s_nationkey = n_nationkey
    and n_name = 'IRAQ'
order by
    s_name;

/*
logical_plan after optd-initial:
OrderBy { ordering_exprs: [ s_name(#17.0) ASC ], (.output_columns): s_address(#17.1), s_name(#17.0), (.cardinality): 0.00 }
└── Project { .table_index: 17, .projections: [ s_name(#1.1), s_address(#1.2) ], (.output_columns): s_address(#17.1), s_name(#17.0), (.cardinality): 0.00 }
    └── Join { .join_type: LeftSemi, .implementation: None, .join_cond: (s_suppkey(#1.0) = ps_suppkey(#16.0)), (.output_columns): n_comment(#2.3), n_name(#2.1), n_nationkey(#2.0), n_regionkey(#2.2), s_acctbal(#1.5), s_address(#1.2), s_comment(#1.6), s_name(#1.1), s_nationkey(#1.3), s_phone(#1.4), s_suppkey(#1.0), (.cardinality): 0.00 }
        ├── Join { .join_type: Inner, .implementation: None, .join_cond: (s_nationkey(#1.3) = n_nationkey(#2.0)), (.output_columns): n_comment(#2.3), n_name(#2.1), n_nationkey(#2.0), n_regionkey(#2.2), s_acctbal(#1.5), s_address(#1.2), s_comment(#1.6), s_name(#1.1), s_nationkey(#1.3), s_phone(#1.4), s_suppkey(#1.0), (.cardinality): 0.00 }
        │   ├── Get { .data_source_id: 4, .table_index: 1, .implementation: None, (.output_columns): s_acctbal(#1.5), s_address(#1.2), s_comment(#1.6), s_name(#1.1), s_nationkey(#1.3), s_phone(#1.4), s_suppkey(#1.0), (.cardinality): 0.00 }
        │   └── Select { .predicate: n_name(#2.1) = IRAQ::utf8_view, (.output_columns): n_comment(#2.3), n_name(#2.1), n_nationkey(#2.0), n_regionkey(#2.2), (.cardinality): 0.00 }
        │       └── Get { .data_source_id: 1, .table_index: 2, .implementation: None, (.output_columns): n_comment(#2.3), n_name(#2.1), n_nationkey(#2.0), n_regionkey(#2.2), (.cardinality): 0.00 }
        └── Remap { .table_index: 16, (.output_columns): ps_suppkey(#16.0), (.cardinality): 0.00 }
            └── Project { .table_index: 15, .projections: [ ps_suppkey(#14.1) ], (.output_columns): ps_suppkey(#15.0), (.cardinality): 0.00 }
                └── Project { .table_index: 14, .projections: [ ps_partkey(#13.0), ps_suppkey(#13.1), ps_availqty(#13.2), ps_supplycost(#13.3), ps_comment(#13.4) ], (.output_columns): ps_availqty(#14.2), ps_comment(#14.4), ps_partkey(#14.0), ps_suppkey(#14.1), ps_supplycost(#14.3), (.cardinality): 0.00 }
                    └── Project
                        ├── .table_index: 13
                        ├── .projections: [ ps_partkey(#3.0), ps_suppkey(#3.1), ps_availqty(#3.2), ps_supplycost(#3.3), ps_comment(#3.4), Float64(0.5) * sum(lineitem.l_quantity)(#12.0), l_partkey(#12.1), l_suppkey(#12.2), __always_true(#12.3) ]
                        ├── (.output_columns): Float64(0.5) * sum(lineitem.l_quantity)(#13.5), __always_true(#13.8), l_partkey(#13.6), l_suppkey(#13.7), ps_availqty(#13.2), ps_comment(#13.4), ps_partkey(#13.0), ps_suppkey(#13.1), ps_supplycost(#13.3)
                        ├── (.cardinality): 0.00
                        └── Join
                            ├── .join_type: Inner
                            ├── .implementation: None
                            ├── .join_cond: (ps_partkey(#3.0) = l_partkey(#12.1)) AND (ps_suppkey(#3.1) = l_suppkey(#12.2)) AND (CAST (ps_availqty(#3.2) AS Float64) > Float64(0.5) * sum(lineitem.l_quantity)(#12.0))
                            ├── (.output_columns): Float64(0.5) * sum(lineitem.l_quantity)(#12.0), __always_true(#12.3), l_partkey(#12.1), l_suppkey(#12.2), ps_availqty(#3.2), ps_comment(#3.4), ps_partkey(#3.0), ps_suppkey(#3.1), ps_supplycost(#3.3)
                            ├── (.cardinality): 0.00
                            ├── Join { .join_type: LeftSemi, .implementation: None, .join_cond: (ps_partkey(#3.0) = p_partkey(#6.0)), (.output_columns): ps_availqty(#3.2), ps_comment(#3.4), ps_partkey(#3.0), ps_suppkey(#3.1), ps_supplycost(#3.3), (.cardinality): 0.00 }
                            │   ├── Get { .data_source_id: 5, .table_index: 3, .implementation: None, (.output_columns): ps_availqty(#3.2), ps_comment(#3.4), ps_partkey(#3.0), ps_suppkey(#3.1), ps_supplycost(#3.3), (.cardinality): 0.00 }
                            │   └── Remap { .table_index: 6, (.output_columns): p_partkey(#6.0), (.cardinality): 0.00 }
                            │       └── Project { .table_index: 5, .projections: p_partkey(#4.0), (.output_columns): p_partkey(#5.0), (.cardinality): 0.00 }
                            │           └── Select { .predicate: p_name(#4.1) LIKE indian%::utf8_view, (.output_columns): p_brand(#4.3), p_comment(#4.8), p_container(#4.6), p_mfgr(#4.2), p_name(#4.1), p_partkey(#4.0), p_retailprice(#4.7), p_size(#4.5), p_type(#4.4), (.cardinality): 0.00 }
                            │               └── Get { .data_source_id: 3, .table_index: 4, .implementation: None, (.output_columns): p_brand(#4.3), p_comment(#4.8), p_container(#4.6), p_mfgr(#4.2), p_name(#4.1), p_partkey(#4.0), p_retailprice(#4.7), p_size(#4.5), p_type(#4.4), (.cardinality): 0.00 }
                            └── Remap { .table_index: 12, (.output_columns): Float64(0.5) * sum(lineitem.l_quantity)(#12.0), __always_true(#12.3), l_partkey(#12.1), l_suppkey(#12.2), (.cardinality): 0.00 }
                                └── Project { .table_index: 11, .projections: [ 0.5::float64 * CAST (sum(lineitem.l_quantity)(#10.3) AS Float64), l_partkey(#10.0), l_suppkey(#10.1), __always_true(#10.2) ], (.output_columns): Float64(0.5) * sum(lineitem.l_quantity)(#11.0), __always_true(#11.3), l_partkey(#11.1), l_suppkey(#11.2), (.cardinality): 0.00 }
                                    └── Project { .table_index: 10, .projections: [ l_partkey(#7.1), l_suppkey(#7.2), true::boolean, sum(lineitem.l_quantity)(#9.0) ], (.output_columns): __always_true(#10.2), l_partkey(#10.0), l_suppkey(#10.1), sum(lineitem.l_quantity)(#10.3), (.cardinality): 0.00 }
                                        └── Aggregate { .key_table_index: 8, .aggregate_table_index: 9, .implementation: None, .exprs: sum(l_quantity(#7.4)), .keys: [ l_partkey(#7.1), l_suppkey(#7.2) ], (.output_columns): lineitem.l_partkey(#8.0), lineitem.l_suppkey(#8.1), sum(lineitem.l_quantity)(#9.0), (.cardinality): 0.00 }
                                            └── Select
                                                ├── .predicate: (l_shipdate(#7.10) >= 1996-01-01::date32) AND (l_shipdate(#7.10) < 1997-01-01::date32)
                                                ├── (.output_columns): l_comment(#7.15), l_commitdate(#7.11), l_discount(#7.6), l_extendedprice(#7.5), l_linenumber(#7.3), l_linestatus(#7.9), l_orderkey(#7.0), l_partkey(#7.1), l_quantity(#7.4), l_receiptdate(#7.12), l_returnflag(#7.8), l_shipdate(#7.10), l_shipinstruct(#7.13), l_shipmode(#7.14), l_suppkey(#7.2), l_tax(#7.7)
                                                ├── (.cardinality): 0.00
                                                └── Get
                                                    ├── .data_source_id: 8
                                                    ├── .table_index: 7
                                                    ├── .implementation: None
                                                    ├── (.output_columns): l_comment(#7.15), l_commitdate(#7.11), l_discount(#7.6), l_extendedprice(#7.5), l_linenumber(#7.3), l_linestatus(#7.9), l_orderkey(#7.0), l_partkey(#7.1), l_quantity(#7.4), l_receiptdate(#7.12), l_returnflag(#7.8), l_shipdate(#7.10), l_shipinstruct(#7.13), l_shipmode(#7.14), l_suppkey(#7.2), l_tax(#7.7)
                                                    └── (.cardinality): 0.00

physical_plan after optd-finalized:
EnforcerSort { tuple_ordering: [(#17.0, Asc)], (.output_columns): s_address(#17.1), s_name(#17.0), (.cardinality): 0.00 }
└── Project { .table_index: 17, .projections: [ s_name(#1.1), s_address(#1.2) ], (.output_columns): s_address(#17.1), s_name(#17.0), (.cardinality): 0.00 }
    └── Join
        ├── .join_type: LeftSemi
        ├── .implementation: None
        ├── .join_cond: s_suppkey(#1.0) = ps_suppkey(#16.0)
        ├── (.output_columns): n_name(#2.1), n_nationkey(#2.0), s_address(#1.2), s_name(#1.1), s_nationkey(#1.3), s_suppkey(#1.0)
        ├── (.cardinality): 0.00
        ├── Join
        │   ├── .join_type: Inner
        │   ├── .implementation: None
        │   ├── .join_cond: s_nationkey(#1.3) = n_nationkey(#2.0)
        │   ├── (.output_columns): n_name(#2.1), n_nationkey(#2.0), s_address(#1.2), s_name(#1.1), s_nationkey(#1.3), s_suppkey(#1.0)
        │   ├── (.cardinality): 0.00
        │   ├── Get { .data_source_id: 4, .table_index: 1, .implementation: None, (.output_columns): s_address(#1.2), s_name(#1.1), s_nationkey(#1.3), s_suppkey(#1.0), (.cardinality): 0.00 }
        │   └── Select { .predicate: n_name(#2.1) = IRAQ::utf8_view, (.output_columns): n_name(#2.1), n_nationkey(#2.0), (.cardinality): 0.00 }
        │       └── Get { .data_source_id: 1, .table_index: 2, .implementation: None, (.output_columns): n_name(#2.1), n_nationkey(#2.0), (.cardinality): 0.00 }
        └── Remap { .table_index: 16, (.output_columns): ps_suppkey(#16.0), (.cardinality): 0.00 }
            └── Project { .table_index: 15, .projections: [ ps_suppkey(#3.1) ], (.output_columns): ps_suppkey(#15.0), (.cardinality): 0.00 }
                └── Join
                    ├── .join_type: Inner
                    ├── .implementation: None
                    ├── .join_cond: (ps_partkey(#3.0) = l_partkey(#12.1)) AND (ps_suppkey(#3.1) = l_suppkey(#12.2)) AND (CAST (ps_availqty(#3.2) AS Float64) > Float64(0.5) * sum(lineitem.l_quantity)(#12.0))
                    ├── (.output_columns): Float64(0.5) * sum(lineitem.l_quantity)(#12.0), __always_true(#12.3), l_partkey(#12.1), l_suppkey(#12.2), ps_availqty(#3.2), ps_partkey(#3.0), ps_suppkey(#3.1)
                    ├── (.cardinality): 0.00
                    ├── Join
                    │   ├── .join_type: LeftSemi
                    │   ├── .implementation: None
                    │   ├── .join_cond: ps_partkey(#3.0) = p_partkey(#6.0)
                    │   ├── (.output_columns): ps_availqty(#3.2), ps_partkey(#3.0), ps_suppkey(#3.1)
                    │   ├── (.cardinality): 0.00
                    │   ├── Get { .data_source_id: 5, .table_index: 3, .implementation: None, (.output_columns): ps_availqty(#3.2), ps_partkey(#3.0), ps_suppkey(#3.1), (.cardinality): 0.00 }
                    │   └── Remap { .table_index: 6, (.output_columns): p_partkey(#6.0), (.cardinality): 0.00 }
                    │       └── Project { .table_index: 5, .projections: [ p_partkey(#4.0) ], (.output_columns): p_partkey(#5.0), (.cardinality): 0.00 }
                    │           └── Select { .predicate: p_name(#4.1) LIKE indian%::utf8_view, (.output_columns): p_name(#4.1), p_partkey(#4.0), (.cardinality): 0.00 }
                    │               └── Get { .data_source_id: 3, .table_index: 4, .implementation: None, (.output_columns): p_name(#4.1), p_partkey(#4.0), (.cardinality): 0.00 }
                    └── Remap { .table_index: 12, (.output_columns): Float64(0.5) * sum(lineitem.l_quantity)(#12.0), __always_true(#12.3), l_partkey(#12.1), l_suppkey(#12.2), (.cardinality): 0.00 }
                        └── Project
                            ├── .table_index: 11
                            ├── .projections: [ 0.5::float64 * CAST (sum(lineitem.l_quantity)(#9.0) AS Float64), l_partkey(#7.1), l_suppkey(#7.2), true::boolean ]
                            ├── (.output_columns): Float64(0.5) * sum(lineitem.l_quantity)(#11.0), __always_true(#11.3), l_partkey(#11.1), l_suppkey(#11.2)
                            ├── (.cardinality): 0.00
                            └── Aggregate
                                ├── .key_table_index: 8
                                ├── .aggregate_table_index: 9
                                ├── .implementation: None
                                ├── .exprs: [ sum(l_quantity(#7.4)) ]
                                ├── .keys: [ l_partkey(#7.1), l_suppkey(#7.2) ]
                                ├── (.output_columns): lineitem.l_partkey(#8.0), lineitem.l_suppkey(#8.1), sum(lineitem.l_quantity)(#9.0)
                                ├── (.cardinality): 0.00
                                └── Select
                                    ├── .predicate: (l_shipdate(#7.10) >= 1996-01-01::date32) AND (l_shipdate(#7.10) < 1997-01-01::date32)
                                    ├── (.output_columns): l_partkey(#7.1), l_quantity(#7.4), l_shipdate(#7.10), l_suppkey(#7.2)
                                    ├── (.cardinality): 0.00
                                    └── Get
                                        ├── .data_source_id: 8
                                        ├── .table_index: 7
                                        ├── .implementation: None
                                        ├── (.output_columns): l_partkey(#7.1), l_quantity(#7.4), l_shipdate(#7.10), l_suppkey(#7.2)
                                        └── (.cardinality): 0.00
*/

