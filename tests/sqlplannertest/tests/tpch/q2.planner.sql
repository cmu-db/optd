-- TPC-H Q2
select
        s_acctbal,
        s_name,
        n_name,
        p_partkey,
        p_mfgr,
        s_address,
        s_phone,
        s_comment
from
        part,
        supplier,
        partsupp,
        nation,
        region
where
        p_partkey = ps_partkey
        and s_suppkey = ps_suppkey
and p_size = 4
and p_type like '%TIN'
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = 'AFRICA'
        and ps_supplycost = (
                select
                        min(ps_supplycost)
                from
                        partsupp,
                        supplier,
                        nation,
                        region
                where
                        p_partkey = ps_partkey
                        and s_suppkey = ps_suppkey
                        and s_nationkey = n_nationkey
                        and n_regionkey = r_regionkey
                        and r_name = 'AFRICA'
        )
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey
limit 100;

/*
logical_plan after optd-initial:
Limit { .skip: 0::bigint, .fetch: 100::bigint, (.output_columns): n_name(#17.2), p_mfgr(#17.4), p_partkey(#17.3), s_acctbal(#17.0), s_address(#17.5), s_comment(#17.7), s_name(#17.1), s_phone(#17.6), (.cardinality): 0.00 }
└── OrderBy { ordering_exprs: [ s_acctbal(#17.0) DESC, n_name(#17.2) ASC, s_name(#17.1) ASC, p_partkey(#17.3) ASC ], (.output_columns): n_name(#17.2), p_mfgr(#17.4), p_partkey(#17.3), s_acctbal(#17.0), s_address(#17.5), s_comment(#17.7), s_name(#17.1), s_phone(#17.6), (.cardinality): 0.00 }
    └── Project { .table_index: 17, .projections: [ s_acctbal(#16.14), s_name(#16.10), n_name(#16.22), p_partkey(#16.0), p_mfgr(#16.2), s_address(#16.11), s_phone(#16.13), s_comment(#16.15) ], (.output_columns): n_name(#17.2), p_mfgr(#17.4), p_partkey(#17.3), s_acctbal(#17.0), s_address(#17.5), s_comment(#17.7), s_name(#17.1), s_phone(#17.6), (.cardinality): 0.00 }
        └── Project
            ├── .table_index: 16
            ├── .projections: [ p_partkey(#15.0), p_name(#15.1), p_mfgr(#15.2), p_brand(#15.3), p_type(#15.4), p_size(#15.5), p_container(#15.6), p_retailprice(#15.7), p_comment(#15.8), s_suppkey(#15.9), s_name(#15.10), s_address(#15.11), s_nationkey(#15.12), s_phone(#15.13), s_acctbal(#15.14), s_comment(#15.15), ps_partkey(#15.16), ps_suppkey(#15.17), ps_availqty(#15.18), ps_supplycost(#15.19), ps_comment(#15.20), n_nationkey(#15.21), n_name(#15.22), n_regionkey(#15.23), n_comment(#15.24), r_regionkey(#15.25), r_name(#15.26), r_comment(#15.27) ]
            ├── (.output_columns): n_comment(#16.24), n_name(#16.22), n_nationkey(#16.21), n_regionkey(#16.23), p_brand(#16.3), p_comment(#16.8), p_container(#16.6), p_mfgr(#16.2), p_name(#16.1), p_partkey(#16.0), p_retailprice(#16.7), p_size(#16.5), p_type(#16.4), ps_availqty(#16.18), ps_comment(#16.20), ps_partkey(#16.16), ps_suppkey(#16.17), ps_supplycost(#16.19), r_comment(#16.27), r_name(#16.26), r_regionkey(#16.25), s_acctbal(#16.14), s_address(#16.11), s_comment(#16.15), s_name(#16.10), s_nationkey(#16.12), s_phone(#16.13), s_suppkey(#16.9)
            ├── (.cardinality): 0.00
            └── Project
                ├── .table_index: 15
                ├── .projections: [ p_partkey(#1.0), p_name(#1.1), p_mfgr(#1.2), p_brand(#1.3), p_type(#1.4), p_size(#1.5), p_container(#1.6), p_retailprice(#1.7), p_comment(#1.8), s_suppkey(#3.0), s_name(#3.1), s_address(#3.2), s_nationkey(#3.3), s_phone(#3.4), s_acctbal(#3.5), s_comment(#3.6), ps_partkey(#2.0), ps_suppkey(#2.1), ps_availqty(#2.2), ps_supplycost(#2.3), ps_comment(#2.4), n_nationkey(#4.0), n_name(#4.1), n_regionkey(#4.2), n_comment(#4.3), r_regionkey(#5.0), r_name(#5.1), r_comment(#5.2), min(partsupp.ps_supplycost)(#14.0), ps_partkey(#14.1), __always_true(#14.2) ]
                ├── (.output_columns): __always_true(#15.30), min(partsupp.ps_supplycost)(#15.28), n_comment(#15.24), n_name(#15.22), n_nationkey(#15.21), n_regionkey(#15.23), p_brand(#15.3), p_comment(#15.8), p_container(#15.6), p_mfgr(#15.2), p_name(#15.1), p_partkey(#15.0), p_retailprice(#15.7), p_size(#15.5), p_type(#15.4), ps_availqty(#15.18), ps_comment(#15.20), ps_partkey(#15.16), ps_partkey(#15.29), ps_suppkey(#15.17), ps_supplycost(#15.19), r_comment(#15.27), r_name(#15.26), r_regionkey(#15.25), s_acctbal(#15.14), s_address(#15.11), s_comment(#15.15), s_name(#15.10), s_nationkey(#15.12), s_phone(#15.13), s_suppkey(#15.9)
                ├── (.cardinality): 0.00
                └── Join
                    ├── .join_type: Inner
                    ├── .implementation: None
                    ├── .join_cond: (p_partkey(#1.0) = ps_partkey(#14.1)) AND (ps_supplycost(#2.3) = min(partsupp.ps_supplycost)(#14.0))
                    ├── (.output_columns): __always_true(#14.2), min(partsupp.ps_supplycost)(#14.0), n_comment(#4.3), n_name(#4.1), n_nationkey(#4.0), n_regionkey(#4.2), p_brand(#1.3), p_comment(#1.8), p_container(#1.6), p_mfgr(#1.2), p_name(#1.1), p_partkey(#1.0), p_retailprice(#1.7), p_size(#1.5), p_type(#1.4), ps_availqty(#2.2), ps_comment(#2.4), ps_partkey(#14.1), ps_partkey(#2.0), ps_suppkey(#2.1), ps_supplycost(#2.3), r_comment(#5.2), r_name(#5.1), r_regionkey(#5.0), s_acctbal(#3.5), s_address(#3.2), s_comment(#3.6), s_name(#3.1), s_nationkey(#3.3), s_phone(#3.4), s_suppkey(#3.0)
                    ├── (.cardinality): 0.00
                    ├── Join
                    │   ├── .join_type: Inner
                    │   ├── .implementation: None
                    │   ├── .join_cond: (n_regionkey(#4.2) = r_regionkey(#5.0))
                    │   ├── (.output_columns): n_comment(#4.3), n_name(#4.1), n_nationkey(#4.0), n_regionkey(#4.2), p_brand(#1.3), p_comment(#1.8), p_container(#1.6), p_mfgr(#1.2), p_name(#1.1), p_partkey(#1.0), p_retailprice(#1.7), p_size(#1.5), p_type(#1.4), ps_availqty(#2.2), ps_comment(#2.4), ps_partkey(#2.0), ps_suppkey(#2.1), ps_supplycost(#2.3), r_comment(#5.2), r_name(#5.1), r_regionkey(#5.0), s_acctbal(#3.5), s_address(#3.2), s_comment(#3.6), s_name(#3.1), s_nationkey(#3.3), s_phone(#3.4), s_suppkey(#3.0)
                    │   ├── (.cardinality): 0.00
                    │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: (s_nationkey(#3.3) = n_nationkey(#4.0)), (.output_columns): n_comment(#4.3), n_name(#4.1), n_nationkey(#4.0), n_regionkey(#4.2), p_brand(#1.3), p_comment(#1.8), p_container(#1.6), p_mfgr(#1.2), p_name(#1.1), p_partkey(#1.0), p_retailprice(#1.7), p_size(#1.5), p_type(#1.4), ps_availqty(#2.2), ps_comment(#2.4), ps_partkey(#2.0), ps_suppkey(#2.1), ps_supplycost(#2.3), s_acctbal(#3.5), s_address(#3.2), s_comment(#3.6), s_name(#3.1), s_nationkey(#3.3), s_phone(#3.4), s_suppkey(#3.0), (.cardinality): 0.00 }
                    │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: (ps_suppkey(#2.1) = s_suppkey(#3.0)), (.output_columns): p_brand(#1.3), p_comment(#1.8), p_container(#1.6), p_mfgr(#1.2), p_name(#1.1), p_partkey(#1.0), p_retailprice(#1.7), p_size(#1.5), p_type(#1.4), ps_availqty(#2.2), ps_comment(#2.4), ps_partkey(#2.0), ps_suppkey(#2.1), ps_supplycost(#2.3), s_acctbal(#3.5), s_address(#3.2), s_comment(#3.6), s_name(#3.1), s_nationkey(#3.3), s_phone(#3.4), s_suppkey(#3.0), (.cardinality): 0.00 }
                    │   │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: (p_partkey(#1.0) = ps_partkey(#2.0)), (.output_columns): p_brand(#1.3), p_comment(#1.8), p_container(#1.6), p_mfgr(#1.2), p_name(#1.1), p_partkey(#1.0), p_retailprice(#1.7), p_size(#1.5), p_type(#1.4), ps_availqty(#2.2), ps_comment(#2.4), ps_partkey(#2.0), ps_suppkey(#2.1), ps_supplycost(#2.3), (.cardinality): 0.00 }
                    │   │   │   │   ├── Select { .predicate: (p_size(#1.5) = 4::integer) AND (p_type(#1.4) LIKE %TIN::utf8_view), (.output_columns): p_brand(#1.3), p_comment(#1.8), p_container(#1.6), p_mfgr(#1.2), p_name(#1.1), p_partkey(#1.0), p_retailprice(#1.7), p_size(#1.5), p_type(#1.4), (.cardinality): 0.00 }
                    │   │   │   │   │   └── Get { .data_source_id: 3, .table_index: 1, .implementation: None, (.output_columns): p_brand(#1.3), p_comment(#1.8), p_container(#1.6), p_mfgr(#1.2), p_name(#1.1), p_partkey(#1.0), p_retailprice(#1.7), p_size(#1.5), p_type(#1.4), (.cardinality): 0.00 }
                    │   │   │   │   └── Get { .data_source_id: 5, .table_index: 2, .implementation: None, (.output_columns): ps_availqty(#2.2), ps_comment(#2.4), ps_partkey(#2.0), ps_suppkey(#2.1), ps_supplycost(#2.3), (.cardinality): 0.00 }
                    │   │   │   └── Get { .data_source_id: 4, .table_index: 3, .implementation: None, (.output_columns): s_acctbal(#3.5), s_address(#3.2), s_comment(#3.6), s_name(#3.1), s_nationkey(#3.3), s_phone(#3.4), s_suppkey(#3.0), (.cardinality): 0.00 }
                    │   │   └── Get { .data_source_id: 1, .table_index: 4, .implementation: None, (.output_columns): n_comment(#4.3), n_name(#4.1), n_nationkey(#4.0), n_regionkey(#4.2), (.cardinality): 0.00 }
                    │   └── Select { .predicate: r_name(#5.1) = AFRICA::utf8_view, (.output_columns): r_comment(#5.2), r_name(#5.1), r_regionkey(#5.0), (.cardinality): 0.00 }
                    │       └── Get { .data_source_id: 2, .table_index: 5, .implementation: None, (.output_columns): r_comment(#5.2), r_name(#5.1), r_regionkey(#5.0), (.cardinality): 0.00 }
                    └── Remap { .table_index: 14, (.output_columns): __always_true(#14.2), min(partsupp.ps_supplycost)(#14.0), ps_partkey(#14.1), (.cardinality): 0.00 }
                        └── Project { .table_index: 13, .projections: [ min(partsupp.ps_supplycost)(#12.2), ps_partkey(#2.0), __always_true(#12.1) ], (.output_columns): __always_true(#13.2), min(partsupp.ps_supplycost)(#13.0), ps_partkey(#13.1), (.cardinality): 0.00 }
                            └── Project { .table_index: 12, .projections: [ ps_partkey(#6.0), true::boolean, min(partsupp.ps_supplycost)(#11.0) ], (.output_columns): __always_true(#12.1), min(partsupp.ps_supplycost)(#12.2), ps_partkey(#12.0), (.cardinality): 0.00 }
                                └── Aggregate { .key_table_index: 10, .aggregate_table_index: 11, .implementation: None, .exprs: min(ps_supplycost(#6.3)), .keys: [ ps_partkey(#6.0) ], (.output_columns): min(partsupp.ps_supplycost)(#11.0), partsupp.ps_partkey(#10.0), (.cardinality): 0.00 }
                                    └── Join { .join_type: Inner, .implementation: None, .join_cond: (n_regionkey(#8.2) = r_regionkey(#9.0)), (.output_columns): n_comment(#8.3), n_name(#8.1), n_nationkey(#8.0), n_regionkey(#8.2), ps_availqty(#6.2), ps_comment(#6.4), ps_partkey(#6.0), ps_suppkey(#6.1), ps_supplycost(#6.3), r_comment(#9.2), r_name(#9.1), r_regionkey(#9.0), s_acctbal(#7.5), s_address(#7.2), s_comment(#7.6), s_name(#7.1), s_nationkey(#7.3), s_phone(#7.4), s_suppkey(#7.0), (.cardinality): 0.00 }
                                        ├── Join { .join_type: Inner, .implementation: None, .join_cond: (s_nationkey(#7.3) = n_nationkey(#8.0)), (.output_columns): n_comment(#8.3), n_name(#8.1), n_nationkey(#8.0), n_regionkey(#8.2), ps_availqty(#6.2), ps_comment(#6.4), ps_partkey(#6.0), ps_suppkey(#6.1), ps_supplycost(#6.3), s_acctbal(#7.5), s_address(#7.2), s_comment(#7.6), s_name(#7.1), s_nationkey(#7.3), s_phone(#7.4), s_suppkey(#7.0), (.cardinality): 0.00 }
                                        │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: (ps_suppkey(#6.1) = s_suppkey(#7.0)), (.output_columns): ps_availqty(#6.2), ps_comment(#6.4), ps_partkey(#6.0), ps_suppkey(#6.1), ps_supplycost(#6.3), s_acctbal(#7.5), s_address(#7.2), s_comment(#7.6), s_name(#7.1), s_nationkey(#7.3), s_phone(#7.4), s_suppkey(#7.0), (.cardinality): 0.00 }
                                        │   │   ├── Get { .data_source_id: 5, .table_index: 6, .implementation: None, (.output_columns): ps_availqty(#6.2), ps_comment(#6.4), ps_partkey(#6.0), ps_suppkey(#6.1), ps_supplycost(#6.3), (.cardinality): 0.00 }
                                        │   │   └── Get { .data_source_id: 4, .table_index: 7, .implementation: None, (.output_columns): s_acctbal(#7.5), s_address(#7.2), s_comment(#7.6), s_name(#7.1), s_nationkey(#7.3), s_phone(#7.4), s_suppkey(#7.0), (.cardinality): 0.00 }
                                        │   └── Get { .data_source_id: 1, .table_index: 8, .implementation: None, (.output_columns): n_comment(#8.3), n_name(#8.1), n_nationkey(#8.0), n_regionkey(#8.2), (.cardinality): 0.00 }
                                        └── Select { .predicate: r_name(#9.1) = AFRICA::utf8_view, (.output_columns): r_comment(#9.2), r_name(#9.1), r_regionkey(#9.0), (.cardinality): 0.00 }
                                            └── Get { .data_source_id: 2, .table_index: 9, .implementation: None, (.output_columns): r_comment(#9.2), r_name(#9.1), r_regionkey(#9.0), (.cardinality): 0.00 }

physical_plan after optd-finalized:
Limit { .skip: 0::bigint, .fetch: 100::bigint, (.output_columns): n_name(#17.2), p_mfgr(#17.4), p_partkey(#17.3), s_acctbal(#17.0), s_address(#17.5), s_comment(#17.7), s_name(#17.1), s_phone(#17.6), (.cardinality): 0.00 }
└── EnforcerSort { tuple_ordering: [(#17.0, Desc), (#17.2, Asc), (#17.1, Asc), (#17.3, Asc)], (.output_columns): n_name(#17.2), p_mfgr(#17.4), p_partkey(#17.3), s_acctbal(#17.0), s_address(#17.5), s_comment(#17.7), s_name(#17.1), s_phone(#17.6), (.cardinality): 0.00 }
    └── Project { .table_index: 17, .projections: [ s_acctbal(#3.5), s_name(#3.1), n_name(#4.1), p_partkey(#1.0), p_mfgr(#1.2), s_address(#3.2), s_phone(#3.4), s_comment(#3.6) ], (.output_columns): n_name(#17.2), p_mfgr(#17.4), p_partkey(#17.3), s_acctbal(#17.0), s_address(#17.5), s_comment(#17.7), s_name(#17.1), s_phone(#17.6), (.cardinality): 0.00 }
        └── Join
            ├── .join_type: Inner
            ├── .implementation: None
            ├── .join_cond: (p_partkey(#1.0) = ps_partkey(#14.1)) AND (ps_supplycost(#2.3) = min(partsupp.ps_supplycost)(#14.0))
            ├── (.output_columns): __always_true(#14.2), min(partsupp.ps_supplycost)(#14.0), n_name(#4.1), n_nationkey(#4.0), n_regionkey(#4.2), p_mfgr(#1.2), p_partkey(#1.0), p_size(#1.5), p_type(#1.4), ps_partkey(#14.1), ps_partkey(#2.0), ps_suppkey(#2.1), ps_supplycost(#2.3), r_name(#5.1), r_regionkey(#5.0), s_acctbal(#3.5), s_address(#3.2), s_comment(#3.6), s_name(#3.1), s_nationkey(#3.3), s_phone(#3.4), s_suppkey(#3.0)
            ├── (.cardinality): 0.00
            ├── Join
            │   ├── .join_type: Inner
            │   ├── .implementation: None
            │   ├── .join_cond: n_regionkey(#4.2) = r_regionkey(#5.0)
            │   ├── (.output_columns): n_name(#4.1), n_nationkey(#4.0), n_regionkey(#4.2), p_mfgr(#1.2), p_partkey(#1.0), p_size(#1.5), p_type(#1.4), ps_partkey(#2.0), ps_suppkey(#2.1), ps_supplycost(#2.3), r_name(#5.1), r_regionkey(#5.0), s_acctbal(#3.5), s_address(#3.2), s_comment(#3.6), s_name(#3.1), s_nationkey(#3.3), s_phone(#3.4), s_suppkey(#3.0)
            │   ├── (.cardinality): 0.00
            │   ├── Join
            │   │   ├── .join_type: Inner
            │   │   ├── .implementation: None
            │   │   ├── .join_cond: s_nationkey(#3.3) = n_nationkey(#4.0)
            │   │   ├── (.output_columns): n_name(#4.1), n_nationkey(#4.0), n_regionkey(#4.2), p_mfgr(#1.2), p_partkey(#1.0), p_size(#1.5), p_type(#1.4), ps_partkey(#2.0), ps_suppkey(#2.1), ps_supplycost(#2.3), s_acctbal(#3.5), s_address(#3.2), s_comment(#3.6), s_name(#3.1), s_nationkey(#3.3), s_phone(#3.4), s_suppkey(#3.0)
            │   │   ├── (.cardinality): 0.00
            │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: ps_suppkey(#2.1) = s_suppkey(#3.0), (.output_columns): p_mfgr(#1.2), p_partkey(#1.0), p_size(#1.5), p_type(#1.4), ps_partkey(#2.0), ps_suppkey(#2.1), ps_supplycost(#2.3), s_acctbal(#3.5), s_address(#3.2), s_comment(#3.6), s_name(#3.1), s_nationkey(#3.3), s_phone(#3.4), s_suppkey(#3.0), (.cardinality): 0.00 }
            │   │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: p_partkey(#1.0) = ps_partkey(#2.0), (.output_columns): p_mfgr(#1.2), p_partkey(#1.0), p_size(#1.5), p_type(#1.4), ps_partkey(#2.0), ps_suppkey(#2.1), ps_supplycost(#2.3), (.cardinality): 0.00 }
            │   │   │   │   ├── Select { .predicate: (p_size(#1.5) = 4::integer) AND (p_type(#1.4) LIKE %TIN::utf8_view), (.output_columns): p_mfgr(#1.2), p_partkey(#1.0), p_size(#1.5), p_type(#1.4), (.cardinality): 0.00 }
            │   │   │   │   │   └── Get { .data_source_id: 3, .table_index: 1, .implementation: None, (.output_columns): p_mfgr(#1.2), p_partkey(#1.0), p_size(#1.5), p_type(#1.4), (.cardinality): 0.00 }
            │   │   │   │   └── Get { .data_source_id: 5, .table_index: 2, .implementation: None, (.output_columns): ps_partkey(#2.0), ps_suppkey(#2.1), ps_supplycost(#2.3), (.cardinality): 0.00 }
            │   │   │   └── Get { .data_source_id: 4, .table_index: 3, .implementation: None, (.output_columns): s_acctbal(#3.5), s_address(#3.2), s_comment(#3.6), s_name(#3.1), s_nationkey(#3.3), s_phone(#3.4), s_suppkey(#3.0), (.cardinality): 0.00 }
            │   │   └── Get { .data_source_id: 1, .table_index: 4, .implementation: None, (.output_columns): n_name(#4.1), n_nationkey(#4.0), n_regionkey(#4.2), (.cardinality): 0.00 }
            │   └── Select { .predicate: r_name(#5.1) = AFRICA::utf8_view, (.output_columns): r_name(#5.1), r_regionkey(#5.0), (.cardinality): 0.00 }
            │       └── Get { .data_source_id: 2, .table_index: 5, .implementation: None, (.output_columns): r_name(#5.1), r_regionkey(#5.0), (.cardinality): 0.00 }
            └── Remap { .table_index: 14, (.output_columns): __always_true(#14.2), min(partsupp.ps_supplycost)(#14.0), ps_partkey(#14.1), (.cardinality): 0.00 }
                └── Project { .table_index: 13, .projections: [ min(partsupp.ps_supplycost)(#11.0), ps_partkey(#2.0), true::boolean ], (.output_columns): __always_true(#13.2), min(partsupp.ps_supplycost)(#13.0), ps_partkey(#13.1), (.cardinality): 0.00 }
                    └── Aggregate { .key_table_index: 10, .aggregate_table_index: 11, .implementation: None, .exprs: [ min(ps_supplycost(#6.3)) ], .keys: [ ps_partkey(#6.0) ], (.output_columns): min(partsupp.ps_supplycost)(#11.0), partsupp.ps_partkey(#10.0), (.cardinality): 0.00 }
                        └── Join { .join_type: Inner, .implementation: None, .join_cond: n_regionkey(#8.2) = r_regionkey(#9.0), (.output_columns): n_nationkey(#8.0), n_regionkey(#8.2), ps_partkey(#6.0), ps_suppkey(#6.1), ps_supplycost(#6.3), r_name(#9.1), r_regionkey(#9.0), s_nationkey(#7.3), s_suppkey(#7.0), (.cardinality): 0.00 }
                            ├── Join { .join_type: Inner, .implementation: None, .join_cond: s_nationkey(#7.3) = n_nationkey(#8.0), (.output_columns): n_nationkey(#8.0), n_regionkey(#8.2), ps_partkey(#6.0), ps_suppkey(#6.1), ps_supplycost(#6.3), s_nationkey(#7.3), s_suppkey(#7.0), (.cardinality): 0.00 }
                            │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: ps_suppkey(#6.1) = s_suppkey(#7.0), (.output_columns): ps_partkey(#6.0), ps_suppkey(#6.1), ps_supplycost(#6.3), s_nationkey(#7.3), s_suppkey(#7.0), (.cardinality): 0.00 }
                            │   │   ├── Get { .data_source_id: 5, .table_index: 6, .implementation: None, (.output_columns): ps_partkey(#6.0), ps_suppkey(#6.1), ps_supplycost(#6.3), (.cardinality): 0.00 }
                            │   │   └── Get { .data_source_id: 4, .table_index: 7, .implementation: None, (.output_columns): s_nationkey(#7.3), s_suppkey(#7.0), (.cardinality): 0.00 }
                            │   └── Get { .data_source_id: 1, .table_index: 8, .implementation: None, (.output_columns): n_nationkey(#8.0), n_regionkey(#8.2), (.cardinality): 0.00 }
                            └── Select { .predicate: r_name(#9.1) = AFRICA::utf8_view, (.output_columns): r_name(#9.1), r_regionkey(#9.0), (.cardinality): 0.00 }
                                └── Get { .data_source_id: 2, .table_index: 9, .implementation: None, (.output_columns): r_name(#9.1), r_regionkey(#9.0), (.cardinality): 0.00 }
*/

