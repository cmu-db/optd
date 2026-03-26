-- TPC-H Q9
SELECT
    nation,
    o_year,
    SUM(amount) AS sum_profit
FROM
    (
        SELECT
            n_name AS nation,
            EXTRACT(YEAR FROM o_orderdate) AS o_year,
            l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
        FROM
            part,
            supplier,
            lineitem,
            partsupp,
            orders,
            nation
        WHERE
            s_suppkey = l_suppkey
            AND ps_suppkey = l_suppkey
            AND ps_partkey = l_partkey
            AND p_partkey = l_partkey
            AND o_orderkey = l_orderkey
            AND s_nationkey = n_nationkey
            AND p_name LIKE '%green%'
    ) AS profit
GROUP BY
    nation,
    o_year
ORDER BY
    nation,
    o_year DESC;

/*
logical_plan after optd-initial:
OrderBy { ordering_exprs: [ nation(#11.0) ASC, o_year(#11.1) DESC ], (.output_columns): nation(#11.0), o_year(#11.1), sum_profit(#11.2), (.cardinality): 0.00 }
└── Project { .table_index: 11, .projections: [ nation(#9.0), o_year(#9.1), sum(profit.amount)(#10.0) ], (.output_columns): nation(#11.0), o_year(#11.1), sum_profit(#11.2), (.cardinality): 0.00 }
    └── Aggregate { .aggregate_table_index: 10, .implementation: None, .exprs: [ sum(amount(#9.2)) ], .keys: [ nation(#9.0), o_year(#9.1) ], (.output_columns): nation(#9.0), o_year(#9.1), sum(profit.amount)(#10.0), (.cardinality): 0.00 }
        └── Remap { .table_index: 9, (.output_columns): amount(#9.2), nation(#9.0), o_year(#9.1), (.cardinality): 0.00 }
            └── Project { .table_index: 8, .projections: [ n_name(#7.47), date_part(YEAR::utf8, o_orderdate(#7.41)), l_extendedprice(#7.21) * 1::decimal128(20, 0) - l_discount(#7.22) - ps_supplycost(#7.35) * l_quantity(#7.20) ], (.output_columns): amount(#8.2), nation(#8.0), o_year(#8.1), (.cardinality): 0.00 }
                └── Project
                    ├── .table_index: 7
                    ├── .projections: [ p_partkey(#1.0), p_name(#1.1), p_mfgr(#1.2), p_brand(#1.3), p_type(#1.4), p_size(#1.5), p_container(#1.6), p_retailprice(#1.7), p_comment(#1.8), s_suppkey(#3.0), s_name(#3.1), s_address(#3.2), s_nationkey(#3.3), s_phone(#3.4), s_acctbal(#3.5), s_comment(#3.6), l_orderkey(#2.0), l_partkey(#2.1), l_suppkey(#2.2), l_linenumber(#2.3), l_quantity(#2.4), l_extendedprice(#2.5), l_discount(#2.6), l_tax(#2.7), l_returnflag(#2.8), l_linestatus(#2.9), l_shipdate(#2.10), l_commitdate(#2.11), l_receiptdate(#2.12), l_shipinstruct(#2.13), l_shipmode(#2.14), l_comment(#2.15), ps_partkey(#4.0), ps_suppkey(#4.1), ps_availqty(#4.2), ps_supplycost(#4.3), ps_comment(#4.4), o_orderkey(#5.0), o_custkey(#5.1), o_orderstatus(#5.2), o_totalprice(#5.3), o_orderdate(#5.4), o_orderpriority(#5.5), o_clerk(#5.6), o_shippriority(#5.7), o_comment(#5.8), n_nationkey(#6.0), n_name(#6.1), n_regionkey(#6.2), n_comment(#6.3) ]
                    ├── (.output_columns): l_comment(#7.31), l_commitdate(#7.27), l_discount(#7.22), l_extendedprice(#7.21), l_linenumber(#7.19), l_linestatus(#7.25), l_orderkey(#7.16), l_partkey(#7.17), l_quantity(#7.20), l_receiptdate(#7.28), l_returnflag(#7.24), l_shipdate(#7.26), l_shipinstruct(#7.29), l_shipmode(#7.30), l_suppkey(#7.18), l_tax(#7.23), n_comment(#7.49), n_name(#7.47), n_nationkey(#7.46), n_regionkey(#7.48), o_clerk(#7.43), o_comment(#7.45), o_custkey(#7.38), o_orderdate(#7.41), o_orderkey(#7.37), o_orderpriority(#7.42), o_orderstatus(#7.39), o_shippriority(#7.44), o_totalprice(#7.40), p_brand(#7.3), p_comment(#7.8), p_container(#7.6), p_mfgr(#7.2), p_name(#7.1), p_partkey(#7.0), p_retailprice(#7.7), p_size(#7.5), p_type(#7.4), ps_availqty(#7.34), ps_comment(#7.36), ps_partkey(#7.32), ps_suppkey(#7.33), ps_supplycost(#7.35), s_acctbal(#7.14), s_address(#7.11), s_comment(#7.15), s_name(#7.10), s_nationkey(#7.12), s_phone(#7.13), s_suppkey(#7.9)
                    ├── (.cardinality): 0.00
                    └── Join
                        ├── .join_type: Inner
                        ├── .implementation: None
                        ├── .join_cond: (s_nationkey(#3.3) = n_nationkey(#6.0))
                        ├── (.output_columns): l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7), n_comment(#6.3), n_name(#6.1), n_nationkey(#6.0), n_regionkey(#6.2), o_clerk(#5.6), o_comment(#5.8), o_custkey(#5.1), o_orderdate(#5.4), o_orderkey(#5.0), o_orderpriority(#5.5), o_orderstatus(#5.2), o_shippriority(#5.7), o_totalprice(#5.3), p_brand(#1.3), p_comment(#1.8), p_container(#1.6), p_mfgr(#1.2), p_name(#1.1), p_partkey(#1.0), p_retailprice(#1.7), p_size(#1.5), p_type(#1.4), ps_availqty(#4.2), ps_comment(#4.4), ps_partkey(#4.0), ps_suppkey(#4.1), ps_supplycost(#4.3), s_acctbal(#3.5), s_address(#3.2), s_comment(#3.6), s_name(#3.1), s_nationkey(#3.3), s_phone(#3.4), s_suppkey(#3.0)
                        ├── (.cardinality): 0.00
                        ├── Join
                        │   ├── .join_type: Inner
                        │   ├── .implementation: None
                        │   ├── .join_cond: (l_orderkey(#2.0) = o_orderkey(#5.0))
                        │   ├── (.output_columns): l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7), o_clerk(#5.6), o_comment(#5.8), o_custkey(#5.1), o_orderdate(#5.4), o_orderkey(#5.0), o_orderpriority(#5.5), o_orderstatus(#5.2), o_shippriority(#5.7), o_totalprice(#5.3), p_brand(#1.3), p_comment(#1.8), p_container(#1.6), p_mfgr(#1.2), p_name(#1.1), p_partkey(#1.0), p_retailprice(#1.7), p_size(#1.5), p_type(#1.4), ps_availqty(#4.2), ps_comment(#4.4), ps_partkey(#4.0), ps_suppkey(#4.1), ps_supplycost(#4.3), s_acctbal(#3.5), s_address(#3.2), s_comment(#3.6), s_name(#3.1), s_nationkey(#3.3), s_phone(#3.4), s_suppkey(#3.0)
                        │   ├── (.cardinality): 0.00
                        │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: (l_suppkey(#2.2) = ps_suppkey(#4.1)) AND (l_partkey(#2.1) = ps_partkey(#4.0)), (.output_columns): l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7), p_brand(#1.3), p_comment(#1.8), p_container(#1.6), p_mfgr(#1.2), p_name(#1.1), p_partkey(#1.0), p_retailprice(#1.7), p_size(#1.5), p_type(#1.4), ps_availqty(#4.2), ps_comment(#4.4), ps_partkey(#4.0), ps_suppkey(#4.1), ps_supplycost(#4.3), s_acctbal(#3.5), s_address(#3.2), s_comment(#3.6), s_name(#3.1), s_nationkey(#3.3), s_phone(#3.4), s_suppkey(#3.0), (.cardinality): 0.00 }
                        │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: (l_suppkey(#2.2) = s_suppkey(#3.0)), (.output_columns): l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7), p_brand(#1.3), p_comment(#1.8), p_container(#1.6), p_mfgr(#1.2), p_name(#1.1), p_partkey(#1.0), p_retailprice(#1.7), p_size(#1.5), p_type(#1.4), s_acctbal(#3.5), s_address(#3.2), s_comment(#3.6), s_name(#3.1), s_nationkey(#3.3), s_phone(#3.4), s_suppkey(#3.0), (.cardinality): 0.00 }
                        │   │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: (p_partkey(#1.0) = l_partkey(#2.1)), (.output_columns): l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7), p_brand(#1.3), p_comment(#1.8), p_container(#1.6), p_mfgr(#1.2), p_name(#1.1), p_partkey(#1.0), p_retailprice(#1.7), p_size(#1.5), p_type(#1.4), (.cardinality): 0.00 }
                        │   │   │   │   ├── Select { .predicate: p_name(#1.1) LIKE %green%::utf8_view, (.output_columns): p_brand(#1.3), p_comment(#1.8), p_container(#1.6), p_mfgr(#1.2), p_name(#1.1), p_partkey(#1.0), p_retailprice(#1.7), p_size(#1.5), p_type(#1.4), (.cardinality): 0.00 }
                        │   │   │   │   │   └── Get { .data_source_id: 3, .table_index: 1, .implementation: None, (.output_columns): p_brand(#1.3), p_comment(#1.8), p_container(#1.6), p_mfgr(#1.2), p_name(#1.1), p_partkey(#1.0), p_retailprice(#1.7), p_size(#1.5), p_type(#1.4), (.cardinality): 0.00 }
                        │   │   │   │   └── Get { .data_source_id: 8, .table_index: 2, .implementation: None, (.output_columns): l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7), (.cardinality): 0.00 }
                        │   │   │   └── Get { .data_source_id: 4, .table_index: 3, .implementation: None, (.output_columns): s_acctbal(#3.5), s_address(#3.2), s_comment(#3.6), s_name(#3.1), s_nationkey(#3.3), s_phone(#3.4), s_suppkey(#3.0), (.cardinality): 0.00 }
                        │   │   └── Get { .data_source_id: 5, .table_index: 4, .implementation: None, (.output_columns): ps_availqty(#4.2), ps_comment(#4.4), ps_partkey(#4.0), ps_suppkey(#4.1), ps_supplycost(#4.3), (.cardinality): 0.00 }
                        │   └── Get { .data_source_id: 7, .table_index: 5, .implementation: None, (.output_columns): o_clerk(#5.6), o_comment(#5.8), o_custkey(#5.1), o_orderdate(#5.4), o_orderkey(#5.0), o_orderpriority(#5.5), o_orderstatus(#5.2), o_shippriority(#5.7), o_totalprice(#5.3), (.cardinality): 0.00 }
                        └── Get { .data_source_id: 1, .table_index: 6, .implementation: None, (.output_columns): n_comment(#6.3), n_name(#6.1), n_nationkey(#6.0), n_regionkey(#6.2), (.cardinality): 0.00 }

physical_plan after optd-finalized:
EnforcerSort { tuple_ordering: [(#11.0, Asc), (#11.1, Desc)], (.output_columns): nation(#11.0), o_year(#11.1), sum_profit(#11.2), (.cardinality): 0.00 }
└── Project { .table_index: 11, .projections: [ nation(#9.0), o_year(#9.1), sum(profit.amount)(#10.0) ], (.output_columns): nation(#11.0), o_year(#11.1), sum_profit(#11.2), (.cardinality): 0.00 }
    └── Aggregate { .aggregate_table_index: 10, .implementation: None, .exprs: [ sum(amount(#9.2)) ], .keys: [ nation(#9.0), o_year(#9.1) ], (.output_columns): nation(#9.0), o_year(#9.1), sum(profit.amount)(#10.0), (.cardinality): 0.00 }
        └── Remap { .table_index: 9, (.output_columns): amount(#9.2), nation(#9.0), o_year(#9.1), (.cardinality): 0.00 }
            └── Project { .table_index: 8, .projections: [ n_name(#6.1), date_part(YEAR::utf8, o_orderdate(#5.4)), l_extendedprice(#2.5) * 1::decimal128(20, 0) - l_discount(#2.6) - ps_supplycost(#4.3) * l_quantity(#2.4) ], (.output_columns): amount(#8.2), nation(#8.0), o_year(#8.1), (.cardinality): 0.00 }
                └── Join
                    ├── .join_type: Inner
                    ├── .implementation: None
                    ├── .join_cond: s_nationkey(#3.3) = n_nationkey(#6.0)
                    ├── (.output_columns): l_discount(#2.6), l_extendedprice(#2.5), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_suppkey(#2.2), n_name(#6.1), n_nationkey(#6.0), o_orderdate(#5.4), o_orderkey(#5.0), p_name(#1.1), p_partkey(#1.0), ps_partkey(#4.0), ps_suppkey(#4.1), ps_supplycost(#4.3), s_nationkey(#3.3), s_suppkey(#3.0)
                    ├── (.cardinality): 0.00
                    ├── Join
                    │   ├── .join_type: Inner
                    │   ├── .implementation: None
                    │   ├── .join_cond: l_orderkey(#2.0) = o_orderkey(#5.0)
                    │   ├── (.output_columns): l_discount(#2.6), l_extendedprice(#2.5), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_suppkey(#2.2), o_orderdate(#5.4), o_orderkey(#5.0), p_name(#1.1), p_partkey(#1.0), ps_partkey(#4.0), ps_suppkey(#4.1), ps_supplycost(#4.3), s_nationkey(#3.3), s_suppkey(#3.0)
                    │   ├── (.cardinality): 0.00
                    │   ├── Join
                    │   │   ├── .join_type: Inner
                    │   │   ├── .implementation: None
                    │   │   ├── .join_cond: (l_suppkey(#2.2) = ps_suppkey(#4.1)) AND (l_partkey(#2.1) = ps_partkey(#4.0))
                    │   │   ├── (.output_columns): l_discount(#2.6), l_extendedprice(#2.5), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_suppkey(#2.2), p_name(#1.1), p_partkey(#1.0), ps_partkey(#4.0), ps_suppkey(#4.1), ps_supplycost(#4.3), s_nationkey(#3.3), s_suppkey(#3.0)
                    │   │   ├── (.cardinality): 0.00
                    │   │   ├── Join
                    │   │   │   ├── .join_type: Inner
                    │   │   │   ├── .implementation: None
                    │   │   │   ├── .join_cond: l_suppkey(#2.2) = s_suppkey(#3.0)
                    │   │   │   ├── (.output_columns): l_discount(#2.6), l_extendedprice(#2.5), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_suppkey(#2.2), p_name(#1.1), p_partkey(#1.0), s_nationkey(#3.3), s_suppkey(#3.0)
                    │   │   │   ├── (.cardinality): 0.00
                    │   │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: p_partkey(#1.0) = l_partkey(#2.1), (.output_columns): l_discount(#2.6), l_extendedprice(#2.5), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_suppkey(#2.2), p_name(#1.1), p_partkey(#1.0), (.cardinality): 0.00 }
                    │   │   │   │   ├── Select { .predicate: p_name(#1.1) LIKE %green%::utf8_view, (.output_columns): p_name(#1.1), p_partkey(#1.0), (.cardinality): 0.00 }
                    │   │   │   │   │   └── Get { .data_source_id: 3, .table_index: 1, .implementation: None, (.output_columns): p_name(#1.1), p_partkey(#1.0), (.cardinality): 0.00 }
                    │   │   │   │   └── Get { .data_source_id: 8, .table_index: 2, .implementation: None, (.output_columns): l_discount(#2.6), l_extendedprice(#2.5), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_suppkey(#2.2), (.cardinality): 0.00 }
                    │   │   │   └── Get { .data_source_id: 4, .table_index: 3, .implementation: None, (.output_columns): s_nationkey(#3.3), s_suppkey(#3.0), (.cardinality): 0.00 }
                    │   │   └── Get { .data_source_id: 5, .table_index: 4, .implementation: None, (.output_columns): ps_partkey(#4.0), ps_suppkey(#4.1), ps_supplycost(#4.3), (.cardinality): 0.00 }
                    │   └── Get { .data_source_id: 7, .table_index: 5, .implementation: None, (.output_columns): o_orderdate(#5.4), o_orderkey(#5.0), (.cardinality): 0.00 }
                    └── Get { .data_source_id: 1, .table_index: 6, .implementation: None, (.output_columns): n_name(#6.1), n_nationkey(#6.0), (.cardinality): 0.00 }
*/

