-- TPC-H Q7
SELECT
    supp_nation,
    cust_nation,
    l_year,
    SUM(volume) AS revenue
FROM
    (
        SELECT
            n1.n_name AS supp_nation,
            n2.n_name AS cust_nation,
            EXTRACT(YEAR FROM l_shipdate) AS l_year,
            l_extendedprice * (1 - l_discount) AS volume
        FROM
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2
        WHERE
            s_suppkey = l_suppkey
            AND o_orderkey = l_orderkey
            AND c_custkey = o_custkey
            AND s_nationkey = n1.n_nationkey
            AND c_nationkey = n2.n_nationkey
            AND (
                (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE')
            )
            AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'
    ) AS shipping
GROUP BY
    supp_nation,
    cust_nation,
    l_year
ORDER BY
    supp_nation,
    cust_nation,
    l_year;

/*
logical_plan after optd-initial:
OrderBy { ordering_exprs: [ supp_nation(#13.0) ASC, cust_nation(#13.1) ASC, l_year(#13.2) ASC ], (.output_columns): cust_nation(#13.1), l_year(#13.2), revenue(#13.3), supp_nation(#13.0), (.cardinality): 0.00 }
└── Project { .table_index: 13, .projections: [ supp_nation(#10.0), cust_nation(#10.1), l_year(#10.2), sum(shipping.volume)(#12.0) ], (.output_columns): cust_nation(#13.1), l_year(#13.2), revenue(#13.3), supp_nation(#13.0), (.cardinality): 0.00 }
    └── Aggregate { .key_table_index: 11, .aggregate_table_index: 12, .implementation: None, .exprs: [ sum(volume(#10.3)) ], .keys: [ supp_nation(#10.0), cust_nation(#10.1), l_year(#10.2) ], (.output_columns): shipping.cust_nation(#11.1), shipping.l_year(#11.2), shipping.supp_nation(#11.0), sum(shipping.volume)(#12.0), (.cardinality): 0.00 }
        └── Remap { .table_index: 10, (.output_columns): cust_nation(#10.1), l_year(#10.2), supp_nation(#10.0), volume(#10.3), (.cardinality): 0.00 }
            └── Project { .table_index: 9, .projections: [ n_name(#6.1), n_name(#8.1), date_part(YEAR::utf8, l_shipdate(#2.10)), l_extendedprice(#2.5) * 1::decimal128(20, 0) - l_discount(#2.6) ], (.output_columns): cust_nation(#9.1), l_year(#9.2), supp_nation(#9.0), volume(#9.3), (.cardinality): 0.00 }
                └── Join
                    ├── .join_type: Inner
                    ├── .implementation: None
                    ├── .join_cond: (c_nationkey(#4.3) = n_nationkey(#8.0)) AND (((n_name(#6.1) = FRANCE::utf8_view) AND (n_name(#8.1) = GERMANY::utf8_view)) OR ((n_name(#6.1) = GERMANY::utf8_view) AND (n_name(#8.1) = FRANCE::utf8_view)))
                    ├── (.output_columns): c_acctbal(#4.5), c_address(#4.2), c_comment(#4.7), c_custkey(#4.0), c_mktsegment(#4.6), c_name(#4.1), c_nationkey(#4.3), c_phone(#4.4), l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7), n_comment(#6.3), n_comment(#8.3), n_name(#6.1), n_name(#8.1), n_nationkey(#6.0), n_nationkey(#8.0), n_regionkey(#6.2), n_regionkey(#8.2), o_clerk(#3.6), o_comment(#3.8), o_custkey(#3.1), o_orderdate(#3.4), o_orderkey(#3.0), o_orderpriority(#3.5), o_orderstatus(#3.2), o_shippriority(#3.7), o_totalprice(#3.3), s_acctbal(#1.5), s_address(#1.2), s_comment(#1.6), s_name(#1.1), s_nationkey(#1.3), s_phone(#1.4), s_suppkey(#1.0)
                    ├── (.cardinality): 0.00
                    ├── Join
                    │   ├── .join_type: Inner
                    │   ├── .implementation: None
                    │   ├── .join_cond: (s_nationkey(#1.3) = n_nationkey(#6.0))
                    │   ├── (.output_columns): c_acctbal(#4.5), c_address(#4.2), c_comment(#4.7), c_custkey(#4.0), c_mktsegment(#4.6), c_name(#4.1), c_nationkey(#4.3), c_phone(#4.4), l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7), n_comment(#6.3), n_name(#6.1), n_nationkey(#6.0), n_regionkey(#6.2), o_clerk(#3.6), o_comment(#3.8), o_custkey(#3.1), o_orderdate(#3.4), o_orderkey(#3.0), o_orderpriority(#3.5), o_orderstatus(#3.2), o_shippriority(#3.7), o_totalprice(#3.3), s_acctbal(#1.5), s_address(#1.2), s_comment(#1.6), s_name(#1.1), s_nationkey(#1.3), s_phone(#1.4), s_suppkey(#1.0)
                    │   ├── (.cardinality): 0.00
                    │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: (o_custkey(#3.1) = c_custkey(#4.0)), (.output_columns): c_acctbal(#4.5), c_address(#4.2), c_comment(#4.7), c_custkey(#4.0), c_mktsegment(#4.6), c_name(#4.1), c_nationkey(#4.3), c_phone(#4.4), l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7), o_clerk(#3.6), o_comment(#3.8), o_custkey(#3.1), o_orderdate(#3.4), o_orderkey(#3.0), o_orderpriority(#3.5), o_orderstatus(#3.2), o_shippriority(#3.7), o_totalprice(#3.3), s_acctbal(#1.5), s_address(#1.2), s_comment(#1.6), s_name(#1.1), s_nationkey(#1.3), s_phone(#1.4), s_suppkey(#1.0), (.cardinality): 0.00 }
                    │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: (l_orderkey(#2.0) = o_orderkey(#3.0)), (.output_columns): l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7), o_clerk(#3.6), o_comment(#3.8), o_custkey(#3.1), o_orderdate(#3.4), o_orderkey(#3.0), o_orderpriority(#3.5), o_orderstatus(#3.2), o_shippriority(#3.7), o_totalprice(#3.3), s_acctbal(#1.5), s_address(#1.2), s_comment(#1.6), s_name(#1.1), s_nationkey(#1.3), s_phone(#1.4), s_suppkey(#1.0), (.cardinality): 0.00 }
                    │   │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: (s_suppkey(#1.0) = l_suppkey(#2.2)), (.output_columns): l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7), s_acctbal(#1.5), s_address(#1.2), s_comment(#1.6), s_name(#1.1), s_nationkey(#1.3), s_phone(#1.4), s_suppkey(#1.0), (.cardinality): 0.00 }
                    │   │   │   │   ├── Get { .data_source_id: 4, .table_index: 1, .implementation: None, (.output_columns): s_acctbal(#1.5), s_address(#1.2), s_comment(#1.6), s_name(#1.1), s_nationkey(#1.3), s_phone(#1.4), s_suppkey(#1.0), (.cardinality): 0.00 }
                    │   │   │   │   └── Select { .predicate: (l_shipdate(#2.10) >= 1995-01-01::date32) AND (l_shipdate(#2.10) <= 1996-12-31::date32), (.output_columns): l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7), (.cardinality): 0.00 }
                    │   │   │   │       └── Get { .data_source_id: 8, .table_index: 2, .implementation: None, (.output_columns): l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7), (.cardinality): 0.00 }
                    │   │   │   └── Get { .data_source_id: 7, .table_index: 3, .implementation: None, (.output_columns): o_clerk(#3.6), o_comment(#3.8), o_custkey(#3.1), o_orderdate(#3.4), o_orderkey(#3.0), o_orderpriority(#3.5), o_orderstatus(#3.2), o_shippriority(#3.7), o_totalprice(#3.3), (.cardinality): 0.00 }
                    │   │   └── Get { .data_source_id: 6, .table_index: 4, .implementation: None, (.output_columns): c_acctbal(#4.5), c_address(#4.2), c_comment(#4.7), c_custkey(#4.0), c_mktsegment(#4.6), c_name(#4.1), c_nationkey(#4.3), c_phone(#4.4), (.cardinality): 0.00 }
                    │   └── Remap { .table_index: 6, (.output_columns): n_comment(#6.3), n_name(#6.1), n_nationkey(#6.0), n_regionkey(#6.2), (.cardinality): 0.00 }
                    │       └── Select { .predicate: (n_name(#5.1) = FRANCE::utf8_view) OR (n_name(#5.1) = GERMANY::utf8_view), (.output_columns): n_comment(#5.3), n_name(#5.1), n_nationkey(#5.0), n_regionkey(#5.2), (.cardinality): 0.00 }
                    │           └── Get { .data_source_id: 1, .table_index: 5, .implementation: None, (.output_columns): n_comment(#5.3), n_name(#5.1), n_nationkey(#5.0), n_regionkey(#5.2), (.cardinality): 0.00 }
                    └── Remap { .table_index: 8, (.output_columns): n_comment(#8.3), n_name(#8.1), n_nationkey(#8.0), n_regionkey(#8.2), (.cardinality): 0.00 }
                        └── Select { .predicate: (n_name(#7.1) = GERMANY::utf8_view) OR (n_name(#7.1) = FRANCE::utf8_view), (.output_columns): n_comment(#7.3), n_name(#7.1), n_nationkey(#7.0), n_regionkey(#7.2), (.cardinality): 0.00 }
                            └── Get { .data_source_id: 1, .table_index: 7, .implementation: None, (.output_columns): n_comment(#7.3), n_name(#7.1), n_nationkey(#7.0), n_regionkey(#7.2), (.cardinality): 0.00 }

physical_plan after optd-finalized:
EnforcerSort { tuple_ordering: [(#13.0, Asc), (#13.1, Asc), (#13.2, Asc)], (.output_columns): cust_nation(#13.1), l_year(#13.2), revenue(#13.3), supp_nation(#13.0), (.cardinality): 0.00 }
└── Project { .table_index: 13, .projections: [ supp_nation(#10.0), cust_nation(#10.1), l_year(#10.2), sum(shipping.volume)(#12.0) ], (.output_columns): cust_nation(#13.1), l_year(#13.2), revenue(#13.3), supp_nation(#13.0), (.cardinality): 0.00 }
    └── Aggregate { .key_table_index: 11, .aggregate_table_index: 12, .implementation: None, .exprs: [ sum(volume(#10.3)) ], .keys: [ supp_nation(#10.0), cust_nation(#10.1), l_year(#10.2) ], (.output_columns): shipping.cust_nation(#11.1), shipping.l_year(#11.2), shipping.supp_nation(#11.0), sum(shipping.volume)(#12.0), (.cardinality): 0.00 }
        └── Remap { .table_index: 10, (.output_columns): cust_nation(#10.1), l_year(#10.2), supp_nation(#10.0), volume(#10.3), (.cardinality): 0.00 }
            └── Project { .table_index: 9, .projections: [ n_name(#6.1), n_name(#8.1), date_part(YEAR::utf8, l_shipdate(#2.10)), l_extendedprice(#2.5) * 1::decimal128(20, 0) - l_discount(#2.6) ], (.output_columns): cust_nation(#9.1), l_year(#9.2), supp_nation(#9.0), volume(#9.3), (.cardinality): 0.00 }
                └── Join
                    ├── .join_type: Inner
                    ├── .implementation: None
                    ├── .join_cond: (c_nationkey(#4.3) = n_nationkey(#8.0)) AND (((n_name(#6.1) = FRANCE::utf8_view) AND (n_name(#8.1) = GERMANY::utf8_view)) OR ((n_name(#6.1) = GERMANY::utf8_view) AND (n_name(#8.1) = FRANCE::utf8_view)))
                    ├── (.output_columns): c_custkey(#4.0), c_nationkey(#4.3), l_discount(#2.6), l_extendedprice(#2.5), l_orderkey(#2.0), l_shipdate(#2.10), l_suppkey(#2.2), n_comment(#6.3), n_comment(#8.3), n_name(#6.1), n_name(#8.1), n_nationkey(#6.0), n_nationkey(#8.0), n_regionkey(#6.2), n_regionkey(#8.2), o_custkey(#3.1), o_orderkey(#3.0), s_nationkey(#1.3), s_suppkey(#1.0)
                    ├── (.cardinality): 0.00
                    ├── Join
                    │   ├── .join_type: Inner
                    │   ├── .implementation: None
                    │   ├── .join_cond: s_nationkey(#1.3) = n_nationkey(#6.0)
                    │   ├── (.output_columns): c_custkey(#4.0), c_nationkey(#4.3), l_discount(#2.6), l_extendedprice(#2.5), l_orderkey(#2.0), l_shipdate(#2.10), l_suppkey(#2.2), n_comment(#6.3), n_name(#6.1), n_nationkey(#6.0), n_regionkey(#6.2), o_custkey(#3.1), o_orderkey(#3.0), s_nationkey(#1.3), s_suppkey(#1.0)
                    │   ├── (.cardinality): 0.00
                    │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: o_custkey(#3.1) = c_custkey(#4.0), (.output_columns): c_custkey(#4.0), c_nationkey(#4.3), l_discount(#2.6), l_extendedprice(#2.5), l_orderkey(#2.0), l_shipdate(#2.10), l_suppkey(#2.2), o_custkey(#3.1), o_orderkey(#3.0), s_nationkey(#1.3), s_suppkey(#1.0), (.cardinality): 0.00 }
                    │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: l_orderkey(#2.0) = o_orderkey(#3.0), (.output_columns): l_discount(#2.6), l_extendedprice(#2.5), l_orderkey(#2.0), l_shipdate(#2.10), l_suppkey(#2.2), o_custkey(#3.1), o_orderkey(#3.0), s_nationkey(#1.3), s_suppkey(#1.0), (.cardinality): 0.00 }
                    │   │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: s_suppkey(#1.0) = l_suppkey(#2.2), (.output_columns): l_discount(#2.6), l_extendedprice(#2.5), l_orderkey(#2.0), l_shipdate(#2.10), l_suppkey(#2.2), s_nationkey(#1.3), s_suppkey(#1.0), (.cardinality): 0.00 }
                    │   │   │   │   ├── Get { .data_source_id: 4, .table_index: 1, .implementation: None, (.output_columns): s_nationkey(#1.3), s_suppkey(#1.0), (.cardinality): 0.00 }
                    │   │   │   │   └── Select { .predicate: (l_shipdate(#2.10) >= 1995-01-01::date32) AND (l_shipdate(#2.10) <= 1996-12-31::date32), (.output_columns): l_discount(#2.6), l_extendedprice(#2.5), l_orderkey(#2.0), l_shipdate(#2.10), l_suppkey(#2.2), (.cardinality): 0.00 }
                    │   │   │   │       └── Get { .data_source_id: 8, .table_index: 2, .implementation: None, (.output_columns): l_discount(#2.6), l_extendedprice(#2.5), l_orderkey(#2.0), l_shipdate(#2.10), l_suppkey(#2.2), (.cardinality): 0.00 }
                    │   │   │   └── Get { .data_source_id: 7, .table_index: 3, .implementation: None, (.output_columns): o_custkey(#3.1), o_orderkey(#3.0), (.cardinality): 0.00 }
                    │   │   └── Get { .data_source_id: 6, .table_index: 4, .implementation: None, (.output_columns): c_custkey(#4.0), c_nationkey(#4.3), (.cardinality): 0.00 }
                    │   └── Remap { .table_index: 6, (.output_columns): n_comment(#6.3), n_name(#6.1), n_nationkey(#6.0), n_regionkey(#6.2), (.cardinality): 0.00 }
                    │       └── Select { .predicate: (n_name(#5.1) = FRANCE::utf8_view) OR (n_name(#5.1) = GERMANY::utf8_view), (.output_columns): n_comment(#5.3), n_name(#5.1), n_nationkey(#5.0), n_regionkey(#5.2), (.cardinality): 0.00 }
                    │           └── Get { .data_source_id: 1, .table_index: 5, .implementation: None, (.output_columns): n_comment(#5.3), n_name(#5.1), n_nationkey(#5.0), n_regionkey(#5.2), (.cardinality): 0.00 }
                    └── Remap { .table_index: 8, (.output_columns): n_comment(#8.3), n_name(#8.1), n_nationkey(#8.0), n_regionkey(#8.2), (.cardinality): 0.00 }
                        └── Select { .predicate: (n_name(#7.1) = GERMANY::utf8_view) OR (n_name(#7.1) = FRANCE::utf8_view), (.output_columns): n_comment(#7.3), n_name(#7.1), n_nationkey(#7.0), n_regionkey(#7.2), (.cardinality): 0.00 }
                            └── Get { .data_source_id: 1, .table_index: 7, .implementation: None, (.output_columns): n_comment(#7.3), n_name(#7.1), n_nationkey(#7.0), n_regionkey(#7.2), (.cardinality): 0.00 }
*/

