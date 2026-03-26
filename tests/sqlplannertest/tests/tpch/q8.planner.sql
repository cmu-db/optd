-- TPC-H Q8 without top-most limit node
select
    o_year,
    sum(case
        when nation = 'IRAQ' then volume
        else 0
    end) / sum(volume) as mkt_share
from
    (
        select
            extract(year from o_orderdate) as o_year,
            l_extendedprice * (1 - l_discount) as volume,
            n2.n_name as nation
        from
            part,
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2,
            region
        where
            p_partkey = l_partkey
            and s_suppkey = l_suppkey
            and l_orderkey = o_orderkey
            and o_custkey = c_custkey
            and c_nationkey = n1.n_nationkey
            and n1.n_regionkey = r_regionkey
            and r_name = 'AMERICA'
            and s_nationkey = n2.n_nationkey
            and o_orderdate between date '1995-01-01' and date '1996-12-31'
            and p_type = 'ECONOMY ANODIZED STEEL'
    ) as all_nations
group by
    o_year
order by
    o_year;

/*
logical_plan after optd-initial:
OrderBy { ordering_exprs: [ o_year(#16.0) ASC ], (.output_columns): mkt_share(#16.1), o_year(#16.0), (.cardinality): 0.00 }
└── Project { .table_index: 16, .projections: [ o_year(#13.0), sum(CASE WHEN all_nations.nation = Utf8("IRAQ") THEN all_nations.volume ELSE Int64(0) END)(#15.0) / sum(all_nations.volume)(#15.1) ], (.output_columns): mkt_share(#16.1), o_year(#16.0), (.cardinality): 0.00 }
    └── Aggregate { .key_table_index: 14, .aggregate_table_index: 15, .implementation: None, .exprs: [ sum(CASE WHEN nation(#13.2) = IRAQ::utf8_view THEN volume(#13.1) ELSE 0::decimal128(38, 4) END), sum(volume(#13.1)) ], .keys: [ o_year(#13.0) ], (.output_columns): all_nations.o_year(#14.0), sum(CASE WHEN all_nations.nation = Utf8("IRAQ") THEN all_nations.volume ELSE Int64(0) END)(#15.0), sum(all_nations.volume)(#15.1), (.cardinality): 0.00 }
        └── Remap { .table_index: 13, (.output_columns): nation(#13.2), o_year(#13.0), volume(#13.1), (.cardinality): 0.00 }
            └── Project { .table_index: 12, .projections: [ date_part(YEAR::utf8, o_orderdate(#11.36)), l_extendedprice(#11.21) * 1::decimal128(20, 0) - l_discount(#11.22), n_name(#11.50) ], (.output_columns): nation(#12.2), o_year(#12.0), volume(#12.1), (.cardinality): 0.00 }
                └── Project
                    ├── .table_index: 11
                    ├── .projections: [ p_partkey(#1.0), p_name(#1.1), p_mfgr(#1.2), p_brand(#1.3), p_type(#1.4), p_size(#1.5), p_container(#1.6), p_retailprice(#1.7), p_comment(#1.8), s_suppkey(#3.0), s_name(#3.1), s_address(#3.2), s_nationkey(#3.3), s_phone(#3.4), s_acctbal(#3.5), s_comment(#3.6), l_orderkey(#2.0), l_partkey(#2.1), l_suppkey(#2.2), l_linenumber(#2.3), l_quantity(#2.4), l_extendedprice(#2.5), l_discount(#2.6), l_tax(#2.7), l_returnflag(#2.8), l_linestatus(#2.9), l_shipdate(#2.10), l_commitdate(#2.11), l_receiptdate(#2.12), l_shipinstruct(#2.13), l_shipmode(#2.14), l_comment(#2.15), o_orderkey(#4.0), o_custkey(#4.1), o_orderstatus(#4.2), o_totalprice(#4.3), o_orderdate(#4.4), o_orderpriority(#4.5), o_clerk(#4.6), o_shippriority(#4.7), o_comment(#4.8), c_custkey(#5.0), c_name(#5.1), c_address(#5.2), c_nationkey(#5.3), c_phone(#5.4), c_acctbal(#5.5), c_mktsegment(#5.6), c_comment(#5.7), n_nationkey(#7.0), n_name(#7.1), n_regionkey(#7.2), n_comment(#7.3), n_nationkey(#9.0), n_name(#9.1), n_regionkey(#9.2), n_comment(#9.3), r_regionkey(#10.0), r_name(#10.1), r_comment(#10.2) ]
                    ├── (.output_columns): c_acctbal(#11.46), c_address(#11.43), c_comment(#11.48), c_custkey(#11.41), c_mktsegment(#11.47), c_name(#11.42), c_nationkey(#11.44), c_phone(#11.45), l_comment(#11.31), l_commitdate(#11.27), l_discount(#11.22), l_extendedprice(#11.21), l_linenumber(#11.19), l_linestatus(#11.25), l_orderkey(#11.16), l_partkey(#11.17), l_quantity(#11.20), l_receiptdate(#11.28), l_returnflag(#11.24), l_shipdate(#11.26), l_shipinstruct(#11.29), l_shipmode(#11.30), l_suppkey(#11.18), l_tax(#11.23), n_comment(#11.52), n_comment(#11.56), n_name(#11.50), n_name(#11.54), n_nationkey(#11.49), n_nationkey(#11.53), n_regionkey(#11.51), n_regionkey(#11.55), o_clerk(#11.38), o_comment(#11.40), o_custkey(#11.33), o_orderdate(#11.36), o_orderkey(#11.32), o_orderpriority(#11.37), o_orderstatus(#11.34), o_shippriority(#11.39), o_totalprice(#11.35), p_brand(#11.3), p_comment(#11.8), p_container(#11.6), p_mfgr(#11.2), p_name(#11.1), p_partkey(#11.0), p_retailprice(#11.7), p_size(#11.5), p_type(#11.4), r_comment(#11.59), r_name(#11.58), r_regionkey(#11.57), s_acctbal(#11.14), s_address(#11.11), s_comment(#11.15), s_name(#11.10), s_nationkey(#11.12), s_phone(#11.13), s_suppkey(#11.9)
                    ├── (.cardinality): 0.00
                    └── Join
                        ├── .join_type: Inner
                        ├── .implementation: None
                        ├── .join_cond: (n_regionkey(#7.2) = r_regionkey(#10.0))
                        ├── (.output_columns): c_acctbal(#5.5), c_address(#5.2), c_comment(#5.7), c_custkey(#5.0), c_mktsegment(#5.6), c_name(#5.1), c_nationkey(#5.3), c_phone(#5.4), l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7), n_comment(#7.3), n_comment(#9.3), n_name(#7.1), n_name(#9.1), n_nationkey(#7.0), n_nationkey(#9.0), n_regionkey(#7.2), n_regionkey(#9.2), o_clerk(#4.6), o_comment(#4.8), o_custkey(#4.1), o_orderdate(#4.4), o_orderkey(#4.0), o_orderpriority(#4.5), o_orderstatus(#4.2), o_shippriority(#4.7), o_totalprice(#4.3), p_brand(#1.3), p_comment(#1.8), p_container(#1.6), p_mfgr(#1.2), p_name(#1.1), p_partkey(#1.0), p_retailprice(#1.7), p_size(#1.5), p_type(#1.4), r_comment(#10.2), r_name(#10.1), r_regionkey(#10.0), s_acctbal(#3.5), s_address(#3.2), s_comment(#3.6), s_name(#3.1), s_nationkey(#3.3), s_phone(#3.4), s_suppkey(#3.0)
                        ├── (.cardinality): 0.00
                        ├── Join { .join_type: Inner, .implementation: None, .join_cond: (s_nationkey(#3.3) = n_nationkey(#9.0)), (.output_columns): c_acctbal(#5.5), c_address(#5.2), c_comment(#5.7), c_custkey(#5.0), c_mktsegment(#5.6), c_name(#5.1), c_nationkey(#5.3), c_phone(#5.4), l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7), n_comment(#7.3), n_comment(#9.3), n_name(#7.1), n_name(#9.1), n_nationkey(#7.0), n_nationkey(#9.0), n_regionkey(#7.2), n_regionkey(#9.2), o_clerk(#4.6), o_comment(#4.8), o_custkey(#4.1), o_orderdate(#4.4), o_orderkey(#4.0), o_orderpriority(#4.5), o_orderstatus(#4.2), o_shippriority(#4.7), o_totalprice(#4.3), p_brand(#1.3), p_comment(#1.8), p_container(#1.6), p_mfgr(#1.2), p_name(#1.1), p_partkey(#1.0), p_retailprice(#1.7), p_size(#1.5), p_type(#1.4), s_acctbal(#3.5), s_address(#3.2), s_comment(#3.6), s_name(#3.1), s_nationkey(#3.3), s_phone(#3.4), s_suppkey(#3.0), (.cardinality): 0.00 }
                        │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: (c_nationkey(#5.3) = n_nationkey(#7.0)), (.output_columns): c_acctbal(#5.5), c_address(#5.2), c_comment(#5.7), c_custkey(#5.0), c_mktsegment(#5.6), c_name(#5.1), c_nationkey(#5.3), c_phone(#5.4), l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7), n_comment(#7.3), n_name(#7.1), n_nationkey(#7.0), n_regionkey(#7.2), o_clerk(#4.6), o_comment(#4.8), o_custkey(#4.1), o_orderdate(#4.4), o_orderkey(#4.0), o_orderpriority(#4.5), o_orderstatus(#4.2), o_shippriority(#4.7), o_totalprice(#4.3), p_brand(#1.3), p_comment(#1.8), p_container(#1.6), p_mfgr(#1.2), p_name(#1.1), p_partkey(#1.0), p_retailprice(#1.7), p_size(#1.5), p_type(#1.4), s_acctbal(#3.5), s_address(#3.2), s_comment(#3.6), s_name(#3.1), s_nationkey(#3.3), s_phone(#3.4), s_suppkey(#3.0), (.cardinality): 0.00 }
                        │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: (o_custkey(#4.1) = c_custkey(#5.0)), (.output_columns): c_acctbal(#5.5), c_address(#5.2), c_comment(#5.7), c_custkey(#5.0), c_mktsegment(#5.6), c_name(#5.1), c_nationkey(#5.3), c_phone(#5.4), l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7), o_clerk(#4.6), o_comment(#4.8), o_custkey(#4.1), o_orderdate(#4.4), o_orderkey(#4.0), o_orderpriority(#4.5), o_orderstatus(#4.2), o_shippriority(#4.7), o_totalprice(#4.3), p_brand(#1.3), p_comment(#1.8), p_container(#1.6), p_mfgr(#1.2), p_name(#1.1), p_partkey(#1.0), p_retailprice(#1.7), p_size(#1.5), p_type(#1.4), s_acctbal(#3.5), s_address(#3.2), s_comment(#3.6), s_name(#3.1), s_nationkey(#3.3), s_phone(#3.4), s_suppkey(#3.0), (.cardinality): 0.00 }
                        │   │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: (l_orderkey(#2.0) = o_orderkey(#4.0)), (.output_columns): l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7), o_clerk(#4.6), o_comment(#4.8), o_custkey(#4.1), o_orderdate(#4.4), o_orderkey(#4.0), o_orderpriority(#4.5), o_orderstatus(#4.2), o_shippriority(#4.7), o_totalprice(#4.3), p_brand(#1.3), p_comment(#1.8), p_container(#1.6), p_mfgr(#1.2), p_name(#1.1), p_partkey(#1.0), p_retailprice(#1.7), p_size(#1.5), p_type(#1.4), s_acctbal(#3.5), s_address(#3.2), s_comment(#3.6), s_name(#3.1), s_nationkey(#3.3), s_phone(#3.4), s_suppkey(#3.0), (.cardinality): 0.00 }
                        │   │   │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: (l_suppkey(#2.2) = s_suppkey(#3.0)), (.output_columns): l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7), p_brand(#1.3), p_comment(#1.8), p_container(#1.6), p_mfgr(#1.2), p_name(#1.1), p_partkey(#1.0), p_retailprice(#1.7), p_size(#1.5), p_type(#1.4), s_acctbal(#3.5), s_address(#3.2), s_comment(#3.6), s_name(#3.1), s_nationkey(#3.3), s_phone(#3.4), s_suppkey(#3.0), (.cardinality): 0.00 }
                        │   │   │   │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: (p_partkey(#1.0) = l_partkey(#2.1)), (.output_columns): l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7), p_brand(#1.3), p_comment(#1.8), p_container(#1.6), p_mfgr(#1.2), p_name(#1.1), p_partkey(#1.0), p_retailprice(#1.7), p_size(#1.5), p_type(#1.4), (.cardinality): 0.00 }
                        │   │   │   │   │   │   ├── Select { .predicate: p_type(#1.4) = ECONOMY ANODIZED STEEL::utf8_view, (.output_columns): p_brand(#1.3), p_comment(#1.8), p_container(#1.6), p_mfgr(#1.2), p_name(#1.1), p_partkey(#1.0), p_retailprice(#1.7), p_size(#1.5), p_type(#1.4), (.cardinality): 0.00 }
                        │   │   │   │   │   │   │   └── Get { .data_source_id: 3, .table_index: 1, .implementation: None, (.output_columns): p_brand(#1.3), p_comment(#1.8), p_container(#1.6), p_mfgr(#1.2), p_name(#1.1), p_partkey(#1.0), p_retailprice(#1.7), p_size(#1.5), p_type(#1.4), (.cardinality): 0.00 }
                        │   │   │   │   │   │   └── Get { .data_source_id: 8, .table_index: 2, .implementation: None, (.output_columns): l_comment(#2.15), l_commitdate(#2.11), l_discount(#2.6), l_extendedprice(#2.5), l_linenumber(#2.3), l_linestatus(#2.9), l_orderkey(#2.0), l_partkey(#2.1), l_quantity(#2.4), l_receiptdate(#2.12), l_returnflag(#2.8), l_shipdate(#2.10), l_shipinstruct(#2.13), l_shipmode(#2.14), l_suppkey(#2.2), l_tax(#2.7), (.cardinality): 0.00 }
                        │   │   │   │   │   └── Get { .data_source_id: 4, .table_index: 3, .implementation: None, (.output_columns): s_acctbal(#3.5), s_address(#3.2), s_comment(#3.6), s_name(#3.1), s_nationkey(#3.3), s_phone(#3.4), s_suppkey(#3.0), (.cardinality): 0.00 }
                        │   │   │   │   └── Select { .predicate: (o_orderdate(#4.4) >= 1995-01-01::date32) AND (o_orderdate(#4.4) <= 1996-12-31::date32), (.output_columns): o_clerk(#4.6), o_comment(#4.8), o_custkey(#4.1), o_orderdate(#4.4), o_orderkey(#4.0), o_orderpriority(#4.5), o_orderstatus(#4.2), o_shippriority(#4.7), o_totalprice(#4.3), (.cardinality): 0.00 }
                        │   │   │   │       └── Get { .data_source_id: 7, .table_index: 4, .implementation: None, (.output_columns): o_clerk(#4.6), o_comment(#4.8), o_custkey(#4.1), o_orderdate(#4.4), o_orderkey(#4.0), o_orderpriority(#4.5), o_orderstatus(#4.2), o_shippriority(#4.7), o_totalprice(#4.3), (.cardinality): 0.00 }
                        │   │   │   └── Get { .data_source_id: 6, .table_index: 5, .implementation: None, (.output_columns): c_acctbal(#5.5), c_address(#5.2), c_comment(#5.7), c_custkey(#5.0), c_mktsegment(#5.6), c_name(#5.1), c_nationkey(#5.3), c_phone(#5.4), (.cardinality): 0.00 }
                        │   │   └── Remap { .table_index: 7, (.output_columns): n_comment(#7.3), n_name(#7.1), n_nationkey(#7.0), n_regionkey(#7.2), (.cardinality): 0.00 }
                        │   │       └── Get { .data_source_id: 1, .table_index: 6, .implementation: None, (.output_columns): n_comment(#6.3), n_name(#6.1), n_nationkey(#6.0), n_regionkey(#6.2), (.cardinality): 0.00 }
                        │   └── Remap { .table_index: 9, (.output_columns): n_comment(#9.3), n_name(#9.1), n_nationkey(#9.0), n_regionkey(#9.2), (.cardinality): 0.00 }
                        │       └── Get { .data_source_id: 1, .table_index: 8, .implementation: None, (.output_columns): n_comment(#8.3), n_name(#8.1), n_nationkey(#8.0), n_regionkey(#8.2), (.cardinality): 0.00 }
                        └── Select { .predicate: r_name(#10.1) = AMERICA::utf8_view, (.output_columns): r_comment(#10.2), r_name(#10.1), r_regionkey(#10.0), (.cardinality): 0.00 }
                            └── Get { .data_source_id: 2, .table_index: 10, .implementation: None, (.output_columns): r_comment(#10.2), r_name(#10.1), r_regionkey(#10.0), (.cardinality): 0.00 }

physical_plan after optd-finalized:
EnforcerSort { tuple_ordering: [(#16.0, Asc)], (.output_columns): mkt_share(#16.1), o_year(#16.0), (.cardinality): 0.00 }
└── Project { .table_index: 16, .projections: [ o_year(#13.0), sum(CASE WHEN all_nations.nation = Utf8("IRAQ") THEN all_nations.volume ELSE Int64(0) END)(#15.0) / sum(all_nations.volume)(#15.1) ], (.output_columns): mkt_share(#16.1), o_year(#16.0), (.cardinality): 0.00 }
    └── Aggregate { .key_table_index: 14, .aggregate_table_index: 15, .implementation: None, .exprs: [ sum(CASE WHEN nation(#13.2) = IRAQ::utf8_view THEN volume(#13.1) ELSE 0::decimal128(38, 4) END), sum(volume(#13.1)) ], .keys: [ o_year(#13.0) ], (.output_columns): all_nations.o_year(#14.0), sum(CASE WHEN all_nations.nation = Utf8("IRAQ") THEN all_nations.volume ELSE Int64(0) END)(#15.0), sum(all_nations.volume)(#15.1), (.cardinality): 0.00 }
        └── Remap { .table_index: 13, (.output_columns): nation(#13.2), o_year(#13.0), volume(#13.1), (.cardinality): 0.00 }
            └── Project { .table_index: 12, .projections: [ date_part(YEAR::utf8, o_orderdate(#4.4)), l_extendedprice(#2.5) * 1::decimal128(20, 0) - l_discount(#2.6), n_name(#7.1) ], (.output_columns): nation(#12.2), o_year(#12.0), volume(#12.1), (.cardinality): 0.00 }
                └── Join
                    ├── .join_type: Inner
                    ├── .implementation: None
                    ├── .join_cond: n_regionkey(#7.2) = r_regionkey(#10.0)
                    ├── (.output_columns): c_custkey(#5.0), c_nationkey(#5.3), l_discount(#2.6), l_extendedprice(#2.5), l_orderkey(#2.0), l_partkey(#2.1), l_suppkey(#2.2), n_comment(#7.3), n_comment(#9.3), n_name(#7.1), n_name(#9.1), n_nationkey(#7.0), n_nationkey(#9.0), n_regionkey(#7.2), n_regionkey(#9.2), o_custkey(#4.1), o_orderdate(#4.4), o_orderkey(#4.0), p_partkey(#1.0), p_type(#1.4), r_name(#10.1), r_regionkey(#10.0), s_nationkey(#3.3), s_suppkey(#3.0)
                    ├── (.cardinality): 0.00
                    ├── Join
                    │   ├── .join_type: Inner
                    │   ├── .implementation: None
                    │   ├── .join_cond: s_nationkey(#3.3) = n_nationkey(#9.0)
                    │   ├── (.output_columns): c_custkey(#5.0), c_nationkey(#5.3), l_discount(#2.6), l_extendedprice(#2.5), l_orderkey(#2.0), l_partkey(#2.1), l_suppkey(#2.2), n_comment(#7.3), n_comment(#9.3), n_name(#7.1), n_name(#9.1), n_nationkey(#7.0), n_nationkey(#9.0), n_regionkey(#7.2), n_regionkey(#9.2), o_custkey(#4.1), o_orderdate(#4.4), o_orderkey(#4.0), p_partkey(#1.0), p_type(#1.4), s_nationkey(#3.3), s_suppkey(#3.0)
                    │   ├── (.cardinality): 0.00
                    │   ├── Join
                    │   │   ├── .join_type: Inner
                    │   │   ├── .implementation: None
                    │   │   ├── .join_cond: c_nationkey(#5.3) = n_nationkey(#7.0)
                    │   │   ├── (.output_columns): c_custkey(#5.0), c_nationkey(#5.3), l_discount(#2.6), l_extendedprice(#2.5), l_orderkey(#2.0), l_partkey(#2.1), l_suppkey(#2.2), n_comment(#7.3), n_name(#7.1), n_nationkey(#7.0), n_regionkey(#7.2), o_custkey(#4.1), o_orderdate(#4.4), o_orderkey(#4.0), p_partkey(#1.0), p_type(#1.4), s_nationkey(#3.3), s_suppkey(#3.0)
                    │   │   ├── (.cardinality): 0.00
                    │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: o_custkey(#4.1) = c_custkey(#5.0), (.output_columns): c_custkey(#5.0), c_nationkey(#5.3), l_discount(#2.6), l_extendedprice(#2.5), l_orderkey(#2.0), l_partkey(#2.1), l_suppkey(#2.2), o_custkey(#4.1), o_orderdate(#4.4), o_orderkey(#4.0), p_partkey(#1.0), p_type(#1.4), s_nationkey(#3.3), s_suppkey(#3.0), (.cardinality): 0.00 }
                    │   │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: l_orderkey(#2.0) = o_orderkey(#4.0), (.output_columns): l_discount(#2.6), l_extendedprice(#2.5), l_orderkey(#2.0), l_partkey(#2.1), l_suppkey(#2.2), o_custkey(#4.1), o_orderdate(#4.4), o_orderkey(#4.0), p_partkey(#1.0), p_type(#1.4), s_nationkey(#3.3), s_suppkey(#3.0), (.cardinality): 0.00 }
                    │   │   │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: l_suppkey(#2.2) = s_suppkey(#3.0), (.output_columns): l_discount(#2.6), l_extendedprice(#2.5), l_orderkey(#2.0), l_partkey(#2.1), l_suppkey(#2.2), p_partkey(#1.0), p_type(#1.4), s_nationkey(#3.3), s_suppkey(#3.0), (.cardinality): 0.00 }
                    │   │   │   │   │   ├── Join { .join_type: Inner, .implementation: None, .join_cond: p_partkey(#1.0) = l_partkey(#2.1), (.output_columns): l_discount(#2.6), l_extendedprice(#2.5), l_orderkey(#2.0), l_partkey(#2.1), l_suppkey(#2.2), p_partkey(#1.0), p_type(#1.4), (.cardinality): 0.00 }
                    │   │   │   │   │   │   ├── Select { .predicate: p_type(#1.4) = ECONOMY ANODIZED STEEL::utf8_view, (.output_columns): p_partkey(#1.0), p_type(#1.4), (.cardinality): 0.00 }
                    │   │   │   │   │   │   │   └── Get { .data_source_id: 3, .table_index: 1, .implementation: None, (.output_columns): p_partkey(#1.0), p_type(#1.4), (.cardinality): 0.00 }
                    │   │   │   │   │   │   └── Get { .data_source_id: 8, .table_index: 2, .implementation: None, (.output_columns): l_discount(#2.6), l_extendedprice(#2.5), l_orderkey(#2.0), l_partkey(#2.1), l_suppkey(#2.2), (.cardinality): 0.00 }
                    │   │   │   │   │   └── Get { .data_source_id: 4, .table_index: 3, .implementation: None, (.output_columns): s_nationkey(#3.3), s_suppkey(#3.0), (.cardinality): 0.00 }
                    │   │   │   │   └── Select { .predicate: (o_orderdate(#4.4) >= 1995-01-01::date32) AND (o_orderdate(#4.4) <= 1996-12-31::date32), (.output_columns): o_custkey(#4.1), o_orderdate(#4.4), o_orderkey(#4.0), (.cardinality): 0.00 }
                    │   │   │   │       └── Get { .data_source_id: 7, .table_index: 4, .implementation: None, (.output_columns): o_custkey(#4.1), o_orderdate(#4.4), o_orderkey(#4.0), (.cardinality): 0.00 }
                    │   │   │   └── Get { .data_source_id: 6, .table_index: 5, .implementation: None, (.output_columns): c_custkey(#5.0), c_nationkey(#5.3), (.cardinality): 0.00 }
                    │   │   └── Remap { .table_index: 7, (.output_columns): n_comment(#7.3), n_name(#7.1), n_nationkey(#7.0), n_regionkey(#7.2), (.cardinality): 0.00 }
                    │   │       └── Get { .data_source_id: 1, .table_index: 6, .implementation: None, (.output_columns): n_comment(#6.3), n_name(#6.1), n_nationkey(#6.0), n_regionkey(#6.2), (.cardinality): 0.00 }
                    │   └── Remap { .table_index: 9, (.output_columns): n_comment(#9.3), n_name(#9.1), n_nationkey(#9.0), n_regionkey(#9.2), (.cardinality): 0.00 }
                    │       └── Get { .data_source_id: 1, .table_index: 8, .implementation: None, (.output_columns): n_comment(#8.3), n_name(#8.1), n_nationkey(#8.0), n_regionkey(#8.2), (.cardinality): 0.00 }
                    └── Select { .predicate: r_name(#10.1) = AMERICA::utf8_view, (.output_columns): r_name(#10.1), r_regionkey(#10.0), (.cardinality): 0.00 }
                        └── Get { .data_source_id: 2, .table_index: 10, .implementation: None, (.output_columns): r_name(#10.1), r_regionkey(#10.0), (.cardinality): 0.00 }
*/

