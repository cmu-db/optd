-- TPC-H Q1
SELECT
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
FROM
    lineitem
WHERE
    l_shipdate <= date '1998-12-01' - interval '90' day
GROUP BY
    l_returnflag, l_linestatus
ORDER BY
    l_returnflag, l_linestatus
LIMIT 3;

/*
logical_plan after optd-initial:
Limit { .skip: 0::bigint, .fetch: 3::bigint, (.output_columns): avg_disc(#4.8), avg_price(#4.7), avg_qty(#4.6), count_order(#4.9), l_linestatus(#4.1), l_returnflag(#4.0), sum_base_price(#4.3), sum_charge(#4.5), sum_disc_price(#4.4), sum_qty(#4.2), (.cardinality): 0.04 }
└── OrderBy { ordering_exprs: [ l_returnflag(#4.0) ASC, l_linestatus(#4.1) ASC ], (.output_columns): avg_disc(#4.8), avg_price(#4.7), avg_qty(#4.6), count_order(#4.9), l_linestatus(#4.1), l_returnflag(#4.0), sum_base_price(#4.3), sum_charge(#4.5), sum_disc_price(#4.4), sum_qty(#4.2), (.cardinality): 0.04 }
    └── Project
        ├── .table_index: 4
        ├── .projections: [ l_returnflag(#2.9), l_linestatus(#2.10), sum(lineitem.l_quantity)(#3.0), sum(lineitem.l_extendedprice)(#3.1), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#3.2), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount * Int64(1) + lineitem.l_tax)(#3.3), avg(lineitem.l_quantity)(#3.4), avg(lineitem.l_extendedprice)(#3.5), avg(lineitem.l_discount)(#3.6), count(Int64(1))(#3.7) ]
        ├── (.output_columns): avg_disc(#4.8), avg_price(#4.7), avg_qty(#4.6), count_order(#4.9), l_linestatus(#4.1), l_returnflag(#4.0), sum_base_price(#4.3), sum_charge(#4.5), sum_disc_price(#4.4), sum_qty(#4.2)
        ├── (.cardinality): 0.04
        └── Aggregate
            ├── .aggregate_table_index: 3
            ├── .implementation: None
            ├── .exprs: [ sum(l_quantity(#2.5)), sum(l_extendedprice(#2.6)), sum(__common_expr_1(#2.0)), sum(__common_expr_1(#2.0) * 1::decimal128(20, 0) + l_tax(#2.8)), avg(l_quantity(#2.5)), avg(l_extendedprice(#2.6)), avg(l_discount(#2.7)), count(1::bigint) ]
            ├── .keys: [ l_returnflag(#2.9), l_linestatus(#2.10) ]
            ├── (.output_columns): avg(lineitem.l_discount)(#3.6), avg(lineitem.l_extendedprice)(#3.5), avg(lineitem.l_quantity)(#3.4), count(Int64(1))(#3.7), l_linestatus(#2.10), l_returnflag(#2.9), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount * Int64(1) + lineitem.l_tax)(#3.3), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#3.2), sum(lineitem.l_extendedprice)(#3.1), sum(lineitem.l_quantity)(#3.0)
            ├── (.cardinality): 0.04
            └── Project
                ├── .table_index: 2
                ├── .projections: [ l_extendedprice(#1.5) * 1::decimal128(20, 0) - l_discount(#1.6), l_orderkey(#1.0), l_partkey(#1.1), l_suppkey(#1.2), l_linenumber(#1.3), l_quantity(#1.4), l_extendedprice(#1.5), l_discount(#1.6), l_tax(#1.7), l_returnflag(#1.8), l_linestatus(#1.9), l_shipdate(#1.10), l_commitdate(#1.11), l_receiptdate(#1.12), l_shipinstruct(#1.13), l_shipmode(#1.14), l_comment(#1.15) ]
                ├── (.output_columns): __common_expr_1(#2.0), l_comment(#2.16), l_commitdate(#2.12), l_discount(#2.7), l_extendedprice(#2.6), l_linenumber(#2.4), l_linestatus(#2.10), l_orderkey(#2.1), l_partkey(#2.2), l_quantity(#2.5), l_receiptdate(#2.13), l_returnflag(#2.9), l_shipdate(#2.11), l_shipinstruct(#2.14), l_shipmode(#2.15), l_suppkey(#2.3), l_tax(#2.8)
                ├── (.cardinality): 0.00
                └── Select { .predicate: l_shipdate(#1.10) <= 1998-09-02::date32, (.output_columns): l_comment(#1.15), l_commitdate(#1.11), l_discount(#1.6), l_extendedprice(#1.5), l_linenumber(#1.3), l_linestatus(#1.9), l_orderkey(#1.0), l_partkey(#1.1), l_quantity(#1.4), l_receiptdate(#1.12), l_returnflag(#1.8), l_shipdate(#1.10), l_shipinstruct(#1.13), l_shipmode(#1.14), l_suppkey(#1.2), l_tax(#1.7), (.cardinality): 0.00 }
                    └── Get
                        ├── .data_source_id: 8
                        ├── .table_index: 1
                        ├── .implementation: None
                        ├── (.output_columns): l_comment(#1.15), l_commitdate(#1.11), l_discount(#1.6), l_extendedprice(#1.5), l_linenumber(#1.3), l_linestatus(#1.9), l_orderkey(#1.0), l_partkey(#1.1), l_quantity(#1.4), l_receiptdate(#1.12), l_returnflag(#1.8), l_shipdate(#1.10), l_shipinstruct(#1.13), l_shipmode(#1.14), l_suppkey(#1.2), l_tax(#1.7)
                        └── (.cardinality): 0.00

physical_plan after optd-finalized:
Limit { .skip: 0::bigint, .fetch: 3::bigint, (.output_columns): avg_disc(#4.8), avg_price(#4.7), avg_qty(#4.6), count_order(#4.9), l_linestatus(#4.1), l_returnflag(#4.0), sum_base_price(#4.3), sum_charge(#4.5), sum_disc_price(#4.4), sum_qty(#4.2), (.cardinality): 0.04 }
└── EnforcerSort { tuple_ordering: [(#4.0, Asc), (#4.1, Asc)], (.output_columns): avg_disc(#4.8), avg_price(#4.7), avg_qty(#4.6), count_order(#4.9), l_linestatus(#4.1), l_returnflag(#4.0), sum_base_price(#4.3), sum_charge(#4.5), sum_disc_price(#4.4), sum_qty(#4.2), (.cardinality): 0.04 }
    └── Project
        ├── .table_index: 4
        ├── .projections: [ l_returnflag(#2.9), l_linestatus(#2.10), sum(lineitem.l_quantity)(#3.0), sum(lineitem.l_extendedprice)(#3.1), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#3.2), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount * Int64(1) + lineitem.l_tax)(#3.3), avg(lineitem.l_quantity)(#3.4), avg(lineitem.l_extendedprice)(#3.5), avg(lineitem.l_discount)(#3.6), count(Int64(1))(#3.7) ]
        ├── (.output_columns): avg_disc(#4.8), avg_price(#4.7), avg_qty(#4.6), count_order(#4.9), l_linestatus(#4.1), l_returnflag(#4.0), sum_base_price(#4.3), sum_charge(#4.5), sum_disc_price(#4.4), sum_qty(#4.2)
        ├── (.cardinality): 0.04
        └── Aggregate
            ├── .aggregate_table_index: 3
            ├── .implementation: None
            ├── .exprs: [ sum(l_quantity(#2.5)), sum(l_extendedprice(#2.6)), sum(__common_expr_1(#2.0)), sum(__common_expr_1(#2.0) * 1::decimal128(20, 0) + l_tax(#2.8)), avg(l_quantity(#2.5)), avg(l_extendedprice(#2.6)), avg(l_discount(#2.7)), count(1::bigint) ]
            ├── .keys: [ l_returnflag(#2.9), l_linestatus(#2.10) ]
            ├── (.output_columns): avg(lineitem.l_discount)(#3.6), avg(lineitem.l_extendedprice)(#3.5), avg(lineitem.l_quantity)(#3.4), count(Int64(1))(#3.7), l_linestatus(#2.10), l_returnflag(#2.9), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount * Int64(1) + lineitem.l_tax)(#3.3), sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)(#3.2), sum(lineitem.l_extendedprice)(#3.1), sum(lineitem.l_quantity)(#3.0)
            ├── (.cardinality): 0.04
            └── Project
                ├── .table_index: 2
                ├── .projections: [ l_extendedprice(#1.5) * 1::decimal128(20, 0) - l_discount(#1.6), l_orderkey(#1.0), l_partkey(#1.1), l_suppkey(#1.2), l_linenumber(#1.3), l_quantity(#1.4), l_extendedprice(#1.5), l_discount(#1.6), l_tax(#1.7), l_returnflag(#1.8), l_linestatus(#1.9), l_shipdate(#1.10), l_commitdate(#1.11), l_receiptdate(#1.12), l_shipinstruct(#1.13), l_shipmode(#1.14), l_comment(#1.15) ]
                ├── (.output_columns): __common_expr_1(#2.0), l_comment(#2.16), l_commitdate(#2.12), l_discount(#2.7), l_extendedprice(#2.6), l_linenumber(#2.4), l_linestatus(#2.10), l_orderkey(#2.1), l_partkey(#2.2), l_quantity(#2.5), l_receiptdate(#2.13), l_returnflag(#2.9), l_shipdate(#2.11), l_shipinstruct(#2.14), l_shipmode(#2.15), l_suppkey(#2.3), l_tax(#2.8)
                ├── (.cardinality): 0.00
                └── Select { .predicate: l_shipdate(#1.10) <= 1998-09-02::date32, (.output_columns): l_comment(#1.15), l_commitdate(#1.11), l_discount(#1.6), l_extendedprice(#1.5), l_linenumber(#1.3), l_linestatus(#1.9), l_orderkey(#1.0), l_partkey(#1.1), l_quantity(#1.4), l_receiptdate(#1.12), l_returnflag(#1.8), l_shipdate(#1.10), l_shipinstruct(#1.13), l_shipmode(#1.14), l_suppkey(#1.2), l_tax(#1.7), (.cardinality): 0.00 }
                    └── Get
                        ├── .data_source_id: 8
                        ├── .table_index: 1
                        ├── .implementation: None
                        ├── (.output_columns): l_comment(#1.15), l_commitdate(#1.11), l_discount(#1.6), l_extendedprice(#1.5), l_linenumber(#1.3), l_linestatus(#1.9), l_orderkey(#1.0), l_partkey(#1.1), l_quantity(#1.4), l_receiptdate(#1.12), l_returnflag(#1.8), l_shipdate(#1.10), l_shipinstruct(#1.13), l_shipmode(#1.14), l_suppkey(#1.2), l_tax(#1.7)
                        └── (.cardinality): 0.00
*/

