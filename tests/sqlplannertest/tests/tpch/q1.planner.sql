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
Limit
├── .skip: 0::bigint
├── .fetch: 3::bigint
├── (.output_columns):
│   ┌── "__#4.avg_disc"(#4.8)
│   ├── "__#4.avg_price"(#4.7)
│   ├── "__#4.avg_qty"(#4.6)
│   ├── "__#4.count_order"(#4.9)
│   ├── "__#4.l_linestatus"(#4.1)
│   ├── "__#4.l_returnflag"(#4.0)
│   ├── "__#4.sum_base_price"(#4.3)
│   ├── "__#4.sum_charge"(#4.5)
│   ├── "__#4.sum_disc_price"(#4.4)
│   └── "__#4.sum_qty"(#4.2)
├── (.cardinality): 0.00
└── OrderBy
    ├── ordering_exprs: [ "__#4.l_returnflag"(#4.0) ASC, "__#4.l_linestatus"(#4.1) ASC ]
    ├── (.output_columns):
    │   ┌── "__#4.avg_disc"(#4.8)
    │   ├── "__#4.avg_price"(#4.7)
    │   ├── "__#4.avg_qty"(#4.6)
    │   ├── "__#4.count_order"(#4.9)
    │   ├── "__#4.l_linestatus"(#4.1)
    │   ├── "__#4.l_returnflag"(#4.0)
    │   ├── "__#4.sum_base_price"(#4.3)
    │   ├── "__#4.sum_charge"(#4.5)
    │   ├── "__#4.sum_disc_price"(#4.4)
    │   └── "__#4.sum_qty"(#4.2)
    ├── (.cardinality): 0.00
    └── Project
        ├── .table_index: 4
        ├── .projections:
        │   ┌── "lineitem.l_returnflag"(#1.8)
        │   ├── "lineitem.l_linestatus"(#1.9)
        │   ├── "__#3.sum(lineitem.l_quantity)"(#3.0)
        │   ├── "__#3.sum(lineitem.l_extendedprice)"(#3.1)
        │   ├── "__#3.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)"(#3.2)
        │   ├── "__#3.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount * Int64(1) + lineitem.l_tax)"(#3.3)
        │   ├── "__#3.avg(lineitem.l_quantity)"(#3.4)
        │   ├── "__#3.avg(lineitem.l_extendedprice)"(#3.5)
        │   ├── "__#3.avg(lineitem.l_discount)"(#3.6)
        │   └── "__#3.count(Int64(1))"(#3.7)
        ├── (.output_columns):
        │   ┌── "__#4.avg_disc"(#4.8)
        │   ├── "__#4.avg_price"(#4.7)
        │   ├── "__#4.avg_qty"(#4.6)
        │   ├── "__#4.count_order"(#4.9)
        │   ├── "__#4.l_linestatus"(#4.1)
        │   ├── "__#4.l_returnflag"(#4.0)
        │   ├── "__#4.sum_base_price"(#4.3)
        │   ├── "__#4.sum_charge"(#4.5)
        │   ├── "__#4.sum_disc_price"(#4.4)
        │   └── "__#4.sum_qty"(#4.2)
        ├── (.cardinality): 0.00
        └── Aggregate
            ├── .key_table_index: 2
            ├── .aggregate_table_index: 3
            ├── .implementation: None
            ├── .exprs:
            │   ┌── sum("lineitem.l_quantity"(#1.4))
            │   ├── sum("lineitem.l_extendedprice"(#1.5))
            │   ├── sum("lineitem.l_extendedprice"(#1.5) * CAST (1::bigint AS Decimal128(20, 0)) - "lineitem.l_discount"(#1.6))
            │   ├── sum("lineitem.l_extendedprice"(#1.5) * CAST (1::bigint AS Decimal128(20, 0)) - "lineitem.l_discount"(#1.6) * CAST (1::bigint AS Decimal128(20, 0)) + "lineitem.l_tax"(#1.7))
            │   ├── avg("lineitem.l_quantity"(#1.4))
            │   ├── avg("lineitem.l_extendedprice"(#1.5))
            │   ├── avg("lineitem.l_discount"(#1.6))
            │   └── count(1::bigint)
            ├── .keys: [ "lineitem.l_returnflag"(#1.8), "lineitem.l_linestatus"(#1.9) ]
            ├── (.output_columns):
            │   ┌── "__#2.l_linestatus"(#2.1)
            │   ├── "__#2.l_returnflag"(#2.0)
            │   ├── "__#3.avg(lineitem.l_discount)"(#3.6)
            │   ├── "__#3.avg(lineitem.l_extendedprice)"(#3.5)
            │   ├── "__#3.avg(lineitem.l_quantity)"(#3.4)
            │   ├── "__#3.count(Int64(1))"(#3.7)
            │   ├── "__#3.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount * Int64(1) + lineitem.l_tax)"(#3.3)
            │   ├── "__#3.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)"(#3.2)
            │   ├── "__#3.sum(lineitem.l_extendedprice)"(#3.1)
            │   └── "__#3.sum(lineitem.l_quantity)"(#3.0)
            ├── (.cardinality): 0.00
            └── Select
                ├── .predicate: "lineitem.l_shipdate"(#1.10) <= CAST ('1998-12-01'::utf8 AS Date32) - IntervalMonthDayNano { months: 0, days: 90, nanoseconds: 0 }::interval_month_day_nano
                ├── (.output_columns):
                │   ┌── "lineitem.l_comment"(#1.15)
                │   ├── "lineitem.l_commitdate"(#1.11)
                │   ├── "lineitem.l_discount"(#1.6)
                │   ├── "lineitem.l_extendedprice"(#1.5)
                │   ├── "lineitem.l_linenumber"(#1.3)
                │   ├── "lineitem.l_linestatus"(#1.9)
                │   ├── "lineitem.l_orderkey"(#1.0)
                │   ├── "lineitem.l_partkey"(#1.1)
                │   ├── "lineitem.l_quantity"(#1.4)
                │   ├── "lineitem.l_receiptdate"(#1.12)
                │   ├── "lineitem.l_returnflag"(#1.8)
                │   ├── "lineitem.l_shipdate"(#1.10)
                │   ├── "lineitem.l_shipinstruct"(#1.13)
                │   ├── "lineitem.l_shipmode"(#1.14)
                │   ├── "lineitem.l_suppkey"(#1.2)
                │   └── "lineitem.l_tax"(#1.7)
                ├── (.cardinality): 0.00
                └── Get
                    ├── .data_source_id: 8
                    ├── .table_index: 1
                    ├── .implementation: None
                    ├── (.output_columns):
                    │   ┌── "lineitem.l_comment"(#1.15)
                    │   ├── "lineitem.l_commitdate"(#1.11)
                    │   ├── "lineitem.l_discount"(#1.6)
                    │   ├── "lineitem.l_extendedprice"(#1.5)
                    │   ├── "lineitem.l_linenumber"(#1.3)
                    │   ├── "lineitem.l_linestatus"(#1.9)
                    │   ├── "lineitem.l_orderkey"(#1.0)
                    │   ├── "lineitem.l_partkey"(#1.1)
                    │   ├── "lineitem.l_quantity"(#1.4)
                    │   ├── "lineitem.l_receiptdate"(#1.12)
                    │   ├── "lineitem.l_returnflag"(#1.8)
                    │   ├── "lineitem.l_shipdate"(#1.10)
                    │   ├── "lineitem.l_shipinstruct"(#1.13)
                    │   ├── "lineitem.l_shipmode"(#1.14)
                    │   ├── "lineitem.l_suppkey"(#1.2)
                    │   └── "lineitem.l_tax"(#1.7)
                    └── (.cardinality): 0.00

physical_plan after optd-finalized:
Limit
├── .skip: 0::bigint
├── .fetch: 3::bigint
├── (.output_columns):
│   ┌── "__#4.avg_disc"(#4.8)
│   ├── "__#4.avg_price"(#4.7)
│   ├── "__#4.avg_qty"(#4.6)
│   ├── "__#4.count_order"(#4.9)
│   ├── "__#4.l_linestatus"(#4.1)
│   ├── "__#4.l_returnflag"(#4.0)
│   ├── "__#4.sum_base_price"(#4.3)
│   ├── "__#4.sum_charge"(#4.5)
│   ├── "__#4.sum_disc_price"(#4.4)
│   └── "__#4.sum_qty"(#4.2)
├── (.cardinality): 0.00
└── EnforcerSort
    ├── tuple_ordering: [(#4.0, Asc), (#4.1, Asc)]
    ├── (.output_columns):
    │   ┌── "__#4.avg_disc"(#4.8)
    │   ├── "__#4.avg_price"(#4.7)
    │   ├── "__#4.avg_qty"(#4.6)
    │   ├── "__#4.count_order"(#4.9)
    │   ├── "__#4.l_linestatus"(#4.1)
    │   ├── "__#4.l_returnflag"(#4.0)
    │   ├── "__#4.sum_base_price"(#4.3)
    │   ├── "__#4.sum_charge"(#4.5)
    │   ├── "__#4.sum_disc_price"(#4.4)
    │   └── "__#4.sum_qty"(#4.2)
    ├── (.cardinality): 0.00
    └── Project
        ├── .table_index: 4
        ├── .projections:
        │   ┌── "lineitem.l_returnflag"(#1.8)
        │   ├── "lineitem.l_linestatus"(#1.9)
        │   ├── "__#3.sum(lineitem.l_quantity)"(#3.0)
        │   ├── "__#3.sum(lineitem.l_extendedprice)"(#3.1)
        │   ├── "__#3.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)"(#3.2)
        │   ├── "__#3.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount * Int64(1) + lineitem.l_tax)"(#3.3)
        │   ├── "__#3.avg(lineitem.l_quantity)"(#3.4)
        │   ├── "__#3.avg(lineitem.l_extendedprice)"(#3.5)
        │   ├── "__#3.avg(lineitem.l_discount)"(#3.6)
        │   └── "__#3.count(Int64(1))"(#3.7)
        ├── (.output_columns):
        │   ┌── "__#4.avg_disc"(#4.8)
        │   ├── "__#4.avg_price"(#4.7)
        │   ├── "__#4.avg_qty"(#4.6)
        │   ├── "__#4.count_order"(#4.9)
        │   ├── "__#4.l_linestatus"(#4.1)
        │   ├── "__#4.l_returnflag"(#4.0)
        │   ├── "__#4.sum_base_price"(#4.3)
        │   ├── "__#4.sum_charge"(#4.5)
        │   ├── "__#4.sum_disc_price"(#4.4)
        │   └── "__#4.sum_qty"(#4.2)
        ├── (.cardinality): 0.00
        └── Aggregate
            ├── .key_table_index: 2
            ├── .aggregate_table_index: 3
            ├── .implementation: None
            ├── .exprs:
            │   ┌── sum("lineitem.l_quantity"(#1.4))
            │   ├── sum("lineitem.l_extendedprice"(#1.5))
            │   ├── sum("lineitem.l_extendedprice"(#1.5) * 1::decimal128(20, 0) - "lineitem.l_discount"(#1.6))
            │   ├── sum("lineitem.l_extendedprice"(#1.5) * 1::decimal128(20, 0) - "lineitem.l_discount"(#1.6) * 1::decimal128(20, 0) + "lineitem.l_tax"(#1.7))
            │   ├── avg("lineitem.l_quantity"(#1.4))
            │   ├── avg("lineitem.l_extendedprice"(#1.5))
            │   ├── avg("lineitem.l_discount"(#1.6))
            │   └── count(1::bigint)
            ├── .keys: [ "lineitem.l_returnflag"(#1.8), "lineitem.l_linestatus"(#1.9) ]
            ├── (.output_columns):
            │   ┌── "__#2.l_linestatus"(#2.1)
            │   ├── "__#2.l_returnflag"(#2.0)
            │   ├── "__#3.avg(lineitem.l_discount)"(#3.6)
            │   ├── "__#3.avg(lineitem.l_extendedprice)"(#3.5)
            │   ├── "__#3.avg(lineitem.l_quantity)"(#3.4)
            │   ├── "__#3.count(Int64(1))"(#3.7)
            │   ├── "__#3.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount * Int64(1) + lineitem.l_tax)"(#3.3)
            │   ├── "__#3.sum(lineitem.l_extendedprice * Int64(1) - lineitem.l_discount)"(#3.2)
            │   ├── "__#3.sum(lineitem.l_extendedprice)"(#3.1)
            │   └── "__#3.sum(lineitem.l_quantity)"(#3.0)
            ├── (.cardinality): 0.00
            └── Select
                ├── .predicate: "lineitem.l_shipdate"(#1.10) <= 1998-09-02::date32
                ├── (.output_columns):
                │   ┌── "lineitem.l_discount"(#1.6)
                │   ├── "lineitem.l_extendedprice"(#1.5)
                │   ├── "lineitem.l_linestatus"(#1.9)
                │   ├── "lineitem.l_quantity"(#1.4)
                │   ├── "lineitem.l_returnflag"(#1.8)
                │   ├── "lineitem.l_shipdate"(#1.10)
                │   └── "lineitem.l_tax"(#1.7)
                ├── (.cardinality): 0.00
                └── Get
                    ├── .data_source_id: 8
                    ├── .table_index: 1
                    ├── .implementation: None
                    ├── (.output_columns):
                    │   ┌── "lineitem.l_discount"(#1.6)
                    │   ├── "lineitem.l_extendedprice"(#1.5)
                    │   ├── "lineitem.l_linestatus"(#1.9)
                    │   ├── "lineitem.l_quantity"(#1.4)
                    │   ├── "lineitem.l_returnflag"(#1.8)
                    │   ├── "lineitem.l_shipdate"(#1.10)
                    │   └── "lineitem.l_tax"(#1.7)
                    └── (.cardinality): 0.00
*/

