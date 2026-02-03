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
+-------------------------------------------------------------------------------------------------------------------------------------------+
| LogicalOrderBy                                                                                                                            |
| ├── ordering_exprs: [ lineitem.l_returnflag(v#8) ASC, lineitem.l_linestatus(v#9) ASC ]                                                    |
| ├── (.output_columns): {v#8, v#9, v#17, v#18, v#19, v#20, v#21, v#22, v#23, v#24}                                                         |
| ├── (.cardinality): 0.04                                                                                                                  |
| └── LogicalProject                                                                                                                        |
|     ├── .projections:                                                                                                                     |
|     │   ┌── lineitem.l_returnflag(v#8) := lineitem.l_returnflag(v#8)                                                                      |
|     │   ├── lineitem.l_linestatus(v#9) := lineitem.l_linestatus(v#9)                                                                      |
|     │   ├── sum_qty(v#17) := sum_qty(v#17)                                                                                                |
|     │   ├── sum_base_price(v#18) := sum_base_price(v#18)                                                                                  |
|     │   ├── sum_disc_price(v#19) := sum_disc_price(v#19)                                                                                  |
|     │   ├── sum_charge(v#20) := sum_charge(v#20)                                                                                          |
|     │   ├── avg_qty(v#21) := avg_qty(v#21)                                                                                                |
|     │   ├── avg_price(v#22) := avg_price(v#22)                                                                                            |
|     │   ├── avg_disc(v#23) := avg_disc(v#23)                                                                                              |
|     │   └── count_order(v#24) := count_order(v#24)                                                                                        |
|     ├── (.output_columns): {v#8, v#9, v#17, v#18, v#19, v#20, v#21, v#22, v#23, v#24}                                                     |
|     ├── (.cardinality): 0.04                                                                                                              |
|     └── LogicalAggregate                                                                                                                  |
|         ├── .exprs:                                                                                                                       |
|         │   ┌── sum_qty(v#17) := sum(lineitem.l_quantity(v#4))                                                                            |
|         │   ├── sum_base_price(v#18) := sum(lineitem.l_extendedprice(v#5))                                                                |
|         │   ├── sum_disc_price(v#19) := sum(__common_expr_1(v#16))                                                                        |
|         │   ├── sum_charge(v#20) := sum(__common_expr_1(v#16) * 1::decimal128(20, 0) + lineitem.l_tax(v#7))                               |
|         │   ├── avg_qty(v#21) := avg(lineitem.l_quantity(v#4))                                                                            |
|         │   ├── avg_price(v#22) := avg(lineitem.l_extendedprice(v#5))                                                                     |
|         │   ├── avg_disc(v#23) := avg(lineitem.l_discount(v#6))                                                                           |
|         │   └── count_order(v#24) := count(1::bigint)                                                                                     |
|         ├── .keys: [ lineitem.l_returnflag(v#8) := lineitem.l_returnflag(v#8), lineitem.l_linestatus(v#9) := lineitem.l_linestatus(v#9) ] |
|         ├── (.output_columns): {v#8, v#9, v#17, v#18, v#19, v#20, v#21, v#22, v#23, v#24}                                                 |
|         ├── (.cardinality): 0.04                                                                                                          |
|         └── LogicalProject                                                                                                                |
|             ├── .projections:                                                                                                             |
|             │   ┌── __common_expr_1(v#16) := lineitem.l_extendedprice(v#5) * 1::decimal128(20, 0) - lineitem.l_discount(v#6)              |
|             │   ├── lineitem.l_quantity(v#4) := lineitem.l_quantity(v#4)                                                                  |
|             │   ├── lineitem.l_extendedprice(v#5) := lineitem.l_extendedprice(v#5)                                                        |
|             │   ├── lineitem.l_discount(v#6) := lineitem.l_discount(v#6)                                                                  |
|             │   ├── lineitem.l_tax(v#7) := lineitem.l_tax(v#7)                                                                            |
|             │   ├── lineitem.l_returnflag(v#8) := lineitem.l_returnflag(v#8)                                                              |
|             │   └── lineitem.l_linestatus(v#9) := lineitem.l_linestatus(v#9)                                                              |
|             ├── (.output_columns): {v#4, v#5, v#6, v#7, v#8, v#9, v#16}                                                                   |
|             ├── (.cardinality): 0.00                                                                                                      |
|             └── LogicalSelect                                                                                                             |
|                 ├── .predicate: lineitem.l_shipdate(v#10) <= 1998-09-02::date32                                                           |
|                 ├── (.output_columns): {v#4, v#5, v#6, v#7, v#8, v#9, v#10}                                                               |
|                 ├── (.cardinality): 0.00                                                                                                  |
|                 └── LogicalGet { .source: 1, (.output_columns): {v#4, v#5, v#6, v#7, v#8, v#9, v#10}, (.cardinality): 0.00 }              |
+-------------------------------------------------------------------------------------------------------------------------------------------+

physical_plan after optd-finalized:
+-------------------------------------------------------------------------------------------------------------------------------------------+
| EnforcerSort                                                                                                                              |
| ├── tuple_ordering: [(v#8, Asc), (v#9, Asc)]                                                                                              |
| ├── (.output_columns): {v#8, v#9, v#17, v#18, v#19, v#20, v#21, v#22, v#23, v#24}                                                         |
| ├── (.cardinality): 0.04                                                                                                                  |
| └── PhysicalProject                                                                                                                       |
|     ├── .projections:                                                                                                                     |
|     │   ┌── lineitem.l_returnflag(v#8) := lineitem.l_returnflag(v#8)                                                                      |
|     │   ├── lineitem.l_linestatus(v#9) := lineitem.l_linestatus(v#9)                                                                      |
|     │   ├── sum_qty(v#17) := sum_qty(v#17)                                                                                                |
|     │   ├── sum_base_price(v#18) := sum_base_price(v#18)                                                                                  |
|     │   ├── sum_disc_price(v#19) := sum_disc_price(v#19)                                                                                  |
|     │   ├── sum_charge(v#20) := sum_charge(v#20)                                                                                          |
|     │   ├── avg_qty(v#21) := avg_qty(v#21)                                                                                                |
|     │   ├── avg_price(v#22) := avg_price(v#22)                                                                                            |
|     │   ├── avg_disc(v#23) := avg_disc(v#23)                                                                                              |
|     │   └── count_order(v#24) := count_order(v#24)                                                                                        |
|     ├── (.output_columns): {v#8, v#9, v#17, v#18, v#19, v#20, v#21, v#22, v#23, v#24}                                                     |
|     ├── (.cardinality): 0.04                                                                                                              |
|     └── PhysicalHashAggregate                                                                                                             |
|         ├── .exprs:                                                                                                                       |
|         │   ┌── sum_qty(v#17) := sum(lineitem.l_quantity(v#4))                                                                            |
|         │   ├── sum_base_price(v#18) := sum(lineitem.l_extendedprice(v#5))                                                                |
|         │   ├── sum_disc_price(v#19) := sum(__common_expr_1(v#16))                                                                        |
|         │   ├── sum_charge(v#20) := sum(__common_expr_1(v#16) * 1::decimal128(20, 0) + lineitem.l_tax(v#7))                               |
|         │   ├── avg_qty(v#21) := avg(lineitem.l_quantity(v#4))                                                                            |
|         │   ├── avg_price(v#22) := avg(lineitem.l_extendedprice(v#5))                                                                     |
|         │   ├── avg_disc(v#23) := avg(lineitem.l_discount(v#6))                                                                           |
|         │   └── count_order(v#24) := count(1::bigint)                                                                                     |
|         ├── .keys: [ lineitem.l_returnflag(v#8) := lineitem.l_returnflag(v#8), lineitem.l_linestatus(v#9) := lineitem.l_linestatus(v#9) ] |
|         ├── (.output_columns): {v#8, v#9, v#17, v#18, v#19, v#20, v#21, v#22, v#23, v#24}                                                 |
|         ├── (.cardinality): 0.04                                                                                                          |
|         └── PhysicalProject                                                                                                               |
|             ├── .projections:                                                                                                             |
|             │   ┌── __common_expr_1(v#16) := lineitem.l_extendedprice(v#5) * 1::decimal128(20, 0) - lineitem.l_discount(v#6)              |
|             │   ├── lineitem.l_quantity(v#4) := lineitem.l_quantity(v#4)                                                                  |
|             │   ├── lineitem.l_extendedprice(v#5) := lineitem.l_extendedprice(v#5)                                                        |
|             │   ├── lineitem.l_discount(v#6) := lineitem.l_discount(v#6)                                                                  |
|             │   ├── lineitem.l_tax(v#7) := lineitem.l_tax(v#7)                                                                            |
|             │   ├── lineitem.l_returnflag(v#8) := lineitem.l_returnflag(v#8)                                                              |
|             │   └── lineitem.l_linestatus(v#9) := lineitem.l_linestatus(v#9)                                                              |
|             ├── (.output_columns): {v#4, v#5, v#6, v#7, v#8, v#9, v#16}                                                                   |
|             ├── (.cardinality): 0.00                                                                                                      |
|             └── PhysicalFilter                                                                                                            |
|                 ├── .predicate: lineitem.l_shipdate(v#10) <= 1998-09-02::date32                                                           |
|                 ├── (.output_columns): {v#4, v#5, v#6, v#7, v#8, v#9, v#10}                                                               |
|                 ├── (.cardinality): 0.00                                                                                                  |
|                 └── PhysicalTableScan { .source: 1, (.output_columns): {v#4, v#5, v#6, v#7, v#8, v#9, v#10}, (.cardinality): 0.00 }       |
+-------------------------------------------------------------------------------------------------------------------------------------------+
*/

