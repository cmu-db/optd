-- TPC-H Q6
SELECT
    SUM(l_extendedprice * l_discount) AS revenue_loss
FROM
    lineitem
WHERE
    l_shipdate >= DATE '2023-01-01'
    AND l_shipdate < DATE '2024-01-01'
    AND l_discount BETWEEN 0.05 AND 0.07
    AND l_quantity < 24;

/*
logical_plan after optd-initial:
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| LogicalProject { .projections: [ revenue_loss(v#16) := revenue_loss(v#16) ], (.output_columns): {v#16}, (.cardinality): 1.00 }                                                                                                                                                                            |
| └── LogicalAggregate { .exprs: [ revenue_loss(v#16) := sum(lineitem.l_extendedprice(v#5) * lineitem.l_discount(v#6)) ], .keys: [], (.output_columns): {v#16}, (.cardinality): 1.00 }                                                                                                                      |
|     └── LogicalProject { .projections: [ lineitem.l_extendedprice(v#5) := lineitem.l_extendedprice(v#5), lineitem.l_discount(v#6) := lineitem.l_discount(v#6) ], (.output_columns): {v#5, v#6}, (.cardinality): 0.00 }                                                                                    |
|         └── LogicalSelect                                                                                                                                                                                                                                                                                 |
|             ├── .predicate: (lineitem.l_shipdate(v#10) >= 2023-01-01::date32) AND (lineitem.l_shipdate(v#10) < 2024-01-01::date32) AND (lineitem.l_discount(v#6) >= 5::decimal128(15, 2)) AND (lineitem.l_discount(v#6) <= 7::decimal128(15, 2)) AND (lineitem.l_quantity(v#4) < 2400::decimal128(15, 2)) |
|             ├── (.output_columns): {v#4, v#5, v#6, v#10}                                                                                                                                                                                                                                                  |
|             ├── (.cardinality): 0.00                                                                                                                                                                                                                                                                      |
|             └── LogicalGet { .source: 1, (.output_columns): {v#4, v#5, v#6, v#10}, (.cardinality): 0.00 }                                                                                                                                                                                                 |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

physical_plan after optd-finalized:
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| PhysicalProject { .projections: [ revenue_loss(v#16) := revenue_loss(v#16) ], (.output_columns): {v#16}, (.cardinality): 1.00 }                                                                                                                                                                           |
| └── PhysicalHashAggregate { .exprs: [ revenue_loss(v#16) := sum(lineitem.l_extendedprice(v#5) * lineitem.l_discount(v#6)) ], .keys: [], (.output_columns): {v#16}, (.cardinality): 1.00 }                                                                                                                 |
|     └── PhysicalProject { .projections: [ lineitem.l_extendedprice(v#5) := lineitem.l_extendedprice(v#5), lineitem.l_discount(v#6) := lineitem.l_discount(v#6) ], (.output_columns): {v#5, v#6}, (.cardinality): 0.00 }                                                                                   |
|         └── PhysicalFilter                                                                                                                                                                                                                                                                                |
|             ├── .predicate: (lineitem.l_shipdate(v#10) >= 2023-01-01::date32) AND (lineitem.l_shipdate(v#10) < 2024-01-01::date32) AND (lineitem.l_discount(v#6) >= 5::decimal128(15, 2)) AND (lineitem.l_discount(v#6) <= 7::decimal128(15, 2)) AND (lineitem.l_quantity(v#4) < 2400::decimal128(15, 2)) |
|             ├── (.output_columns): {v#4, v#5, v#6, v#10}                                                                                                                                                                                                                                                  |
|             ├── (.cardinality): 0.00                                                                                                                                                                                                                                                                      |
|             └── PhysicalTableScan { .source: 1, (.output_columns): {v#4, v#5, v#6, v#10}, (.cardinality): 0.00 }                                                                                                                                                                                          |
+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+

NULL
*/

