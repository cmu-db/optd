-- TPC-H Q3
SELECT
    l_orderkey,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue,
    o_orderdate,
    o_shippriority 
FROM
    customer,
    orders,
    lineitem 
WHERE
    c_mktsegment = 'FURNITURE' 
    AND c_custkey = o_custkey 
    AND l_orderkey = o_orderkey 
    AND o_orderdate < DATE '1995-03-29' 
    AND l_shipdate > DATE '1995-03-29' 
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority 
ORDER BY
    revenue DESC,
    o_orderdate LIMIT 10;

/*
logical_plan after optd-initial:
+---------------------------------------------------------------------------------------------------------------------------------+
| LogicalOrderBy                                                                                                                  |
| ├── ordering_exprs: [ revenue(v#33) DESC, orders.o_orderdate(v#12) ASC ]                                                        |
| ├── (.output_columns): {v#12, v#15, v#17, v#33}                                                                                 |
| ├── (.cardinality): 0.01                                                                                                        |
| └── LogicalProject                                                                                                              |
|     ├── .projections:                                                                                                           |
|     │   ┌── lineitem.l_orderkey(v#17) := lineitem.l_orderkey(v#17)                                                              |
|     │   ├── revenue(v#33) := revenue(v#33)                                                                                      |
|     │   ├── orders.o_orderdate(v#12) := orders.o_orderdate(v#12)                                                                |
|     │   └── orders.o_shippriority(v#15) := orders.o_shippriority(v#15)                                                          |
|     ├── (.output_columns): {v#12, v#15, v#17, v#33}                                                                             |
|     ├── (.cardinality): 0.01                                                                                                    |
|     └── LogicalAggregate                                                                                                        |
|         ├── .exprs: [ revenue(v#33) := sum(lineitem.l_extendedprice(v#22) * 1::decimal128(20, 0) - lineitem.l_discount(v#23)) ] |
|         ├── .keys:                                                                                                              |
|         │   ┌── lineitem.l_orderkey(v#17) := lineitem.l_orderkey(v#17)                                                          |
|         │   ├── orders.o_orderdate(v#12) := orders.o_orderdate(v#12)                                                            |
|         │   └── orders.o_shippriority(v#15) := orders.o_shippriority(v#15)                                                      |
|         ├── (.output_columns): {v#12, v#15, v#17, v#33}                                                                         |
|         ├── (.cardinality): 0.01                                                                                                |
|         └── LogicalProject                                                                                                      |
|             ├── .projections:                                                                                                   |
|             │   ┌── orders.o_orderdate(v#12) := orders.o_orderdate(v#12)                                                        |
|             │   ├── orders.o_shippriority(v#15) := orders.o_shippriority(v#15)                                                  |
|             │   ├── lineitem.l_orderkey(v#17) := lineitem.l_orderkey(v#17)                                                      |
|             │   ├── lineitem.l_extendedprice(v#22) := lineitem.l_extendedprice(v#22)                                            |
|             │   └── lineitem.l_discount(v#23) := lineitem.l_discount(v#23)                                                      |
|             ├── (.output_columns): {v#12, v#15, v#17, v#22, v#23}                                                               |
|             ├── (.cardinality): 0.00                                                                                            |
|             └── LogicalJoin                                                                                                     |
|                 ├── .join_type: Inner                                                                                           |
|                 ├── .join_cond: (orders.o_orderkey(v#8) = lineitem.l_orderkey(v#17))                                            |
|                 ├── (.output_columns): {v#8, v#12, v#15, v#17, v#22, v#23}                                                      |
|                 ├── (.cardinality): 0.00                                                                                        |
|                 ├── LogicalProject                                                                                              |
|                 │   ├── .projections:                                                                                           |
|                 │   │   ┌── orders.o_orderkey(v#8) := orders.o_orderkey(v#8)                                                    |
|                 │   │   ├── orders.o_orderdate(v#12) := orders.o_orderdate(v#12)                                                |
|                 │   │   └── orders.o_shippriority(v#15) := orders.o_shippriority(v#15)                                          |
|                 │   ├── (.output_columns): {v#8, v#12, v#15}                                                                    |
|                 │   ├── (.cardinality): 0.00                                                                                    |
|                 │   └── LogicalJoin                                                                                             |
|                 │       ├── .join_type: Inner                                                                                   |
|                 │       ├── .join_cond: (customer.c_custkey(v#0) = orders.o_custkey(v#9))                                       |
|                 │       ├── (.output_columns): {v#0, v#8, v#9, v#12, v#15}                                                      |
|                 │       ├── (.cardinality): 0.00                                                                                |
|                 │       ├── LogicalProject                                                                                      |
|                 │       │   ├── .projections: [ customer.c_custkey(v#0) := customer.c_custkey(v#0) ]                            |
|                 │       │   ├── (.output_columns): {v#0}                                                                        |
|                 │       │   ├── (.cardinality): 0.00                                                                            |
|                 │       │   └── LogicalSelect                                                                                   |
|                 │       │       ├── .predicate: customer.c_mktsegment(v#6) = FURNITURE::utf8_view                               |
|                 │       │       ├── (.output_columns): {v#0, v#6}                                                               |
|                 │       │       ├── (.cardinality): 0.00                                                                        |
|                 │       │       └── LogicalGet { .source: 1, (.output_columns): {v#0, v#6}, (.cardinality): 0.00 }              |
|                 │       └── LogicalSelect                                                                                       |
|                 │           ├── .predicate: orders.o_orderdate(v#12) < 1995-03-29::date32                                       |
|                 │           ├── (.output_columns): {v#8, v#9, v#12, v#15}                                                       |
|                 │           ├── (.cardinality): 0.00                                                                            |
|                 │           └── LogicalGet { .source: 2, (.output_columns): {v#8, v#9, v#12, v#15}, (.cardinality): 0.00 }      |
|                 └── LogicalProject                                                                                              |
|                     ├── .projections:                                                                                           |
|                     │   ┌── lineitem.l_orderkey(v#17) := lineitem.l_orderkey(v#17)                                              |
|                     │   ├── lineitem.l_extendedprice(v#22) := lineitem.l_extendedprice(v#22)                                    |
|                     │   └── lineitem.l_discount(v#23) := lineitem.l_discount(v#23)                                              |
|                     ├── (.output_columns): {v#17, v#22, v#23}                                                                   |
|                     ├── (.cardinality): 0.00                                                                                    |
|                     └── LogicalSelect                                                                                           |
|                         ├── .predicate: lineitem.l_shipdate(v#27) > 1995-03-29::date32                                          |
|                         ├── (.output_columns): {v#17, v#22, v#23, v#27}                                                         |
|                         ├── (.cardinality): 0.00                                                                                |
|                         └── LogicalGet { .source: 3, (.output_columns): {v#17, v#22, v#23, v#27}, (.cardinality): 0.00 }        |
+---------------------------------------------------------------------------------------------------------------------------------+

physical_plan after optd-finalized:
+-----------------------------------------------------------------------------------------------------------------------------------+
| EnforcerSort { tuple_ordering: [(v#33, Desc), (v#12, Asc)], (.output_columns): {v#12, v#15, v#17, v#33}, (.cardinality): 0.01 }   |
| └── PhysicalProject                                                                                                               |
|     ├── .projections:                                                                                                             |
|     │   ┌── lineitem.l_orderkey(v#17) := lineitem.l_orderkey(v#17)                                                                |
|     │   ├── revenue(v#33) := revenue(v#33)                                                                                        |
|     │   ├── orders.o_orderdate(v#12) := orders.o_orderdate(v#12)                                                                  |
|     │   └── orders.o_shippriority(v#15) := orders.o_shippriority(v#15)                                                            |
|     ├── (.output_columns): {v#12, v#15, v#17, v#33}                                                                               |
|     ├── (.cardinality): 0.01                                                                                                      |
|     └── PhysicalHashAggregate                                                                                                     |
|         ├── .exprs: [ revenue(v#33) := sum(lineitem.l_extendedprice(v#22) * 1::decimal128(20, 0) - lineitem.l_discount(v#23)) ]   |
|         ├── .keys:                                                                                                                |
|         │   ┌── lineitem.l_orderkey(v#17) := lineitem.l_orderkey(v#17)                                                            |
|         │   ├── orders.o_orderdate(v#12) := orders.o_orderdate(v#12)                                                              |
|         │   └── orders.o_shippriority(v#15) := orders.o_shippriority(v#15)                                                        |
|         ├── (.output_columns): {v#12, v#15, v#17, v#33}                                                                           |
|         ├── (.cardinality): 0.01                                                                                                  |
|         └── PhysicalProject                                                                                                       |
|             ├── .projections:                                                                                                     |
|             │   ┌── orders.o_orderdate(v#12) := orders.o_orderdate(v#12)                                                          |
|             │   ├── orders.o_shippriority(v#15) := orders.o_shippriority(v#15)                                                    |
|             │   ├── lineitem.l_orderkey(v#17) := lineitem.l_orderkey(v#17)                                                        |
|             │   ├── lineitem.l_extendedprice(v#22) := lineitem.l_extendedprice(v#22)                                              |
|             │   └── lineitem.l_discount(v#23) := lineitem.l_discount(v#23)                                                        |
|             ├── (.output_columns): {v#12, v#15, v#17, v#22, v#23}                                                                 |
|             ├── (.cardinality): 0.00                                                                                              |
|             └── PhysicalHashJoin                                                                                                  |
|                 ├── .join_type: Inner                                                                                             |
|                 ├── .join_conds: (orders.o_orderkey(v#8) = lineitem.l_orderkey(v#17)) AND (true::boolean)                         |
|                 ├── (.output_columns): {v#8, v#12, v#15, v#17, v#22, v#23}                                                        |
|                 ├── (.cardinality): 0.00                                                                                          |
|                 ├── PhysicalProject                                                                                               |
|                 │   ├── .projections:                                                                                             |
|                 │   │   ┌── orders.o_orderkey(v#8) := orders.o_orderkey(v#8)                                                      |
|                 │   │   ├── orders.o_orderdate(v#12) := orders.o_orderdate(v#12)                                                  |
|                 │   │   └── orders.o_shippriority(v#15) := orders.o_shippriority(v#15)                                            |
|                 │   ├── (.output_columns): {v#8, v#12, v#15}                                                                      |
|                 │   ├── (.cardinality): 0.00                                                                                      |
|                 │   └── PhysicalHashJoin                                                                                          |
|                 │       ├── .join_type: Inner                                                                                     |
|                 │       ├── .join_conds: (customer.c_custkey(v#0) = orders.o_custkey(v#9)) AND (true::boolean)                    |
|                 │       ├── (.output_columns): {v#0, v#8, v#9, v#12, v#15}                                                        |
|                 │       ├── (.cardinality): 0.00                                                                                  |
|                 │       ├── PhysicalProject                                                                                       |
|                 │       │   ├── .projections: [ customer.c_custkey(v#0) := customer.c_custkey(v#0) ]                              |
|                 │       │   ├── (.output_columns): {v#0}                                                                          |
|                 │       │   ├── (.cardinality): 0.00                                                                              |
|                 │       │   └── PhysicalFilter                                                                                    |
|                 │       │       ├── .predicate: customer.c_mktsegment(v#6) = FURNITURE::utf8_view                                 |
|                 │       │       ├── (.output_columns): {v#0, v#6}                                                                 |
|                 │       │       ├── (.cardinality): 0.00                                                                          |
|                 │       │       └── PhysicalTableScan { .source: 1, (.output_columns): {v#0, v#6}, (.cardinality): 0.00 }         |
|                 │       └── PhysicalFilter                                                                                        |
|                 │           ├── .predicate: orders.o_orderdate(v#12) < 1995-03-29::date32                                         |
|                 │           ├── (.output_columns): {v#8, v#9, v#12, v#15}                                                         |
|                 │           ├── (.cardinality): 0.00                                                                              |
|                 │           └── PhysicalTableScan { .source: 2, (.output_columns): {v#8, v#9, v#12, v#15}, (.cardinality): 0.00 } |
|                 └── PhysicalProject                                                                                               |
|                     ├── .projections:                                                                                             |
|                     │   ┌── lineitem.l_orderkey(v#17) := lineitem.l_orderkey(v#17)                                                |
|                     │   ├── lineitem.l_extendedprice(v#22) := lineitem.l_extendedprice(v#22)                                      |
|                     │   └── lineitem.l_discount(v#23) := lineitem.l_discount(v#23)                                                |
|                     ├── (.output_columns): {v#17, v#22, v#23}                                                                     |
|                     ├── (.cardinality): 0.00                                                                                      |
|                     └── PhysicalFilter                                                                                            |
|                         ├── .predicate: lineitem.l_shipdate(v#27) > 1995-03-29::date32                                            |
|                         ├── (.output_columns): {v#17, v#22, v#23, v#27}                                                           |
|                         ├── (.cardinality): 0.00                                                                                  |
|                         └── PhysicalTableScan { .source: 3, (.output_columns): {v#17, v#22, v#23, v#27}, (.cardinality): 0.00 }   |
+-----------------------------------------------------------------------------------------------------------------------------------+
*/

