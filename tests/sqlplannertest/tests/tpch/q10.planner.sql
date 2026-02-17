-- TPC-H Q10
SELECT
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
FROM
    customer,
    orders,
    lineitem,
    nation
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND o_orderdate >= DATE '1993-07-01'
    AND o_orderdate < DATE '1993-07-01' + INTERVAL '3' MONTH
    AND l_returnflag = 'R'
    AND c_nationkey = n_nationkey
GROUP BY
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
ORDER BY
    revenue DESC
LIMIT 20;

/*
logical_plan after optd-initial:
+--------------------------------------------------------------------------------------------------------------------------------------------------------------+
| LogicalOrderBy { ordering_exprs: [ revenue(v#37) DESC ], (.output_columns): {v#0, v#1, v#2, v#4, v#5, v#7, v#34, v#37}, (.cardinality): 0.00 }               |
| └── LogicalProject                                                                                                                                           |
|     ├── .projections:                                                                                                                                        |
|     │   ┌── customer.c_custkey(v#0) := customer.c_custkey(v#0)                                                                                               |
|     │   ├── customer.c_name(v#1) := customer.c_name(v#1)                                                                                                     |
|     │   ├── revenue(v#37) := revenue(v#37)                                                                                                                   |
|     │   ├── customer.c_acctbal(v#5) := customer.c_acctbal(v#5)                                                                                               |
|     │   ├── nation.n_name(v#34) := nation.n_name(v#34)                                                                                                       |
|     │   ├── customer.c_address(v#2) := customer.c_address(v#2)                                                                                               |
|     │   ├── customer.c_phone(v#4) := customer.c_phone(v#4)                                                                                                   |
|     │   └── customer.c_comment(v#7) := customer.c_comment(v#7)                                                                                               |
|     ├── (.output_columns): {v#0, v#1, v#2, v#4, v#5, v#7, v#34, v#37}                                                                                        |
|     ├── (.cardinality): 0.00                                                                                                                                 |
|     └── LogicalAggregate                                                                                                                                     |
|         ├── .exprs: [ revenue(v#37) := sum(lineitem.l_extendedprice(v#22) * 1::decimal128(20, 0) - lineitem.l_discount(v#23)) ]                              |
|         ├── .keys:                                                                                                                                           |
|         │   ┌── customer.c_custkey(v#0) := customer.c_custkey(v#0)                                                                                           |
|         │   ├── customer.c_name(v#1) := customer.c_name(v#1)                                                                                                 |
|         │   ├── customer.c_acctbal(v#5) := customer.c_acctbal(v#5)                                                                                           |
|         │   ├── customer.c_phone(v#4) := customer.c_phone(v#4)                                                                                               |
|         │   ├── nation.n_name(v#34) := nation.n_name(v#34)                                                                                                   |
|         │   ├── customer.c_address(v#2) := customer.c_address(v#2)                                                                                           |
|         │   └── customer.c_comment(v#7) := customer.c_comment(v#7)                                                                                           |
|         ├── (.output_columns): {v#0, v#1, v#2, v#4, v#5, v#7, v#34, v#37}                                                                                    |
|         ├── (.cardinality): 0.00                                                                                                                             |
|         └── LogicalProject                                                                                                                                   |
|             ├── .projections:                                                                                                                                |
|             │   ┌── customer.c_custkey(v#0) := customer.c_custkey(v#0)                                                                                       |
|             │   ├── customer.c_name(v#1) := customer.c_name(v#1)                                                                                             |
|             │   ├── customer.c_address(v#2) := customer.c_address(v#2)                                                                                       |
|             │   ├── customer.c_phone(v#4) := customer.c_phone(v#4)                                                                                           |
|             │   ├── customer.c_acctbal(v#5) := customer.c_acctbal(v#5)                                                                                       |
|             │   ├── customer.c_comment(v#7) := customer.c_comment(v#7)                                                                                       |
|             │   ├── lineitem.l_extendedprice(v#22) := lineitem.l_extendedprice(v#22)                                                                         |
|             │   ├── lineitem.l_discount(v#23) := lineitem.l_discount(v#23)                                                                                   |
|             │   └── nation.n_name(v#34) := nation.n_name(v#34)                                                                                               |
|             ├── (.output_columns): {v#0, v#1, v#2, v#4, v#5, v#7, v#22, v#23, v#34}                                                                          |
|             ├── (.cardinality): 0.00                                                                                                                         |
|             └── LogicalJoin                                                                                                                                  |
|                 ├── .join_type: Inner                                                                                                                        |
|                 ├── .join_cond: (customer.c_nationkey(v#3) = nation.n_nationkey(v#33))                                                                       |
|                 ├── (.output_columns): {v#0, v#1, v#2, v#3, v#4, v#5, v#7, v#22, v#23, v#33, v#34}                                                           |
|                 ├── (.cardinality): 0.00                                                                                                                     |
|                 ├── LogicalProject                                                                                                                           |
|                 │   ├── .projections:                                                                                                                        |
|                 │   │   ┌── customer.c_custkey(v#0) := customer.c_custkey(v#0)                                                                               |
|                 │   │   ├── customer.c_name(v#1) := customer.c_name(v#1)                                                                                     |
|                 │   │   ├── customer.c_address(v#2) := customer.c_address(v#2)                                                                               |
|                 │   │   ├── customer.c_nationkey(v#3) := customer.c_nationkey(v#3)                                                                           |
|                 │   │   ├── customer.c_phone(v#4) := customer.c_phone(v#4)                                                                                   |
|                 │   │   ├── customer.c_acctbal(v#5) := customer.c_acctbal(v#5)                                                                               |
|                 │   │   ├── customer.c_comment(v#7) := customer.c_comment(v#7)                                                                               |
|                 │   │   ├── lineitem.l_extendedprice(v#22) := lineitem.l_extendedprice(v#22)                                                                 |
|                 │   │   └── lineitem.l_discount(v#23) := lineitem.l_discount(v#23)                                                                           |
|                 │   ├── (.output_columns): {v#0, v#1, v#2, v#3, v#4, v#5, v#7, v#22, v#23}                                                                   |
|                 │   ├── (.cardinality): 0.00                                                                                                                 |
|                 │   └── LogicalJoin                                                                                                                          |
|                 │       ├── .join_type: Inner                                                                                                                |
|                 │       ├── .join_cond: (orders.o_orderkey(v#8) = lineitem.l_orderkey(v#17))                                                                 |
|                 │       ├── (.output_columns): {v#0, v#1, v#2, v#3, v#4, v#5, v#7, v#8, v#17, v#22, v#23}                                                    |
|                 │       ├── (.cardinality): 0.00                                                                                                             |
|                 │       ├── LogicalProject                                                                                                                   |
|                 │       │   ├── .projections:                                                                                                                |
|                 │       │   │   ┌── customer.c_custkey(v#0) := customer.c_custkey(v#0)                                                                       |
|                 │       │   │   ├── customer.c_name(v#1) := customer.c_name(v#1)                                                                             |
|                 │       │   │   ├── customer.c_address(v#2) := customer.c_address(v#2)                                                                       |
|                 │       │   │   ├── customer.c_nationkey(v#3) := customer.c_nationkey(v#3)                                                                   |
|                 │       │   │   ├── customer.c_phone(v#4) := customer.c_phone(v#4)                                                                           |
|                 │       │   │   ├── customer.c_acctbal(v#5) := customer.c_acctbal(v#5)                                                                       |
|                 │       │   │   ├── customer.c_comment(v#7) := customer.c_comment(v#7)                                                                       |
|                 │       │   │   └── orders.o_orderkey(v#8) := orders.o_orderkey(v#8)                                                                         |
|                 │       │   ├── (.output_columns): {v#0, v#1, v#2, v#3, v#4, v#5, v#7, v#8}                                                                  |
|                 │       │   ├── (.cardinality): 0.00                                                                                                         |
|                 │       │   └── LogicalJoin                                                                                                                  |
|                 │       │       ├── .join_type: Inner                                                                                                        |
|                 │       │       ├── .join_cond: (customer.c_custkey(v#0) = orders.o_custkey(v#9))                                                            |
|                 │       │       ├── (.output_columns): {v#0, v#1, v#2, v#3, v#4, v#5, v#7, v#8, v#9}                                                         |
|                 │       │       ├── (.cardinality): 0.00                                                                                                     |
|                 │       │       ├── LogicalGet { .source: 1, (.output_columns): {v#0, v#1, v#2, v#3, v#4, v#5, v#7}, (.cardinality): 0.00 }                  |
|                 │       │       └── LogicalProject                                                                                                           |
|                 │       │           ├── .projections: [ orders.o_orderkey(v#8) := orders.o_orderkey(v#8), orders.o_custkey(v#9) := orders.o_custkey(v#9) ]   |
|                 │       │           ├── (.output_columns): {v#8, v#9}                                                                                        |
|                 │       │           ├── (.cardinality): 0.00                                                                                                 |
|                 │       │           └── LogicalSelect                                                                                                        |
|                 │       │               ├── .predicate: (orders.o_orderdate(v#12) >= 1993-07-01::date32) AND (orders.o_orderdate(v#12) < 1993-10-01::date32) |
|                 │       │               ├── (.output_columns): {v#8, v#9, v#12}                                                                              |
|                 │       │               ├── (.cardinality): 0.00                                                                                             |
|                 │       │               └── LogicalGet { .source: 2, (.output_columns): {v#8, v#9, v#12}, (.cardinality): 0.00 }                             |
|                 │       └── LogicalProject                                                                                                                   |
|                 │           ├── .projections:                                                                                                                |
|                 │           │   ┌── lineitem.l_orderkey(v#17) := lineitem.l_orderkey(v#17)                                                                   |
|                 │           │   ├── lineitem.l_extendedprice(v#22) := lineitem.l_extendedprice(v#22)                                                         |
|                 │           │   └── lineitem.l_discount(v#23) := lineitem.l_discount(v#23)                                                                   |
|                 │           ├── (.output_columns): {v#17, v#22, v#23}                                                                                        |
|                 │           ├── (.cardinality): 0.00                                                                                                         |
|                 │           └── LogicalSelect                                                                                                                |
|                 │               ├── .predicate: lineitem.l_returnflag(v#25) = R::utf8_view                                                                   |
|                 │               ├── (.output_columns): {v#17, v#22, v#23, v#25}                                                                              |
|                 │               ├── (.cardinality): 0.00                                                                                                     |
|                 │               └── LogicalGet { .source: 3, (.output_columns): {v#17, v#22, v#23, v#25}, (.cardinality): 0.00 }                             |
|                 └── LogicalGet { .source: 4, (.output_columns): {v#33, v#34}, (.cardinality): 0.00 }                                                         |
+--------------------------------------------------------------------------------------------------------------------------------------------------------------+

physical_plan after optd-finalized:
+--------------------------------------------------------------------------------------------------------------------------------------------------------------+
| EnforcerSort { tuple_ordering: [(v#37, Desc)], (.output_columns): {v#0, v#1, v#2, v#4, v#5, v#7, v#34, v#37}, (.cardinality): 0.00 }                         |
| └── PhysicalProject                                                                                                                                          |
|     ├── .projections:                                                                                                                                        |
|     │   ┌── customer.c_custkey(v#0) := customer.c_custkey(v#0)                                                                                               |
|     │   ├── customer.c_name(v#1) := customer.c_name(v#1)                                                                                                     |
|     │   ├── revenue(v#37) := revenue(v#37)                                                                                                                   |
|     │   ├── customer.c_acctbal(v#5) := customer.c_acctbal(v#5)                                                                                               |
|     │   ├── nation.n_name(v#34) := nation.n_name(v#34)                                                                                                       |
|     │   ├── customer.c_address(v#2) := customer.c_address(v#2)                                                                                               |
|     │   ├── customer.c_phone(v#4) := customer.c_phone(v#4)                                                                                                   |
|     │   └── customer.c_comment(v#7) := customer.c_comment(v#7)                                                                                               |
|     ├── (.output_columns): {v#0, v#1, v#2, v#4, v#5, v#7, v#34, v#37}                                                                                        |
|     ├── (.cardinality): 0.00                                                                                                                                 |
|     └── PhysicalHashAggregate                                                                                                                                |
|         ├── .exprs: [ revenue(v#37) := sum(lineitem.l_extendedprice(v#22) * 1::decimal128(20, 0) - lineitem.l_discount(v#23)) ]                              |
|         ├── .keys:                                                                                                                                           |
|         │   ┌── customer.c_custkey(v#0) := customer.c_custkey(v#0)                                                                                           |
|         │   ├── customer.c_name(v#1) := customer.c_name(v#1)                                                                                                 |
|         │   ├── customer.c_acctbal(v#5) := customer.c_acctbal(v#5)                                                                                           |
|         │   ├── customer.c_phone(v#4) := customer.c_phone(v#4)                                                                                               |
|         │   ├── nation.n_name(v#34) := nation.n_name(v#34)                                                                                                   |
|         │   ├── customer.c_address(v#2) := customer.c_address(v#2)                                                                                           |
|         │   └── customer.c_comment(v#7) := customer.c_comment(v#7)                                                                                           |
|         ├── (.output_columns): {v#0, v#1, v#2, v#4, v#5, v#7, v#34, v#37}                                                                                    |
|         ├── (.cardinality): 0.00                                                                                                                             |
|         └── PhysicalProject                                                                                                                                  |
|             ├── .projections:                                                                                                                                |
|             │   ┌── customer.c_custkey(v#0) := customer.c_custkey(v#0)                                                                                       |
|             │   ├── customer.c_name(v#1) := customer.c_name(v#1)                                                                                             |
|             │   ├── customer.c_address(v#2) := customer.c_address(v#2)                                                                                       |
|             │   ├── customer.c_phone(v#4) := customer.c_phone(v#4)                                                                                           |
|             │   ├── customer.c_acctbal(v#5) := customer.c_acctbal(v#5)                                                                                       |
|             │   ├── customer.c_comment(v#7) := customer.c_comment(v#7)                                                                                       |
|             │   ├── lineitem.l_extendedprice(v#22) := lineitem.l_extendedprice(v#22)                                                                         |
|             │   ├── lineitem.l_discount(v#23) := lineitem.l_discount(v#23)                                                                                   |
|             │   └── nation.n_name(v#34) := nation.n_name(v#34)                                                                                               |
|             ├── (.output_columns): {v#0, v#1, v#2, v#4, v#5, v#7, v#22, v#23, v#34}                                                                          |
|             ├── (.cardinality): 0.00                                                                                                                         |
|             └── PhysicalHashJoin                                                                                                                             |
|                 ├── .join_type: Inner                                                                                                                        |
|                 ├── .join_conds: (customer.c_nationkey(v#3) = nation.n_nationkey(v#33)) AND (true::boolean)                                                  |
|                 ├── (.output_columns): {v#0, v#1, v#2, v#3, v#4, v#5, v#7, v#22, v#23, v#33, v#34}                                                           |
|                 ├── (.cardinality): 0.00                                                                                                                     |
|                 ├── PhysicalProject                                                                                                                          |
|                 │   ├── .projections:                                                                                                                        |
|                 │   │   ┌── customer.c_custkey(v#0) := customer.c_custkey(v#0)                                                                               |
|                 │   │   ├── customer.c_name(v#1) := customer.c_name(v#1)                                                                                     |
|                 │   │   ├── customer.c_address(v#2) := customer.c_address(v#2)                                                                               |
|                 │   │   ├── customer.c_nationkey(v#3) := customer.c_nationkey(v#3)                                                                           |
|                 │   │   ├── customer.c_phone(v#4) := customer.c_phone(v#4)                                                                                   |
|                 │   │   ├── customer.c_acctbal(v#5) := customer.c_acctbal(v#5)                                                                               |
|                 │   │   ├── customer.c_comment(v#7) := customer.c_comment(v#7)                                                                               |
|                 │   │   ├── lineitem.l_extendedprice(v#22) := lineitem.l_extendedprice(v#22)                                                                 |
|                 │   │   └── lineitem.l_discount(v#23) := lineitem.l_discount(v#23)                                                                           |
|                 │   ├── (.output_columns): {v#0, v#1, v#2, v#3, v#4, v#5, v#7, v#22, v#23}                                                                   |
|                 │   ├── (.cardinality): 0.00                                                                                                                 |
|                 │   └── PhysicalHashJoin                                                                                                                     |
|                 │       ├── .join_type: Inner                                                                                                                |
|                 │       ├── .join_conds: (orders.o_orderkey(v#8) = lineitem.l_orderkey(v#17)) AND (true::boolean)                                            |
|                 │       ├── (.output_columns): {v#0, v#1, v#2, v#3, v#4, v#5, v#7, v#8, v#17, v#22, v#23}                                                    |
|                 │       ├── (.cardinality): 0.00                                                                                                             |
|                 │       ├── PhysicalProject                                                                                                                  |
|                 │       │   ├── .projections:                                                                                                                |
|                 │       │   │   ┌── customer.c_custkey(v#0) := customer.c_custkey(v#0)                                                                       |
|                 │       │   │   ├── customer.c_name(v#1) := customer.c_name(v#1)                                                                             |
|                 │       │   │   ├── customer.c_address(v#2) := customer.c_address(v#2)                                                                       |
|                 │       │   │   ├── customer.c_nationkey(v#3) := customer.c_nationkey(v#3)                                                                   |
|                 │       │   │   ├── customer.c_phone(v#4) := customer.c_phone(v#4)                                                                           |
|                 │       │   │   ├── customer.c_acctbal(v#5) := customer.c_acctbal(v#5)                                                                       |
|                 │       │   │   ├── customer.c_comment(v#7) := customer.c_comment(v#7)                                                                       |
|                 │       │   │   └── orders.o_orderkey(v#8) := orders.o_orderkey(v#8)                                                                         |
|                 │       │   ├── (.output_columns): {v#0, v#1, v#2, v#3, v#4, v#5, v#7, v#8}                                                                  |
|                 │       │   ├── (.cardinality): 0.00                                                                                                         |
|                 │       │   └── PhysicalHashJoin                                                                                                             |
|                 │       │       ├── .join_type: Inner                                                                                                        |
|                 │       │       ├── .join_conds: (customer.c_custkey(v#0) = orders.o_custkey(v#9)) AND (true::boolean)                                       |
|                 │       │       ├── (.output_columns): {v#0, v#1, v#2, v#3, v#4, v#5, v#7, v#8, v#9}                                                         |
|                 │       │       ├── (.cardinality): 0.00                                                                                                     |
|                 │       │       ├── PhysicalTableScan { .source: 1, (.output_columns): {v#0, v#1, v#2, v#3, v#4, v#5, v#7}, (.cardinality): 0.00 }           |
|                 │       │       └── PhysicalProject                                                                                                          |
|                 │       │           ├── .projections: [ orders.o_orderkey(v#8) := orders.o_orderkey(v#8), orders.o_custkey(v#9) := orders.o_custkey(v#9) ]   |
|                 │       │           ├── (.output_columns): {v#8, v#9}                                                                                        |
|                 │       │           ├── (.cardinality): 0.00                                                                                                 |
|                 │       │           └── PhysicalFilter                                                                                                       |
|                 │       │               ├── .predicate: (orders.o_orderdate(v#12) >= 1993-07-01::date32) AND (orders.o_orderdate(v#12) < 1993-10-01::date32) |
|                 │       │               ├── (.output_columns): {v#8, v#9, v#12}                                                                              |
|                 │       │               ├── (.cardinality): 0.00                                                                                             |
|                 │       │               └── PhysicalTableScan { .source: 2, (.output_columns): {v#8, v#9, v#12}, (.cardinality): 0.00 }                      |
|                 │       └── PhysicalProject                                                                                                                  |
|                 │           ├── .projections:                                                                                                                |
|                 │           │   ┌── lineitem.l_orderkey(v#17) := lineitem.l_orderkey(v#17)                                                                   |
|                 │           │   ├── lineitem.l_extendedprice(v#22) := lineitem.l_extendedprice(v#22)                                                         |
|                 │           │   └── lineitem.l_discount(v#23) := lineitem.l_discount(v#23)                                                                   |
|                 │           ├── (.output_columns): {v#17, v#22, v#23}                                                                                        |
|                 │           ├── (.cardinality): 0.00                                                                                                         |
|                 │           └── PhysicalFilter                                                                                                               |
|                 │               ├── .predicate: lineitem.l_returnflag(v#25) = R::utf8_view                                                                   |
|                 │               ├── (.output_columns): {v#17, v#22, v#23, v#25}                                                                              |
|                 │               ├── (.cardinality): 0.00                                                                                                     |
|                 │               └── PhysicalTableScan { .source: 3, (.output_columns): {v#17, v#22, v#23, v#25}, (.cardinality): 0.00 }                      |
|                 └── PhysicalTableScan { .source: 4, (.output_columns): {v#33, v#34}, (.cardinality): 0.00 }                                                  |
+--------------------------------------------------------------------------------------------------------------------------------------------------------------+
*/

