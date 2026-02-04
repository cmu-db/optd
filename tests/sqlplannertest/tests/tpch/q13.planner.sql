-- TPC-H Q13
select
    c_count,
    count(*) as custdist
from
    (
        select
            c_custkey,
            count(o_orderkey)
        from
            customer left outer join orders on
                c_custkey = o_custkey
                and o_comment not like '%special%requests%'
        group by
            c_custkey
    ) as c_orders (c_custkey, c_count)
group by
    c_count
order by
    custdist desc,
    c_count desc;

/*
logical_plan after optd-initial:
+--------------------------------------------------------------------------------------------------------------------------------------+
| LogicalOrderBy                                                                                                                       |
| ├── ordering_exprs: [ custdist(v#19) DESC, c_orders.c_count(v#18) DESC ]                                                             |
| ├── (.output_columns): {v#18, v#19}                                                                                                  |
| ├── (.cardinality): 0.20                                                                                                             |
| └── LogicalProject                                                                                                                   |
|     ├── .projections: [ c_orders.c_count(v#18) := c_orders.c_count(v#18), custdist(v#19) := custdist(v#19) ]                         |
|     ├── (.output_columns): {v#18, v#19}                                                                                              |
|     ├── (.cardinality): 0.20                                                                                                         |
|     └── LogicalAggregate                                                                                                             |
|         ├── .exprs: [ custdist(v#19) := count(1::bigint) ]                                                                           |
|         ├── .keys: [ c_orders.c_count(v#18) := c_orders.c_count(v#18) ]                                                              |
|         ├── (.output_columns): {v#18, v#19}                                                                                          |
|         ├── (.cardinality): 0.20                                                                                                     |
|         └── LogicalRemap { .mappings: [ c_orders.c_count(v#18) := c_count(v#17) ], (.output_columns): {v#18}, (.cardinality): 0.20 } |
|             └── LogicalProject { .projections: [ c_count(v#17) := c_count(v#17) ], (.output_columns): {v#17}, (.cardinality): 0.20 } |
|                 └── LogicalAggregate                                                                                                 |
|                     ├── .exprs: [ c_count(v#17) := count(orders.o_orderkey(v#8)) ]                                                   |
|                     ├── .keys: [ customer.c_custkey(v#0) := customer.c_custkey(v#0) ]                                                |
|                     ├── (.output_columns): {v#0, v#17}                                                                               |
|                     ├── (.cardinality): 0.20                                                                                         |
|                     └── LogicalProject                                                                                               |
|                         ├── .projections:                                                                                            |
|                         │   ┌── customer.c_custkey(v#0) := customer.c_custkey(v#0)                                                   |
|                         │   └── orders.o_orderkey(v#8) := orders.o_orderkey(v#8)                                                     |
|                         ├── (.output_columns): {v#0, v#8}                                                                            |
|                         ├── (.cardinality): 0.00                                                                                     |
|                         └── LogicalJoin                                                                                              |
|                             ├── .join_type: Left                                                                                     |
|                             ├── .join_cond: (customer.c_custkey(v#0) = orders.o_custkey(v#9))                                        |
|                             ├── (.output_columns): {v#0, v#8, v#9}                                                                   |
|                             ├── (.cardinality): 0.00                                                                                 |
|                             ├── LogicalGet { .source: 1, (.output_columns): {v#0}, (.cardinality): 0.00 }                            |
|                             └── LogicalProject                                                                                       |
|                                 ├── .projections:                                                                                    |
|                                 │   ┌── orders.o_orderkey(v#8) := orders.o_orderkey(v#8)                                             |
|                                 │   └── orders.o_custkey(v#9) := orders.o_custkey(v#9)                                               |
|                                 ├── (.output_columns): {v#8, v#9}                                                                    |
|                                 ├── (.cardinality): 0.00                                                                             |
|                                 └── LogicalSelect                                                                                    |
|                                     ├── .predicate: orders.o_comment(v#16) NOT LIKE %special%requests%::utf8_view                    |
|                                     ├── (.output_columns): {v#8, v#9, v#16}                                                          |
|                                     ├── (.cardinality): 0.00                                                                         |
|                                     └── LogicalGet { .source: 2, (.output_columns): {v#8, v#9, v#16}, (.cardinality): 0.00 }         |
+--------------------------------------------------------------------------------------------------------------------------------------+

physical_plan after optd-finalized:
+---------------------------------------------------------------------------------------------------------------------------------------+
| EnforcerSort { tuple_ordering: [(v#19, Desc), (v#18, Desc)], (.output_columns): {v#18, v#19}, (.cardinality): 0.20 }                  |
| └── PhysicalProject                                                                                                                   |
|     ├── .projections: [ c_orders.c_count(v#18) := c_orders.c_count(v#18), custdist(v#19) := custdist(v#19) ]                          |
|     ├── (.output_columns): {v#18, v#19}                                                                                               |
|     ├── (.cardinality): 0.20                                                                                                          |
|     └── PhysicalHashAggregate                                                                                                         |
|         ├── .exprs: [ custdist(v#19) := count(1::bigint) ]                                                                            |
|         ├── .keys: [ c_orders.c_count(v#18) := c_orders.c_count(v#18) ]                                                               |
|         ├── (.output_columns): {v#18, v#19}                                                                                           |
|         ├── (.cardinality): 0.20                                                                                                      |
|         └── LogicalRemap { .mappings: [ c_orders.c_count(v#18) := c_count(v#17) ], (.output_columns): {v#18}, (.cardinality): 0.20 }  |
|             └── PhysicalProject { .projections: [ c_count(v#17) := c_count(v#17) ], (.output_columns): {v#17}, (.cardinality): 0.20 } |
|                 └── PhysicalHashAggregate                                                                                             |
|                     ├── .exprs: [ c_count(v#17) := count(orders.o_orderkey(v#8)) ]                                                    |
|                     ├── .keys: [ customer.c_custkey(v#0) := customer.c_custkey(v#0) ]                                                 |
|                     ├── (.output_columns): {v#0, v#17}                                                                                |
|                     ├── (.cardinality): 0.20                                                                                          |
|                     └── PhysicalProject                                                                                               |
|                         ├── .projections:                                                                                             |
|                         │   ┌── customer.c_custkey(v#0) := customer.c_custkey(v#0)                                                    |
|                         │   └── orders.o_orderkey(v#8) := orders.o_orderkey(v#8)                                                      |
|                         ├── (.output_columns): {v#0, v#8}                                                                             |
|                         ├── (.cardinality): 0.00                                                                                      |
|                         └── PhysicalHashJoin                                                                                          |
|                             ├── .join_type: Left                                                                                      |
|                             ├── .join_conds: (customer.c_custkey(v#0) = orders.o_custkey(v#9)) AND (true::boolean)                    |
|                             ├── (.output_columns): {v#0, v#8, v#9}                                                                    |
|                             ├── (.cardinality): 0.00                                                                                  |
|                             ├── PhysicalTableScan { .source: 1, (.output_columns): {v#0}, (.cardinality): 0.00 }                      |
|                             └── PhysicalProject                                                                                       |
|                                 ├── .projections:                                                                                     |
|                                 │   ┌── orders.o_orderkey(v#8) := orders.o_orderkey(v#8)                                              |
|                                 │   └── orders.o_custkey(v#9) := orders.o_custkey(v#9)                                                |
|                                 ├── (.output_columns): {v#8, v#9}                                                                     |
|                                 ├── (.cardinality): 0.00                                                                              |
|                                 └── PhysicalFilter                                                                                    |
|                                     ├── .predicate: orders.o_comment(v#16) NOT LIKE %special%requests%::utf8_view                     |
|                                     ├── (.output_columns): {v#8, v#9, v#16}                                                           |
|                                     ├── (.cardinality): 0.00                                                                          |
|                                     └── PhysicalTableScan { .source: 2, (.output_columns): {v#8, v#9, v#16}, (.cardinality): 0.00 }   |
+---------------------------------------------------------------------------------------------------------------------------------------+
*/

