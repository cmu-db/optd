-- (no id or description)
create table t1(t1v1 int, t1v2 int);
create table t2(t2v1 int, t2v3 int);
insert into t1 values (0, 0), (1, 1), (2, 2);
insert into t2 values (0, 200), (1, 201), (2, 202);

/*
3
3
*/

-- Test repr.
select * from t1, t2 where t1.t1v1 = t2.t2v1 and t1.t1v2 < 2;;

/*
logical_plan after optd-conversion:
+-----------------------------------------------+
| LogicalJoin                                   |
| ├── .join_type: Inner                         |
| ├── .join_cond: (t1.t1v1(v#0) = t2.t2v1(v#2)) |
| ├── (.output_columns): {v#0, v#1, v#2, v#3}   |
| ├── (.cardinality): 0.36                      |
| ├── LogicalSelect                             |
| │   ├── .predicate: t1.t1v2(v#1) < 2::integer |
| │   ├── (.output_columns): {v#0, v#1}         |
| │   ├── (.cardinality): 0.30                  |
| │   └── LogicalGet                            |
| │       ├── .source: 1                        |
| │       ├── (.output_columns): {v#0, v#1}     |
| │       └── (.cardinality): 3.00              |
| └── LogicalGet                                |
|     ├── .source: 2                            |
|     ├── (.output_columns): {v#2, v#3}         |
|     └── (.cardinality): 3.00                  |
+-----------------------------------------------+

physical_plan after optd-finalized:
+--------------------------------------------------------------------+
| PhysicalHashJoin                                                   |
| ├── .join_type: Inner                                              |
| ├── .join_conds: (t1.t1v1(v#0) = t2.t2v1(v#2)) AND (true::boolean) |
| ├── (.output_columns): {v#0, v#1, v#2, v#3}                        |
| ├── (.cardinality): 0.36                                           |
| ├── PhysicalFilter                                                 |
| │   ├── .predicate: t1.t1v2(v#1) < 2::integer                      |
| │   ├── (.output_columns): {v#0, v#1}                              |
| │   ├── (.cardinality): 0.30                                       |
| │   └── PhysicalTableScan                                          |
| │       ├── .source: 1                                             |
| │       ├── (.output_columns): {v#0, v#1}                          |
| │       └── (.cardinality): 3.00                                   |
| └── PhysicalTableScan                                              |
|     ├── .source: 2                                                 |
|     ├── (.output_columns): {v#2, v#3}                              |
|     └── (.cardinality): 3.00                                       |
+--------------------------------------------------------------------+

0 0 0 200
1 1 1 201
*/

