-- (no id or description)
create table t1(t1v1 int, t1v2 int);
create table t2(t2v1 int, t2v2 int);
create table t3(t3v1 int, t3v2 int);
insert into t1 values (0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6), (7, 7), (8, 8), (9, 9);
insert into t2 values (0, 200), (1, 201), (2, 202);
insert into t3 values (201, 300), (202, 301), (203, 302);
set optd.optd_strict_mode = true;

/*
10
3
3
*/

-- Test representation.
select * from t1, t2, t3 where t1.t1v1 = t2.t2v1 and t1.t1v2 < 2 and t2.t2v2 = t3.t3v1;

/*
logical_plan after optd-initial:
+----------------------------------------------------------------------------------------------------------------------+
| LogicalJoin                                                                                                          |
| ├── .join_type: Inner                                                                                                |
| ├── .join_cond: (t2.t2v2(v#3) = t3.t3v1(v#4))                                                                        |
| ├── (.output_columns): {v#0, v#1, v#2, v#3, v#4, v#5}                                                                |
| ├── (.cardinality): 1.44                                                                                             |
| ├── LogicalJoin                                                                                                      |
| │   ├── .join_type: Inner                                                                                            |
| │   ├── .join_cond: (t1.t1v1(v#0) = t2.t2v1(v#2))                                                                    |
| │   ├── (.output_columns): {v#0, v#1, v#2, v#3}                                                                      |
| │   ├── (.cardinality): 1.20                                                                                         |
| │   ├── LogicalSelect { .predicate: t1.t1v2(v#1) < 2::integer, (.output_columns): {v#0, v#1}, (.cardinality): 1.00 } |
| │   │   └── LogicalGet { .source: 1, (.output_columns): {v#0, v#1}, (.cardinality): 10.00 }                          |
| │   └── LogicalGet { .source: 2, (.output_columns): {v#2, v#3}, (.cardinality): 3.00 }                               |
| └── LogicalGet { .source: 3, (.output_columns): {v#4, v#5}, (.cardinality): 3.00 }                                   |
+----------------------------------------------------------------------------------------------------------------------+

physical_plan after optd-finalized:
+-----------------------------------------------------------------------------------------------------------------------+
| PhysicalHashJoin                                                                                                      |
| ├── .join_type: Inner                                                                                                 |
| ├── .join_conds: (t2.t2v2(v#3) = t3.t3v1(v#4)) AND (true::boolean)                                                    |
| ├── (.output_columns): {v#0, v#1, v#2, v#3, v#4, v#5}                                                                 |
| ├── (.cardinality): 1.44                                                                                              |
| ├── PhysicalHashJoin                                                                                                  |
| │   ├── .join_type: Inner                                                                                             |
| │   ├── .join_conds: (t1.t1v1(v#0) = t2.t2v1(v#2)) AND (true::boolean)                                                |
| │   ├── (.output_columns): {v#0, v#1, v#2, v#3}                                                                       |
| │   ├── (.cardinality): 1.20                                                                                          |
| │   ├── PhysicalFilter { .predicate: t1.t1v2(v#1) < 2::integer, (.output_columns): {v#0, v#1}, (.cardinality): 1.00 } |
| │   │   └── PhysicalTableScan { .source: 1, (.output_columns): {v#0, v#1}, (.cardinality): 10.00 }                    |
| │   └── PhysicalTableScan { .source: 2, (.output_columns): {v#2, v#3}, (.cardinality): 3.00 }                         |
| └── PhysicalTableScan { .source: 3, (.output_columns): {v#4, v#5}, (.cardinality): 3.00 }                             |
+-----------------------------------------------------------------------------------------------------------------------+

1 1 1 201 201 300
*/

