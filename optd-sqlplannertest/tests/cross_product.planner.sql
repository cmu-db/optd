-- (no id or description)
create table t1(t1v1 int);
create table t2(t2v1 int);
insert into t1 values (0), (1), (2);
insert into t2 values (0), (1), (2);

/*
3
3
*/

-- Test optimizer logical for a cross product.
select * from t1, t2;

/*
LogicalProjection { exprs: [ #0, #1 ] }
└── LogicalJoin { join_type: Cross, cond: true }
    ├── LogicalScan { table: t1 }
    └── LogicalScan { table: t2 }
PhysicalProjection { exprs: [ #0, #1 ] }
└── PhysicalNestedLoopJoin { join_type: Cross, cond: true }
    ├── PhysicalScan { table: t1 }
    └── PhysicalScan { table: t2 }
0 0
0 1
0 2
1 0
1 1
1 2
2 0
2 1
2 2
*/

