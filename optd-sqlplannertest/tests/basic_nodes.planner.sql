-- (no id or description)
create table t1(t1v1 int, t1v2 int);
create table t2(t2v1 int, t2v3 int);
insert into t1 values (0, 0), (1, 1), (2, 2);
insert into t2 values (0, 200), (1, 201), (2, 202);

/*
3
3
*/

-- Test limit nodes
select * from t1 limit 1;
select * from t1 limit 3;
select * from t1 limit 5;

/*
LogicalLimit { skip: 0, fetch: 1 }
└── LogicalProjection { exprs: [ #0, #1 ] }
    └── LogicalScan { table: t1 }
PhysicalLimit { skip: 0, fetch: 1 }
└── PhysicalProjection { exprs: [ #0, #1 ] }
    └── PhysicalScan { table: t1 }
0 0
0 0
1 1
2 2
0 0
1 1
2 2
*/

