-- (no id or description)
create table t1(t1v1 int, t1v2 int);
create table t2(t2v1 int, t2v3 int);
insert into t1 values (0, 0), (1, 1), (2, 2);
insert into t2 values (0, 200), (1, 201), (2, 202);

/*
3
3
*/

-- Test EliminateLimitRule (with 0 limit clause)
select * from t1 LIMIT 0;

/*
LogicalLimit { skip: 0, fetch: 0 }
└── LogicalProjection { exprs: [ #0, #1 ] }
    └── LogicalScan { table: t1 }
PhysicalEmptyRelation { produce_one_row: false }
*/

