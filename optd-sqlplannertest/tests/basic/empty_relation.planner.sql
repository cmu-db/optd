-- (no id or description)
create table t1(t1v1 int, t1v2 int);
create table t2(t2v1 int, t2v3 int);
insert into t1 values (0, 0), (1, 1), (2, 2);
insert into t2 values (0, 200), (1, 201), (2, 202);

/*
3
3
*/

-- Test whether the optimizer handles empty relation correctly.
select 64 + 1;
select 64 + 1 from t1;

/*
LogicalProjection
├── exprs:Add
│   ├── 64(i64)
│   └── 1(i64)
└── LogicalEmptyRelation { produce_one_row: true }
PhysicalProjection
├── exprs:Add
│   ├── 64(i64)
│   └── 1(i64)
└── PhysicalEmptyRelation { produce_one_row: true }
65
65
65
65
*/

-- Test whether the optimizer eliminates join to empty relation
select * from t1 inner join t2 on false;
select 64+1 from t1 inner join t2 on false;

/*
LogicalProjection { exprs: [ #0, #1, #2, #3 ] }
└── LogicalJoin { join_type: Inner, cond: false }
    ├── LogicalScan { table: t1 }
    └── LogicalScan { table: t2 }
LogicalEmptyRelation { produce_one_row: false }
PhysicalEmptyRelation { produce_one_row: false }
*/

