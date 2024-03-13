-- (no id or description)
create table t1(v1 int);
insert into t1 values (0), (1), (2), (3);

/*
4
*/

-- Test non-verbose explain
select * from t1;

/*
PhysicalProjection { exprs: [ #0 ] }
└── PhysicalScan { table: t1 }
*/

-- Test verbose explain
select * from t1;

/*
PhysicalProjection { exprs: [ #0 ], rows: 1 }
└── PhysicalScan { table: t1, rows: 1 }
*/

-- Test verbose explain
select count(*) from t1;

/*
PhysicalProjection { exprs: [ #0 ], rows: 1 }
└── PhysicalAgg
    ├── aggrs:Agg(Count)
    │   └── [ 1 ]
    ├── groups: []
    ├── rows: 1
    └── PhysicalScan { table: t1, rows: 1 }
*/

