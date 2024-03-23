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
PhysicalProjection { exprs: [ #0 ], cost: weighted=1.06,row_cnt=1.00,compute=0.06,io=1.00 }
└── PhysicalScan { table: t1, cost: weighted=1.00,row_cnt=1.00,compute=0.00,io=1.00 }
*/

-- Test verbose explain with aggregation
select count(*) from t1;

/*
PhysicalProjection { exprs: [ #0 ], cost: weighted=21.18,row_cnt=1.00,compute=20.18,io=1.00 }
└── PhysicalAgg
    ├── aggrs:Agg(Count)
    │   └── [ 1 ]
    ├── groups: []
    ├── cost: weighted=21.12,row_cnt=1.00,compute=20.12,io=1.00
    └── PhysicalScan { table: t1, cost: weighted=1.00,row_cnt=1.00,compute=0.00,io=1.00 }
*/

