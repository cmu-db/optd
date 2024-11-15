-- Setup Test Table
CREATE TABLE xxx (a INTEGER, b INTEGER);
INSERT INTO xxx VALUES (0, 0), (1, 1), (2, 2);
SELECT * FROM xxx WHERE a = 0;

/*
3
0 0
*/

-- (no id or description)
SELECT * FROM xxx WHERE a + 0 = b + 0;

/*
0 0
1 1
2 2

LogicalProjection { exprs: [ #0, #1 ] }
└── LogicalFilter
    ├── cond:Eq
    │   ├── Add
    │   │   ├── Cast { cast_to: Int64, child: #0 }
    │   │   └── 0(i64)
    │   └── Add
    │       ├── Cast { cast_to: Int64, child: #1 }
    │       └── 0(i64)
    └── LogicalScan { table: xxx }
PhysicalFilter
├── cond:Eq
│   ├── Add
│   │   ├── Cast { cast_to: Int64, child: #0 }
│   │   └── 0(i64)
│   └── Add
│       ├── Cast { cast_to: Int64, child: #1 }
│       └── 0(i64)
└── PhysicalScan { table: xxx }
*/

