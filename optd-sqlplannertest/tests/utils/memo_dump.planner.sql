-- (no id or description)
create table t1(t1v1 int, t1v2 int);
create table t2(t2v1 int, t2v3 int);
insert into t1 values (0, 0), (1, 1), (2, 2);
insert into t2 values (0, 200), (1, 201), (2, 202);

/*
3
3
*/

-- test self join
select * from t1 as a, t1 as b where a.t1v1 = b.t1v1 order by a.t1v1;

/*
PhysicalSort
├── exprs:SortOrder { order: Asc }
│   └── #0
└── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] }
    ├── PhysicalScan { table: t1 }
    └── PhysicalScan { table: t1 }
group_id=!2 winner=23 weighted_cost=1000 cost={compute=0,io=1000} stat={row_cnt=1000} | (PhysicalScan P0)
  schema=[t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1]
  expr_id=1 | (Scan P0)
  expr_id=23 | (PhysicalScan P0)
  P0=(Constant(Utf8String) "t1")
  step=1/6 apply_rule group_id=!2 applied_expr_id=1 produced_expr_id=23 rule_id=0
  step=1/7 decide_winner group_id=!2 proposed_winner_expr=23 children_winner_exprs=[] total_weighted_cost=1000
  step=2/1 decide_winner group_id=!2 proposed_winner_expr=23 children_winner_exprs=[] total_weighted_cost=1000
group_id=!6 winner=21 weighted_cost=1003000 cost={compute=1001000,io=2000} stat={row_cnt=10000} | (PhysicalNestedLoopJoin(Inner) !2 !2 P4)
  schema=[t1v1:Int32, t1v2:Int32, t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1, t1.0, t1.1]
  expr_id=5 | (Join(Inner) !2 !2 P4)
  expr_id=21 | (PhysicalNestedLoopJoin(Inner) !2 !2 P4)
  expr_id=42 | (Projection !6 P32)
  expr_id=49 | (Projection !6 P37)
  P4=(Constant(Bool) true)
  P32=(List (ColumnRef 2(u64)) (ColumnRef 3(u64)) (ColumnRef 0(u64)) (ColumnRef 1(u64)))
  P37=(List (ColumnRef 0(u64)) (ColumnRef 1(u64)) (ColumnRef 2(u64)) (ColumnRef 3(u64)))
  step=1/1 apply_rule group_id=!6 applied_expr_id=5 produced_expr_id=5 rule_id=19
  step=1/5 apply_rule group_id=!6 applied_expr_id=5 produced_expr_id=21 rule_id=2
  step=1/8 decide_winner group_id=!6 proposed_winner_expr=21 children_winner_exprs=[23,23] total_weighted_cost=1003000
  step=2/9 apply_rule group_id=!6 applied_expr_id=5 produced_expr_id=42 rule_id=13
  step=2/10 apply_rule group_id=!6 applied_expr_id=42 produced_expr_id=49 rule_id=17
  step=2/11 apply_rule group_id=!6 applied_expr_id=49 produced_expr_id=42 rule_id=17
  step=2/12 apply_rule group_id=!6 applied_expr_id=49 produced_expr_id=49 rule_id=17
group_id=!12 winner=17 weighted_cost=11908.75477931522 cost={compute=9908.75477931522,io=2000} stat={row_cnt=1000} | (PhysicalSort !31 P10)
  schema=[t1v1:Int32, t1v2:Int32, t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1, t1.0, t1.1]
  expr_id=11 | (Sort !31 P10)
  expr_id=17 | (PhysicalSort !31 P10)
  P10=(List (SortOrder(Asc) (ColumnRef 0(u64))))
  step=1/3 apply_rule group_id=!12 applied_expr_id=11 produced_expr_id=17 rule_id=4
  step=1/13 decide_winner group_id=!12 proposed_winner_expr=17 children_winner_exprs=[28] total_weighted_cost=11908.75477931522
  step=2/28 decide_winner group_id=!12 proposed_winner_expr=17 children_winner_exprs=[28] total_weighted_cost=11908.75477931522
group_id=!31 winner=28 weighted_cost=5000 cost={compute=3000,io=2000} stat={row_cnt=1000} | (PhysicalHashJoin(Inner) !2 !2 P26 P26)
  schema=[t1v1:Int32, t1v2:Int32, t1v1:Int32, t1v2:Int32]
  column_ref=[t1.0, t1.1, t1.0, t1.1]
  expr_id=8 | (Filter !6 P7)
  expr_id=15 | (Join(Inner) !2 !2 P7)
  expr_id=19 | (PhysicalFilter !6 P7)
  expr_id=25 | (PhysicalNestedLoopJoin(Inner) !2 !2 P7)
  expr_id=28 | (PhysicalHashJoin(Inner) !2 !2 P26 P26)
  expr_id=30 | (Join(Inner) !2 !2 P29)
  expr_id=33 | (Projection !31 P32)
  expr_id=38 | (Projection !31 P37)
  expr_id=45 | (Filter !6 P29)
  expr_id=58 | (PhysicalProjection !31 P32)
  expr_id=60 | (PhysicalNestedLoopJoin(Inner) !2 !2 P29)
  expr_id=71 | (PhysicalProjection !31 P37)
  expr_id=73 | (PhysicalFilter !6 P29)
  P7=(BinOp(Eq) (ColumnRef 0(u64)) (ColumnRef 2(u64)))
  P26=(List (ColumnRef 0(u64)))
  P29=(BinOp(Eq) (ColumnRef 2(u64)) (ColumnRef 0(u64)))
  P32=(List (ColumnRef 2(u64)) (ColumnRef 3(u64)) (ColumnRef 0(u64)) (ColumnRef 1(u64)))
  P37=(List (ColumnRef 0(u64)) (ColumnRef 1(u64)) (ColumnRef 2(u64)) (ColumnRef 3(u64)))
  step=1/2 apply_rule group_id=!9 applied_expr_id=8 produced_expr_id=15 rule_id=9
  step=1/4 apply_rule group_id=!9 applied_expr_id=8 produced_expr_id=19 rule_id=3
  step=1/9 decide_winner group_id=!9 proposed_winner_expr=19 children_winner_exprs=[21] total_weighted_cost=1033000
  step=1/10 apply_rule group_id=!9 applied_expr_id=15 produced_expr_id=25 rule_id=2
  step=1/11 apply_rule group_id=!9 applied_expr_id=15 produced_expr_id=28 rule_id=12
  step=1/12 decide_winner group_id=!9 proposed_winner_expr=28 children_winner_exprs=[23,23] total_weighted_cost=5000
  step=2/2 decide_winner group_id=!9 proposed_winner_expr=28 children_winner_exprs=[23,23] total_weighted_cost=5000
  step=2/3 apply_rule group_id=!9 applied_expr_id=15 produced_expr_id=33 rule_id=13
  step=2/4 apply_rule group_id=!31 applied_expr_id=30 produced_expr_id=36 rule_id=13
  step=2/5 apply_rule group_id=!31 applied_expr_id=36 produced_expr_id=38 rule_id=17
  step=2/6 apply_rule group_id=!31 applied_expr_id=38 produced_expr_id=36 rule_id=17
  step=2/7 apply_rule group_id=!31 applied_expr_id=38 produced_expr_id=38 rule_id=17
  step=2/8 apply_rule group_id=!31 applied_expr_id=36 produced_expr_id=45 rule_id=21
  step=2/13 apply_rule group_id=!31 applied_expr_id=45 produced_expr_id=36 rule_id=8
  step=2/14 apply_rule group_id=!31 applied_expr_id=45 produced_expr_id=38 rule_id=8
  step=2/15 apply_rule group_id=!31 applied_expr_id=45 produced_expr_id=30 rule_id=9
  step=2/16 apply_rule group_id=!9 applied_expr_id=33 produced_expr_id=58 rule_id=1
  step=2/17 apply_rule group_id=!31 applied_expr_id=30 produced_expr_id=60 rule_id=2
  step=2/18 apply_rule group_id=!31 applied_expr_id=30 produced_expr_id=28 rule_id=12
  step=2/19 decide_winner group_id=!31 proposed_winner_expr=28 children_winner_exprs=[23,23] total_weighted_cost=5000
  step=2/20 apply_rule group_id=!31 applied_expr_id=33 produced_expr_id=38 rule_id=17
  step=2/21 apply_rule group_id=!31 applied_expr_id=33 produced_expr_id=33 rule_id=17
  step=2/22 apply_rule group_id=!31 applied_expr_id=33 produced_expr_id=45 rule_id=21
  step=2/23 apply_rule group_id=!31 applied_expr_id=33 produced_expr_id=8 rule_id=21
  step=2/24 apply_rule group_id=!31 applied_expr_id=36 produced_expr_id=58 rule_id=1
  step=2/25 apply_rule group_id=!31 applied_expr_id=38 produced_expr_id=71 rule_id=1
  step=2/26 apply_rule group_id=!31 applied_expr_id=45 produced_expr_id=73 rule_id=3
  step=2/27 decide_winner group_id=!9 proposed_winner_expr=58 children_winner_exprs=[28] total_weighted_cost=10000
*/

