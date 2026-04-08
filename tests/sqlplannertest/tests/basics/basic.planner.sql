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
Project
├── .table_index: 4
├── .projections: [ `t1`.`t1v1`(#1.0), `t1`.`t1v2`(#1.1), `t2`.`t2v1`(#2.0), `t2`.`t2v2`(#2.1), `t3`.`t3v1`(#3.0), `t3`.`t3v2`(#3.1) ]
├── (.output_columns): `__internal_#4`.`t1v1`(#4.0), `__internal_#4`.`t1v2`(#4.1), `__internal_#4`.`t2v1`(#4.2), `__internal_#4`.`t2v2`(#4.3), `__internal_#4`.`t3v1`(#4.4), `__internal_#4`.`t3v2`(#4.5)
├── (.cardinality): 1.44
└── Select
    ├── .predicate: `t1`.`t1v2`(#1.1) < 2::integer
    ├── (.output_columns): `t1`.`t1v1`(#1.0), `t1`.`t1v2`(#1.1), `t2`.`t2v1`(#2.0), `t2`.`t2v2`(#2.1), `t3`.`t3v1`(#3.0), `t3`.`t3v2`(#3.1)
    ├── (.cardinality): 1.44
    └── Join
        ├── .join_type: Inner
        ├── .implementation: None
        ├── .join_cond: (`t2`.`t2v2`(#2.1) = `t3`.`t3v1`(#3.0))
        ├── (.output_columns): `t1`.`t1v1`(#1.0), `t1`.`t1v2`(#1.1), `t2`.`t2v1`(#2.0), `t2`.`t2v2`(#2.1), `t3`.`t3v1`(#3.0), `t3`.`t3v2`(#3.1)
        ├── (.cardinality): 14.40
        ├── Join
        │   ├── .join_type: Inner
        │   ├── .implementation: None
        │   ├── .join_cond: (`t1`.`t1v1`(#1.0) = `t2`.`t2v1`(#2.0))
        │   ├── (.output_columns): `t1`.`t1v1`(#1.0), `t1`.`t1v2`(#1.1), `t2`.`t2v1`(#2.0), `t2`.`t2v2`(#2.1)
        │   ├── (.cardinality): 12.00
        │   ├── Get { .data_source_id: 1, .table_index: 1, .implementation: None, (.output_columns): `t1`.`t1v1`(#1.0), `t1`.`t1v2`(#1.1), (.cardinality): 10.00 }
        │   └── Get { .data_source_id: 2, .table_index: 2, .implementation: None, (.output_columns): `t2`.`t2v1`(#2.0), `t2`.`t2v2`(#2.1), (.cardinality): 3.00 }
        └── Get { .data_source_id: 3, .table_index: 3, .implementation: None, (.output_columns): `t3`.`t3v1`(#3.0), `t3`.`t3v2`(#3.1), (.cardinality): 3.00 }

physical_plan after optd-finalized:
Project
├── .table_index: 4
├── .projections: [ `t1`.`t1v1`(#1.0), `t1`.`t1v2`(#1.1), `t2`.`t2v1`(#2.0), `t2`.`t2v2`(#2.1), `t3`.`t3v1`(#3.0), `t3`.`t3v2`(#3.1) ]
├── (.output_columns): `__internal_#4`.`t1v1`(#4.0), `__internal_#4`.`t1v2`(#4.1), `__internal_#4`.`t2v1`(#4.2), `__internal_#4`.`t2v2`(#4.3), `__internal_#4`.`t3v1`(#4.4), `__internal_#4`.`t3v2`(#4.5)
├── (.cardinality): 1.44
└── Join
    ├── .join_type: Inner
    ├── .implementation: Some(Hash { build_side: Outer, keys: [(#2.1, #3.0)] })
    ├── .join_cond: `t2`.`t2v2`(#2.1) = `t3`.`t3v1`(#3.0)
    ├── (.output_columns): `t1`.`t1v1`(#1.0), `t1`.`t1v2`(#1.1), `t2`.`t2v1`(#2.0), `t2`.`t2v2`(#2.1), `t3`.`t3v1`(#3.0), `t3`.`t3v2`(#3.1)
    ├── (.cardinality): 1.44
    ├── Join
    │   ├── .join_type: Inner
    │   ├── .implementation: Some(Hash { build_side: Outer, keys: [(#1.0, #2.0)] })
    │   ├── .join_cond: `t1`.`t1v1`(#1.0) = `t2`.`t2v1`(#2.0)
    │   ├── (.output_columns): `t1`.`t1v1`(#1.0), `t1`.`t1v2`(#1.1), `t2`.`t2v1`(#2.0), `t2`.`t2v2`(#2.1)
    │   ├── (.cardinality): 1.20
    │   ├── Select { .predicate: `t1`.`t1v2`(#1.1) < 2::integer, (.output_columns): `t1`.`t1v1`(#1.0), `t1`.`t1v2`(#1.1), (.cardinality): 1.00 }
    │   │   └── Get { .data_source_id: 1, .table_index: 1, .implementation: None, (.output_columns): `t1`.`t1v1`(#1.0), `t1`.`t1v2`(#1.1), (.cardinality): 10.00 }
    │   └── Get { .data_source_id: 2, .table_index: 2, .implementation: None, (.output_columns): `t2`.`t2v1`(#2.0), `t2`.`t2v2`(#2.1), (.cardinality): 3.00 }
    └── Get { .data_source_id: 3, .table_index: 3, .implementation: None, (.output_columns): `t3`.`t3v1`(#3.0), `t3`.`t3v2`(#3.1), (.cardinality): 3.00 }

1 1 1 201 201 300
*/

