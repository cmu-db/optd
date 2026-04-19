-- Setup scalar feature tables.
create table numbers(id int, grp int, val int, bonus int, note varchar);
create table pairs(lhs int, rhs int);
insert into numbers values
  (1, 1, 10, 0, 'Alpha'),
  (2, 1, 20, 5, 'alphabet'),
  (3, 2, 15, 5, 'Beta'),
  (4, 2, 30, 10, 'gamma'),
  (5, 3, 8, 0, 'delta');
insert into pairs values
  (1, 1),
  (2, 1),
  (NULL, NULL),
  (3, NULL);
set optd.optd_strict_mode = true;
set optd.optd_only = true;

/*
5
4
*/

-- Arithmetic projections with aliases and modulo.
select id, val + bonus as total, val % 7 as remainder
from numbers
order by id;

/*
logical_plan after optd-initial:
OrderBy
├── ordering_exprs: "__#2.id"(#2.0) ASC
├── (.output_columns):
│   ┌── "__#2.id"(#2.0)
│   ├── "__#2.remainder"(#2.2)
│   └── "__#2.total"(#2.1)
├── (.cardinality): 5.00
└── Project
    ├── .table_index: 2
    ├── .projections:
    │   ┌── "numbers.id"(#1.0)
    │   ├── "numbers.val"(#1.2) + "numbers.bonus"(#1.3)
    │   └── CAST ("numbers.val"(#1.2) AS Int64) % 7::bigint
    ├── (.output_columns):
    │   ┌── "__#2.id"(#2.0)
    │   ├── "__#2.remainder"(#2.2)
    │   └── "__#2.total"(#2.1)
    ├── (.cardinality): 5.00
    └── Get
        ├── .data_source_id: 1
        ├── .table_index: 1
        ├── .implementation: None
        ├── (.output_columns):
        │   ┌── "numbers.bonus"(#1.3)
        │   ├── "numbers.grp"(#1.1)
        │   ├── "numbers.id"(#1.0)
        │   ├── "numbers.note"(#1.4)
        │   └── "numbers.val"(#1.2)
        └── (.cardinality): 5.00

physical_plan after optd-finalized:
EnforcerSort
├── tuple_ordering: [(#2.0, Asc)]
├── (.output_columns):
│   ┌── "__#2.id"(#2.0)
│   ├── "__#2.remainder"(#2.2)
│   └── "__#2.total"(#2.1)
├── (.cardinality): 5.00
└── Project
    ├── .table_index: 2
    ├── .projections:
    │   ┌── "numbers.id"(#1.0)
    │   ├── "numbers.val"(#1.2) + "numbers.bonus"(#1.3)
    │   └── CAST ("numbers.val"(#1.2) AS Int64) % 7::bigint
    ├── (.output_columns):
    │   ┌── "__#2.id"(#2.0)
    │   ├── "__#2.remainder"(#2.2)
    │   └── "__#2.total"(#2.1)
    ├── (.cardinality): 5.00
    └── Get
        ├── .data_source_id: 1
        ├── .table_index: 1
        ├── .implementation: None
        ├── (.output_columns):
        │   ┌── "numbers.bonus"(#1.3)
        │   ├── "numbers.id"(#1.0)
        │   └── "numbers.val"(#1.2)
        └── (.cardinality): 5.00

1 10 3
2 25 6
3 20 1
4 40 2
5 8 1
*/

-- Casts in projections and disjunctive filters.
select id, cast(val as bigint) + cast(bonus as bigint) as widened_total
from numbers
where cast(val as bigint) >= 20 or cast(bonus as bigint) = 5
order by id;

/*
logical_plan after optd-initial:
OrderBy
├── ordering_exprs: "__#2.id"(#2.0) ASC
├── (.output_columns): [ "__#2.id"(#2.0), "__#2.widened_total"(#2.1) ]
├── (.cardinality): 0.50
└── Project
    ├── .table_index: 2
    ├── .projections: [ "numbers.id"(#1.0), CAST ("numbers.val"(#1.2) AS Int64) + CAST ("numbers.bonus"(#1.3) AS Int64) ]
    ├── (.output_columns): [ "__#2.id"(#2.0), "__#2.widened_total"(#2.1) ]
    ├── (.cardinality): 0.50
    └── Select
        ├── .predicate: (CAST ("numbers.val"(#1.2) AS Int64) >= 20::bigint) OR (CAST ("numbers.bonus"(#1.3) AS Int64) = 5::bigint)
        ├── (.output_columns):
        │   ┌── "numbers.bonus"(#1.3)
        │   ├── "numbers.grp"(#1.1)
        │   ├── "numbers.id"(#1.0)
        │   ├── "numbers.note"(#1.4)
        │   └── "numbers.val"(#1.2)
        ├── (.cardinality): 0.50
        └── Get
            ├── .data_source_id: 1
            ├── .table_index: 1
            ├── .implementation: None
            ├── (.output_columns):
            │   ┌── "numbers.bonus"(#1.3)
            │   ├── "numbers.grp"(#1.1)
            │   ├── "numbers.id"(#1.0)
            │   ├── "numbers.note"(#1.4)
            │   └── "numbers.val"(#1.2)
            └── (.cardinality): 5.00

physical_plan after optd-finalized:
EnforcerSort
├── tuple_ordering: [(#2.0, Asc)]
├── (.output_columns): [ "__#2.id"(#2.0), "__#2.widened_total"(#2.1) ]
├── (.cardinality): 0.50
└── Project
    ├── .table_index: 2
    ├── .projections:
    │   ┌── "numbers.id"(#1.0)
    │   └── CAST ("numbers.val"(#1.2) AS Int64) + CAST ("numbers.bonus"(#1.3) AS Int64)
    ├── (.output_columns): [ "__#2.id"(#2.0), "__#2.widened_total"(#2.1) ]
    ├── (.cardinality): 0.50
    └── Select
        ├── .predicate: ("numbers.val"(#1.2) >= 20::integer) OR ("numbers.bonus"(#1.3) = 5::integer)
        ├── (.output_columns): [ "numbers.bonus"(#1.3), "numbers.id"(#1.0), "numbers.val"(#1.2) ]
        ├── (.cardinality): 0.50
        └── Get
            ├── .data_source_id: 1
            ├── .table_index: 1
            ├── .implementation: None
            ├── (.output_columns):
            │   ┌── "numbers.bonus"(#1.3)
            │   ├── "numbers.id"(#1.0)
            │   └── "numbers.val"(#1.2)
            └── (.cardinality): 5.00

2 25
3 20
4 40
*/

-- LIKE and NOT LIKE predicates.
select id, note
from numbers
where note like 'Al%' or note not like '%ta'
order by id;

/*
logical_plan after optd-initial:
OrderBy { ordering_exprs: "__#2.id"(#2.0) ASC, (.output_columns): [ "__#2.id"(#2.0), "__#2.note"(#2.1) ], (.cardinality): 0.50 }
└── Project
    ├── .table_index: 2
    ├── .projections: [ "numbers.id"(#1.0), "numbers.note"(#1.4) ]
    ├── (.output_columns): [ "__#2.id"(#2.0), "__#2.note"(#2.1) ]
    ├── (.cardinality): 0.50
    └── Select
        ├── .predicate: ("numbers.note"(#1.4) LIKE CAST ('Al%'::utf8 AS Utf8View)) OR ("numbers.note"(#1.4) NOT LIKE CAST ('%ta'::utf8 AS Utf8View))
        ├── (.output_columns): [ "numbers.bonus"(#1.3), "numbers.grp"(#1.1), "numbers.id"(#1.0), "numbers.note"(#1.4), "numbers.val"(#1.2) ]
        ├── (.cardinality): 0.50
        └── Get
            ├── .data_source_id: 1
            ├── .table_index: 1
            ├── .implementation: None
            ├── (.output_columns): [ "numbers.bonus"(#1.3), "numbers.grp"(#1.1), "numbers.id"(#1.0), "numbers.note"(#1.4), "numbers.val"(#1.2) ]
            └── (.cardinality): 5.00

physical_plan after optd-finalized:
EnforcerSort
├── tuple_ordering: [(#2.0, Asc)]
├── (.output_columns): [ "__#2.id"(#2.0), "__#2.note"(#2.1) ]
├── (.cardinality): 0.50
└── Project
    ├── .table_index: 2
    ├── .projections: [ "numbers.id"(#1.0), "numbers.note"(#1.4) ]
    ├── (.output_columns): [ "__#2.id"(#2.0), "__#2.note"(#2.1) ]
    ├── (.cardinality): 0.50
    └── Select
        ├── .predicate: ("numbers.note"(#1.4) LIKE 'Al%'::utf8_view) OR ("numbers.note"(#1.4) NOT LIKE '%ta'::utf8_view)
        ├── (.output_columns): [ "numbers.id"(#1.0), "numbers.note"(#1.4) ]
        ├── (.cardinality): 0.50
        └── Get
            ├── .data_source_id: 1
            ├── .table_index: 1
            ├── .implementation: None
            ├── (.output_columns): [ "numbers.id"(#1.0), "numbers.note"(#1.4) ]
            └── (.cardinality): 5.00

1 Alpha
2 alphabet
4 gamma
*/

-- Case insensitive LIKE predicates.
select id
from numbers
where note ilike 'al%'
order by id;

/*
logical_plan after optd-initial:
OrderBy
├── ordering_exprs: "__#2.id"(#2.0) ASC
├── (.output_columns): "__#2.id"(#2.0)
├── (.cardinality): 0.50
└── Project
    ├── .table_index: 2
    ├── .projections: "numbers.id"(#1.0)
    ├── (.output_columns): "__#2.id"(#2.0)
    ├── (.cardinality): 0.50
    └── Select
        ├── .predicate: "numbers.note"(#1.4) ILIKE CAST ('al%'::utf8 AS Utf8View)
        ├── (.output_columns):
        │   ┌── "numbers.bonus"(#1.3)
        │   ├── "numbers.grp"(#1.1)
        │   ├── "numbers.id"(#1.0)
        │   ├── "numbers.note"(#1.4)
        │   └── "numbers.val"(#1.2)
        ├── (.cardinality): 0.50
        └── Get
            ├── .data_source_id: 1
            ├── .table_index: 1
            ├── .implementation: None
            ├── (.output_columns):
            │   ┌── "numbers.bonus"(#1.3)
            │   ├── "numbers.grp"(#1.1)
            │   ├── "numbers.id"(#1.0)
            │   ├── "numbers.note"(#1.4)
            │   └── "numbers.val"(#1.2)
            └── (.cardinality): 5.00

physical_plan after optd-finalized:
EnforcerSort
├── tuple_ordering: [(#2.0, Asc)]
├── (.output_columns): "__#2.id"(#2.0)
├── (.cardinality): 0.50
└── Project
    ├── .table_index: 2
    ├── .projections: "numbers.id"(#1.0)
    ├── (.output_columns): "__#2.id"(#2.0)
    ├── (.cardinality): 0.50
    └── Select
        ├── .predicate: "numbers.note"(#1.4) ILIKE 'al%'::utf8_view
        ├── (.output_columns):
        │   ┌── "numbers.id"(#1.0)
        │   └── "numbers.note"(#1.4)
        ├── (.cardinality): 0.50
        └── Get
            ├── .data_source_id: 1
            ├── .table_index: 1
            ├── .implementation: None
            ├── (.output_columns):
            │   ┌── "numbers.id"(#1.0)
            │   └── "numbers.note"(#1.4)
            └── (.cardinality): 5.00

1
2
*/

-- IS NOT DISTINCT FROM including NULL-safe equality.
select lhs, rhs
from pairs
where lhs is not distinct from rhs
order by lhs;

/*
logical_plan after optd-initial:
OrderBy
├── ordering_exprs: "__#2.lhs"(#2.0) ASC
├── (.output_columns): [ "__#2.lhs"(#2.0), "__#2.rhs"(#2.1) ]
├── (.cardinality): 0.40
└── Project
    ├── .table_index: 2
    ├── .projections: [ "pairs.lhs"(#1.0), "pairs.rhs"(#1.1) ]
    ├── (.output_columns): [ "__#2.lhs"(#2.0), "__#2.rhs"(#2.1) ]
    ├── (.cardinality): 0.40
    └── Select
        ├── .predicate: "pairs.lhs"(#1.0) IS NOT DISTINCT FROM "pairs.rhs"(#1.1)
        ├── (.output_columns): [ "pairs.lhs"(#1.0), "pairs.rhs"(#1.1) ]
        ├── (.cardinality): 0.40
        └── Get
            ├── .data_source_id: 2
            ├── .table_index: 1
            ├── .implementation: None
            ├── (.output_columns): [ "pairs.lhs"(#1.0), "pairs.rhs"(#1.1) ]
            └── (.cardinality): 4.00

physical_plan after optd-finalized:
EnforcerSort
├── tuple_ordering: [(#2.0, Asc)]
├── (.output_columns): [ "__#2.lhs"(#2.0), "__#2.rhs"(#2.1) ]
├── (.cardinality): 0.40
└── Project
    ├── .table_index: 2
    ├── .projections: [ "pairs.lhs"(#1.0), "pairs.rhs"(#1.1) ]
    ├── (.output_columns): [ "__#2.lhs"(#2.0), "__#2.rhs"(#2.1) ]
    ├── (.cardinality): 0.40
    └── Select
        ├── .predicate: "pairs.lhs"(#1.0) IS NOT DISTINCT FROM "pairs.rhs"(#1.1)
        ├── (.output_columns): [ "pairs.lhs"(#1.0), "pairs.rhs"(#1.1) ]
        ├── (.cardinality): 0.40
        └── Get
            ├── .data_source_id: 2
            ├── .table_index: 1
            ├── .implementation: None
            ├── (.output_columns): [ "pairs.lhs"(#1.0), "pairs.rhs"(#1.1) ]
            └── (.cardinality): 4.00

1 1
NULL NULL
*/

-- ORDER BY with LIMIT and OFFSET.
select id
from numbers
order by val desc
limit 2 offset 1;

/*
logical_plan after optd-initial:
Limit
├── .skip: 1::bigint
├── .fetch: 2::bigint
├── (.output_columns): "__#3.id"(#3.0)
├── (.cardinality): 2.00
└── Project
    ├── .table_index: 3
    ├── .projections: "__#2.id"(#2.0)
    ├── (.output_columns): "__#3.id"(#3.0)
    ├── (.cardinality): 5.00
    └── OrderBy
        ├── ordering_exprs: "__#2.val"(#2.1) DESC
        ├── (.output_columns):
        │   ┌── "__#2.id"(#2.0)
        │   └── "__#2.val"(#2.1)
        ├── (.cardinality): 5.00
        └── Project
            ├── .table_index: 2
            ├── .projections:
            │   ┌── "numbers.id"(#1.0)
            │   └── "numbers.val"(#1.2)
            ├── (.output_columns):
            │   ┌── "__#2.id"(#2.0)
            │   └── "__#2.val"(#2.1)
            ├── (.cardinality): 5.00
            └── Get
                ├── .data_source_id: 1
                ├── .table_index: 1
                ├── .implementation: None
                ├── (.output_columns):
                │   ┌── "numbers.bonus"(#1.3)
                │   ├── "numbers.grp"(#1.1)
                │   ├── "numbers.id"(#1.0)
                │   ├── "numbers.note"(#1.4)
                │   └── "numbers.val"(#1.2)
                └── (.cardinality): 5.00

physical_plan after optd-finalized:
Project
├── .table_index: 3
├── .projections: "__#2.id"(#2.0)
├── (.output_columns): "__#3.id"(#3.0)
├── (.cardinality): 2.00
└── Limit
    ├── .skip: 1::bigint
    ├── .fetch: 2::bigint
    ├── (.output_columns):
    │   ┌── "__#2.id"(#2.0)
    │   └── "__#2.val"(#2.1)
    ├── (.cardinality): 2.00
    └── EnforcerSort
        ├── tuple_ordering: [(#2.1, Desc)]
        ├── (.output_columns):
        │   ┌── "__#2.id"(#2.0)
        │   └── "__#2.val"(#2.1)
        ├── (.cardinality): 5.00
        └── Project
            ├── .table_index: 2
            ├── .projections:
            │   ┌── "numbers.id"(#1.0)
            │   └── "numbers.val"(#1.2)
            ├── (.output_columns):
            │   ┌── "__#2.id"(#2.0)
            │   └── "__#2.val"(#2.1)
            ├── (.cardinality): 5.00
            └── Get
                ├── .data_source_id: 1
                ├── .table_index: 1
                ├── .implementation: None
                ├── (.output_columns):
                │   ┌── "numbers.id"(#1.0)
                │   └── "numbers.val"(#1.2)
                └── (.cardinality): 5.00

2
3
*/

