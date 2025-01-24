-- Deregisters the logical filter operator.
DELETE FROM logical_op_kinds where name = 'LogicalFilter';

DROP TABLE logical_filters;

