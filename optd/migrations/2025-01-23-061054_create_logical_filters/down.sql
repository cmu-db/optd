-- Deregisters the logical filter operator.
DELETE FROM logical_op_descs where name = 'LogicalFilter';

DROP TABLE logical_filters;

