-- Deregisters the logical join operator.
DELETE FROM logical_op_descs where name = 'LogicalJoin';

DROP TABLE logical_joins;
