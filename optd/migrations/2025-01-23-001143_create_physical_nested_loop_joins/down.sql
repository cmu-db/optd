-- Deregister the physical nested loop join operator.
DELETE FROM physical_op_descs where name = 'PhysicalNestedLoopJoin';

DROP TABLE physical_nested_loop_joins;

