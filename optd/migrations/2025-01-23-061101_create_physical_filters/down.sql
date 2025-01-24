-- Deregisters the physical filter operator.
DELETE FROM physical_op_kinds where name = 'PhysicalFilter';

DROP TABLE physical_filters;

