-- Deregister the physical nested loop join operator.
DELETE FROM physical_op_kinds where name = 'PhysicalNLJoin';

DROP TABLE physical_nljoins;

