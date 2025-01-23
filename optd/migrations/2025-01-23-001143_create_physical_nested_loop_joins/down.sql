-- Deregister the physical nested loop join operator.
DELETE FROM physical_op_descs where name = 'PhysicalNLJoin';

DROP TABLE physical_nljoins;

