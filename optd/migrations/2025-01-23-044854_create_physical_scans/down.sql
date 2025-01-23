-- Deregister the physical nested loop join operator.
DELETE FROM physical_op_descs where name = 'PhysicalScan';

DROP TABLE physical_scans;
