-- Deregisters the logical scan operator.
DELETE FROM logical_op_descs where name = 'LogicalScan';

DROP TABLE logical_scans;
