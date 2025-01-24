-- Deregisters the logical scan operator.
DELETE FROM logical_op_kinds where name = 'LogicalScan';

DROP TABLE logical_scans;
