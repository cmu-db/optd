-- Deregister the physical nested loop join operator.
DELETE FROM physical_op_kinds where name = 'PhysicalTableScan';

DROP TABLE physical_table_scans;
