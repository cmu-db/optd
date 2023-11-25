-- sql1
CREATE TABLE t1(v1 int);

/*

*/

-- sql2
CREATE TABLE t2(v2 int);

/*

*/

-- Test whether join works correctly.
SELECT * FROM t1, t2 WHERE t1.v1 = t2.v2;

/*
plan_type plan
logical_plan "Inner Join: t1.v1 = t2.v2
  TableScan: t1 projection=[v1]
  TableScan: t2 projection=[v2]"
physical_plan "CoalesceBatchesExec: target_batch_size=8192
  HashJoinExec: mode=Partitioned, join_type=Inner, on=[(v1@0, v2@0)]
    CoalesceBatchesExec: target_batch_size=8192
      RepartitionExec: partitioning=Hash([v1@0], 10), input_partitions=10
        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
          MemoryExec: partitions=1, partition_sizes=[0]
    CoalesceBatchesExec: target_batch_size=8192
      RepartitionExec: partitioning=Hash([v2@0], 10), input_partitions=10
        RepartitionExec: partitioning=RoundRobinBatch(10), input_partitions=1
          MemoryExec: partitions=1, partition_sizes=[0]
"
*/

