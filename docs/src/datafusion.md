# Integration with Datafusion

optd is currently used as a physical optimizer for Apache Arrow Datafusion. To interact with Datafusion, you may use the following command to start the Datafusion cli.

```bash
cargo run --bin datafusion-optd-cli
cargo run --bin datafusion-optd-cli -- -f datafusion-optd-cli/tpch-sf0_01/test.sql # run TPC-H queries
```

optd is designed as a flexible optimizer framework that can be used in any database systems. The core of optd is in `optd-core`, which contains the Cascades optimizer implementation and the definition of key structures in the optimization process. Users can implement the interfaces and use optd in their own database systems by using the `optd-core` crate.

The optd Datafusion representation contains Datafusion plan nodes, SQL expressions, optimizer rules, properties, and cost models, as in the `optd-datafusion-repr` crate.

The `optd-datafusion-bridge` crate contains necessary code to convert Datafusion logical plans into optd Datafusion representation and convert optd Datafusion representation back into Datafusion physical plans. It implements the `QueryPlanner` trait so that it can be easily integrated into Datafusion.

![integration with Datafusion](./optd-cascades/optd-datafusion-overview.svg)

## Plan Nodes

This is an incomplete list of all Datafusion plan nodes and their representations that we have implemented in the system.

```
Join(type) left:PlanNode right:PlanNode cond:Expr
Projection expr_list:ExprList
Agg child:PlanNode expr_list:ExprList groups:ExprList
Scan table:String
ExprList ...children:Expr
Sort child:PlanNode sort_exprs:ExprList <- requiring SortExprs
... and others
```

Note that only `ExprList` or `List` can have variable number of children. All plan nodes only have a fixed number of children. For projections and aggregations where users will need to provide a list of expressions, they will have `List` node as their direct child.

Developers can use the `define_plan_node` macro to add new plan nodes into the optd-datafusion-repr.

```rust
#[derive(Clone, Debug)]
pub struct LogicalJoin(pub PlanNode);

define_plan_node!(
    LogicalJoin : PlanNode,
    Join, [
        { 0, left: PlanNode },
        { 1, right: PlanNode }
    ], [
        { 2, cond: Expr }
    ], { join_type: JoinType }
);
```

Developers will also need to add the plan node type into the `OptRelNodeTyp` enum, implement `is_plan_node` and `is_expression` for them, and implement the explain format in `explain`.

## Expressions

SQL Expressions are also a kind of `RelNode`. We have binary expressions, function calls, etc. in the representation.

Notably, we convert all column references into column indexes in the Datafusion bridge. For example, if Datafusion yields a logical plan of:

```
LogicalJoin { a = b }
  Scan t1 [a, v1, v2]
  Scan t2 [b, v3, v4]
```

It will be converted to:

```
LogicalJoin { #0 = #3 }
  Scan t1 
  Scan t2
```

in the optd representation.

For SQL expressions, the optd Datafusion representation does not do cost-based searches on expressions, though this is supported in optd-core. Each SQL expression can only have one binding in the current implementation.

## Explain

We use risinglightdb's pretty-xmlish crate and implement a custom explain format for Datafusion plan nodes.

```rust
PhysicalProjection { exprs: [ #0 ] }                                             
└── PhysicalHashJoin { join_type: Inner, left_keys: [ #0 ], right_keys: [ #0 ] } 
    ├── PhysicalProjection { exprs: [ #0 ] }                                     
    │   └── PhysicalScan { table: t1 }                                           
    └── PhysicalProjection { exprs: [ #0 ] }                                     
        └── PhysicalScan { table: t2 }
```

This is different from the default Lisp-representation of the `RelNode`.

## Rules

Currently, we have a few rules that pulls filters and projections up and down through joins. Also, we have join assoc and join commute rules to reorder the joins.

## Properties

We have the `Schema` property that will be used in the optimizer rules to determine number of columns of each plan nodes so that we can rewrite column reference expressions correctly.

## Cost Model

We have a simple cost model that computes I/O cost and compute cost based on number of rows of the children plan nodes.

## Cardinality Estimation

As per [Leis 2015](https://15721.courses.cs.cmu.edu/spring2024/papers/16-costmodels/p204-leis.pdf), we define cardinality estimation to be a separate component from the cost model. Statistics are considered a part of the cardinality estimation component. The internal name for our cardinality estimation component is Gungnir™. Gungnir is the mythical spear wielded by Odin which _adaptively_ changes course mid-air so as to never miss its mark. It represents both the accuracy and adaptivity of our cardinality estimation subsystem.

Our base cardinality estimation scheme is inspired by [Postgres](https://www.postgresql.org/docs/current/planner-stats-details.html). We utilize roughly the same four per-column statistics as Postgres: the most common values of that column, the # of distinct values of that column, the fraction of nulls of that column, and a distribution of values for that column. Our base predicate (filter or join) selectivity formulas are also the same as Postgres. This is as opposed to [Microsoft SQLServer](https://learn.microsoft.com/en-us/previous-versions/sql/sql-server-2008/dd535534(v=sql.100)?redirectedfrom=MSDN), for instance, which utilizes very different per-column statistics and predicate selectivity formulas. Our statistics are not exactly the same as Postgres though. For one, while Postgres uses a simple equi-height histogram, we utilize the more advanced T-Digest data structure to model the distribution of values. Additionally, Postgres samples its tables to build its statistics whereas we do a full sequential scan of all tables. This full sequential scan is made efficient by the fact that we use sketches, which have a low time complexity, and we implemented our sketching algorithms to be easily parallelizable.

## Statistics

We obtain our statistics with high parallelism and a minimal memory footprint, using probabilistic algorithms that trade off accuracy for scalability. Specifically:

1. The distribution of each column (i.e., CDF) is computed using the TDigest algorithm designed by [Ted Dunning et al.](https://arxiv.org/pdf/1902.04023.pdf), rather than traditional equi-width histograms. TDigests can be seen as dynamically resizable histograms that offer particular precision at the tails of the distribution.

2. The value of N-Distinct is calculated using [Philippe Flajolet et al.](https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf) HyperLogLog algorithm. Similarly, it estimates the unique number of values within a column by examining the pattern of the most unique element. This approach is memory-bounded and doesn't require a hash-set, unlike traditional methods.

3. (TODO @AlSchlo: Whatever we use for MCV)

All of these techniques are embarrassingly parallel tasks by design: threads first scan a partition of the data, maintain a memory-bounded structure (with higher memory leading to higher accuracy), and then merge their individual results into the final column statistics.

(TODO @CostModelTeam: explain adaptivity once we've implemented it)
