TODO make names make sense.

# Type Model

There are several concepts in the optimizer that we would like to model with strong types.

## Memo Table

The memo table will be stored on disk, which means it would be ideal if the in-memory structure of
memo table objects are exactly the same as the on-disk representation to allow for zero-copy
deserialization.

The memo data structure is a mutually recursive structure connected by identifiers (not pointers).
Relational expressions have group identifiers as children, and groups themselves represent a set of
equivalent "things" (where "things" can be logical expressions, physical expressions, or scalars).

There should be 1 overarching type to represent a node / object in the memo table, which I propose
should be called `MemoNode`. This means that `MemoNode` should be the representation that we use to
ingest and extract data to and from the persistent memo table.

```rust
/// A type representing an optimization node / object in the memo table.
pub enum MemoNode {
    Relation(Relation),
    Scalar(Scalar),
}

/// A type representing logical or physical operators in a relational algebraic query plan.
pub enum Relation {
    Logical(LogicalExpression),
    Physical(PhysicalExpression),
}

/// A type that represent scalar SQL expression / predicates.
///
/// TODO Add fields to this type.
pub struct Scalar;
```

Since we want to store both relational operators as well as scalar SQL expressions in the memo
table, we represent a `MemoNode` as sum type over a `Scalar` type and a `Relation` type, where
`Relation` is a sum over `LogicalExpression` and `PhysicalExpression`.

For `LogicalExpression` and `PhysicalExpression`, these are sum types representing all of the
possible relational operators, both logical and physical.

```rust
/// A type representing different kinds of logical expressions / operators.
pub enum LogicalExpression {
    Scan(Scan),
    Filter(Filter),
    Join(Join),
    Sort(Sort),
    <-- snip -->
}

/// A logical Filter expression / relation.
///
/// This type will have a dedicated SQL table associated with it.
struct Filter {
    /// A Filter relation has only 1 child.
    child: GroupId,
    /// Note that we use groups to handle equivalent SQL predicates as well.
    predicate: GroupId,
}

/// A logical Join expression / relation.
///
/// This type will have a dedicated SQL table associated with it.
struct Join {
    /// The left side of the Join relation.
    left: GroupId,
    /// The right side of the Join relation.
    right: GroupId,
    /// Note that we use groups to handle equivalent SQL predicates as well.
    condition: GroupId,
}

/// A type representing different kinds of physical expressions / operators.
pub enum PhysicalExpression {
    TableScan(TableScan),
    PhysicalFilter(PhysicalFilter),
    SortMergeJoin(SortMergeJoin),
    HashJoin(HashJoin),
    MergeSort(MergeSort),
    <-- snip -->
}
```

The goal is to have a table representing each variant, which would enable zero-copy deserialization
from the memo table into one of these expressions. This means that retrieving a `MemoNode` from the
memo table should be as simple as wrapping a `LogicalExpression` or `PhysicalExpression` in a
`Relation` and then a `MemoNode`, meaning CRUD operations should be very simple.

## Search

Since the memo table does not actually encode query plans directly (it encodes _groups_ of
relational expressions), we should not be using the same `MemoNode` type to model operations that
the search and rule engine need.

### Input and Output

There are a few concepts that we need to encode. The first is a type to represent the initial
input query plan. This should be an in-memory tree of _only_ logical operators.

The second is a type to represent the outgoing output query plan. This should be an in-memory tree
of _only_ physical operators.

These two types should probably look _similar_ to this type from the Rust Substrait crate:
https://docs.rs/substrait/latest/substrait/proto/rel/enum.RelType.html, but I am not sure of all the
information we need yet.

```rust
pub enum LogicalPlan {
    ???
}

pub enum PhysicalPlan {
    ???
}
```

### Rule Engine

The rule engine needs to support 4 main operations: a `check_pattern` operation and an `apply_rule`
operation for both transformation (logical -> logical) and implementation (logical -> physical)
rules.

From the previous semester, we know that rule matching takes a significant portion of optimization
time, which means that we need to be able to match patterns and apply rules quickly. We do not want
to traverse the memo table 2 times for every rule. Thus, we can introduce a sort of
**intermediate rule representation** for the rule engine. In fact, we should have 2 of these IRRs,
one that encodes the information needed for transformation rules and one that encodes the
information for implementation rules.

```rust
/// A type representing a tree of logical nodes, scalar nodes, and group IDs.
///
/// Note that group IDs must be leaves of this tree.
///
/// TODO Make this an actual tree with the correct modeling of types.
/// TODO Figure out exact semantics of this type.
pub enum PartialLogicalExpression {
    LogicalOperator(LogicalOperator),
    Scalar(Scalar),
    GroupId(GroupId),
}

/// A type representing a tree of logical nodes, physical nodes, scalar nodes, and group IDs.
///
/// Note that group IDs must be leaves of this tree, and that physical nodes cannot have children
/// that are logical nodes.
///
/// TODO Make this an actual tree with the correct modeling of types.
/// TODO Figure out exact semantics of this type.
pub enum PartialPhysicalExpression {
    LogicalOperator(LogicalOperator),
    PhysicalOperator(PhysicalOperator),
    Scalar(Scalar),
    GroupId(GroupId),
}
```

These types are used in this "interface" for rules (we would need to figure out where these
functions belong first):

```rust
/// Checks if the transformation rule matches the current expression and any of it's children.
/// Returns a vector of partially materialized plans.
///
/// This returns a vector because the rule matching the input root expression could have matched
/// with multiple child expressions.
///
/// For example, let's say the input expression is `Filter(G1)`, and the group G1 has two
/// expressions `e1 = Join(Join(A, B), C)` and `e2 = Join(A, Join(B, C))`.
///
/// If the rule wants to match against `Filter(Join(?L, ?R))`, then this function will partially
/// materialize two expressions `Filter(e1)` and `Filter(e2)`.
pub async fn check_transformation(
    &self,
    expr: LogicalExpression,
    rule: Rule,
) -> Vec<PartialLogicalExpression> {}

pub async fn check_implementation(
    &self,
    expr: LogicalExpression,
    rule: Rule,
) -> Vec<PartialPhysicalExpression> {}

pub fn apply_transformation(
    &mut self,
    expr: PartialLogicalExpression,
    rule: Rule,
) -> Vec<MemoNode> {}

pub fn apply_implementation(
    &mut self,
    expr: PartialPhysicalExpression,
    rule: Rule,
) -> Vec<MemoNode> {}

pub fn add_expressions(&mut self, new_exprs: Vec<MemoNode>) {}
```

## Pipeline Flow

TODO End-to-end description
