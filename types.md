TODO make names make sense.

# Type Model

There are several concepts in the optimizer that we would like to model with strong types.

## Proposed Glossary

In memo table land:

- Expression: A logical, physical, or scalar operator in the mutually recursive memo table structure
- Relation(al) Expression: A logical or physical expression in the memo table
- Logical Expression: A logical relational operator with group identifiers as children
- Physical Expression: A physical relational operator with group identifiers as children
- Scalar Expression: A scalar operator, otherwise known as SQL expression or a SQL predicate

In search / plan enumeration land:

- Logical Plan: The initial in-memory input logical plan of operators (entrypoint for optimization)
- Physical Plan: The final in-memory output physical plan of operators (materialized from memo)
- Logical Operator: An in-memory logical node in a logical plan
- Physical Operator: An in-memory physical node in a physical plan
- (TODO) Partial Logical Plan: Intermediate state for transformation rule matching and application
- (TODO) Partial Physical Plan: Intermediate state for implementation rule matching and application

## Memo Table

The memo table will be stored on disk, which means it would be ideal if the in-memory structure of
memo table objects is exactly the same as the on-disk representation to allow for zero-copy
deserialization.

The memo data structure is a mutually recursive structure connected by identifiers (not pointers).
Expressions have group identifiers as children, and groups themselves represent a set of equivalent
"things" (where "things" can be logical, physical, or scalar expressions).

There should be 1 overarching type to represent a relation / operator in the memo table, which I
propose should be called `Expression`. This means that `Expression` should be the representation
that we use to ingest and extract data to and from the persistent memo table. This is _different_
from the representation we use to ingest the initial `LogicalPlan` and output the final
`PhysicalPlan`.

```rust
/// A type representing an optimization operator in the memo table.
pub enum Expression {
    RelationExpression(RelationExpression),
    ScalarExpression(ScalarExpression),
}

/// A type representing logical or physical expressions in the memo table.
pub enum RelationExpression {
    LogicalExpression(LogicalExpression),
    PhysicalExpression(PhysicalExpression),
}

/// A type that represent scalar SQL expression / predicates.
///
/// TODO Add fields to this type.
pub struct Scalar;
```

Since we want to store both relational operators as well as scalar SQL expressions in the memo
table, we represent a `Expression` as sum type over a `ScalarExpression` type and a
`RelationExpression` type, where `RelationExpression` is a sum over `LogicalExpression` and
`PhysicalExpression`.

`LogicalExpression` and `PhysicalExpression` are sum types representing all of the possible
relational operators, both logical and physical.

```rust
/// A type representing different kinds of logical expressions in the memo table.
pub enum LogicalExpression {
    Scan(Scan),
    Filter(Filter),
    Join(Join),
    Sort(Sort),
}

/// A logical `Filter` operator.
///
/// This type will have a dedicated SQL table associated with it.
struct Filter {
    /// A `Filter` relation has only 1 child.
    child: GroupId,
    /// Note that we use groups to handle equivalent SQL predicates as well.
    predicate: GroupId,
}

/// A logical `Join` operator.
///
/// This type will have a dedicated SQL table associated with it.
struct Join {
    /// The left side of the `Join` relation.
    left: GroupId,
    /// The right side of the `Join` relation.
    right: GroupId,
    /// Note that we use groups to handle equivalent SQL predicates as well.
    condition: GroupId,
}

/// A type representing different kinds of physical expressions in the memo table.
pub enum PhysicalExpression {
    TableScan(TableScan),
    PhysicalFilter(PhysicalFilter),
    SortMergeJoin(SortMergeJoin),
    HashJoin(HashJoin),
    MergeSort(MergeSort),
}
```

The goal is to have a table representing each variant, which would enable zero-copy deserialization
from the memo table into one of these expressions. This means that retrieving a `Expression`
from the memo table should be as simple as wrapping a `LogicalExpression` or `PhysicalExpression` in
a `RelationExpression` and then a `Expression`, meaning CRUD operations should be very simple.

## Search

Since the memo table does not actually encode query plans directly (it encodes _groups_ of
relational expressions), we should not be using the same `Expression` type to model operations
that the search and rule engine need.

### Input and Output

There are a few concepts that we need to encode. The first is a type to represent the initial
input query plan. This should be an in-memory tree of _only_ logical operators.

The second is a type to represent the outgoing output query plan. This should be an in-memory tree
of _only_ physical operators.

These two types should probably look _similar_ to this type from the Rust Substrait crate:
https://docs.rs/substrait/latest/substrait/proto/rel/enum.RelType.html, but I am not sure of all the
information we need yet.

```rust
/// An in-memory tree of logical operators. Used as the input / entrypoint of the optimizer.
pub enum LogicalPlan {
    ???
}

/// An in-memory tree of physical operators. Used as the final materialized output of the optimizer.
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
pub enum PartialLogicalPlan {
    LogicalOperator(LogicalOperator),
    ScalarOperator(ScalarOperator),
    GroupId(GroupId),
}

/// A type representing a tree of logical nodes, physical nodes, scalar nodes, and group IDs.
///
/// Note that group IDs must be leaves of this tree, and that physical nodes cannot have children
/// that are logical nodes.
///
/// TODO Make this an actual tree with the correct modeling of types.
/// TODO Figure out exact semantics of this type.
pub enum PartialPhysicalPlan {
    LogicalOperator(LogicalOperator),
    PhysicalOperator(PhysicalOperator),
    ScalarOperator(ScalarOperator),
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
) -> Vec<PartialLogicalPlan> {}

pub async fn check_implementation(
    &self,
    expr: LogicalExpression,
    rule: Rule,
) -> Vec<PartialPhysicalPlan> {}

pub fn apply_transformation(
    &mut self,
    expr: PartialLogicalPlan,
    rule: Rule,
) -> Vec<Expression> {}

pub fn apply_implementation(
    &mut self,
    expr: PartialPhysicalPlan,
    rule: Rule,
) -> Vec<Expression> {}

pub async fn add_expressions(&mut self, new_exprs: Vec<Expression>) {}
```

These types should be _private_ to the rule engine, and it should not be exposed to rule writers.
Obviously this is easier said than done, and we will likely have to write a proc macro for this.

## Pipeline Flow

The end-to-end pipeline should look something like this:

1. We ingest the input `LogicalPlan` in memory and recursively insert the initial groups and
expressions into the memo table.
2. We start up optimization by recursively optimizing the top-level group in the memo table.
4. For every rule application, we are matching against a `LogicalExpression` in the memo table via
the functions `check_transformation` or `check_implementation`.
    - Rules might need to match against specific children of `LogicalExpression`, which means we
    need to query for specific operators in a group in the memo table.
    - The `check` functions returned partially materialized plans that have a mix of materialized
    operators and group IDs that make it easy to manipulate in memory.
5. We take every partially materialized plan that gets generated by the `check` functions and apply
the transformations, generating new `Expression`s that need to be inserted into the memo table.
6. At the very end, materialize the final physical plan.
