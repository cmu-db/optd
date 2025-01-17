# Glossary

We have found internally that definitions in query optimization have become overloaded. This
document defines key names and definitions for concepts that are required in optimization.

Many of the names and definitions will be inspired by the Cascades framework. However, there are a
few important differences that need to be addressed considering our memo table will be persistent.

# Contents

-   [Memo Table]
-   [Expression]
    -   [Relational Expression]
        -   [Logical Expression]
        -   [Physical Expression]
    -   [Scalar Expression]
    -   **[Equivalence of Expressions](#expression-equivalence)**
-   [Group]
    -   [Relational Group]
    -   [Scalar Group]
-   [Plan]
    -   [Logical Plan]
    -   [Physical Plan]
-   [Operator] / [Plan Node]
    -   [Logical Operator]
    -   [Physical Operator]
    -   [Scalar Operator]
-   Property
    -   Logical Property
    -   Physical Property
    -   ? Derived Property ?
-   Rule
    -   Transformation Rule
    -   Implementation Rule

[Memo Table]: #memo-table
[Expression]: #expression-logical-physical-scalar
[Relational Expression]: #relational-expression
[Logical Expression]: #logical-expression
[Physical Expression]: #physical-expression
[Scalar Expression]: #scalar-expression
[Group]: #group
[Relational Group]: #relational-group
[Scalar Group]: #scalar-group
[Plan]: #query-plan
[Logical Plan]: #logical-plan
[Physical Plan]: #physical-plan
[Plan Node]: #operator
[Operator]: #operator
[Logical Operator]: #logical-operator
[Physical Operator]: #physical-operator
[Scalar Operator]: #scalar-operator

# Comparison with Cascades

In the Cascades framework, an expression is a tree of operators. In `optd`, we are instead defining
a logical or physical query [Plan] to be a tree or DAG of [Operator]s. An expression in `optd`
strictly refers to the representation of an operator in the [Memo Table], not in query plans.

See the [section below](#expression-logical-physical-scalar) on the kinds of expressions for more
information.

Most other terms in `optd` are similar to Cascades or are self-explanatory.

# Memo Table Terms

This section describes names and definitions of concepts related to the memo table.

## Memo Table

The memo table is the data structure used for dynamic programming in a top-down plan enumeration
search algorithm. The memo table consists of a mutually recursive data structure made up of
[Expression]s and [Group]s.

## Expression (Logical, Physical, Scalar)

An **expression** is the representation of a non-materialized operator _inside_ of the [Memo Table].

There are 2 types of expressions: [Relational Expression]s and [Scalar Expression]s. A [Relational
Expression] can be either a [Logical Expression] or a [Physical Expression].

Note that different kinds of expressions can have the same names as [Operator]s or [Plan Node]s, but
expressions solely indicate non-materialized relational or scalar operators in the [Memo Table].

Operators outside of the [Memo Table] should _**not**_ be referred to as expressions, and should
instead be referred to as [Operator]s or [Plan Node]s.

Notably, when we refer to an expression, _we are specifically talking about the representation of_
_operators inside the memo table_. A logical operator from an incoming logical plan should _not_
be called an [Logical Expression], and similarly a physical execution operator in the final output
physical plan should also _not_ be called an [Physical Expression].

Another way to think about this is that expressions are _not_ materialized, and plan nodes and
operators inside query plans _are_ materialized. Operators inside of query plans (both logical and
physical) should be referred to as either logical or physical [Operator]s or logical or physical
[Plan Node]s.

Another key difference between expressions and [Plan Node]s is that expressions have 0 or more
**Group Identifiers** as children, and [Plan Node]s have 0 or more other [Plan Node]s as children.

## Relational Expression

A relational expression is either a [Logical Expression] or a [Physical Expression].

When we say "relational", we mean representations of operations in the relational algebra of SQL.

Relational expressions differ from [Scalar Expression]s in that the result of algebraically
evaluating a relational expression produces a bag of tuples instead of a single scalar value.

See the following sections for more information.

## Logical Expression

A logical expression is a version of a [Relational Expression].

TODO(connor) Add more details.

Examples of logical expressions include Logical Scan, Logical Join, or Logical Sort expressions
(which can just be shorthanded to Scan, Join, or Sort).

## Physical Expression

A physical expression is a version of a [Relational Expression].

TODO(connor) Add more details.

Examples of Physical Expressions include Table Scan, Index Scan, Hash Join, or Sort Merge Join.

## Scalar Expression

A scalar expression is a version of an [Expression].

TODO(everyone) Figure out the semantics of what a scalar expression really is.

Examples of Scalar Expressions include the expressions `t1.a < 42` or `t1.b = t2.c`.

## Expression Equivalence

Two Logical Expressions are equivalent if the **Logical Properties** of the two Expressions are the
same. In other words, the Logical Plans they represent produce the same set of rows and columns.

Two Physical Expressions are equivalent if their Logical and **Physical Properties** are the same.
In other words, the Physical Plans they represent produce the same set of rows and columns, in the
exact same order and distribution.

A Logical Expression with a required Physical Property is equivalent to a Physical Expression if the
Physical Expression has the same Logical Property and delivers the Physical Property. (FIXME unclear?)

## Group

A **Group** is a set of equivalent [Expression]s.

We follow the definition of groups in the Volcano and Cascades frameworks. From the EQOP Microsoft
article (Section 2.2, page 205):

> In the memo, each class of equivalent expressions is called an _equivalence class_ or a _group_,
> and all equivalent expressions within the class are called _group expressions_ or simply
> _expressions_.

## Relational Group

A relational group is a set of 1 or more equivalent [Logical Expression]s and 0 or more equivalent
[Physical Expression]s.

TODO(connor) Add more details.

TODO(connor) Add example.

## Scalar Group

A scalar group consists of equivalent [Scalar Expression]s.

TODO(connor) Add more details.

TODO(connor) Add example.

# Plan Enumeration and Search Concepts

This section describes names and definitions of concepts related to the general plan enumeration and
search of optimal query plans.

## Query Plan

TODO

## Logical Plan

A **Logical Plan** is a tree or DAG of **Logical Operators** that can be evaluated to produce a bag
of tuples. This can also be referred to as a Logical Query Plan. The Operators that make up this
Logical Plan can be considered Logical Plan Nodes.

## Physical Plan

A **Physical Plan** is a tree or DAG of **Physical Operators** that can be evaluated by an execution
engine to produce a table. This can also be referred to as a Physical Query Plan. The Operators that
make up this Physical Plan can be considered Physical Plan Nodes.

## Operator

TODO

## Logical Operator

A **Logical Operator** is a node in a Logical Plan (which is a tree or DAG).

## Physical Operator

A **Physical Operator** is a node in a Physical Plan (which is a tree or DAG).

## Scalar Operator

A **Scalar Operator** describes an operation that can be evaluated to obtain a single value. This
can also be referred to as a SQL expression, a row expression, or a SQL predicate.

---

---

---

TODO: Cleanup

## Properties

**Properties** are metadata computed (and sometimes stored) for each node in an expression.
Properties of an expression may be **required** by the original SQL query or **derived** from **physical properties of one of its inputs.**

**Logical properties** describe the structure and content of data returned by an expression.

-   Examples: row count, operator type,statistics, whether relational output columns can contain nulls.

**Physical properties** are characteristics of an expression that
impact its layout, presentation, or location, but not its logical content.

-   Examples: order and data distribution.

## Rule

a **rule** in Cascades transforms an expression into equivalent expressions. It has the following interface.

```rust
trait Rule {
    /// Checks whether the rule is applicable on the input expression.
	fn check_pattern(expr: Expr) -> bool;
    /// Transforms the expression into one or more equivalent expressions.
	fn transform(expr: Expr) -> Vec<Expr>;
}
```

A **transformation rule** transforms a **part** of the logical expression into logical expressions. This is also called a logical to logical transformation in other systems.

A **implementation rule** transforms a **part** of a logical expression to an equivalent physical expression with physical properties.

In Cascades, you don't need to materialize the entire query tree when applying rules. Instead, you can materialize expressions on demand while leaving unrelated parts of the tree as group identifiers.

In other systems, there are physical to physical expression transformation for execution engine specific optimization, physical property enforcement, or distributed planning. At the moment, we are **not** considering physical-to-physical transformations.

**Enforcer rule:** _TODO!_
