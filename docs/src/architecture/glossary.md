# Glossary

We have found that definitions in query optimization have internally become overloaded. This
document defines key names and definitions for concepts that are required in optimization.

Many of the names and definitions will be inspired by the Cascades framework. However, there are a
few important differences that need to be addressed considering our memo table will be persistent.

## Memo Table Concepts

This section describes names and definitions of concepts related to the **Memo Table**.

### Memo Table

The **Memo Table** is the data structure used for dynamic programming in a top-down plan enumeration
search algorithm. The memo table consists of a mutually recursive data structure made up of
**Expressions** and **Groups**.

### Expression (Logical, Physical, Scalar)

An **Expression** is the representation of an operator **inside of the Memo Table**.

<details>

<summary> Types of Expressions </summary>

There are two types of Expressions, **Relational Expressions** and **Scalar Expressions**. A
Relational Expression can be either a **Logical Expression** or a **Physical Expression**.

Examples of Logical Expressions include Logical Scan, Logical Join, or Logical Sort Expressions.

Examples of Physical Expressions include Table Scan, Hash Join, or Sort Merge Join.

Examples of Scalar Expressions include the expressions `t1.a < 42` or `t1.b = t2.c`.

Note that different kinds of Expressions can be named the same as Operators or Plan Nodes, but
Expressions solely indicate objects in the Memo Table.

</details>

<br>

Operators _outside of the memo table_ should _**not**_ be referred to as Expressions, and should
be referred to as **Operators** or **Plan Nodes**.

<details>

<summary> Expressions vs Operators / Plan Nodes </summary>

Notably, when we refer to an Expression, _we are specifically talking about the representation of_
_operators inside the memo table_. A logical operator from an incoming logical plan should _not_
be called an Logical Expression, and similarly a physical execution operator in the final output
physical plan should also _not_ be called an Physical Expression.

Those should be referred to as **Logical/Physical Operators** or **Logical/Physical Plan Nodes**.

Another key difference between Plan Nodes and Expressions is that Expressions have 0 or more
**Group Identifiers** as children, not other Plan Nodes as children.

</details>

### Expression Equivalence

Two Logical Expressions are equivalent if the **Logical Properties** of the two Expressions are the
same. In other words, the Logical Plans they represent produce the same set of rows and columns.

Two Physical Expressions are equivalent if their Logical and **Physical Properties** are the same.
In other words, the Physical Plans they represent produce the same set of rows and columns, in the
exact same order and distribution.

A Logical Expression with a required Physical Property is equivalent to a Physical Expression if the
Physical Expression has the same Logical Property and delivers the Physical Property. (FIXME unclear?)

### Group

A **Group** is a set of equivalent **Expressions**.

<details>

<summary> Types of Groups </summary>

We follow the definition of groups in the Volcano and Cascades frameworks. From the EQOP Microsoft
article (Section 2.2, page 205):

> In the memo, each class of equivalent expressions is called an _equivalence class_ or a _group_,
> and all equivalent expression within the class are called _group expressions_ or simply
> _expressions_.

A **Relational Group** is a set of 1 or more equivalent Logical Expressions and 0 or more equivalent
Physical Expressions.

A **Scalar Group** consists of equivalent Scalar Expressions.

</details>

<br>

## Plan Enumeration and Search Concepts

This section describes names and definitions of concepts related to the general plan enumeration and
search of optimal query plans.

### Logical / Physical Plans

A **Logical Plan** is a tree or DAG of **Logical Operators** that can be evaluated to produce a bag
of tuples. This can also be referred to as a Logical Query Plan. The Operators that make up this
Logical Plan can be considered Logical Plan Nodes.

A **Physical Plan** is a tree or DAG of **Physical Operators** that can be evaluated by an execution
engine to produce a table. This can also be referred to as a Physical Query Plan. The Operators that
make up this Physical Plan can be considered Physical Plan Nodes.

### Scalar Operator

A **Scalar Operator** describes an operation that can be evaluated to obtain a single value. This
can also be referred to as a SQL expression, a row expression, or a SQL predicate.

---

### Properties

**Properties** are metadata computed (and sometimes stored) for each node in an expression.
Properties of an expression may be **required** by the original SQL query or **derived** from **physical properties of one of its inputs.**

**Logical properties** describe the structure and content of data returned by an expression.

-   Examples: row count, operator type,statistics, whether relational output columns can contain nulls.

**Physical properties** are characteristics of an expression that
impact its layout, presentation, or location, but not its logical content.

-   Examples: order and data distribution.

### Rule

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
