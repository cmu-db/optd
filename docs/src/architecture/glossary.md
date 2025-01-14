# Glossary

Definitions in query optimization can get very overloaded. Below is the language optd developers speak.

### Relational operator 
A **relation operator** (`RelNode`) describes an operation that can be evaluated to obtain a bag of tuples. In other literature this is also referred to as a query plan. A relational operator can be either logical or physical.

### Scalar operator

A **scalar operator** (`ScalarNode`) describes an operation that can be evaluated to obtain a single value. In other literature this is also referred to as a sql expression or a row expression. 

## Cascades

### Expressions 

A **logical expression** is a tree/DAG of logical operators.

A **physical expression** is a tree/DAG of physical operators.

The term **expression** in the context of Cascades can refer to either a relational or a scalar expression.

### Properties

**Properties** are metadata computed (and sometimes stored) for each node in an expression.
Properties of an expression may be **required** by the original SQL query or **derived** from **physical properties of one of its inputs.**


**Logical properties** describe the structure and content of data returned by an expression.

- Examples: row count, operator type,statistics, whether relational output columns can contain nulls.

**Physical properties** are characteristics of an expression that
impact its layout, presentation, or location, but not its logical content. 

- Examples: order and data distribution.


### Equivalence

Two logical expressions are equivalent if the logical properties of the two expressions are the same. They should produce the same set of rows and columns.

Two physical expressions are equivalent if their logical and physical properties are the same.

Logical expression with a required physical property is equivalent to a physical expression if the physical expression has the same logical property and delivers the physical property.


### Group

A **group** consists of equivalent logical expressions.

A **relational group** consists of logically equivalent logical relational operators.

A **scalar group** consists of logically equivalent logical scalar operators.

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

**Enforcer rule:** *TODO!*
