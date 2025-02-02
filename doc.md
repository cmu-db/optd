# OPTD Language Specification

## 1. Type System

### 1.1 Basic Types
The language provides a fixed set of primitive types for use in operator metadata:

- `int64`: 64-bit signed integer, range [-2^63, 2^63-1]
- `string`: UTF-8 encoded string of arbitrary length
- `bool`: Boolean value (true/false)

### 1.2 Composite Types

#### 1.2.1 Arrays
An array type is denoted as `Array<T>` where T is any valid type. Arrays represent variable-length sequences of values of type T.


#### 1.2.2 Enums
Enumerated types allow users to define custom sum types. Each variant in an enum can carry zero or more fields of any valid type T. The syntax is as follows:

```
ENUM TypeName
    Variant1                            # No fields
    Variant2(int64, string)            # Two fields
    Variant3(
        ENUM NestedEnum                # Nested enum definition
            SubVariant1(float64)
            SubVariant2(bool, string)
        END,
        Array<int64>                   # Second field is an array
    )
END
```

Enum definitions must follow these rules:
- The enum name must be unique in the type system
- Each variant name must be unique within its enum
- Variant fields can be:
  - Any basic type (int64, uint64, float64, string, bool)
  - Another enum type (as long as no recursion / cycles)
  - An array type of any valid type
- A variant must have zero or more fields, each with an explicit type
- Nested enum definitions are allowed and can be used as field types
- Recursive or cyclic enum definitions are not permitted

### 1.2.3 Optional

An optional type is denoted as `Optional<T>`, where T is any valid type.

## 2. Operator System

### 2.1 Operator Categories
The system defines two distinct categories of operators:
- Scalar operators: Can only have scalar children
- Logical operators: Can have both scalar and logical children

### 2.2 Operator Definition Structure

An operator defines its category (SCALAR or LOGICAL) followed by optional sections for metadata, logical children (for logical operators), and scalar children. Each section can contain any number of fields, including zero. There are no required fields or sections.

Fields can be variable size `ARRAY`, or `Optional`.

```
[CATEGORY] OperatorName
    DOC: "Documentation string"
    METADATA
        # Any number of metadata fields (or none)
        field1: Type1
        field2: Type2
        ...
    LOGICAL_CHILDREN    # Only valid for logical operators
        # Any number of logical children (or none)
        child1: Logical
        child2: Array<Logical>
        ...
    SCALAR_CHILDREN
        # Any number of scalar children (or none)
        child1: Optional<Scalar>
        child2: Array<Scalar>
        ...
END
```

The only constraints are:
- SCALAR operators cannot have LOGICAL_CHILDREN section
- Each field name within a section must be unique
- All types must be valid according to the type system

For example, these are all valid operators:
```
LOGICAL Join
    # A join with only logical children
    LOGICAL_CHILDREN
        left: Logical
        right: Logical
    SCALAR_CHILDREN
	    predicate: Option<Scalar>

SCALAR Constant
    # A constant with only metadata
    METADATA
        value: float64

LOGICAL Project
    # A project with both scalar children and metadata
    METADATA
        schema: string
    SCALAR_CHILDREN
        expressions: Array<Scalar>
```

### 2.3 Operator Child Constraints
- Scalar operators:
  - Cannot have logical children
  - Can have zero or more scalar children

- Logical operators:
  - Can have both logical and scalar children
  - Can have fixed or variable number of either type



## 3. Rules and Pattern Matching

The rule system provides the core transformation and analysis capabilities of OPTD. Each rule defines how to match tree patterns, compose transformations, and produce either new plan structures or analytical results.

### 3.1 Rule Definition and Output Types

A rule always takes a partial logical plan as input but can produce one of two types of output:

1. A Partial Logical Plan: These rules perform structural transformations on the input plan, such as commuting joins or pushing down filters. The output maintains the plan structure while reorganizing its components.

2. An OPTD Type: These rules analyze plan structures and produce custom data types as results. They enable extraction of information, analysis of properties, and computation of metrics about plan fragments.

All APPLY expressions within a single rule must produce the same type of output, ensuring type consistency regardless of which pattern matches.

### 3.2 Pattern Matching

The pattern matching system distinguishes between three distinct types of patterns, each tailored to match different components of the optimization tree:

#### Logical Patterns
Logical patterns match against logical operators in the plan tree. Their structure encompasses:
```
LogicalPattern ::=
    | ANY                          # Matches any logical operator
    | Bind(name, pattern)          # Binds a logical subtree
    | NOT(pattern)                 # Negative logical matching
    | Operator {                   # Matches specific logical operator
        op_type: string,           # Operator type to match
        metadata: TypePattern[],# Metadata patterns
        logical_children: LogicalPattern[], # Logical child patterns
        scalar_children: ScalarPattern[]  # Scalar child patterns
    }
```

#### Scalar Patterns
Scalar patterns match against scalar expressions, with a more restricted structure:
```
ScalarPattern ::=
    | ANY                          # Matches any scalar expression
    | Bind(name, pattern)          # Binds a scalar subtree
    | Operator {                   # Matches specific scalar operator
        op_type: string,           # Operator type to match
        metadata: TypePattern,  # Metadata pattern
        scalar_children: ScalarPattern[] # Only scalar children allowed
    }
```

#### Type Patterns
OPTD type patterns handle matching against metadata values:
```
TypePattern ::=
    | ANY                          # Matches any metadata value
    | Bind(name, value)            # Binds a metadata value
```

This three-tier pattern system ensures type safety throughout the matching process. Each pattern type enforces appropriate constraints:
- Logical patterns can match both logical and scalar children
- Scalar patterns can only match scalar children
- OPTD type patterns match leaf values in metadata

For example, matching a join operator with specific metadata would look like:
```
Operator {
    op_type: "Join",
    metadata: [Bind("join_type", "Inner")],
    logical_children: [
        Bind("left", ANY),
        Bind("right", ANY)
    ],
    scalar_children: [
        Bind("condition", ANY)
    ]
}
```

### 3.3 Rule Composition

The WITH clause enables rules to build complex transformations by composing simpler rules. This composition mechanism allows rules to analyze subtrees and use the results in their final transformation.

Rule composition follows this structure:
```
WITH:
    result1 = rule1(tree_ref, arg1, arg2)
    result2 = rule2(another_ref)
```

Each composition statement:
1. Names the result for later use
2. Specifies which rule to apply
3. Provides a tree reference as its first argument
4. Optionally provides additional arguments specific to that rule

The tree reference must be either:
- A pattern binding from the MATCH phase
- A result from a previous rule application

The additional arguments are always user defined types (expressions allowed).

Rule applications are evaluated in order, with each result available to subsequent applications.

### 3.4 Application Expressions

Application expressions define how a rule produces its output. The system distinguishes between three types of applications, each with specific capabilities and constraints:

#### Type Applications
These expressions operate on and produce optd types:
```
TypeExpr ::=
    | TypeRef(String)                # Reference to bound optd type
    | IfThenElse {
        condition: TypeExpr,
        then_branch: TypeExpr,
        else_branch: TypeExpr
    }
    | Eq {                               # Value equality comparison
        left: TypeExpr,
        right: TypeExpr
    }
    | Match {                            # Pattern matching on optd types
        expr: TypeExpr,
        cases: [(TypeExpr, TypeExpr)]
    }
```

#### Scalar Applications
These expressions construct scalar operators, but their conditions must be optd type expressions:
```
ScalarExpr ::=
    | ScalarRef(String)                  # Reference to bound scalar
    | IfThenElse {
        condition: TypeExpr,         # Condition must be optd type
        then_branch: ScalarExpr,
        else_branch: ScalarExpr
    }
    | Match {                            # Pattern match on optd type
        expr: TypeExpr,
        cases: [(TypeExpr, ScalarExpr)]
    }
```

#### Logical Applications
Similar to scalar applications, logical expressions construct plan operators:
```
LogicalExpr ::=
    | LogicalRef(String)                 # Reference to bound logical plan
    | IfThenElse {
        condition: TypeExpr,         # Condition must be optd type
        then_branch: LogicalExpr,
        else_branch: LogicalExpr
    }
    | Match {                            # Pattern match on optd type
        expr: TypeExpr,
        cases: [(TypeExpr, LogicalExpr)]
    }
```

Key constraints:
1. Conditions in all control flow constructs must be optd type expressions
2. Each application type can only reference bindings of its corresponding type
3. Match expressions must have consistent result types across all cases
4. All application expressions within a single rule must produce the same type

This strict typing ensures that transformations remain type-safe while enabling sophisticated control flow based on analysis results.

### 3.5 Complete Examples

### 3.5 Rule Examples

Let's examine three progressively complex examples that demonstrate the key capabilities of the rule system.

#### Basic Plan Transformation: Join Commutativity

The simplest rules perform pure structural transformations on plans. This join commutativity rule demonstrates basic pattern matching and plan reconstruction:

```
RULE join_commute:
    MATCH: Join(
        metadata: {
            join_type: "Inner"
        },
        logical_children: {
            left: Bind("left", ANY),
            right: Bind("right", ANY)
        },
        scalar_children: {
            condition: Bind("cond", ANY)
        }
    )
    APPLY: Join(
        metadata: {
            join_type: "Inner"
        },
        logical_children: {
            left: Ref("right"),
            right: Ref("left")
        },
        scalar_children: {
            condition: Ref("cond")
        }
    )
```

This rule only swaps the children of inner joins, preserving the join condition and type. It demonstrates basic pattern matching and transformation without needing additional analysis or control flow.

#### Recursive Analysis: Constant Folding

The constant folding rule shows how recursion and custom types enable expression analysis and transformation:

```
RULE constant_fold:
    # Match addition of two expressions
    MATCH: Bind("add", Add {
        scalar_children: {
            left: Bind("left", ANY),
            right: Bind("right", ANY)
        }
    })
    WITH:
        left_val = constant_fold(Ref("left"))
        right_val = constant_fold(Ref("right"))
    APPLY: left_val + right_val

    # Base case - match an integer constant
    MATCH: Bind("const", Constant {
        metadata: {
            value: Bind("val", ANY)
        }
    })
    APPLY: Ref("val")  # Simply return the integer value
   ```

This rule recursively evaluates expressions, producing an analysis result that tracks constant values. The transformation uses pattern matching on the analysis results to determine whether folding is possible.

#### Complex Analysis and Transformation: Filter Pushdown

```
# Extract table references from expressions like ((A.d = 4) AND (B.c = 6) AND ...)
RULE get_table_refs:
    MATCH: Eq {
        scalar_children: {
            left: Bind("col", 
                ColumnRef {
                    metadata: {
                        table: Bind("table", ANY)
                    },
                })
            right: TYPE(Constant)
	    }
    }
    WITH:
        left_refs = get_table_refs(Ref("left"))
        right_refs = get_table_refs(Ref("right"))
    APPLY: concat(left_refs, right_refs)  # Combine table references

    MATCH: And {
        scalar_children: {
            left: Bind("left", ANY),
            right: Bind("right", ANY)
        }
    }
    WITH:
        left_refs = get_table_refs(Ref("left"))
        right_refs = get_table_refs(Ref("right"))
    APPLY: concat(left_refs, right_refs)  # Combine table references

# Main filter pushdown rule
RULE filter_pushdown:
    MATCH: Filter(
        logical_children: {
            input: Join(
                logical_children: {
                    left: Bind("left", ANY),
                    right: Bind("right", ANY)
                }
                scalar_children: {
                    predicate: Bind("join_pred", ANY)
                }
            )
        },
        scalar_children: {
            predicate: Bind("filter_pred", ANY)
        }
    )
    WITH:
        pred_refs = get_table_refs(Ref("filter_pred"))
        join_refs = get_table_refs(Ref("join_pred"))
    APPLY:
        IF pred_refs IN join_refs THEN
            Join(
                logical_children: {
                    left: Ref("left"),
                    right: Ref("right")
                }
                scalar_children: {
	                predicate: And {
	                    left: Ref("join_pred"),
	                    right: Ref("filter_pred")
	                }
                }
            )
        ELSE
            None # Rule fails
```
This last example might contain lots of boilerplate code, and only pushes down if all references match, but we could come up with more intricate rules.

### 3.6 Execution Model

Rule execution follows these steps:

1. Match input against each pattern in sequence
2. For first successful match:
   - Bind pattern variables to matched subtrees
   - Execute WITH clause rule applications in order
   - Evaluate APPLY expression with control flow
3. If pattern match or WITH clause fails (i.e. any rule inside fails), or APPLY fails (i.e. returns None) try next pattern
4. If no patterns succeed, rule application fails
5. Success produces either a new plan or user-defined type

This model enables both structural transformation and analytical processing while maintaining type safety and compositionality through the optimization pipeline.
