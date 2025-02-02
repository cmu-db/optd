# OPTD Language Specification

## 1. Type System

### 1.1 Basic Types
The language provides a fixed set of primitive types for use in operator content:

- `int64`: 64-bit signed integer, range [-2^63, 2^63-1]
- `uint64`: 64-bit unsigned integer, range [0, 2^64-1]
- `float64`: 64-bit IEEE 754 floating point number
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

An operator defines its category (SCALAR or LOGICAL) followed by optional sections for content, logical children (for logical operators), and scalar children. Each section can contain any number of fields, including zero. There are no required fields or sections.

Fields can be variable size `ARRAY`, or `Optional`.

```
[CATEGORY] OperatorName
    DOC: "Documentation string"
    CONTENT
        # Any number of content fields (or none)
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
    # A constant with only content
    CONTENT
        value: float64

LOGICAL Project
    # A project with both scalar children and content
    CONTENT
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

2. A User-Defined Type: These rules analyze plan structures and produce custom data types as results. They enable extraction of information, analysis of properties, and computation of metrics about plan fragments.

All TRANSFORM expressions within a single rule must produce the same type of output, ensuring type consistency regardless of which pattern matches.

### 3.2 Pattern Matching

The pattern system enables structural matching with precise control over operator structure and bindings. The pattern grammar is:

```
pattern ::=
    | ANY                          # Matches any subtree without binding
    | Bind(name, pattern)          # Binds the result of matching pattern to name
    | NOT(pattern)                 # Negative matching
    | TYPE(operator_type) {        # Matches operator type with children patterns (type safe)
        logical_children: {
            name: pattern,
            ...
        },
        scalar_children: {
            name: pattern,
            ...
        }
    }
    | operator(                    # Matches operator with children (type safe)
        content: {
            field: pattern,
            ...
        },
        logical_children: {
            name: pattern,
            ...
        },
        scalar_children: {
            name: pattern,
            ...
        }
    )
```

### 3.3 Rule Application

The WITH clause enables rule composition through explicit rule application:

```
binding = rule_name(arg1, arg2, ...)
```

Arguments can come from pattern bindings, previous rule applications, or rule parameters. The binding captures the result for use in subsequent clauses. The first argument of any rule composition should always be a partial logical tree to match on.

### 3.4 Transformation and Control Flow

The TRANSFORM clause combines operator construction, control flow, and collection manipulation:

```
transform ::=
    | Ref(name)                   # Reference to bound variable
    | operator(                   # Construct new operator
        content: {...},
        logical_children: {...},
        scalar_children: {...}
    )
    | IF condition THEN transform ELSE transform
```

Built-in operations include:
- Array operations: isEmpty(), length(), contains(), toArray(), append()
- Boolean operations: and(), or(), not()
- Enum handling: match() for pattern matching on variants
- Type checking: is(), as() for runtime type verification
- Collection building: collect(), flatten() for manipulating result sets

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
    TRANSFORM: Join(
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
    TRANSFORM: left_val + right_val

    # Base case - match an integer constant
    MATCH: Bind("const", TYPE(Constant) {
        metadata: {
            value: Bind("val", ANY)
        }
    })
    TRANSFORM: Ref("val")  # Simply return the integer value
   ```

This rule recursively evaluates expressions, producing an analysis result that tracks constant values. The transformation uses pattern matching on the analysis results to determine whether folding is possible.

#### Complex Analysis and Transformation: Filter Pushdown

```
# Extract table references from expressions like ((A.d = 4) AND (B.c = 6) AND ...)
RULE get_table_refs:
    MATCH: Eq {
	      scalar_children: {
	          left: Bind("col", ColumnRef {
                metadata: {
                table: Bind("table", ANY)
           },
            right: TYPE(Constant)
	      }
    }
    WITH:
        left_refs = get_table_refs(Ref("left"))
        right_refs = get_table_refs(Ref("right"))
    TRANSFORM: concat(left_refs, right_refs)  # Combine table references

    MATCH: And {
        scalar_children: {
            left: Bind("left", ANY),
            right: Bind("right", ANY)
        }
    }
    WITH:
        left_refs = get_table_refs(Ref("left"))
        right_refs = get_table_refs(Ref("right"))
    TRANSFORM: concat(left_refs, right_refs)  # Combine table references

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
    TRANSFORM:
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
   - Evaluate TRANSFORM expression with control flow
3. If pattern match or WITH clause fails (i.e. any rule inside fails), or TRANSFORM fails (i.e. returns None) try next pattern
4. If no patterns succeed, rule application fails
5. Success produces either a new plan or user-defined type

This model enables both structural transformation and analytical processing while maintaining type safety and compositionality through the optimization pipeline.
